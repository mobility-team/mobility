"""Try profile-aware physical timing in the cache-only Geneva simulation.

This patches the evaluator only for this process. It uses the factored input
only when each physical step and travel profile has one set of timing inputs;
otherwise it uses the ordinary evaluator. No simulation state format changes.
"""
import json
import os
import runpy
from pathlib import Path
from time import perf_counter

import polars as pl

from mobility.trips.group_day_trips.core.performance_trace import _ACTIVE_TRACE
from mobility.trips.group_day_trips.plans.plan_ids import PLAN_KEY_COLS
from mobility.trips.group_day_trips.plans.plan_schedule_updater import PlanScheduleUpdater
from mobility.trips.group_day_trips.plans.plan_updater import PlanUpdater
import mobility.trips.group_day_trips.plans.plan_updater as plan_updater_module


unit = ["demand_group_id", "demand_subgroup_id"]
physical_plan = [column for column in PLAN_KEY_COLS if column not in unit] + ["utility_profile_id"]
physical_step = physical_plan + ["seq_step_index"]
times = ["departure_time", "arrival_time", "next_departure_time", "duration_per_pers"]
values = ["activity", "from", "to", "mode", "time"] + times
original_evaluate = PlanUpdater.evaluate_candidate_pool
original_update = PlanScheduleUpdater.update_plan_timings
original_index = plan_updater_module.assign_time_sequence_ids
current = {}


def evaluate(updater, *args, **kwargs):
    contexts = kwargs.get("scoring_contexts")
    enabled = (kwargs.get("compact_contexts") and contexts is not None
               and kwargs.get("factored_candidate_steps") is not None
               and kwargs.get("shared_candidate_plan_steps") is not None)
    if not enabled:
        return original_evaluate(updater, *args, **kwargs)
    current["contexts"] = contexts.demand_groups.select(unit + ["utility_profile_id"])
    try:
        return original_evaluate(updater, *args, **kwargs)
    finally:
        current.clear()


def defer_timing(updater, steps, **kwargs):
    if "contexts" not in current:
        return original_update(updater, steps, **kwargs)
    current["updater"] = updater
    current["settings"] = kwargs
    return steps


def factor_and_index(steps, catalogue, **kwargs):
    if "settings" not in current:
        return original_index(steps, catalogue, **kwargs)
    settings = current.pop("settings")
    updater = current.pop("updater")
    start = perf_counter()
    contexts = current["contexts"]
    distinct = (steps.select(unit + [column for column in physical_step if column != "utility_profile_id"] + values)
                .join(contexts, on=unit).drop(unit).unique())
    if distinct.select(physical_step).n_unique() != distinct.height:
        timed = original_update(updater, steps.lazy(), **settings).collect(engine="streaming")
        print("FACTORED_TIMING_FALLBACK", flush=True)
        return original_index(timed, catalogue, **kwargs)
    factored = distinct.rename({"utility_profile_id": "demand_group_id"}).with_columns(
        demand_subgroup_id=pl.lit(0, dtype=pl.UInt32))
    narrow_timed = original_update(updater, factored.lazy(), **settings).collect(engine="streaming")
    narrow, new_catalogue, narrow_mapping = original_index(narrow_timed, catalogue, **kwargs)
    physical_mapping = (narrow_mapping.drop("demand_subgroup_id")
                        .rename({"demand_group_id": "utility_profile_id", "time_seq_id": "_modeled_time_seq_id",
                                 "previous_time_seq_id": "time_seq_id"}))
    modeled_steps = (narrow.drop("demand_subgroup_id").rename({"demand_group_id": "utility_profile_id",
                       "time_seq_id": "_modeled_time_seq_id", **{name: f"_modeled_{name}" for name in times}})
                     .select([name for name in physical_step if name != "time_seq_id"]
                             + ["_modeled_time_seq_id"] + [f"_modeled_{name}" for name in times]).unique())
    physical_results = physical_mapping.join(modeled_steps, on=[name for name in physical_plan
                                                                 if name != "time_seq_id"] + ["_modeled_time_seq_id"])
    rebuilt = (steps.join(contexts, on=unit).join(physical_results, on=physical_step)
               .with_columns(pl.col("_modeled_time_seq_id").alias("time_seq_id"),
                             *[pl.col(f"_modeled_{name}").alias(name) for name in times])
               .select(steps.columns))
    mapping = (steps.select(PLAN_KEY_COLS).unique().join(contexts, on=unit)
               .join(physical_mapping, on=physical_plan)
               .rename({"time_seq_id": "previous_time_seq_id", "_modeled_time_seq_id": "time_seq_id"})
               .select(narrow_mapping.columns))
    active = _ACTIVE_TRACE.get()
    labels = active[2] if active else {}
    print("FACTORED_TIMING", json.dumps({"step": labels.get("step"), "batch": labels.get("search_batch"),
          "rows": steps.height, "physical_rows": factored.height, "seconds": perf_counter() - start}), flush=True)
    return rebuilt, new_catalogue, mapping


PlanUpdater.evaluate_candidate_pool = evaluate
PlanScheduleUpdater.update_plan_timings = defer_timing
plan_updater_module.assign_time_sequence_ids = factor_and_index
runpy.run_path(str(Path(__file__).with_name("inspect_grand_geneve_execution.py")), run_name="__main__")
