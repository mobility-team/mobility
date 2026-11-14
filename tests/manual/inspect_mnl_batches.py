"""Capture fresh choice estimates during a fixed-state Geneva search."""
import json
import os
from time import perf_counter

import polars as pl

from mobility.trips.group_day_trips.core.multi_scenario import allocation_od_mode_flows, flow_change_index
from mobility.trips.group_day_trips.core.performance_trace import _ACTIVE_TRACE
from mobility.trips.group_day_trips.plans.plan_ids import PLAN_KEY_COLS
from mobility.trips.group_day_trips.plans.plan_updater import PlanUpdater


def install_mnl_batch_probe(folder, logit_scale):
    folder.mkdir(exist_ok=True)
    original_estimate = PlanUpdater.estimate_reachable_allocation
    previous_flows = {}
    previous_choices = {}
    selected_unit = {}

    def estimate(updater, evaluation, current_plans, iteration, scope, **kwargs):
        started = perf_counter()
        reachable = original_estimate(updater, evaluation, current_plans, iteration, scope, **kwargs)
        reachable_seconds = perf_counter() - started
        active = _ACTIVE_TRACE.get()
        labels = active[2] if active else {}
        batch, scenario = labels.get("search_batch"), labels.get("scenario")
        if batch is None:
            return reachable
        diagnostic_started = perf_counter()
        started = perf_counter()
        allocation, _ = updater.estimate_mnl_allocation(
            evaluation, evaluation.scoring_contexts.demand_groups,
            logit_scale=logit_scale, include_plan_steps=False,
        )
        mnl_seconds = perf_counter() - started
        started = perf_counter()
        mnl_flows = allocation_od_mode_flows(allocation, evaluation.possible_plan_steps, include_activity=True)
        reachable_flows = allocation_od_mode_flows(reachable, evaluation.possible_plan_steps, include_activity=True)
        flow_seconds = perf_counter() - started
        key = (iteration, scenario)
        choice = allocation.select(PLAN_KEY_COLS + [pl.col("n_persons").alias("people")])
        if os.environ.get("MOBILITY_PROBE_TRACE_LARGEST_SUBGROUP") == "1":
            # Demand subgroups can tie at the configured size cap. Pick the
            # lowest IDs among the largest, consistently across batches.
            if not selected_unit:
                unit = (
                    current_plans.group_by("demand_group_id", "demand_subgroup_id")
                    .agg(pl.col("n_persons").sum())
                    .sort(["n_persons", "demand_group_id", "demand_subgroup_id"],
                          descending=[True, False, False]).head(1)
                )
                mapping = unit.join(evaluation.scoring_contexts.unit_map,
                                    on=["demand_group_id", "demand_subgroup_id"])
                selected_unit.update(mapping.row(0, named=True))
                selected_unit["scoring_context"] = selected_unit.pop("_score_context_id")
                context = evaluation.scoring_contexts.demand_groups.filter(
                    pl.col("demand_group_id") == selected_unit["scoring_context"]
                )
                selected_unit["context_details"] = context.to_dicts()[0]
                (folder / "selected-subgroup.json").write_text(
                    json.dumps(selected_unit, default=str, indent=2), encoding="utf-8"
                )
            context_id = selected_unit["scoring_context"]
            chosen = allocation.filter(pl.col("demand_group_id") == context_id)
            chosen.write_parquet(folder / f"batch-{batch}-subgroup-plans.parquet")
            (
                evaluation.possible_plan_steps.lazy()
                .filter(pl.col("demand_group_id") == context_id)
                .select(PLAN_KEY_COLS + ["plan_id", "seq_step_index", "activity", "from", "to", "mode",
                                         "departure_time", "arrival_time", "next_departure_time",
                                         "duration_per_pers", "time", "distance", "cost"])
                .collect(engine="streaming")
                .write_parquet(folder / f"batch-{batch}-subgroup-steps.parquet")
            )
            if os.environ.get("MOBILITY_PROBE_PREVIOUS_UTILITY_BATCH") == str(batch):
                (
                    evaluation.possible_plan_steps.lazy()
                    .filter(pl.col("demand_group_id") == context_id)
                    .select(PLAN_KEY_COLS + ["plan_id", "seq_step_index", "activity", "departure_time",
                            "arrival_time", "next_departure_time", "duration_per_pers", "time", "cost",
                            "utility", "min_activity_time"])
                    .collect(engine="streaming")
                    .write_parquet(folder / f"batch-{batch}-subgroup-utility-steps.parquet")
                )
                evaluation.time_sequence_steps.write_parquet(folder / f"batch-{batch}-schedule-catalog.parquet")
        if os.environ.get("MOBILITY_PROBE_RIGID_UTILITY_BATCH") == str(batch):
            # Follow a context with substantial fresh-MNL volume on plans
            # containing a non-first rigid work/study arrival.
            rigid_plans = (
                evaluation.possible_plan_steps.lazy()
                .filter(pl.col("activity").is_in(["work", "studies"]) & (pl.col("seq_step_index") > 1))
                .select("demand_group_id", "plan_id").unique().collect(engine="streaming")
            )
            context = (
                rigid_plans.join(allocation.select("demand_group_id", "plan_id", "n_persons"),
                                 on=["demand_group_id", "plan_id"])
                .group_by("demand_group_id").agg(pl.col("n_persons").sum())
                .sort(["n_persons", "demand_group_id"], descending=[True, False]).head(1)
            )
            if context.height:
                context_id = context["demand_group_id"].item()
                context = context.join(
                    evaluation.scoring_contexts.demand_groups.select("demand_group_id", "utility_profile_id"),
                    on="demand_group_id",
                )
                (folder / "rigid-subgroup.json").write_text(
                    json.dumps(context.row(0, named=True), default=str, indent=2), encoding="utf-8",
                )
                allocation.filter(pl.col("demand_group_id") == context_id).write_parquet(
                    folder / f"batch-{batch}-rigid-plans.parquet"
                )
                (
                    evaluation.possible_plan_steps.lazy().filter(pl.col("demand_group_id") == context_id)
                    .select(PLAN_KEY_COLS + ["plan_id", "seq_step_index", "activity", "departure_time",
                            "arrival_time", "next_departure_time", "duration_per_pers", "time", "cost",
                            "utility", "min_activity_time", "from", "to", "mode"])
                    .collect(engine="streaming").write_parquet(folder / f"batch-{batch}-rigid-steps.parquet")
                )
                evaluation.time_sequence_steps.write_parquet(folder / f"batch-{batch}-schedule-catalog.parquet")
                evaluation.time_mapping.filter(pl.col("demand_group_id") == context_id).write_parquet(
                    folder / f"batch-{batch}-rigid-time-mapping.parquet"
                )
        previous = previous_choices.get(key)
        # Compare candidates against the best feasible alternative in the
        # same scoring context, in the units used by the choice model.
        candidates = evaluation.possible_plan_utility.filter(pl.col("mode_seq_id") != 0).with_columns(
            best_feasible=pl.when(pl.col("is_feasible")).then(pl.col("utility"))
            .otherwise(None).max().over("demand_group_id", "demand_subgroup_id")
        )
        candidate_counts = candidates.select(
            pl.len().alias("mobile_candidates"),
            (~pl.col("is_feasible")).sum().alias("infeasible_candidates"),
            (pl.col("is_feasible") & ((pl.col("best_feasible") - pl.col("utility")) * logit_scale > 4.605170186)).sum()
            .alias("below_one_percent_of_best"),
        ).row(0, named=True)
        unit_keys = ["demand_group_id", "demand_subgroup_id"]
        best_utility = (
            evaluation.possible_plan_utility.filter(pl.col("is_feasible"))
            .group_by(unit_keys).agg(best_utility=pl.col("utility").max())
            .join(evaluation.scoring_contexts.demand_groups.select(unit_keys + ["n_persons"]), on=unit_keys)
        )
        best_utility.write_parquet(folder / f"batch-{batch}-context-utility.parquet")
        if os.environ.get("MOBILITY_PROBE_UTILITY_POOLS") == "1":
            evaluation.possible_plan_utility.select(
                "demand_group_id", "demand_subgroup_id", "plan_id", "mode_seq_id", "utility", "is_feasible"
            ).write_parquet(folder / f"batch-{batch}-utility-pool.parquet")
        mean_best_utility = best_utility.select(
            (pl.col("best_utility") * pl.col("n_persons")).sum() / pl.col("n_persons").sum()
        ).item()
        mean_chosen_utility = allocation.select(
            (pl.col("utility") * pl.col("n_persons")).sum() / pl.col("n_persons").sum()
        ).item()
        choice_change = None
        if previous is not None:
            joined = previous.rename({"people": "previous"}).join(
                choice.rename({"people": "current"}), on=PLAN_KEY_COLS, how="full", coalesce=True,
            ).with_columns(pl.col("previous", "current").fill_null(0.0))
            diff, total = joined.select(
                (pl.col("previous") - pl.col("current")).abs().sum().alias("diff"),
                (pl.col("previous") + pl.col("current")).sum().alias("total"),
            ).row(0)
            choice_change = float(diff / total) if total else 0.0
        old_flows = previous_flows.get(key)
        summary = {
            "step": iteration, "batch": batch, "scenario": scenario,
            "reachable_seconds": reachable_seconds, "mnl_seconds": mnl_seconds,
            "flow_reduction_seconds": flow_seconds,
            "mnl_flow_change": None if old_flows is None else flow_change_index(old_flows, mnl_flows),
            "mnl_choice_change": choice_change,
            "mnl_vs_reachable_flow": flow_change_index(mnl_flows, reachable_flows),
            "choice_rows": choice.height,
            "mnl_mobile_people": allocation.filter(pl.col("mode_seq_id") != 0)["n_persons"].sum(),
            "mnl_total_people": allocation["n_persons"].sum(),
            "mean_best_utility": mean_best_utility,
            "mean_chosen_utility": mean_chosen_utility,
            **candidate_counts,
        }
        mnl_flows.write_parquet(folder / f"batch-{batch}-flows.parquet")
        choice.write_parquet(folder / f"batch-{batch}-choices.parquet")
        previous_choices[key], previous_flows[key] = choice, mnl_flows
        summary["logit_scale"] = logit_scale
        summary["diagnostic_seconds"] = perf_counter() - diagnostic_started
        file = (folder / "batches.jsonl").open("a", encoding="utf-8")
        try:
            file.write(json.dumps(summary) + "\n")
        finally:
            file.close()
        print("MNL_BATCH", json.dumps(summary), flush=True)
        return reachable

    PlanUpdater.estimate_reachable_allocation = estimate
