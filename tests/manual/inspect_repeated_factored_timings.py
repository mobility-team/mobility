"""Measure whether timed physical plans repeat across captured inner batches."""
import argparse
import json
from pathlib import Path
from time import perf_counter

import polars as pl

from mobility.trips.group_day_trips.plans.demand_subgroups import DEMAND_UNIT_COLS
from mobility.trips.group_day_trips.plans.factored_plan_timing import time_factored_candidates


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("folder", type=Path)
args = parser.parse_args()
keys = ["activity_seq_id", "previous_time_seq_id", "dest_seq_id", "mode_seq_id", "utility_profile_id"]
times = ["departure_time", "arrival_time", "next_departure_time", "duration_per_pers", "time_seq_id"]
previous = None
previous_plans = None
for batch in (1, 2):
    folder = args.folder / f"batch-{batch}"
    physical = pl.read_parquet(folder / "physical.parquet")
    members = pl.read_parquet(folder / "members.parquet")
    contexts = pl.read_parquet(folder / "contexts.parquet")
    lookup = pl.read_parquet(folder / "step-scores.parquet")
    schedules = pl.read_parquet(folder / "schedule-catalogue.parquet")
    settings = json.loads((folder / "settings.json").read_text(encoding="utf-8"))
    travel = lookup.select("utility_profile_id", "from", "to", "mode", "time").unique()
    start = perf_counter()
    timed = time_factored_candidates(physical, members, contexts, travel, schedules, **settings)
    elapsed = perf_counter() - start
    plans = (timed.context_mapping.join(contexts.select(DEMAND_UNIT_COLS + ["utility_profile_id"]),
                                        on=DEMAND_UNIT_COLS)
             .select(keys + ["_timing_plan"]).unique())
    values = (plans.join(timed.steps.select(["_timing_plan", "seq_step_index"] + times), on="_timing_plan")
              .drop("_timing_plan"))
    print(f"TIMING_BATCH batch={batch} physical_profiles={plans.height} steps={values.height} seconds={elapsed:.3f}",
          flush=True)
    if previous is not None:
        common = values.join(previous, on=keys + ["seq_step_index"], suffix="_previous")
        changed = common.filter(pl.any_horizontal([
            ~pl.col(name).eq_missing(pl.col(name + "_previous")) for name in times
        ]))
        print(f"TIMING_REUSE common_steps={common.height} changed_steps={changed.height} "
              f"common_profile_plans={common.select(keys).unique().height}", flush=True)
        # Time only plan/profile combinations absent from the previous batch.
        incremental_start = perf_counter()
        input_keys = ["activity_seq_id", "time_seq_id", "dest_seq_id", "mode_seq_id", "utility_profile_id"]
        old_keys = previous_plans.rename({"previous_time_seq_id": "time_seq_id"}).select(input_keys)
        with_profiles = members.join(contexts.select(DEMAND_UNIT_COLS + ["utility_profile_id"]),
                                     on=DEMAND_UNIT_COLS)
        new_members = with_profiles.join(old_keys, on=input_keys, how="anti").drop("utility_profile_id")
        plan_columns = input_keys[:-1]
        new_physical = physical.join(new_members.select(plan_columns).unique(), on=plan_columns, how="semi")
        new_timing = time_factored_candidates(new_physical, new_members, contexts, travel, schedules, **settings)
        new_mapping = (new_timing.context_mapping
                       .join(contexts.select(DEMAND_UNIT_COLS + ["utility_profile_id"]), on=DEMAND_UNIT_COLS)
                       .select(keys + ["time_seq_id"]).unique())
        full_mapping = timed.context_mapping.join(
            contexts.select(DEMAND_UNIT_COLS + ["utility_profile_id"]), on=DEMAND_UNIT_COLS
        ).select(keys + ["time_seq_id"]).unique()
        compared = new_mapping.join(full_mapping, on=keys, suffix="_full")
        changed_ids = compared.filter(pl.col("time_seq_id") != pl.col("time_seq_id_full")).height
        reused_mapping = previous.select(keys + ["time_seq_id"]).unique().join(
            full_mapping.select(keys), on=keys, how="semi"
        )
        combined = pl.concat([new_mapping, reused_mapping]).unique()
        new_values = (new_timing.context_mapping
                      .join(contexts.select(DEMAND_UNIT_COLS + ["utility_profile_id"]), on=DEMAND_UNIT_COLS)
                      .select(keys + ["_timing_plan"]).unique()
                      .join(new_timing.steps.select(["_timing_plan", "seq_step_index"] + times), on="_timing_plan")
                      .drop("_timing_plan"))
        reused_values = previous.join(reused_mapping.select(keys), on=keys, how="semi")
        combined_values = pl.concat([new_values, reused_values])
        incremental_seconds = perf_counter() - incremental_start
        mapping_errors = (combined.join(full_mapping, on=keys, how="full", coalesce=True)
                          .filter(~pl.col("time_seq_id").eq_missing(pl.col("time_seq_id_right"))).height)
        step_comparison = combined_values.join(values, on=keys + ["seq_step_index"], how="full", coalesce=True)
        step_errors = step_comparison.filter(pl.any_horizontal([
            ~pl.col(name).eq_missing(pl.col(name + "_right")) for name in times
        ])).height
        print(f"TIMING_INCREMENTAL new_profile_plans={new_mapping.height} "
              f"seconds={incremental_seconds:.3f} changed_schedule_ids={changed_ids} "
              f"combined_mapping_errors={mapping_errors} step_errors={step_errors}", flush=True)
    previous = values
    previous_plans = plans
