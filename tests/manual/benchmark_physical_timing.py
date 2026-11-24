"""Time modeled schedules once per physical plan on a captured inner batch.

Usage: pixi run python tests/manual/benchmark_physical_timing.py CAPTURE_FOLDER
The physical input is factored before timing, representing the data structure a
future shared-plan evaluator could use. This capture must have identical timing
inputs for each physical step; real scenarios can require additional profile
keys. Timings exclude the one-time construction of the factored input and do
not measure scoring, transitions or a complete simulation.
"""
import argparse
import json
from pathlib import Path
from time import perf_counter

import polars as pl
from polars.testing import assert_frame_equal

from mobility.trips.group_day_trips.plans.plan_ids import PLAN_KEY_COLS
from mobility.trips.group_day_trips.plans.plan_schedule_updater import PlanScheduleUpdater
from mobility.trips.group_day_trips.plans.time_sequences import TimeSequenceIndexCache, assign_time_sequence_ids


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("folder", type=Path)
parser.add_argument("--contexts", type=Path, help="Scoring contexts with their utility profile IDs")
args = parser.parse_args()
folder = args.folder
settings = json.loads((folder / "settings.json").read_text(encoding="utf-8"))
first = pl.read_parquet(folder / "input-1.parquet")
second = pl.read_parquet(folder / "input-2.parquet")
catalogue = pl.read_parquet(folder / "catalogue-before-1.parquet")
updater = PlanScheduleUpdater()
index_cache = TimeSequenceIndexCache()
first_timed = updater.update_plan_timings(first.lazy(), **settings).collect(engine="streaming")
_, catalogue, _ = assign_time_sequence_ids(first_timed, catalogue, index_cache=index_cache)
assert_frame_equal(catalogue, pl.read_parquet(folder / "catalogue-before-2.parquet"))
times = ["departure_time", "arrival_time", "next_departure_time", "duration_per_pers"]
unit = ["demand_group_id", "demand_subgroup_id"]
if args.contexts:
    context_profiles = pl.read_parquet(args.contexts).select(unit + ["utility_profile_id"])
else:
    context_profiles = second.select(unit).unique().with_columns(utility_profile_id=pl.lit(0, dtype=pl.UInt32))
physical_plan = [column for column in PLAN_KEY_COLS if column not in unit] + ["utility_profile_id"]
physical_step = physical_plan + ["seq_step_index"]
values = ["activity", "from", "to", "mode", "time"] + times
start = perf_counter()
distinct = second.select(unit + [column for column in physical_step if column != "utility_profile_id"] + values) \
    .join(context_profiles, on=unit).drop(unit).unique()
if distinct.select(physical_step).n_unique() != distinct.height:
    raise ValueError("A physical plan has differing timing inputs; a profile key is required.")
factored = distinct.rename({"utility_profile_id": "demand_group_id"}).with_columns(
    demand_subgroup_id=pl.lit(0, dtype=pl.UInt32))
factor_seconds = perf_counter() - start

for repetition in range(3):
    cache = TimeSequenceIndexCache(schedules=catalogue, index=index_cache.index)
    start = perf_counter()
    full_timed = updater.update_plan_timings(second.lazy(), **settings).collect(engine="streaming")
    full, full_catalogue, full_mapping = assign_time_sequence_ids(full_timed, catalogue, index_cache=cache)
    full_seconds = perf_counter() - start

    cache = TimeSequenceIndexCache(schedules=catalogue, index=index_cache.index)
    start = perf_counter()
    narrow_timed = updater.update_plan_timings(factored.lazy(), **settings).collect(engine="streaming")
    narrow, narrow_catalogue, narrow_mapping = assign_time_sequence_ids(narrow_timed, catalogue, index_cache=cache)
    physical_seconds = perf_counter() - start

    start = perf_counter()
    # Attach the adjusted times back to the original input physical keys.
    physical_mapping = (narrow_mapping.drop("demand_subgroup_id")
                        .rename({"demand_group_id": "utility_profile_id"})
                        .rename({"time_seq_id": "_modeled_time_seq_id", "previous_time_seq_id": "time_seq_id"}))
    modeled_steps = (narrow.drop("demand_subgroup_id").rename({"demand_group_id": "utility_profile_id"})
                     .rename({"time_seq_id": "_modeled_time_seq_id",
                              **{name: f"_modeled_{name}" for name in times}})
                     .select([name for name in physical_step if name != "time_seq_id"]
                             + ["_modeled_time_seq_id"] + [f"_modeled_{name}" for name in times]).unique())
    physical_results = physical_mapping.join(modeled_steps, on=[name for name in physical_plan
                                                                 if name != "time_seq_id"] + ["_modeled_time_seq_id"])
    rebuilt = (second.join(context_profiles, on=unit).join(physical_results, on=physical_step)
               .with_columns(pl.col("_modeled_time_seq_id").alias("time_seq_id"),
                             *[pl.col(f"_modeled_{name}").alias(name) for name in times])
               .select(full.columns))
    rebuilt_mapping = (second.select(PLAN_KEY_COLS).unique().join(context_profiles, on=unit)
                       .join(physical_mapping, on=physical_plan)
                       .rename({"time_seq_id": "previous_time_seq_id", "_modeled_time_seq_id": "time_seq_id"})
                       .select(full_mapping.columns))
    expand_seconds = perf_counter() - start
    assert_frame_equal(full.sort(full.columns), rebuilt.sort(full.columns))
    assert_frame_equal(full_mapping.sort(full_mapping.columns), rebuilt_mapping.sort(full_mapping.columns))
    assert_frame_equal(full_catalogue, narrow_catalogue)
    if repetition == 0:
        assert_frame_equal(full.sort(full.columns), pl.read_parquet(folder / "indexed-2.parquet").sort(full.columns))
    print("PHYSICAL_TIMING_REPETITION", json.dumps({"repetition": repetition + 1, "full_seconds": full_seconds,
          "physical_seconds": physical_seconds, "expand_seconds": expand_seconds}), flush=True)

print("PHYSICAL_TIMING_RESULT", json.dumps({"context_steps": second.height,
      "physical_steps": factored.height, "factored_estimated_bytes": factored.estimated_size(),
      "factor_seconds": factor_seconds}), flush=True)
