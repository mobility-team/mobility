"""Compare full timing with a narrow first-batch timing cache.

Usage: pixi run python tests/manual/benchmark_repeated_timing.py CAPTURE_FOLDER
The input is created by capture_repeated_timing.py. This only measures timing
and schedule indexing, not candidate scoring, allocation or the whole run.
"""
import json
from pathlib import Path
from time import perf_counter
import argparse

import polars as pl
from polars.testing import assert_frame_equal

from mobility.trips.group_day_trips.plans.plan_ids import PLAN_KEY_COLS
from mobility.trips.group_day_trips.plans.plan_schedule_updater import PlanScheduleUpdater
from mobility.trips.group_day_trips.plans.time_sequences import TimeSequenceIndexCache, assign_time_sequence_ids


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("folder", type=Path)
args = parser.parse_args()
folder = args.folder
settings = json.loads((folder / "settings.json").read_text(encoding="utf-8"))
first = pl.read_parquet(folder / "input-1.parquet")
second = pl.read_parquet(folder / "input-2.parquet")
catalogue = pl.read_parquet(folder / "catalogue-before-1.parquet")
updater = PlanScheduleUpdater()
index_cache = TimeSequenceIndexCache()
start = perf_counter()
first_timed = updater.update_plan_timings(first.lazy(), **settings).collect(engine="streaming")
first_indexed, catalogue, first_mapping = assign_time_sequence_ids(first_timed, catalogue, index_cache=index_cache)
first_seconds = perf_counter() - start
assert_frame_equal(catalogue, pl.read_parquet(folder / "catalogue-before-2.parquet"))
time_columns = ["departure_time", "arrival_time", "next_departure_time", "duration_per_pers"]
step_keys = PLAN_KEY_COLS + ["seq_step_index"]

# Keep only input plan/step keys, modeled times and their modeled schedule ID.
# The original scored columns and history will come from the current batch.
start = perf_counter()
mapping_keys = ["previous_time_seq_id" if key == "time_seq_id" else key for key in PLAN_KEY_COLS]
cached_steps = (
    first.select(step_keys).rename({"time_seq_id": "previous_time_seq_id"})
    .join(first_mapping, on=mapping_keys)
    .join(first_indexed.select(step_keys + time_columns).unique(), on=step_keys)
    .rename({"time_seq_id": "_modeled_time_seq_id", "previous_time_seq_id": "time_seq_id",
             **{column: f"_modeled_{column}" for column in time_columns}})
    .select(step_keys + ["_modeled_time_seq_id"] + [f"_modeled_{column}" for column in time_columns])
    .unique()
)
if cached_steps.select(step_keys).n_unique() != cached_steps.height:
    raise ValueError("One input step maps to more than one timed step.")
cached_plans = cached_steps.select(PLAN_KEY_COLS).unique()
cache_seconds = perf_counter() - start

def same_rows(left, right, keys):
    assert_frame_equal(left.sort(keys), right.sort(keys), check_row_order=True)


full_times = []
incremental_times = []
for repetition in range(3):
    cache = TimeSequenceIndexCache(schedules=catalogue, index=index_cache.index)
    start = perf_counter()
    full_timed = updater.update_plan_timings(second.lazy(), **settings).collect(engine="streaming")
    full, full_catalogue, full_mapping = assign_time_sequence_ids(full_timed, catalogue, index_cache=cache)
    full_times.append(perf_counter() - start)

    cache = TimeSequenceIndexCache(schedules=catalogue, index=index_cache.index)
    start = perf_counter()
    second_plans = second.select(PLAN_KEY_COLS).unique()
    new_plans = second_plans.join(cached_plans, on=PLAN_KEY_COLS, how="anti")
    new_rows = second.join(new_plans, on=PLAN_KEY_COLS, how="semi")
    new_timed = updater.update_plan_timings(new_rows.lazy(), **settings).collect(engine="streaming")
    new, new_catalogue, new_mapping = assign_time_sequence_ids(new_timed, catalogue, index_cache=cache)
    old = (second.join(cached_steps, on=step_keys, how="inner")
           .with_columns(pl.col("_modeled_time_seq_id").alias("time_seq_id"),
                         *[pl.col(f"_modeled_{column}").alias(column) for column in time_columns])
           .select(full.columns))
    combined = pl.concat([old, new.select(full.columns)], how="vertical_relaxed")
    old_mapping = (cached_steps.select(PLAN_KEY_COLS + ["_modeled_time_seq_id"]).unique()
                   .join(second_plans, on=PLAN_KEY_COLS, how="semi")
                   .rename({"time_seq_id": "previous_time_seq_id", "_modeled_time_seq_id": "time_seq_id"})
                   .select(full_mapping.columns))
    combined_mapping = pl.concat([old_mapping, new_mapping], how="vertical_relaxed")
    incremental_times.append(perf_counter() - start)

    same_rows(full, combined, full.columns)
    same_rows(full_mapping, combined_mapping, full_mapping.columns)
    assert_frame_equal(full_catalogue, new_catalogue)
    if repetition == 0:
        same_rows(full, pl.read_parquet(folder / "indexed-2.parquet"), full.columns)
        same_rows(full_mapping, pl.read_parquet(folder / "mapping-2.parquet"), full_mapping.columns)
        assert_frame_equal(full_catalogue, pl.read_parquet(folder / "catalogue-after-2.parquet"))
    print("TIMING_REPETITION", json.dumps({"repetition": repetition + 1, "full_seconds": full_times[-1],
                                            "incremental_seconds": incremental_times[-1]}), flush=True)

print("TIMING_RESULT", json.dumps({"first_rows": first.height, "second_rows": second.height,
    "second_plans": second_plans.height, "new_plans": new_plans.height, "new_rows": new_rows.height,
    "cached_bytes": cached_steps.estimated_size(), "first_seconds": first_seconds,
    "cache_construction_seconds": cache_seconds, "full_seconds": full_times,
    "incremental_seconds": incremental_times}), flush=True)
