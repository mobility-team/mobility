"""Measure eager timing-update stages against the current lazy calculation.

Run on a capture from MOBILITY_PROBE_CAPTURE_TIMING_STEP:
    pixi run python tests/manual/inspect_timing_update.py CAPTURE_DIRECTORY

Sampling can miss short-lived peaks. Running stages eagerly changes their
memory lifetimes, so these figures do not partition the lazy query's peak.
"""

import argparse
import json
import threading
from pathlib import Path
from time import perf_counter

import polars as pl
import psutil

from mobility.trips.group_day_trips.plans.plan_ids import PLAN_KEY_COLS
from mobility.trips.group_day_trips.plans.plan_schedule_updater import PlanScheduleUpdater


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("capture", type=Path)
args = parser.parse_args()
settings = json.loads((args.capture / "timing-settings.json").read_text(encoding="utf-8"))
steps = pl.read_parquet(args.capture / "timing-input.parquet")
updater = PlanScheduleUpdater()
process = psutil.Process()
peak = [process.memory_info().rss]
stop = threading.Event()


def monitor():
    while not stop.wait(0.005):
        peak[0] = max(peak[0], process.memory_info().rss)


def measure(name, operation):
    start_rss = process.memory_info().rss
    peak[0] = start_rss
    start = perf_counter()
    result = operation()
    print(json.dumps({
        "stage": name, "seconds": perf_counter() - start,
        "start_rss": start_rss, "peak_rss": max(peak[0], process.memory_info().rss),
        "rows": result.height, "estimated_bytes": result.estimated_size(),
    }), flush=True)
    return result


thread = threading.Thread(target=monitor, daemon=True)
thread.start()
try:
    # Compare the whole result before testing deliberately materialized stages.
    expected = measure("lazy query", lambda: updater.update_plan_timings(
        steps.lazy(), **settings
    ).collect(engine="streaming"))
    rigidity = pl.DataFrame({
        "activity": list(settings["activity_time_rigidity_by_activity"]),
        "activity_time_rigidity": list(settings["activity_time_rigidity_by_activity"].values()),
    }).with_columns(activity=pl.col("activity").cast(steps.schema["activity"]))
    time_dtype = steps.schema["departure_time"]
    duration_dtype = steps.schema["duration_per_pers"]
    current = measure("join rigidity", lambda: steps.join(rigidity, on="activity", how="left"))
    current = measure("sort plans", lambda: current.sort(PLAN_KEY_COLS + ["seq_step_index"]))
    current = measure("adjust times", lambda: current.with_columns(
        reference_travel_time=(pl.col("arrival_time") - pl.col("departure_time")).clip(0.0, 24.0),
        previous_rigidity=pl.col("activity_time_rigidity").shift(1).over(PLAN_KEY_COLS)
        .fill_null(settings["activity_time_rigidity_by_activity"].get("home", 0.0)),
    ).with_columns(
        travel_time_delta=pl.col("time") - pl.col("reference_travel_time"),
        departure_share=(1.0 + pl.col("activity_time_rigidity") - pl.col("previous_rigidity")) / 2.0,
    ).with_columns(
        departure_time=(pl.col("departure_time") - pl.col("departure_share") * pl.col("travel_time_delta")).cast(time_dtype),
        arrival_time=(pl.col("arrival_time") + (1.0 - pl.col("departure_share")) * pl.col("travel_time_delta")).cast(time_dtype),
    ))
    sorted_steps = current
    current = measure("next departure window", lambda: current.with_columns(
        next_departure_time=pl.col("departure_time").shift(-1).over(PLAN_KEY_COLS)
        .fill_null(pl.col("next_departure_time")).cast(time_dtype),
    ))
    raw_duration = pl.col("next_departure_time") - pl.col("arrival_time")
    current = measure("activity duration", lambda: current.with_columns(
        duration_per_pers=pl.when(raw_duration < updater.MIN_ACTIVITY_DURATION_HOURS)
        .then(pl.lit(updater.MIN_ACTIVITY_DURATION_HOURS)).otherwise(raw_duration).cast(duration_dtype),
    ).drop(["activity_time_rigidity", "previous_rigidity", "departure_share", "reference_travel_time", "travel_time_delta"]))
    print("SAME_SCHEMA_AND_ROWS", current.schema == expected.schema and current.equals(expected), flush=True)
    # Adjacent rows belong to the same plan only when every sorted key agrees.
    # This avoids grouping a second time after the global plan-key sort.
    adjacent = measure("next departure adjacent keys", lambda: sorted_steps.with_columns(
        next_departure_time=pl.when(pl.all_horizontal([
            pl.col(key).eq_missing(pl.col(key).shift(-1)) for key in PLAN_KEY_COLS
        ])).then(pl.col("departure_time").shift(-1))
        .otherwise(pl.col("next_departure_time")).fill_null(pl.col("next_departure_time")).cast(time_dtype),
    ).with_columns(
        duration_per_pers=pl.when(raw_duration < updater.MIN_ACTIVITY_DURATION_HOURS)
        .then(pl.lit(updater.MIN_ACTIVITY_DURATION_HOURS)).otherwise(raw_duration).cast(duration_dtype),
    ).drop(["activity_time_rigidity", "previous_rigidity", "departure_share", "reference_travel_time", "travel_time_delta"]))
    print("ADJACENT_SAME_SCHEMA_AND_ROWS", adjacent.schema == expected.schema and adjacent.equals(expected), flush=True)
finally:
    stop.set()
    thread.join()
