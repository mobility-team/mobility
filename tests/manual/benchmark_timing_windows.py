"""Compare exact shared timing windows with the current complete-plan updater.

Inputs are captured timing inputs for two consecutive batches. The diagnostic
keeps complete plan rows for comparison; it does not model a full simulation.
"""
import argparse
import json
from pathlib import Path
from time import perf_counter

import polars as pl
from polars.testing import assert_frame_equal

from mobility.trips.group_day_trips.plans.plan_ids import PLAN_KEY_COLS
from mobility.trips.group_day_trips.plans.plan_schedule_updater import PlanScheduleUpdater


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("folder", type=Path)
parser.add_argument("settings", type=Path)
args = parser.parse_args()
settings = json.loads(args.settings.read_text(encoding="utf-8"))
rigidity = settings["activity_time_rigidity_by_activity"]
updater = PlanScheduleUpdater()
cache = None
keys = ["activity", "_previous_activity", "departure_time", "arrival_time", "time", "_next_activity",
        "_next_reference_departure", "_next_reference_arrival", "_next_travel_time", "next_departure_time"]
modeled = ["_departure", "_arrival", "_next_departure", "_duration"]

for batch in (1, 2):
    steps = pl.read_parquet(args.folder / f"input-{batch}.parquet")
    start = perf_counter()
    expected = updater.update_plan_timings(steps.lazy(), **settings).collect(engine="streaming")
    baseline_seconds = perf_counter() - start
    start = perf_counter()
    rows = steps.sort(PLAN_KEY_COLS + ["seq_step_index"]).with_columns(
        _previous_activity=pl.col("activity").shift(1).over(PLAN_KEY_COLS),
        _next_activity=pl.col("activity").shift(-1).over(PLAN_KEY_COLS),
        _next_reference_departure=pl.col("departure_time").shift(-1).over(PLAN_KEY_COLS),
        _next_reference_arrival=pl.col("arrival_time").shift(-1).over(PLAN_KEY_COLS),
        _next_travel_time=pl.col("time").shift(-1).over(PLAN_KEY_COLS),
    )
    windows = rows.select(keys).unique()
    new = windows if cache is None else windows.join(cache.select(keys), on=keys, how="anti", nulls_equal=True)
    time_dtype = steps.schema["departure_time"]
    duration_dtype = steps.schema["duration_per_pers"]
    def rigid(column):
        return pl.col(column).cast(pl.String).replace_strict(rigidity, default=None, return_dtype=pl.Float64)
    home_rigidity = rigidity.get("home", 0.0)
    new = (new.with_columns(
        _current_rigidity=rigid("activity"),
        _previous_rigidity=rigid("_previous_activity").fill_null(home_rigidity),
        _following_rigidity=rigid("_next_activity"),
    ).with_columns(
        _delta=pl.col("time") - (pl.col("arrival_time") - pl.col("departure_time")).clip(0.0, 24.0),
        _next_delta=pl.col("_next_travel_time")
        - (pl.col("_next_reference_arrival") - pl.col("_next_reference_departure")).clip(0.0, 24.0),
    ).with_columns(
        _departure=(pl.col("departure_time") - (1.0 + pl.col("_current_rigidity")
                    - pl.col("_previous_rigidity")) / 2.0 * pl.col("_delta")).cast(time_dtype),
        _arrival=(pl.col("arrival_time") + (1.0 - (1.0 + pl.col("_current_rigidity")
                  - pl.col("_previous_rigidity")) / 2.0) * pl.col("_delta")).cast(time_dtype),
        _next_modeled_departure=(pl.col("_next_reference_departure")
            - (1.0 + pl.col("_following_rigidity") - pl.col("_current_rigidity").fill_null(home_rigidity))
            / 2.0 * pl.col("_next_delta")).cast(time_dtype),
    ).with_columns(
        _next_departure=pl.coalesce("_next_modeled_departure", "next_departure_time").cast(time_dtype),
    ).with_columns(
        _duration=(pl.when(pl.col("_next_departure") - pl.col("_arrival") < updater.MIN_ACTIVITY_DURATION_HOURS)
                   .then(pl.lit(updater.MIN_ACTIVITY_DURATION_HOURS))
                   .otherwise(pl.col("_next_departure") - pl.col("_arrival"))).cast(duration_dtype),
    ).select(keys + modeled))
    cache = new if cache is None else pl.concat([cache, new])
    actual = (rows.join(cache, on=keys, how="left", nulls_equal=True)
              .with_columns(departure_time=pl.col("_departure"), arrival_time=pl.col("_arrival"),
                            next_departure_time=pl.col("_next_departure"), duration_per_pers=pl.col("_duration"))
              .select(steps.columns))
    elapsed = perf_counter() - start
    order = PLAN_KEY_COLS + ["seq_step_index"]
    assert_frame_equal(actual.sort(order), expected.sort(order), check_exact=True)
    print(f"TIMING_WINDOWS batch={batch} rows={steps.height} unique={windows.height} "
          f"new={new.height} baseline_seconds={baseline_seconds:.3f} "
          f"window_seconds={elapsed:.3f} cache_bytes={cache.estimated_size()}", flush=True)
