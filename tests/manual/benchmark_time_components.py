"""Compare exact and time-grid scoring on captured Geneva candidate steps.

Usage: pixi run python tests/manual/benchmark_time_components.py CAPTURE

The modeled grid rounds reference clock times and travel times to the nearest
interval, then rounds the rigidity adjustment. Activity durations come from the
resulting arrival and next departure. A zero-duration activity retains the
existing 0.001-hour utility floor. The input is already expanded, so the
measured factor-and-join time includes rebuilding and reattaching components;
it does not measure a future evaluator that starts from indexed components.
"""
import argparse
import json
from pathlib import Path
from time import perf_counter

import numpy as np
import polars as pl

from mobility.trips.group_day_trips.plans.plan_ids import PLAN_KEY_COLS
from mobility.trips.group_day_trips.plans.plan_schedule_updater import PlanScheduleUpdater


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("folder", type=Path)
parser.add_argument("--repetitions", type=int, default=3)
args = parser.parse_args()
settings = json.loads((args.folder / "settings.json").read_text(encoding="utf-8"))
key = PLAN_KEY_COLS + ["seq_step_index"]
columns = key + ["activity", "departure_time", "arrival_time", "next_departure_time", "duration_per_pers",
                 "time", "cost", "is_feasible", "min_activity_time", "activity_utility_scale"]
original = pl.read_parquet(args.folder / "input-2.parquet").select(columns).sort(key)
if original.select(key).n_unique() != original.height:
    raise ValueError("Input contains duplicate plan step keys.")
updater = PlanScheduleUpdater()
lookup = pl.DataFrame({"activity": list(settings["arrival_time_rigidity_by_activity"]),
                       "rigidity": list(settings["arrival_time_rigidity_by_activity"].values())}).with_columns(
                           activity=pl.col("activity").cast(original.schema["activity"]))


def second_times():
    return (updater.update_plan_timings(original.lazy(), **settings)
            .with_columns(*[((pl.col(col).cast(pl.Float64) * 3600).round(0) / 3600)
                            .cast(pl.Float32).alias(col) for col in
                            ("departure_time", "arrival_time", "next_departure_time", "duration_per_pers")])
            .collect(engine="streaming"))


def grid_inputs(minutes):
    return (original.join(lookup, on="activity", how="left", maintain_order="left")
            .with_columns(*[((pl.col(col).cast(pl.Float64) * 60 / minutes).round(0) * minutes)
                            .cast(pl.Int32).alias("_" + col) for col in
                            ("departure_time", "arrival_time", "next_departure_time", "time")]))


def time_grid(minutes, reuse):
    data = grid_inputs(minutes)
    trip = ["_departure_time", "_arrival_time", "_time", "rigidity"]
    if reuse:
        distinct = data.select(trip).unique()
        times = distinct.with_columns(
            _delta=pl.col("_time") - (pl.col("_arrival_time") - pl.col("_departure_time")).clip(0, 1440)
        ).with_columns(
            _modeled_departure=(pl.col("_departure_time")
                                - ((pl.col("rigidity") * pl.col("_delta") / minutes).round(0) * minutes))
                                .cast(pl.Int32),
            _modeled_arrival=(pl.col("_arrival_time")
                              + (((1 - pl.col("rigidity")) * pl.col("_delta") / minutes).round(0) * minutes))
                              .cast(pl.Int32),
        ).drop("_delta")
        data = data.join(times, on=trip, how="left", maintain_order="left")
    else:
        data = data.with_columns(
            _delta=pl.col("_time") - (pl.col("_arrival_time") - pl.col("_departure_time")).clip(0, 1440)
        ).with_columns(
            _modeled_departure=(pl.col("_departure_time")
                                - ((pl.col("rigidity") * pl.col("_delta") / minutes).round(0) * minutes))
                                .cast(pl.Int32),
            _modeled_arrival=(pl.col("_arrival_time")
                              + (((1 - pl.col("rigidity")) * pl.col("_delta") / minutes).round(0) * minutes))
                              .cast(pl.Int32),
        ).drop("_delta")
    # Both joins explicitly retain the already sorted input order, so each
    # plan's following departure can be read without another global sort.
    return (data.with_columns(
        _modeled_next=pl.col("_modeled_departure").shift(-1).over(PLAN_KEY_COLS)
        .fill_null(pl.col("_next_departure_time"))
    ).with_columns(
        _duration_minutes=(pl.col("_modeled_next") - pl.col("_modeled_arrival")).clip(lower_bound=0),
        _timing_feasible=((pl.col("_modeled_next") >= pl.col("_modeled_arrival"))
                          | (pl.col("seq_step_index") == pl.col("seq_step_index").max().over(PLAN_KEY_COLS))),
    ))


def grid_terms(data):
    duration = np.maximum(data["_duration_minutes"].to_numpy().astype(np.float64) / 60, 0.001)
    threshold = data["min_activity_time"].to_numpy()
    scale = data["activity_utility_scale"].to_numpy()
    return scale * np.maximum(np.log(duration / threshold), 0) - data["cost"].to_numpy()


def lookup_terms(data):
    duration = data["_duration_minutes"].to_numpy()
    threshold = data["min_activity_time"].to_numpy()
    scale = data["activity_utility_scale"].to_numpy()
    # The dense array covers the observed range; zero retains the current
    # 0.001-hour duration floor. Thresholds and scales remain exact.
    thresholds, indices = np.unique(threshold, return_inverse=True)
    maximum = int(duration.max())
    durations = np.maximum(np.arange(maximum + 1, dtype=np.float64) / 60, 0.001)
    table = np.maximum(np.log(durations[None, :] / thresholds[:, None]), 0)
    return (scale, table, indices, duration, data["cost"].to_numpy())


start = perf_counter()
baseline = second_times()
baseline_seconds = perf_counter() - start
mapping_path = args.folder / "mapping-2.parquet"
if mapping_path.exists():
    # The saved modeled IDs differ from incoming IDs. Verify our timing
    # baseline against the real evaluator before drawing any comparisons.
    mapping = pl.read_parquet(mapping_path)
    incoming_keys = ["previous_time_seq_id" if name == "time_seq_id" else name for name in PLAN_KEY_COLS]
    baseline_keys = (baseline.rename({"time_seq_id": "previous_time_seq_id"})
                     .join(mapping, on=incoming_keys))
    output_columns = ["departure_time", "arrival_time", "next_departure_time", "duration_per_pers"]
    captured = (pl.read_parquet(args.folder / "indexed-2.parquet").select(key + output_columns)
                .unique(subset=key, keep="first"))
    compared = baseline_keys.join(captured, on=key, suffix="_captured")
    if (compared.height != baseline.height or compared.filter(pl.any_horizontal([
            ~pl.col(name).eq_missing(pl.col(name + "_captured")) for name in output_columns
    ])).height):
        raise ValueError("The timing baseline does not match the captured modeled steps.")
    del mapping, baseline_keys, captured, compared
baseline = baseline.with_columns(
    _baseline_feasible=(pl.col("is_feasible") & ((pl.col("next_departure_time") >= pl.col("arrival_time"))
                        | (pl.col("seq_step_index") == pl.col("seq_step_index").max().over(PLAN_KEY_COLS)))),
    _baseline_step_utility=(pl.col("activity_utility_scale")
                            * (pl.col("duration_per_pers") / pl.col("min_activity_time")).log().clip(0.0)
                            - pl.col("cost")),
)
print("BASELINE", json.dumps({"rows": original.height, "plans": original.select(PLAN_KEY_COLS).n_unique(),
      "seconds": baseline_seconds}), flush=True)
for minutes in (1, 5, 15):
    timings = []
    for repeat in range(args.repetitions):
        start = perf_counter()
        direct = time_grid(minutes, False)
        direct_s = perf_counter() - start
        start = perf_counter()
        shared = time_grid(minutes, True)
        shared_s = perf_counter() - start
        comparison = ["_modeled_departure", "_modeled_arrival", "_modeled_next", "_duration_minutes", "_timing_feasible"]
        if not direct.select(comparison).equals(shared.select(comparison)):
            raise ValueError("Shared trip timing differs from direct timing.")
        start = perf_counter()
        direct_utility = grid_terms(direct)
        direct_utility_s = perf_counter() - start
        start = perf_counter()
        scale, table, indices, duration, cost = lookup_terms(shared)
        build_lookup_s = perf_counter() - start
        start = perf_counter()
        shared_utility = scale * table[indices, duration] - cost
        lookup_s = perf_counter() - start
        lookup_bytes = table.nbytes
        if not np.allclose(direct_utility, shared_utility, rtol=1e-12, atol=1e-12, equal_nan=True):
            raise ValueError("The activity lookup disagrees with direct utility.")
        timings.append({"direct_grid_seconds": direct_s, "shared_grid_seconds": shared_s,
                        "direct_utility_seconds": direct_utility_s, "build_lookup_seconds": build_lookup_s,
                        "lookup_utility_seconds": lookup_s})
    compared = (direct.with_columns(
        _grid_step_utility=pl.Series(direct_utility),
        _grid_feasible=pl.col("is_feasible") & pl.col("_timing_feasible"),
    ).with_columns(
        _baseline_feasible=baseline["_baseline_feasible"],
        _baseline_step_utility=baseline["_baseline_step_utility"],
    ).group_by(PLAN_KEY_COLS).agg(
        pl.col("_grid_feasible").all().alias("grid_feasible"),
        pl.col("_baseline_feasible").all().alias("baseline_feasible"),
        (pl.col("_grid_step_utility") - pl.col("_baseline_step_utility")).sum().abs().alias("step_utility_error"),
    ))
    both = compared.filter(pl.col("grid_feasible") & pl.col("baseline_feasible"))
    print("GRID", json.dumps({"minutes": minutes,
          "trip_timing_keys": direct.select("_departure_time", "_arrival_time", "_time", "rigidity").n_unique(),
          "lookup_bytes": lookup_bytes,
          "became_feasible": compared.filter(~pl.col("baseline_feasible") & pl.col("grid_feasible")).height,
          "became_infeasible": compared.filter(pl.col("baseline_feasible") & ~pl.col("grid_feasible")).height,
          "feasible_both": both.height,
          "step_utility_error_median": both["step_utility_error"].median(),
          "step_utility_error_p90": both["step_utility_error"].quantile(0.9),
          "step_utility_error_p99": both["step_utility_error"].quantile(0.99),
          "seconds_by_repetition": timings}), flush=True)
