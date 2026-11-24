"""Score captured candidates using shared components and compact references.

Usage: pixi run python tests/manual/benchmark_hierarchical_utility.py CAPTURE

Inputs start as expanded rows because that is what the capture saved. Build a
compact trip catalogue, plan offsets and per-step references once, then score
plans by gathering those shared components into numerical arrays. This compares
the indexed calculation with a direct array calculation, not with a complete
simulation. Supply matching choice and home-duration captures to score the
home-night term as well.
"""
import argparse
import json
from pathlib import Path
from time import perf_counter

import numpy as np
import polars as pl

from mobility.trips.group_day_trips.plans.plan_ids import PLAN_KEY_COLS
from mobility.trips.group_day_trips.plans.plan_schedule_updater import PlanScheduleUpdater
import run_time_grid_probe as grid_probe


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("folder", type=Path)
parser.add_argument("--minutes", type=int, default=5)
parser.add_argument("--repetitions", type=int, default=5)
parser.add_argument("--utility-capture", type=Path)
parser.add_argument("--home-durations", type=Path)
parser.add_argument("--constant", type=float, default=1.25)
args = parser.parse_args()
if args.minutes <= 0:
    raise ValueError("The time grid must be positive.")
start = perf_counter()
settings = json.loads((args.folder / "settings.json").read_text(encoding="utf-8"))
step_key = PLAN_KEY_COLS + ["seq_step_index"]
source = (pl.read_parquet(args.folder / "input-2.parquet")
          .select(step_key + ["activity", "departure_time", "arrival_time", "next_departure_time", "duration_per_pers", "time",
                              "min_activity_time", "activity_utility_scale", "cost", "is_feasible"])
          .sort(step_key))
if source.select(step_key).n_unique() != source.height:
    raise ValueError("Plan step keys must be unique.")
rigidities = pl.DataFrame({"activity": list(settings["arrival_time_rigidity_by_activity"]),
                           "rigidity": list(settings["arrival_time_rigidity_by_activity"].values())}) \
    .with_columns(activity=pl.col("activity").cast(source.schema["activity"]))
rows = source.join(rigidities, on="activity", maintain_order="left").with_columns(*[
    ((pl.col(name).cast(pl.Float64) * 60 / args.minutes).round(0) * args.minutes)
    .cast(pl.Int32).alias("grid_" + name)
    for name in ("departure_time", "arrival_time", "next_departure_time", "time")
])
trip_keys = ["grid_departure_time", "grid_arrival_time", "grid_time", "rigidity"]
trips = rows.select(trip_keys).unique(maintain_order=True).with_row_index("trip_index")
plans = rows.select(PLAN_KEY_COLS).unique(maintain_order=True).with_row_index("plan_index")
rows = (rows.join(trips, on=trip_keys, maintain_order="left")
        .join(plans, on=PLAN_KEY_COLS, maintain_order="left"))
plan_index = rows["plan_index"].to_numpy()
offsets = np.flatnonzero(np.r_[True, plan_index[1:] != plan_index[:-1]])
ends = np.r_[offsets[1:] - 1, len(plan_index) - 1]
if len(offsets) != plans.height or not np.array_equal(plan_index[offsets], np.arange(plans.height)):
    raise ValueError("Plan steps are not contiguous in plan order.")
trip_index = rows["trip_index"].to_numpy()
tail_departure = rows["grid_next_departure_time"].to_numpy()[ends]
thresholds, threshold_index = np.unique(rows["min_activity_time"].to_numpy(), return_inverse=True)
scale = rows["activity_utility_scale"].to_numpy()
cost = rows["cost"].to_numpy()
cost_feasible = rows["is_feasible"].to_numpy()
trip_dep = trips["grid_departure_time"].to_numpy()
trip_arr = trips["grid_arrival_time"].to_numpy()
trip_time = trips["grid_time"].to_numpy()
trip_rigidity = trips["rigidity"].to_numpy()
home_min = home_scale = None
if args.utility_capture or args.home_durations:
    if not args.utility_capture or not args.home_durations or args.constant <= 0:
        raise ValueError("Both matching choice and home-duration captures and a positive constant are required.")
    unit = ["demand_group_id", "demand_subgroup_id"]
    contexts = pl.read_parquet(args.utility_capture / "contexts.parquet").select(unit + ["country", "csp"])
    home = pl.read_parquet(args.home_durations).select("country", "csp", "mean_home_night_per_pers")
    stay = (pl.read_parquet(args.utility_capture / "candidate-utility.parquet")
            .filter(pl.col("mode_seq_id") == 0).select(unit + ["utility"]))
    home_parameters = (plans.join(contexts, on=unit, maintain_order="left")
                       .join(home, on=["country", "csp"], maintain_order="left")
                       .join(stay, on=unit, maintain_order="left"))
    if home_parameters.height != plans.height or home_parameters["utility"].null_count():
        raise ValueError("Missing or duplicate home-night parameters for plan contexts.")
    home_min = home_parameters["mean_home_night_per_pers"].to_numpy() * np.exp(-args.constant)
    # Stay-home utility is home value-of-time * mean duration * constant.
    home_scale = home_parameters["utility"].to_numpy() / args.constant
preparation_seconds = perf_counter() - start


def score(use_components):
    dep = trip_dep if use_components else rows["grid_departure_time"].to_numpy()
    arr = trip_arr if use_components else rows["grid_arrival_time"].to_numpy()
    travel = trip_time if use_components else rows["grid_time"].to_numpy()
    rigidity = trip_rigidity if use_components else rows["rigidity"].to_numpy()
    delta = travel - np.clip(arr - dep, 0, 1440)
    departure = dep - np.rint(rigidity * delta / args.minutes).astype(np.int32) * args.minutes
    arrival = arr + np.rint((1 - rigidity) * delta / args.minutes).astype(np.int32) * args.minutes
    if use_components:
        departure = departure[trip_index]
        arrival = arrival[trip_index]
        travel = travel[trip_index]
    following = np.empty_like(departure)
    following[:-1] = departure[1:]
    following[ends] = tail_departure
    duration = np.maximum(following - arrival, 0)
    step_feasible = (following >= arrival) & cost_feasible
    step_feasible[ends] = cost_feasible[ends]
    feasible = np.logical_and.reduceat(step_feasible, offsets)
    # The existing schedule index rounds the 0.001-hour floor to four seconds.
    # This matches the hybrid probe; a new integer-time schema must define its
    # own zero-duration policy explicitly.
    floor_hours = round(0.001 * 3600) / 3600
    if use_components:
        minute_values = np.maximum(np.arange(int(duration.max()) + 1, dtype=np.float64) / 60, floor_hours)
        duration_terms = np.maximum(np.log(minute_values[None, :] / thresholds[:, None]), 0)
        step_utility = scale * duration_terms[threshold_index, duration] - cost
    else:
        duration_terms = None
        step_utility = scale * np.maximum(np.log(np.maximum(duration / 60, floor_hours)
                                                   / thresholds[threshold_index]), 0) - cost
    # The final home-night contribution depends on total time. Return its
    # physical input so callers can add it with the correct profile parameters.
    step_utility_sum = np.add.reduceat(step_utility, offsets)
    home_hours = np.maximum(24 - np.add.reduceat((np.maximum(duration / 60, floor_hours) + travel / 60), offsets), 0)
    total_utility = (step_utility_sum + home_scale * np.maximum(np.log(home_hours / home_min), 0)
                     if home_min is not None else step_utility_sum)
    return feasible, total_utility, home_hours, duration_terms


durations = []
for repeat in range(args.repetitions):
    start = perf_counter()
    direct = score(False)
    direct_seconds = perf_counter() - start
    start = perf_counter()
    shared = score(True)
    shared_seconds = perf_counter() - start
    if (not np.array_equal(direct[0], shared[0])
            or not np.allclose(direct[1], shared[1], rtol=1e-12, atol=1e-12)
            or not np.allclose(direct[2], shared[2], rtol=1e-12, atol=1e-12)):
        raise ValueError("Shared scoring does not match direct scoring.")
    durations.append({"direct_seconds": direct_seconds, "shared_seconds": shared_seconds})

# Check against the actual hybrid Polars scorer, including its Float32 and
# second-index rounding. This validation is outside the measured kernel.
grid_probe.interval = args.minutes
modeled = (grid_probe.update_grid(PlanScheduleUpdater(), source.lazy(), **settings)
           .with_columns(*[((pl.col(name).cast(pl.Float64) * 3600).round(0) / 3600)
                           .cast(pl.Float32).alias(name) for name in
                           ("departure_time", "arrival_time", "next_departure_time", "duration_per_pers")])
           .collect(engine="streaming"))
modeled_duration = modeled["duration_per_pers"].to_numpy().astype(np.float64)
modeled_time = modeled["time"].to_numpy()
modeled_feasible = ((modeled["next_departure_time"].to_numpy() >= modeled["arrival_time"].to_numpy())
                    & cost_feasible)
modeled_feasible[ends] = cost_feasible[ends]
modeled_feasible = np.logical_and.reduceat(modeled_feasible, offsets)
expected_utility = np.add.reduceat(scale * np.maximum(np.log(modeled_duration / thresholds[threshold_index]), 0)
                                  - cost, offsets)
expected_home = np.maximum(24 - np.add.reduceat(modeled_duration + modeled_time, offsets), 0)
if home_min is not None:
    expected_utility += home_scale * np.maximum(np.log(expected_home / home_min), 0)
finite = np.isfinite(expected_utility) & np.isfinite(shared[1])
max_error = float(np.max(np.abs(expected_utility[finite] - shared[1][finite]))) if finite.any() else 0.0
if not np.array_equal(modeled_feasible, shared[0]) or max_error > 0.0001:
    raise ValueError(f"Hierarchical scoring disagrees with the hybrid scorer: max error {max_error}")

# These arrays exclude the captured dataframe, the temporary construction
# tables, outputs, and Python/allocator overhead. They are a lower bound.
arrays = [offsets, ends, trip_index, tail_departure, threshold_index, thresholds,
          scale, cost, cost_feasible, trip_dep, trip_arr, trip_time, trip_rigidity, shared[3]]
if home_min is not None:
    arrays += [home_min, home_scale]
print("HIERARCHICAL_UTILITY", json.dumps({"minutes": args.minutes, "steps": rows.height,
      "plans": plans.height, "distinct_trips": trips.height, "thresholds": len(thresholds),
      "compact_array_bytes_lower_bound": sum(array.nbytes for array in arrays),
      "preparation_seconds_from_expanded_rows": preparation_seconds,
      "feasible_plans": int(shared[0].sum()), "includes_home_night": home_min is not None,
      "hybrid_scorer_max_utility_error": max_error,
      "seconds_by_repetition": durations}), flush=True)
