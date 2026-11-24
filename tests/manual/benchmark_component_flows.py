"""Compare expanded and shared-component flow aggregation on real allocations.

Usage: pixi run python tests/manual/benchmark_component_flows.py TIMING_CAPTURE
       EMBEDDING_CAPTURE --method expanded|shared --output FOLDER

Both captures must describe the same batch, and the timing capture must contain
exact modeled durations. A 100-context high-effort timing capture is supported;
the embedding capture may cover more contexts. Run each method in a fresh
process for comparable peak-RSS measurements. This is not a simulation.
"""
import argparse
import json
import threading
from pathlib import Path
from time import perf_counter

import polars as pl
import psutil

from mobility.trips.group_day_trips.plans.plan_ids import PLAN_KEY_COLS


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("timing", type=Path)
parser.add_argument("capture", type=Path)
parser.add_argument("--method", choices=["expanded", "shared"], required=True)
parser.add_argument("--output", type=Path, required=True)
parser.add_argument("--repetitions", type=int, default=3)
parser.add_argument("--od-only", action="store_true", help="Use the full candidate capture without modeled durations")
args = parser.parse_args()
args.output.mkdir(parents=True, exist_ok=True)
process = psutil.Process()
stop = threading.Event()
rss_peak = [process.memory_info().rss]


def monitor():
    while not stop.wait(0.01):
        rss_peak[0] = max(rss_peak[0], process.memory_info().rss)


monitor_thread = threading.Thread(target=monitor, daemon=True)
monitor_thread.start()
load_start = perf_counter()
unit = ["demand_group_id", "demand_subgroup_id"]
step_keys = PLAN_KEY_COLS + ["seq_step_index"]
scored = None
if args.od_only:
    contexts = pl.read_parquet(args.capture / "contexts.parquet").select(unit).unique()
else:
    scored = (pl.read_parquet(args.timing / "indexed-2.parquet")
              .select(step_keys + ["from", "to", "mode", "activity", "duration_per_pers",
                                   "departure_time", "arrival_time", "next_departure_time"])
              .unique(subset=step_keys, keep="first"))
    contexts = scored.select(unit).unique()
candidate = (pl.scan_parquet(args.capture / "candidate-utility.parquet")
             .join(contexts.lazy(), on=unit, how="semi")
             .select(PLAN_KEY_COLS + ["plan_id"]).collect(engine="streaming"))
unit_map = pl.read_parquet(args.capture / "unit-map.parquet")
allocations = (pl.read_parquet(args.capture / "allocated-plans.parquet")
               .filter(pl.col("mode_seq_id") != 0)
               .drop("plan_id")
               .join(unit_map, on=unit, how="left")
               .rename({"demand_group_id": "unit_group", "demand_subgroup_id": "unit_subgroup"})
               .with_columns(demand_group_id=pl.col("_score_context_id"),
                             demand_subgroup_id=pl.lit(0, dtype=pl.UInt32))
               .join(contexts, on=unit, how="semi"))
unmatched = allocations.join(candidate, on=PLAN_KEY_COLS, how="anti")
if unmatched.height:
    raise ValueError(f"{unmatched.height} allocated plans did not match the candidate capture.")
allocated = allocations.join(candidate, on=PLAN_KEY_COLS, how="inner").select(
    "unit_group", "unit_subgroup", "plan_id", "n_persons"
)
if args.od_only:
    steps = (pl.scan_parquet(args.capture / "candidate-steps.parquet")
             .join(allocated.select("plan_id").unique().lazy(), on="plan_id", how="semi")
             .select("plan_id", "seq_step_index", "from", "to", "mode").collect(engine="streaming"))
else:
    steps = (scored.join(candidate, on=PLAN_KEY_COLS, how="inner")
             .join(allocated.select("plan_id").unique(), on="plan_id", how="semi")
             .select("plan_id", "seq_step_index", "from", "to", "mode", "activity", "duration_per_pers",
                     "departure_time", "arrival_time", "next_departure_time"))
missing_steps = allocated.join(steps.select("plan_id").unique(), on="plan_id", how="anti")
if missing_steps.height:
    raise ValueError(f"{missing_steps.height} allocated plans have no scored steps.")
if not args.od_only:
    comparison_columns = ["from", "to", "mode", "activity", "departure_time", "arrival_time", "next_departure_time"]
    captured_steps = (pl.scan_parquet(args.capture / "candidate-steps.parquet")
                      .join(steps.select("plan_id").unique().lazy(), on="plan_id", how="semi")
                      .collect(engine="streaming"))
    matched_steps = steps.join(captured_steps, on=["plan_id", "seq_step_index"], suffix="_captured")
    if (matched_steps.height != steps.height or matched_steps.filter(pl.any_horizontal([
            ~pl.col(name).eq_missing(pl.col(name + "_captured")) for name in comparison_columns
    ])).height):
        raise ValueError("Allocated modeled steps do not match between captures.")
    steps = steps.drop("departure_time", "arrival_time", "next_departure_time")
    del captured_steps, matched_steps
load_seconds = perf_counter() - load_start
print("INPUT", json.dumps({"method": args.method, "contexts": contexts.height,
      "allocated_unit_plans": allocated.height, "allocated_context_plans": allocated["plan_id"].n_unique(),
      "context_plan_steps": steps.height, "allocated_people": allocated["n_persons"].sum(),
      "load_seconds": load_seconds, "input_bytes": allocated.estimated_size() + steps.estimated_size()}), flush=True)
del scored, candidate, allocations, unmatched, unit_map

trip_keys = ["from", "to", "mode"]
activity_keys = ["to", "activity", "duration_per_pers"]


def stable_sum(name):
    return pl.col(name).cast(pl.Float64).sort().implode().list.sum()


if args.method == "shared":
    start = perf_counter()
    trips = steps.select(trip_keys).unique().with_row_index("trip_component")
    trip_members = (steps.select(["plan_id"] + trip_keys).join(trips, on=trip_keys)
                    .group_by("plan_id", "trip_component").len(name="occurrences"))
    if not args.od_only:
        activities = (steps.filter(pl.col("activity") != "home").select(activity_keys).unique()
                      .with_row_index("activity_component"))
        activity_members = (steps.filter(pl.col("activity") != "home").select(["plan_id"] + activity_keys)
                            .join(activities, on=activity_keys)
                            .group_by("plan_id", "activity_component").len(name="occurrences"))
    build_seconds = perf_counter() - start
    print("HIERARCHY", json.dumps({"trip_components": trips.height,
          "activity_components": None if args.od_only else activities.height,
          "plan_trip_references": trip_members.height,
          "plan_activity_references": None if args.od_only else activity_members.height,
          "trip_occurrences": int(trip_members["occurrences"].sum()),
          "build_seconds": build_seconds,
          "estimated_bytes": sum(frame.estimated_size() for frame in
                                 ([trips, trip_members] if args.od_only else
                                  [trips, activities, trip_members, activity_members]))}), flush=True)

for repetition in range(args.repetitions):
    start = perf_counter()
    if args.method == "expanded":
        expanded = allocated.join(steps, on="plan_id", how="inner")
        if not args.od_only:
            expanded = expanded.with_columns((pl.col("duration_per_pers") * pl.col("n_persons"))
                                             .cast(pl.Float32).alias("duration"))
        od = expanded.group_by(trip_keys).agg(stable_sum("n_persons").alias("volume")).sort(trip_keys)
        if not args.od_only:
            occupation = (expanded.filter(pl.col("activity") != "home")
                          .group_by("to", "activity").agg(stable_sum("duration").alias("occupation"))
                          .sort("to", "activity"))
        rows = expanded.height
        del expanded
    else:
        volume = allocated.group_by("plan_id").agg(stable_sum("n_persons").alias("persons"))
        trip_volume = (trip_members.join(volume, on="plan_id")
                       .with_columns((pl.col("persons") * pl.col("occurrences")).alias("contribution"))
                       .group_by("trip_component").agg(stable_sum("contribution").alias("volume")))
        od = trips.join(trip_volume, on="trip_component").select(trip_keys + ["volume"]).sort(trip_keys)
        if not args.od_only:
            activity_volume = (activity_members.join(volume, on="plan_id")
                               .with_columns((pl.col("persons") * pl.col("occurrences")).alias("contribution"))
                               .group_by("activity_component").agg(stable_sum("contribution").alias("persons")))
            occupation = (activities.join(activity_volume, on="activity_component")
                          .with_columns((pl.col("duration_per_pers").cast(pl.Float64) * pl.col("persons"))
                                        .alias("weighted_duration"))
                          .group_by("to", "activity").agg(stable_sum("weighted_duration").alias("occupation"))
                          .sort("to", "activity"))
        rows = trip_members.height + (0 if args.od_only else activity_members.height)
    seconds = perf_counter() - start
    print("AGGREGATION", json.dumps({"repetition": repetition + 1, "seconds": seconds,
          "rows": rows, "od_cells": od.height,
          "occupation_cells": None if args.od_only else occupation.height}), flush=True)
od.write_parquet(args.output / "od.parquet")
if not args.od_only:
    occupation.write_parquet(args.output / "occupation.parquet")
stop.set()
monitor_thread.join()
print("PEAK_RSS_BYTES", rss_peak[0], flush=True)
