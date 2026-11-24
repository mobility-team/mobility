"""Benchmark the expanded equivalent of the pre-expansion section prototype.

Uses the same captured physical candidates, memberships and precomputed
scoring lookup as benchmark_preexpanded_sections.py. It expands the candidates
and calculates timing, feasibility and utility per scoring context. Neither
benchmark measures generation, lookup construction or modeled-ID assignment.
"""
import argparse
import json
import threading
from pathlib import Path
from time import perf_counter

import numpy as np
import polars as pl
import psutil

from mobility.trips.group_day_trips.plans.compact_candidate_steps import PLAN_COLUMNS
from mobility.trips.group_day_trips.plans.plan_ids import PLAN_KEY_COLS
from mobility.trips.group_day_trips.plans.plan_schedule_updater import PlanScheduleUpdater


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("folder", type=Path)
parser.add_argument("--constant", type=float, default=1.25)
args = parser.parse_args()
unit = ["demand_group_id", "demand_subgroup_id"]
score_keys = ["country", "csp", "activity", "from", "to", "mode", "utility_profile_id"]
process = psutil.Process()
stop = threading.Event()
peak = [process.memory_info().rss]


def monitor():
    while not stop.wait(0.01):
        peak[0] = max(peak[0], process.memory_info().rss)


thread = threading.Thread(target=monitor, daemon=True)
thread.start()
start = perf_counter()
physical = pl.read_parquet(args.folder / "physical.parquet")
members = pl.read_parquet(args.folder / "members.parquet")
contexts = pl.read_parquet(args.folder / "contexts.parquet")
scores = pl.read_parquet(args.folder / "step-scores.parquet")
settings = json.loads((args.folder / "settings.json").read_text(encoding="utf-8"))
if scores.select(score_keys).n_unique() != scores.height:
    raise ValueError("The captured scoring lookup contains duplicate keys.")
load_seconds = perf_counter() - start

start = perf_counter()
expanded = members.join(physical, on=PLAN_COLUMNS).join(
    contexts.select(unit + ["utility_profile_id"]), on=unit)
expanded_count = expanded.height
expansion_seconds = perf_counter() - start
start = perf_counter()
scored = expanded.join(scores.select(score_keys + ["cost", "time", "is_feasible", "min_activity_time",
                                                   "activity_utility_scale"]), on=score_keys, how="left")
if scored["cost"].null_count() or scored.height != expanded.height:
    raise ValueError("Expanded candidates do not match the captured scoring lookup.")
del expanded
attach_seconds = perf_counter() - start
start = perf_counter()
timed = (PlanScheduleUpdater().update_plan_timings(scored.lazy(), **settings)
         .with_columns(*[((pl.col(name).cast(pl.Float64) * 3600).round(0) / 3600)
                         .cast(pl.Float32).alias(name) for name in
                         ("departure_time", "arrival_time", "next_departure_time", "duration_per_pers")])
         .with_columns(step_feasible=(pl.col("is_feasible").fill_null(False)
                       & ((pl.col("next_departure_time") >= pl.col("arrival_time"))
                          | (pl.col("seq_step_index") == pl.col("seq_step_index").max().over(PLAN_KEY_COLS)))))
         .collect(engine="streaming"))
del scored
timing_seconds = perf_counter() - start
start = perf_counter()
plan_totals = (timed.with_columns(step_utility=pl.col("activity_utility_scale")
                                 * (pl.col("duration_per_pers") / pl.col("min_activity_time")).log().clip(0.0)
                                 - pl.col("cost"))
               .group_by(PLAN_KEY_COLS).agg(
                   pl.col("step_utility").cast(pl.Float64).sort().implode().list.sum().alias("step_utility"),
                   pl.col("duration_per_pers").cast(pl.Float64).sort().implode().list.sum().alias("duration"),
                   pl.col("time").fill_null(0.0).cast(pl.Float64).sort().implode().list.sum().alias("travel"),
                   pl.col("step_feasible").all().alias("result_feasible")))
del timed
home = pl.read_parquet(args.folder / "home-durations.parquet")
stay = (pl.read_parquet(args.folder / "utilities.parquet").filter(pl.col("mode_seq_id") == 0)
        .select(unit + [pl.col("utility").alias("stay_utility")]))
result = (plan_totals.join(contexts.select(unit + ["country", "csp"]), on=unit)
          .join(home, on=["country", "csp"]).join(stay, on=unit)
          .with_columns(home_hours=(24 - pl.col("duration") - pl.col("travel")).clip(0.0))
          .with_columns(result_utility=pl.col("step_utility") + pl.col("stay_utility") / args.constant
                        * (pl.col("home_hours") / (pl.col("mean_home_night_per_pers") * np.exp(-args.constant)))
                        .log().clip(0.0)))
plan_seconds = perf_counter() - start
rss_before_validation = max(peak[0], process.memory_info().rss)

mapping = pl.read_parquet(args.folder / "modeled-mapping.parquet").rename(
    {"time_seq_id": "modeled_time_seq_id", "previous_time_seq_id": "time_seq_id"})
expected = (pl.read_parquet(args.folder / "utilities.parquet").filter(pl.col("mode_seq_id") != 0)
            .rename({"time_seq_id": "modeled_time_seq_id", "utility": "expected_utility",
                     "is_feasible": "expected_feasible"}))
comparison = result.join(mapping, on=PLAN_KEY_COLS).join(
    expected, on=[name for name in PLAN_KEY_COLS if name != "time_seq_id"] + ["modeled_time_seq_id"])
if comparison.height != members.height or comparison.filter(
    pl.col("result_feasible") != pl.col("expected_feasible")
).height:
    raise ValueError("Expanded scoring does not match all captured plans and feasibility flags.")
max_error = (comparison.filter(pl.col("expected_feasible"))
             .select((pl.col("result_utility") - pl.col("expected_utility")).abs().max()).item())
if max_error > 1e-8:
    raise ValueError(f"Expanded plan utility differs by {max_error}.")

reference = pl.read_parquet(args.folder / "expanded-scored.parquet")
check_keys = PLAN_KEY_COLS + ["seq_step_index"]
check_columns = ["cost", "time", "is_feasible", "min_activity_time", "activity_utility_scale"]
validation_scored = (members.join(physical, on=PLAN_COLUMNS)
                     .join(contexts.select(unit + ["utility_profile_id"]), on=unit)
                     .join(scores.select(score_keys + check_columns), on=score_keys))
checked = validation_scored.select(check_keys + check_columns).join(
    reference.select(check_keys + check_columns), on=check_keys, suffix="_captured")
if checked.height != reference.height or checked.filter(pl.any_horizontal([
    ~pl.col(name).eq_missing(pl.col(name + "_captured")) for name in check_columns
])).height:
    raise ValueError("Expanded scoring inputs do not match the captured evaluator.")
stop.set()
thread.join()
print("EXPANDED_BASELINE_RESULT", json.dumps({"physical_steps": physical.height, "context_plans": members.height,
      "expanded_steps": expanded_count, "matched_context_plans": comparison.height,
      "matched_scored_steps": checked.height, "max_feasible_utility_error": max_error,
      "load_seconds": load_seconds, "candidate_expansion_seconds": expansion_seconds,
      "score_attachment_seconds": attach_seconds, "timing_seconds": timing_seconds,
      "plan_scoring_seconds": plan_seconds, "peak_process_rss_before_validation": rss_before_validation,
      "peak_process_rss_bytes_including_validation": peak[0]}), flush=True)
