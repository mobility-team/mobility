"""Benchmark reusing duration utility terms on a captured modeled step table.

Usage: pixi run python tests/manual/benchmark_activity_terms.py CAPTURE_FOLDER
This isolates activity utility aggregation. It does not measure full scoring,
night-at-home utility, choice probabilities, histories or simulation runtime.
"""
import argparse
import json
from pathlib import Path
from time import perf_counter

import polars as pl
from polars.testing import assert_frame_equal

from mobility.trips.group_day_trips.plans.plan_ids import PLAN_KEY_COLS


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("folder", type=Path)
args = parser.parse_args()
source = pl.read_parquet(args.folder / "indexed-2.parquet")
steps = source.unique(subset=PLAN_KEY_COLS + ["seq_step_index"], keep="first")
term_keys = ["time_seq_id", "seq_step_index", "country", "csp"]
term_values = ["duration_per_pers", "min_activity_time"]
start = perf_counter()
inputs = steps.select(term_keys + term_values).unique()
if inputs.select(term_keys).n_unique() != inputs.height:
    raise ValueError("Identical modeled schedules have differing activity-duration inputs.")
terms = inputs.with_columns(
    duration_term=(pl.col("duration_per_pers") / pl.col("min_activity_time")).log().clip(0.0)
).select(term_keys + ["duration_term"])
precompute_seconds = perf_counter() - start

for repetition in range(3):
    start = perf_counter()
    full = (steps.lazy().with_columns(
        utility=pl.col("activity_utility_scale")
        * (pl.col("duration_per_pers") / pl.col("min_activity_time")).log().clip(0.0) - pl.col("cost")
    ).group_by(PLAN_KEY_COLS).agg(pl.col("utility").sort().implode().list.sum()).collect(engine="streaming"))
    full_seconds = perf_counter() - start
    start = perf_counter()
    cached = (steps.lazy().join(terms.lazy(), on=term_keys).with_columns(
        utility=pl.col("activity_utility_scale") * pl.col("duration_term") - pl.col("cost")
    ).group_by(PLAN_KEY_COLS).agg(pl.col("utility").sort().implode().list.sum()).collect(engine="streaming"))
    cached_seconds = perf_counter() - start
    assert_frame_equal(full.sort(PLAN_KEY_COLS), cached.sort(PLAN_KEY_COLS), check_exact=True)
    print("ACTIVITY_TERM_REPETITION", json.dumps({"repetition": repetition + 1,
          "full_seconds": full_seconds, "cached_seconds": cached_seconds}), flush=True)

print("ACTIVITY_TERM_RESULT", json.dumps({"rows": steps.height, "terms": terms.height,
      "term_bytes": terms.estimated_size(), "precompute_seconds": precompute_seconds}), flush=True)
