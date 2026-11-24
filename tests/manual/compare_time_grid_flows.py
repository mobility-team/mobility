"""Compare OD/mode/activity flows from two time-grid diagnostic runs."""
import argparse
import json
from pathlib import Path

import polars as pl


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("baseline", type=Path)
parser.add_argument("grid", type=Path)
args = parser.parse_args()
keys = ["from", "to", "mode", "activity"]
for baseline_file in sorted(args.baseline.glob("flows-*.parquet")):
    other_file = args.grid / baseline_file.name
    if not other_file.exists():
        raise ValueError(f"Missing corresponding flow file: {other_file}")
    baseline = pl.read_parquet(baseline_file).rename({"persons": "baseline"})
    grid = pl.read_parquet(other_file).rename({"persons": "grid"})
    joined = (baseline.join(grid, on=keys, how="full", coalesce=True)
              .with_columns(pl.col("baseline").fill_null(0.0), pl.col("grid").fill_null(0.0))
              .with_columns(diff=(pl.col("grid") - pl.col("baseline")).abs()))
    ref = joined["baseline"].sum()
    print("FLOW_COMPARISON", json.dumps({"step": int(baseline_file.stem.split("-")[-1]),
          "baseline_trips": ref, "grid_trips": joined["grid"].sum(),
          "absolute_difference_over_baseline": joined["diff"].sum() / ref if ref else None,
          "baseline_cells": baseline.height, "grid_cells": grid.height,
          "by_mode": joined.group_by("mode").agg(
              pl.col("baseline").sum(), pl.col("grid").sum(), pl.col("diff").sum()
          ).sort("mode").to_dicts()}), flush=True)
