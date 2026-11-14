"""Compare captured simulation effort runs; the reference is not ground truth.

Usage: pixi run python tests/manual/compare_effort_metrics.py LOW_FOLDER REFERENCE_FOLDER
Folders are the effort-metrics directories printed by the Geneva probe.
"""
import argparse
import json
from pathlib import Path

import polars as pl


def compare_flows(low, reference, keys):
    low = low.group_by(keys).agg(pl.col("trips").sum().alias("low"))
    reference = reference.group_by(keys).agg(pl.col("trips").sum().alias("reference"))
    compared = low.join(reference, on=keys, how="full", coalesce=True).with_columns(
        pl.col("low").fill_null(0.0), pl.col("reference").fill_null(0.0),
    )
    reference_total = compared["reference"].sum()
    difference = (compared["low"] - compared["reference"]).abs().sum()
    return {"reference_trips": reference_total, "low_trips": compared["low"].sum(),
            "relative_absolute_difference": difference / reference_total if reference_total else None}


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("low", type=Path)
parser.add_argument("reference", type=Path)
args = parser.parse_args()
for low_file in sorted(args.low.glob("step-*-run-*-flows.parquet")):
    reference_file = args.reference / low_file.name
    if not reference_file.exists():
        raise FileNotFoundError(f"Reference flow table is missing: {reference_file}")
    low = pl.read_parquet(low_file).with_columns(pl.col("mode", "activity").cast(pl.String))
    reference = pl.read_parquet(reference_file).with_columns(pl.col("mode", "activity").cast(pl.String))
    keys = {
        "mode": ["mode"], "activity": ["activity"],
        "od_mode": ["from", "to", "mode"],
        "od_mode_activity": ["from", "to", "mode", "activity"],
    }
    name = low_file.name.removesuffix("-flows.parquet") + ".json"
    result = {"step": json.loads((args.low / name).read_text())["step"],
              "low": json.loads((args.low / name).read_text()),
              "reference": json.loads((args.reference / name).read_text()),
              "flows": {label: compare_flows(low, reference, columns) for label, columns in keys.items()}}
    for dimension in ("mode", "activity"):
        categories = sorted(set(low[dimension].to_list()) | set(reference[dimension].to_list()))
        result["by_" + dimension] = {
            category: compare_flows(
                low.filter(pl.col(dimension) == category),
                reference.filter(pl.col(dimension) == category), ["from", "to"],
            )
            for category in categories
        }
    print(json.dumps(result))
