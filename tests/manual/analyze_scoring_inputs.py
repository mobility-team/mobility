"""Count repeated scoring inputs in a capture from capture_scoring_inputs.py.

Usage: pixi run python tests/manual/analyze_scoring_inputs.py CAPTURE_FOLDER
The counts compare input keys only: they do not establish that scenario costs,
parameters or modeled schedules are equal, or that cached scores are valid.
"""

import argparse
from pathlib import Path

import polars as pl


parser = argparse.ArgumentParser()
parser.add_argument("capture_folder", type=Path)
parser.add_argument("--step", type=int, default=4)
args = parser.parse_args()

keys = {
    "current scoring key": [
        "country", "activity_seq_id", "time_seq_id", "dest_seq_id", "mode_seq_id",
        "seq_step_index", "activity", "from", "to", "mode", "duration_per_pers",
        "departure_time", "arrival_time", "next_departure_time", "csp", "utility_profile_id",
    ],
    "step inputs without plan IDs": [
        "country", "csp", "activity", "from", "to", "mode", "duration_per_pers",
        "departure_time", "arrival_time", "next_departure_time", "utility_profile_id",
    ],
    "travel cost inputs": ["utility_profile_id", "from", "to", "mode"],
    "activity parameter inputs": ["country", "csp", "activity", "to"],
}


def read(batch, scenario):
    return pl.scan_parquet(args.capture_folder / f"step-{args.step}-batch-{batch}-{scenario}.parquet")


def count_distinct(frame, columns):
    return frame.select(pl.struct(columns).n_unique()).collect(engine="streaming").item()


for batch in (1, 2):
    rows = read(batch, "carbon_tax")
    print(f"Batch {batch}: {rows.select(pl.len()).collect(engine='streaming').item():,} rows")
    for name, columns in keys.items():
        print(f"  {name}: {count_distinct(rows, columns):,}")

for label, left, right in (
    ("Scenarios, batch 1", read(1, "carbon_tax"), read(1, "default")),
    ("Carbon tax, batches 1 and 2", read(1, "carbon_tax"), read(2, "carbon_tax")),
):
    print(label)
    for name, columns in keys.items():
        left_keys = left.select(columns)
        right_keys = right.select(columns)
        left_count = count_distinct(left_keys, columns)
        right_count = count_distinct(right_keys, columns)
        union_count = count_distinct(pl.concat([left_keys, right_keys]), columns)
        print(
            f"  {name}: {left_count + right_count - union_count:,} in both; "
            f"{union_count - right_count:,} only in first; {union_count - left_count:,} only in second"
        )
