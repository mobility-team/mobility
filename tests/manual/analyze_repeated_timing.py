"""Measure timing and scoring reuse in consecutive captured inner batches."""
import argparse
import json
from pathlib import Path

import polars as pl

from mobility.trips.group_day_trips.plans.plan_ids import PLAN_KEY_COLS


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("folder", type=Path)
args = parser.parse_args()
keys = PLAN_KEY_COLS + ["seq_step_index"]
physical = [column for column in keys if column not in ("demand_group_id", "demand_subgroup_id")]
times = ["departure_time", "arrival_time", "next_departure_time", "duration_per_pers"]
before = pl.read_parquet(args.folder / "input-1.parquet")
current = pl.read_parquet(args.folder / "input-2.parquet")
indexed = pl.read_parquet(args.folder / "indexed-2.parquet")
mapping = pl.read_parquet(args.folder / "mapping-2.parquet")

# Compare the actual modeled seconds with each candidate's incoming seconds.
incoming = current.select(keys + ["mode"] + times).rename({"time_seq_id": "previous_time_seq_id"})
input_keys = ["previous_time_seq_id" if name == "time_seq_id" else name for name in PLAN_KEY_COLS]
modeled = indexed.select(keys + times).unique()
rows = (incoming.join(mapping, on=input_keys)
        .join(modeled, on=keys, suffix="_modeled")
        .with_columns(*[((pl.col(name).cast(pl.Float64) * 3600).round(0) / 3600)
                        .cast(current.schema[name]).alias(name) for name in times])
        .with_columns(*[(~pl.col(name).eq_missing(pl.col(f"{name}_modeled"))).alias(f"{name}_changed")
                        for name in times]))
if rows.height != current.height:
    raise ValueError("Timing mapping did not preserve one row per input step.")
flags = [f"{name}_changed" for name in times]
print("TIMING_CHANGES_BY_MODE", json.dumps(rows.group_by("mode").agg(
    pl.len().alias("rows"), *[pl.col(flag).sum() for flag in flags],
    pl.any_horizontal(flags).sum().alias("any_changed"),
).sort("mode").to_dicts()))
print("TIMING_CHANGES_BY_PLAN", json.dumps(rows.group_by(input_keys).agg(
    pl.any_horizontal(flags).any().alias("any_changed")
).select(pl.len().alias("plans"), pl.col("any_changed").sum().alias("changed_plans")).to_dicts()))

# A timing result can be shared only if each physical step sees identical
# timing inputs; different cost profiles may legitimately make this untrue.
timing_inputs = ["activity", "from", "to", "mode", "time"] + times
physical_groups = current.group_by(physical).agg(pl.struct(timing_inputs).n_unique().alias("versions"))
print("PHYSICAL_TIMING_INPUTS", json.dumps({"context_steps": current.height,
    "physical_steps": physical_groups.height, "conflicting_steps": physical_groups.filter(pl.col("versions") > 1).height,
    "context_plans": current.select(PLAN_KEY_COLS).n_unique(),
    "physical_plans": current.select([name for name in PLAN_KEY_COLS
                                      if name not in ("demand_group_id", "demand_subgroup_id")]).n_unique()}))

# Scoring inputs should remain fixed between inner batches. Compare them here
# for evidence, not as a proposed production-time full-row check.
values = ["activity", "from", "to", "mode", "cost", "time", "distance", "is_feasible",
          "min_activity_time", "activity_utility_scale"] + times
old = before.select(keys + values).unique()
new = current.select(keys + values).unique()
joined = new.join(old, on=keys, how="inner", suffix="_previous")
differences = [~pl.col(name).eq_missing(pl.col(f"{name}_previous")) for name in values]
print("REPEATED_SCORING_INPUTS", json.dumps({"previous_rows": old.height, "current_rows": new.height,
    "repeated_rows": joined.height, "changed_repeated_rows": joined.filter(pl.any_horizontal(differences)).height}))
