"""Compare captured per-mode scoring costs across successive outer steps."""
import argparse
import json
from pathlib import Path

import polars as pl


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("folder", type=Path)
args = parser.parse_args()
folder = args.folder
unit = ["demand_group_id", "demand_subgroup_id"]
keys = ["utility_profile_id", "from", "to", "mode"]
profiles = [pl.read_parquet(folder / f"profiles-{step}.parquet").sort(unit) for step in (1, 2, 3)]
for step in (2, 3):
    if not profiles[step - 2].equals(profiles[step - 1]):
        raise ValueError("Profile IDs changed; compare resolved profile identities before using these results.")
    before = pl.read_parquet(folder / f"costs-{step-1}.parquet").with_columns(pl.col("mode").cast(pl.String))
    after = pl.read_parquet(folder / f"costs-{step}.parquet").with_columns(pl.col("mode").cast(pl.String))
    if before.select(keys).n_unique() != before.height or after.select(keys).n_unique() != after.height:
        raise ValueError("Captured cost rows do not have unique profile/OD/mode keys.")
    shared = before.join(after, on=keys, how="inner", suffix="_new")
    result = shared.group_by("mode").agg(
        pl.len().alias("common"),
        *[(~pl.col(name).eq_missing(pl.col(f"{name}_new"))).sum().alias(f"{name}_changed")
          for name in ("cost", "time", "distance")],
        *[((pl.col(name) - pl.col(f"{name}_new")).abs() > 1e-9).fill_null(False).sum()
          .alias(f"{name}_changed_over_1e9") for name in ("cost", "time", "distance")],
    ).sort("mode")
    old_counts = before.group_by("mode").len(name="previous_keys")
    new_counts = after.group_by("mode").len(name="current_keys")
    result = result.join(old_counts, on="mode").join(new_counts, on="mode")
    print("COST_CHANGES", step, json.dumps(result.to_dicts()))

    a = pl.read_parquet(folder / f"opportunities-{step-1}.parquet")
    b = pl.read_parquet(folder / f"opportunities-{step}.parquet")
    joined = a.join(b, on=["to", "activity"], suffix="_new")
    print("OPPORTUNITY_CHANGES", step, json.dumps(joined.select(
        pl.len().alias("common"),
        *[((pl.col(name) - pl.col(f"{name}_new")).abs() > 1e-9).fill_null(False).sum().alias(name)
          for name in ("opportunity_occupation", "k_saturation_utility", "destination_shadow_price")],
    ).to_dicts()))
