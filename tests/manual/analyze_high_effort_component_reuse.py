"""Count modeled component reuse in a deterministic high-effort sample.

Usage: pixi run python tests/manual/analyze_high_effort_component_reuse.py CAPTURE
The capture lacks complete scoring and reference timing inputs. These counts
are an optimistic upper bound on reuse, not a utility or speed benchmark.
"""
import argparse
import json
from pathlib import Path

import polars as pl


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("folder", type=Path)
parser.add_argument("--per-country", type=int, default=50)
args = parser.parse_args()
unit = ["demand_group_id", "demand_subgroup_id"]
utility = pl.read_parquet(args.folder / "candidate-utility.parquet").filter(
    pl.col("is_feasible") & (pl.col("mode_seq_id") != 0)
)
contexts = pl.read_parquet(args.folder / "contexts.parquet")
counts = utility.group_by(unit).len(name="candidates").filter(pl.col("candidates") >= 2)
choices = contexts.join(counts, on=unit).sort("country", "n_persons", "demand_group_id")
selected = []
for country in choices.partition_by("country", maintain_order=True):
    n = min(args.per_country, country.height)
    selected += [country["demand_group_id"][(2 * i + 1) * country.height // (2 * n)] for i in range(n)]
plans = utility.filter(pl.col("demand_group_id").is_in(selected)).select("plan_id", "demand_group_id")
steps = (pl.scan_parquet(args.folder / "candidate-steps.parquet")
         .join(plans.lazy(), on="plan_id", how="inner").collect(engine="streaming")
         .sort("plan_id", "seq_step_index"))
profile = contexts.select("demand_group_id", "utility_profile_id")
steps = steps.join(profile, on="demand_group_id", how="left")
steps = steps.with_columns(
    duration=(pl.col("next_departure_time").cast(pl.Float64) - pl.col("arrival_time")).clip(lower_bound=0.001)
)
base_keys = ["utility_profile_id", "activity", "from", "to", "mode"]
times = ["departure_time", "arrival_time", "next_departure_time"]
print("HIGH_CAPTURE", json.dumps({"contexts": len(selected), "plans": plans.height,
      "steps": steps.height,
      "profile_od_mode": steps.select("utility_profile_id", "from", "to", "mode").n_unique(),
      "modeled_step_keys": steps.select(base_keys + times).n_unique(),
      "activity_duration_keys": steps.select("activity", "to", "duration").n_unique()}), flush=True)

for minutes in (1, 5, 15):
    interval = minutes / 60.0
    rounded = steps.with_columns(*[
        (pl.col(name).cast(pl.Float64) / interval).round(0).cast(pl.Int32).alias(name + "_bin")
        for name in times
    ], (pl.col("duration") / interval).round(0).cast(pl.Int32).alias("duration_bin"))
    print("HIGH_BIN", json.dumps({"minutes": minutes,
          "modeled_step_keys": rounded.select(base_keys + [name + "_bin" for name in times]).n_unique(),
          "activity_duration_keys": rounded.select("activity", "to", "duration_bin").n_unique()}), flush=True)

# Every adjacent pair is retained with modeled details, destination, mode and
# profile. Other timing or scoring inputs absent from this capture can further
# reduce true reuse. The final step has no following trip.
section_columns = base_keys + times
paired = steps.with_columns(*[
    pl.col(name).shift(-1).over("plan_id").alias("following_" + name) for name in section_columns
])
print("HIGH_SECTIONS", json.dumps({"occurrences": paired.height,
      "unique_pairs": paired.select(section_columns + ["following_" + name for name in section_columns]).n_unique()}), flush=True)
