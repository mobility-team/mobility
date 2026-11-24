"""Compare context-to-utility joins on a captured large simulation batch."""
import argparse
from pathlib import Path
from time import perf_counter

import polars as pl

from mobility.trips.group_day_trips.plans.plan_ids import PLAN_KEY_COLS


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("folder", type=Path)
parser.add_argument("strategy", choices=["streaming", "batched"])
parser.add_argument("--output", type=Path)
args = parser.parse_args()
contexts = pl.read_parquet(args.folder / "contexts.parquet")
scores = pl.read_parquet(args.folder / "utilities.parquet")
profiles = ["country", "csp", "utility_profile_id"]
values = ["utility", "is_feasible", "missing_cost_row_count", "invalid_cost_row_count"]
start = perf_counter()
if args.strategy == "streaming":
    result = (contexts.lazy().select(PLAN_KEY_COLS + profiles + ["_timing_plan"])
              .join(scores.lazy(), on=profiles + ["_timing_plan"], how="inner")
              .select(PLAN_KEY_COLS + values)
              .unique(subset=PLAN_KEY_COLS, keep="first", maintain_order=True)
              .collect(engine="streaming"))
else:
    parts = []
    for profile in scores.select(profiles).unique(maintain_order=True).iter_rows():
        profile_filter = pl.all_horizontal([
            pl.col(name).eq_missing(pl.lit(value)) for name, value in zip(profiles, profile)
        ])
        candidates = contexts.filter(profile_filter).select(PLAN_KEY_COLS + ["_timing_plan"])
        bins = (candidates.group_by("demand_group_id").len().sort("demand_group_id")
                .with_columns(_bucket=(pl.col("len").cast(pl.UInt64).cum_sum() - pl.col("len")) // 100_000)
                .select("demand_group_id", "_bucket"))
        candidates = candidates.join(bins, on="demand_group_id")
        profile_scores = scores.filter(profile_filter).select(["_timing_plan"] + values)
        for batch in candidates.partition_by("_bucket", maintain_order=True):
            selected = profile_scores.join(batch.select("_timing_plan").unique(), on="_timing_plan", how="semi")
            parts.append(batch.join(selected, on="_timing_plan", how="inner")
                         .select(PLAN_KEY_COLS + values)
                         .unique(subset=PLAN_KEY_COLS, keep="first", maintain_order=True))
    result = pl.concat(parts)
print(f"CONTEXT_JOIN strategy={args.strategy} rows={result.height} seconds={perf_counter() - start:.3f} "
      f"checksum={result.hash_rows(seed=42).sum()} bytes={result.estimated_size()}", flush=True)
if args.output:
    result.write_parquet(args.output)
