"""Measure temporary plan indexing and attachments on captured candidate scores."""
import argparse
from pathlib import Path
from time import perf_counter

import polars as pl
import psutil

from mobility.trips.group_day_trips.plans.plan_ids import PLAN_KEY_COLS


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("folder", type=Path)
parser.add_argument("--direct", action="store_true")
parser.add_argument("--streaming", action="store_true")
parser.add_argument("--aligned", action="store_true")
args = parser.parse_args()
scores = pl.read_parquet(args.folder / "modeled-utilities.parquet")
contexts = pl.read_parquet(args.folder / "contexts.parquet")
process = psutil.Process()


def report(stage, start, table):
    print(f"PLAN_INDEX stage={stage} rows={table.height} seconds={perf_counter()-start:.3f} "
          f"rss_gib={process.memory_info().rss / 1024**3:.2f}", flush=True)


start = perf_counter()
home = scores.select("demand_group_id", "demand_subgroup_id").unique().with_columns(
    **{name: pl.lit(0, dtype=pl.UInt32) for name in PLAN_KEY_COLS[2:]}
).select(PLAN_KEY_COLS)
if args.direct:
    mobile = scores.sort(PLAN_KEY_COLS).with_row_index("plan_id")
    home = home.sort(PLAN_KEY_COLS).with_row_index("plan_id", offset=mobile.height)
    index = pl.concat([mobile.select(PLAN_KEY_COLS + ["plan_id"]), home.select(PLAN_KEY_COLS + ["plan_id"])])
else:
    index = pl.concat([scores.select(PLAN_KEY_COLS).sort(PLAN_KEY_COLS), home.sort(PLAN_KEY_COLS)]) \
        .with_row_index("plan_id").select(PLAN_KEY_COLS + ["plan_id"])
report("index", start, index)
start = perf_counter()
if not args.direct:
    mobile = scores.join(index, on=PLAN_KEY_COLS)
report("utilities", start, mobile)
start = perf_counter()
if args.aligned:
    classes = (contexts.unique(subset=PLAN_KEY_COLS, keep="first").sort(PLAN_KEY_COLS)
               .with_row_index("plan_id").select(contexts.columns + ["plan_id"]))
    if not classes.select(PLAN_KEY_COLS).equals(mobile.select(PLAN_KEY_COLS)):
        raise ValueError("The context keys differ from the scored mobile-plan keys.")
elif args.streaming:
    classes = (contexts.lazy().unique(subset=PLAN_KEY_COLS, keep="first")
               .join(index.lazy(), on=PLAN_KEY_COLS).collect(engine="streaming"))
else:
    classes = contexts.unique(subset=PLAN_KEY_COLS, keep="first").join(index, on=PLAN_KEY_COLS)
report("context plans", start, classes)
print(f"PLAN_INDEX checksum={classes.hash_rows(seed=42).sum()}", flush=True)
