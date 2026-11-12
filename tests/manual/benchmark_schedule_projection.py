"""Compare schedule-catalogue joins using captured Geneva candidate rows.

The input rows are real; the existing schedule catalogue and extra scoring
columns are constructed. Run each method in a separate process:
    pixi run python tests/manual/benchmark_schedule_projection.py INPUT.parquet full
    pixi run python tests/manual/benchmark_schedule_projection.py INPUT.parquet narrow
"""

import argparse
import threading
from time import perf_counter

import polars as pl
import psutil


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("input")
parser.add_argument("method", choices=["full", "narrow"])
parser.add_argument("--rows", type=int, default=1_000_000)
args = parser.parse_args()

steps = pl.scan_parquet(args.input).head(args.rows).collect(engine="streaming").with_columns(
    reference_time_seq_id=pl.col("time_seq_id"),
    *[pl.lit(float(index)).alias(f"scored_value_{index}") for index in range(12)],
)
schedule_columns = [
    "activity_seq_id", "time_seq_id", "reference_time_seq_id", "seq_step_index", "activity",
    "departure_time", "arrival_time", "next_departure_time", "duration_per_pers",
]
existing_ids = steps.select("time_seq_id").unique().sort("time_seq_id").head(500)
anchors = steps.select("activity").unique().with_columns(is_anchor=pl.lit(True))

process = psutil.Process()
peak = [process.memory_info().rss]
stop = threading.Event()


def monitor():
    while not stop.wait(0.02):
        peak[0] = max(peak[0], process.memory_info().rss)


threading.Thread(target=monitor, daemon=True).start()
started = perf_counter()
try:
    selected = steps if args.method == "full" else steps.select(schedule_columns)
    result = (
        selected.join(existing_ids, on="time_seq_id", how="anti")
        .join(anchors, on="activity", how="left")
        .with_columns(step_count=pl.col("seq_step_index").max().over("time_seq_id"))
        .select(schedule_columns + ["is_anchor", "step_count"])
        .sort(["time_seq_id", "reference_time_seq_id", "seq_step_index"])
        .unique(subset=["time_seq_id", "seq_step_index"], keep="first")
    )
    print("ROWS", result.height, "HASHES", result.select(
        pl.struct(pl.all()).hash(seed=0).sum().alias("hash_0"),
        pl.struct(pl.all()).hash(seed=1).sum().alias("hash_1"),
    ).row(0), flush=True)
finally:
    stop.set()
    print("SECONDS", round(perf_counter() - started, 3),
          "PEAK_GIB", round(peak[0] / 1024**3, 3), flush=True)
