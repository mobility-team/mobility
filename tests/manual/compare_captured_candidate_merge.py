"""Compare merge approaches on captured real candidate frames.

Run each approach in a separate process, passing the capture folder and either
``eager`` or ``streaming``. The capture is produced by
``compare_grand_geneve_iterations.py`` with ``MOBILITY_COMPARE_CAPTURE_MERGE=1``.
Only the first scenario's three input frames are used, keeping the experiment
smaller than the original two-scenario merge.
"""

import argparse
import _thread
import hashlib
import threading
from pathlib import Path
from time import perf_counter

import polars as pl
import psutil

from mobility.trips.group_day_trips.core.multi_scenario import CANDIDATE_ID_COLS
from mobility.trips.group_day_trips.plans.candidate_plan_steps import CandidatePlanStepsAsset
from mobility.trips.group_day_trips.plans.shared_candidate_pool import SharedCandidatePoolAsset


parser = argparse.ArgumentParser()
parser.add_argument("capture", type=Path)
parser.add_argument("method", choices=["eager", "streaming"])
parser.add_argument("--scenarios", type=int, choices=[1, 2], default=1)
parser.add_argument("--memory-limit-gib", type=float, default=16.0)
args = parser.parse_args()
paths = [args.capture / f"step-2-merge-input-{index}.parquet" for index in range(3 * args.scenarios)]
process = psutil.Process()
peak = [process.memory_info().rss]
stop = threading.Event()


def sample_memory():
    while not stop.wait(0.1):
        peak[0] = max(peak[0], process.memory_info().rss)
        if peak[0] > args.memory_limit_gib * 2**30 or psutil.virtual_memory().available < 8 * 2**30:
            print("STOPPING: memory guard reached", flush=True)
            _thread.interrupt_main()
            return


thread = threading.Thread(target=sample_memory, daemon=True)
thread.start()
started = perf_counter()
try:
    if args.method == "eager":
        frames = [pl.read_parquet(path) for path in paths]
        pool = object.__new__(SharedCandidatePoolAsset)
        result = pool._merge_candidate_content(frames)
    else:
        group_columns = CANDIDATE_ID_COLS + [
            "activity_seq_id", "time_seq_id", "destination_sequence_key_1",
            "destination_sequence_key_2", "mode_sequence_key_1",
            "mode_sequence_key_2", "seq_step_index",
        ]
        first_columns = [
            column for column in CandidatePlanStepsAsset.STRUCTURAL_COLUMNS
            if column not in set(group_columns) | {"dest_seq_id", "mode_seq_id", "iteration"}
        ] + ["reference_time_seq_id"]
        result = (
            pl.concat([pl.scan_parquet(path) for path in paths], how="vertical_relaxed")
            .group_by(group_columns)
            .agg(
                [pl.col(column).first() for column in first_columns]
                + [pl.col("iteration").min(), pl.col("first_seen_iteration").min(),
                   pl.col("last_seen_iteration").max(), pl.col("last_active_iteration").max()]
            )
            .sort(group_columns)
            .collect(engine="streaming")
        )
    digest = hashlib.sha256(result.hash_rows(seed=0).to_numpy().tobytes()).hexdigest()
    print(f"method={args.method} rows={result.height} fingerprint={digest}")
finally:
    stop.set()
    thread.join()
    print(f"wall_seconds={perf_counter() - started:.2f} peak_gib={peak[0] / 2**30:.2f}")
