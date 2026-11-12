"""Capture one real candidate-compaction input for opt-in experiments.

Inputs for the Grand Geneve probe must already be cached. The capture stops
before scoring the selected batch and does not write simulation state.
"""

import os
import runpy
import tempfile
from pathlib import Path
from time import perf_counter

import polars as pl
from polars.testing import assert_frame_equal

from mobility.trips.group_day_trips.core.performance_trace import _ACTIVE_TRACE
from mobility.trips.group_day_trips.plans.plan_ids import PLAN_KEY_COLS
from mobility.trips.group_day_trips.plans.scoring_contexts import ScoringContexts


class CaptureComplete(Exception):
    """Stop after writing one candidate table and its context mapping."""


step = int(os.environ.get("MOBILITY_COMPACTION_CAPTURE_STEP", "2"))
batch = int(os.environ.get("MOBILITY_COMPACTION_CAPTURE_BATCH", "2"))
directory = Path(tempfile.mkdtemp(prefix="mobility-candidate-compaction-"))
print("COMPACTION_CAPTURE_FILES", directory, flush=True)
original = ScoringContexts.collapse_step_rows
original_factored = ScoringContexts.collapse_factored_step_rows


def capture_factored(contexts, *args):
    active = _ACTIVE_TRACE.get()
    labels = active[2] if active else {}
    if (
        os.environ.get("MOBILITY_COMPACTION_MEASURE_FACTORED") == "1"
        and labels.get("step") == step and labels.get("search_batch") == batch
    ):
        started = perf_counter()
        collapsed = original_factored(contexts, *args)
        print("FACTORED_SECONDS", round(perf_counter() - started, 3),
              "ROWS", collapsed.height if collapsed is not None else None, flush=True)
        reference_folder = os.environ.get("MOBILITY_COMPACTION_REFERENCE_FOLDER")
        if collapsed is not None and reference_folder:
            reference = Path(reference_folder)
            assert_frame_equal(
                contexts.unit_map.sort(["demand_group_id", "demand_subgroup_id"]),
                pl.read_parquet(reference / "unit-map.parquet").sort(
                    ["demand_group_id", "demand_subgroup_id"]
                ),
            )
            reference_rows = original(contexts, pl.scan_parquet(reference / "candidate-steps.parquet"))
            keys = PLAN_KEY_COLS + ["seq_step_index"]
            assert_frame_equal(collapsed.sort(keys), reference_rows.sort(keys))
            print("REFERENCE_CONTEXT_ROWS_MATCH", collapsed.height, flush=True)
        raise CaptureComplete()
    return original_factored(contexts, *args)


def capture(contexts, steps):
    active = _ACTIVE_TRACE.get()
    labels = active[2] if active else {}
    if labels.get("step") == step and labels.get("search_batch") == batch:
        print("CAPTURING", step, batch, labels.get("scenario"), flush=True)
        (directory / "candidate-query.txt").write_text(steps.lazy().explain(), encoding="utf-8")
        if os.environ.get("MOBILITY_COMPACTION_MEASURE_BOUNDARY") == "1":
            started = perf_counter()
            rows = steps.lazy().collect(engine="streaming")
            attached = perf_counter()
            collapsed = original(contexts, rows)
            print("ATTACH_AND_MERGE_SECONDS", round(attached - started, 3), "ROWS", rows.height, flush=True)
            print("CONTEXT_GROUP_SECONDS", round(perf_counter() - attached, 3), "ROWS", collapsed.height, flush=True)
            reference_folder = os.environ.get("MOBILITY_COMPACTION_REFERENCE_FOLDER")
            if reference_folder:
                reference = Path(reference_folder)
                assert_frame_equal(
                    contexts.unit_map.sort(["demand_group_id", "demand_subgroup_id"]),
                    pl.read_parquet(reference / "unit-map.parquet").sort(
                        ["demand_group_id", "demand_subgroup_id"]
                    ),
                )
                reference_rows = original(contexts, pl.scan_parquet(reference / "candidate-steps.parquet"))
                keys = PLAN_KEY_COLS + ["seq_step_index"]
                assert_frame_equal(collapsed.sort(keys), reference_rows.sort(keys))
                print("REFERENCE_CONTEXT_ROWS_MATCH", collapsed.height, flush=True)
            raise CaptureComplete()
        contexts.unit_map.write_parquet(directory / "unit-map.parquet")
        steps.lazy().sink_parquet(directory / "candidate-steps.parquet", engine="streaming")
        raise CaptureComplete()
    return original(contexts, steps)


ScoringContexts.collapse_step_rows = capture
ScoringContexts.collapse_factored_step_rows = capture_factored
try:
    runpy.run_path(str(Path(__file__).with_name("inspect_grand_geneve_execution.py")), run_name="__main__")
except CaptureComplete:
    print("COMPACTION_CAPTURE_COMPLETE", flush=True)
