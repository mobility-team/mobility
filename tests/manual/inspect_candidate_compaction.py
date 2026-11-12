"""Find why an actual Grand Geneve candidate history cannot be compacted.

This runs the cache-only two-step comparison and stops on the first large
history that cannot be represented without loss. Inputs must already exist.
"""

import os
import runpy
from pathlib import Path

import polars as pl

from mobility.trips.group_day_trips.plans.candidate_plan_steps import CandidatePlanStepsAsset
from mobility.trips.group_day_trips.plans.compact_candidate_steps import (
    CompactCandidateSteps, MEMBERSHIP_COLUMNS, PLAN_COLUMNS, STEP_COLUMNS, STEP_KEY_COLUMNS,
)
from mobility.trips.group_day_trips.plans.plan_ids import PLAN_KEY_COLS
from mobility.trips.group_day_trips.plans.plan_updater import PlanUpdater


class CompactionInvestigated(Exception):
    """Stop after recording the first large uncompacted history."""


original = CompactCandidateSteps.from_frame


def inspect_compaction(cls, frame, **kwargs):
    result = original(frame, **kwargs)
    if isinstance(result, CompactCandidateSteps) and frame.height >= 1_000_000:
        print("COMPACTED_HISTORY", frame.height, "BYTES", result.estimated_size(), flush=True)
    output = os.environ.get("MOBILITY_INSPECT_HISTORY_OUTPUT")
    if output and frame.height >= 1_000_000 and frame["iteration"].max() >= 2:
        if isinstance(result, CompactCandidateSteps):
            result.memberships().collect(engine="streaming").write_parquet(output)
            print("HISTORY_SAVED", output, flush=True)
            raise CompactionInvestigated()
    if os.environ.get("MOBILITY_INSPECT_REFERENCE_HISTORY") == "1" and frame.height >= 1_000_000:
        if frame["iteration"].max() >= 2:
            print("REFERENCE_COMPACT", isinstance(result, CompactCandidateSteps), flush=True)
            history = result.memberships().collect(engine="streaming") if isinstance(result, CompactCandidateSteps) else frame.select(MEMBERSHIP_COLUMNS).unique()
            print("REFERENCE_SAMPLES", history.filter(
                pl.col("demand_group_id").is_in([13651, 17929, 22204, 23447, 18834])
                & pl.col("activity_seq_id").is_in([491, 1197, 3345, 1021, 615])
                & pl.col("time_seq_id").is_in([267413, 311366, 224604, 241075, 328998])
            ).sort(PLAN_KEY_COLS).to_dicts(), flush=True)
            raise CompactionInvestigated()
    if isinstance(result, CompactCandidateSteps) or frame.height < 1_000_000:
        return result
    print("UNCOMPACTED_ROWS", frame.height, "COLUMNS", frame.columns, flush=True)
    if set(frame.columns) == set(STEP_COLUMNS + MEMBERSHIP_COLUMNS):
        steps = frame.lazy().select(STEP_COLUMNS).unique().collect(engine="streaming")
        conflicts = steps.group_by(STEP_KEY_COLUMNS).len().filter(pl.col("len") > 1)
        members = frame.lazy().select(MEMBERSHIP_COLUMNS).unique().collect(engine="streaming")
        member_conflicts = members.group_by(PLAN_KEY_COLS).len().filter(pl.col("len") > 1)
        print("STEP_CONFLICTS", conflicts.height, "MEMBERSHIP_CONFLICTS", member_conflicts.height, flush=True)
        if conflicts.height:
            print("STEP_EXAMPLES", steps.join(conflicts.head(5), on=STEP_KEY_COLUMNS).sort(STEP_KEY_COLUMNS), flush=True)
        if member_conflicts.height:
            print("MEMBERSHIP_EXAMPLES", members.join(member_conflicts.head(5), on=PLAN_KEY_COLS), flush=True)
        unique_count = frame.lazy().select(CandidatePlanStepsAsset.DEDUPE_COLUMNS).unique().select(
            pl.len()
        ).collect(engine="streaming").item()
        actual = frame.lazy().group_by(PLAN_KEY_COLS).len(name="actual").collect(engine="streaming")
        expected = steps.group_by(PLAN_COLUMNS).len(name="expected")
        incomplete = actual.join(expected, on=PLAN_COLUMNS).filter(pl.col("actual") != pl.col("expected"))
        print("UNIQUE_KEYS", unique_count, "INCOMPLETE_PLANS", incomplete.height, flush=True)
        if incomplete.height:
            print("INCOMPLETE_EXAMPLES", incomplete.head(5), flush=True)
    raise CompactionInvestigated()


CompactCandidateSteps.from_frame = classmethod(inspect_compaction)
if os.environ.get("MOBILITY_INSPECT_REFERENCE_HISTORY") == "1":
    evaluate = PlanUpdater.evaluate_candidate_pool

    def evaluate_without_reuse(updater, *args, **kwargs):
        return evaluate(updater, *args, **{
            **kwargs, "inner_plan_cache": None, "compact_plan_id_index": None, "scoring_lookup": None,
        })

    PlanUpdater.evaluate_candidate_pool = evaluate_without_reuse
os.environ.setdefault("MOBILITY_COMPARE_MEMORY_LIMIT_GIB", "none")
try:
    runpy.run_path(str(Path(__file__).with_name("compare_grand_geneve_iterations.py")), run_name="__main__")
except CompactionInvestigated:
    print("COMPACTION_INVESTIGATION_COMPLETE", flush=True)
