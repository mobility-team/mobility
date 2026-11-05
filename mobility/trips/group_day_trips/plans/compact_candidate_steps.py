"""Keep candidate steps separate from the demand units that can use them."""

from dataclasses import dataclass

import polars as pl

from .candidate_plan_steps import CandidatePlanStepsAsset
from .plan_ids import PLAN_KEY_COLS
from .demand_subgroups import DEMAND_UNIT_COLS


PLAN_COLUMNS = [column for column in PLAN_KEY_COLS if column not in DEMAND_UNIT_COLS]
STEP_KEY_COLUMNS = PLAN_COLUMNS + ["seq_step_index"]
STEP_COLUMNS = STEP_KEY_COLUMNS + [
    "activity", "from", "to", "mode", "duration_per_pers",
    "departure_time", "arrival_time", "next_departure_time",
]
MEMBERSHIP_COLUMNS = PLAN_KEY_COLS + [
    "country", "csp", "iteration", "first_seen_iteration",
    "last_seen_iteration", "last_active_iteration",
]


@dataclass(frozen=True)
class CompactCandidateSteps:
    """A plan-step table and one membership row per demand unit and plan."""

    steps: pl.DataFrame
    membership: pl.DataFrame
    columns: tuple[str, ...]

    def lazy(self) -> pl.LazyFrame:
        """Reconstruct candidate rows only when an operation needs them."""
        return (
            self.membership.lazy()
            .join(self.steps.lazy(), on=PLAN_COLUMNS, how="inner")
            .select(self.columns)
            .sort(CandidatePlanStepsAsset.DEDUPE_COLUMNS)
        )

    def estimated_size(self) -> int:
        return self.steps.estimated_size() + self.membership.estimated_size()

    @classmethod
    def from_frame(cls, frame: pl.DataFrame) -> "CompactCandidateSteps | pl.DataFrame":
        """Compact only when the input can be reconstructed without loss."""
        if frame.is_empty():
            return frame
        if set(frame.columns) != set(STEP_COLUMNS + MEMBERSHIP_COLUMNS):
            return frame
        steps = frame.select(STEP_COLUMNS).unique()
        if steps.group_by(STEP_KEY_COLUMNS).len().filter(pl.col("len") > 1).height:
            return frame
        membership = frame.select(MEMBERSHIP_COLUMNS).unique()
        if membership.group_by(PLAN_KEY_COLS).len().filter(pl.col("len") > 1).height:
            return frame
        # Duplicate or incomplete plan rows cannot be represented by the join.
        if frame.select(CandidatePlanStepsAsset.DEDUPE_COLUMNS).unique().height != frame.height:
            return frame
        actual_counts = frame.group_by(PLAN_KEY_COLS).len(name="actual")
        step_counts = steps.group_by(PLAN_COLUMNS).len(name="expected")
        if actual_counts.join(step_counts, on=PLAN_COLUMNS).filter(pl.col("actual") != pl.col("expected")).height:
            return frame
        compact = cls(steps=steps, membership=membership, columns=tuple(frame.columns))
        return compact if compact.estimated_size() < frame.estimated_size() else frame
