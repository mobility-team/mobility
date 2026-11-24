"""Keep candidate steps separate from the demand units that can use them."""

from dataclasses import dataclass, replace

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
class SectionedPhysicalSteps:
    """Store identical ordered physical sections once across candidate plans.

    These sections contain the exact modeled steps, including their times.
    They are a lossless storage representation, not a cache of scores across
    changed costs. Section IDs are local to this table.
    """

    sections: pl.DataFrame
    references: pl.DataFrame
    sequence_dtype: pl.DataType

    @classmethod
    def from_steps(cls, steps: pl.DataFrame, rigid_activities: set[str]) -> "SectionedPhysicalSteps":
        """Cut after rigid activities and index equal ordered step sequences."""
        step_values = [name for name in STEP_COLUMNS if name not in PLAN_COLUMNS + ["seq_step_index"]]
        rows = (
            steps.sort(PLAN_COLUMNS + ["seq_step_index"])
            .with_columns(_rigid=pl.col("activity").cast(pl.String).is_in(sorted(rigid_activities)))
            .with_columns(_boundary=pl.col("_rigid").shift(1).over(PLAN_COLUMNS).fill_null(False))
            .with_columns(_section=pl.col("_boundary").cast(pl.UInt32).cum_sum().over(PLAN_COLUMNS))
            .with_columns(_first_step=pl.col("seq_step_index").min().over(PLAN_COLUMNS + ["_section"]))
            .with_columns(_offset=(pl.col("seq_step_index") - pl.col("_first_step")).cast(steps.schema["seq_step_index"]))
        )
        grouped = rows.group_by(PLAN_COLUMNS + ["_section"], maintain_order=True).agg(
            pl.col("_first_step").first(), pl.struct(["_offset"] + step_values).alias("_steps"),
        )
        sections = grouped.select("_steps").unique(maintain_order=True).with_row_index("_section_id")
        references = grouped.join(sections, on="_steps", maintain_order="left").select(
            PLAN_COLUMNS + ["_section", "_first_step", "_section_id"]
        )
        return cls(sections, references, steps.schema["seq_step_index"])

    def lazy(self, plans: pl.LazyFrame | None = None) -> pl.LazyFrame:
        """Reconstruct only the requested physical plans when possible."""
        references = self.references.lazy()
        if plans is not None:
            references = references.join(plans.select(PLAN_COLUMNS).unique(), on=PLAN_COLUMNS, how="semi")
        return (
            references.join(self.sections.lazy(), on="_section_id", how="inner")
            .explode("_steps").unnest("_steps")
            .with_columns(seq_step_index=(pl.col("_first_step") + pl.col("_offset")).cast(self.sequence_dtype))
            .select(STEP_COLUMNS)
        )

    def estimated_size(self) -> int:
        return self.sections.estimated_size() + self.references.estimated_size()


@dataclass(frozen=True)
class ScoringCandidateSteps:
    """Shared physical steps and lazy demand-unit memberships in an inner batch."""

    steps: pl.DataFrame
    membership: pl.LazyFrame


@dataclass(frozen=True)
class CompactCandidateSteps:
    """A plan-step table and one membership row per demand unit and plan."""

    steps: pl.DataFrame | SectionedPhysicalSteps
    membership: pl.DataFrame
    columns: tuple[str, ...]
    unit_map: pl.DataFrame | None = None

    def with_sections(self, rigid_activities: set[str] | None) -> "CompactCandidateSteps":
        """Store physical steps as sections when that reduces retained size."""
        if rigid_activities is None or isinstance(self.steps, SectionedPhysicalSteps) or self.steps.height < 50_000:
            return self
        sectioned = SectionedPhysicalSteps.from_steps(self.steps, rigid_activities)
        return replace(self, steps=sectioned) if sectioned.estimated_size() < self.steps.estimated_size() * 0.9 else self

    def physical_steps(self, plans: pl.LazyFrame | None = None) -> pl.LazyFrame:
        """Expose all physical steps or only those needed for selected plans."""
        if isinstance(self.steps, SectionedPhysicalSteps):
            return self.steps.lazy(plans)
        result = self.steps.lazy()
        return result if plans is None else result.join(plans.select(PLAN_COLUMNS).unique(), on=PLAN_COLUMNS, how="semi")

    def memberships(self) -> pl.LazyFrame:
        """Expand shared context history only when demand-unit rows are needed."""
        if self.unit_map is None:
            return self.membership.lazy()
        return (
            self.membership.lazy().drop("demand_subgroup_id")
            .rename({"demand_group_id": "_score_context_id"})
            .join(self.unit_map.lazy(), on="_score_context_id")
            .select(MEMBERSHIP_COLUMNS)
        )

    def memberships_for_plans(self, plans: pl.LazyFrame) -> pl.LazyFrame:
        """Find occupied memberships without expanding the entire history."""
        plans = plans.select(PLAN_KEY_COLS)
        if self.unit_map is None:
            return self.membership.lazy().join(plans, on=PLAN_KEY_COLS, how="semi")
        # Start with the occupied unit/plan pairs. Each pair identifies its
        # stored context membership without joining all plans to all units in
        # that context first.
        return (
            plans.unique().join(self.unit_map.lazy(), on=DEMAND_UNIT_COLS, how="inner")
            .join(
                self.membership.lazy().drop("demand_subgroup_id")
                .rename({"demand_group_id": "_score_context_id"}),
                on=["_score_context_id"] + PLAN_COLUMNS, how="inner",
            )
            .select(MEMBERSHIP_COLUMNS)
        )

    def lazy(self) -> pl.LazyFrame:
        """Reconstruct candidate rows only when an operation needs them."""
        return (
            self.memberships()
            .join(self.physical_steps(), on=PLAN_COLUMNS, how="inner")
            .select(self.columns)
            .sort(CandidatePlanStepsAsset.DEDUPE_COLUMNS)
        )

    def estimated_size(self) -> int:
        return (self.steps.estimated_size() + self.membership.estimated_size()
                + (self.unit_map.estimated_size() if self.unit_map is not None else 0))

    @classmethod
    def from_frame(
        cls, frame: pl.DataFrame, *, prefer_compact: bool = False,
        rigid_activities: set[str] | None = None,
    ) -> "CompactCandidateSteps | pl.DataFrame":
        """Compact losslessly, optionally anticipating later membership expansion."""
        if frame.is_empty():
            return frame
        if set(frame.columns) != set(STEP_COLUMNS + MEMBERSHIP_COLUMNS):
            return frame
        steps = frame.lazy().select(STEP_COLUMNS).unique().collect(engine="streaming")
        if steps.group_by(STEP_KEY_COLUMNS).len().filter(pl.col("len") > 1).height:
            return frame
        membership = frame.lazy().select(MEMBERSHIP_COLUMNS).unique().collect(engine="streaming")
        if membership.group_by(PLAN_KEY_COLS).len().filter(pl.col("len") > 1).height:
            return frame
        # Duplicate or incomplete plan rows cannot be represented by the join.
        unique_rows = (
            frame.lazy().select(CandidatePlanStepsAsset.DEDUPE_COLUMNS)
            .unique().select(pl.len()).collect(engine="streaming").item()
        )
        if unique_rows != frame.height:
            return frame
        actual_counts = frame.lazy().group_by(PLAN_KEY_COLS).len(name="actual").collect(engine="streaming")
        step_counts = steps.group_by(PLAN_COLUMNS).len(name="expected")
        if actual_counts.join(step_counts, on=PLAN_COLUMNS).filter(pl.col("actual") != pl.col("expected")).height:
            return frame
        compact = cls(steps=steps, membership=membership, columns=tuple(frame.columns))
        compact = compact.with_sections(rigid_activities)
        return compact if prefer_compact or compact.estimated_size() < frame.estimated_size() else frame
