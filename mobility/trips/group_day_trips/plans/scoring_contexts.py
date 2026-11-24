"""Group equivalent demand units before scoring and estimating inner demand."""

from dataclasses import dataclass
import logging
from typing import Any, Callable

import polars as pl

from mobility.trips.group_day_trips.core.performance_trace import trace_simulation_step

from .candidate_plan_steps import CandidatePlanStepsAsset
from .compact_candidate_steps import (
    CompactCandidateSteps, ScoringCandidateSteps, MEMBERSHIP_COLUMNS, PLAN_COLUMNS, STEP_COLUMNS,
    STEP_KEY_COLUMNS,
)
from .demand_subgroups import DEMAND_UNIT_COLS
from .plan_ids import PLAN_KEY_COLS
from .stable_key_index import StableKeyIndex


@dataclass
class ScoringMemoryBudget:
    """Limit retained lookup tables across the scenarios in one inner loop."""

    max_bytes: int
    used_bytes: int = 0

    def retain(self, previous: pl.DataFrame | None, table: pl.DataFrame) -> pl.DataFrame | None:
        """Replace a retained table if it fits within the shared budget."""
        previous_bytes = previous.estimated_size() if previous is not None else 0
        self.used_bytes -= previous_bytes
        size = table.estimated_size()
        if self.used_bytes + size > self.max_bytes:
            return None
        self.used_bytes += size
        return table


@dataclass
class ScoringLookup:
    """Keep step scores while their scenario inputs remain fixed."""

    budget: ScoringMemoryBudget
    scores: pl.DataFrame | None = None
    section_cache: Any | None = None

    def retain(self, scores: pl.DataFrame) -> None:
        """Retain a lookup only while all scenarios fit the shared budget."""
        self.scores = self.budget.retain(self.scores, scores)
        if self.scores is None and self.section_cache is not None:
            # The compact trip lookup is needed to avoid expanding every plan.
            # Section scores can be recalculated; prefer retaining the lookup.
            self.section_cache.clear()
            self.scores = self.budget.retain(None, scores)


@dataclass(frozen=True)
class ScoringContexts:
    """Compact population groups and the mapping back to original demand units."""

    demand_groups: pl.DataFrame
    unit_map: pl.DataFrame

    @classmethod
    @trace_simulation_step("build scoring contexts")
    def build(cls, demand_groups, transport_costs, population_segments, *, assignments=None):
        if assignments is None:
            assignments, _ = transport_costs.get_utility_profiles(
                demand_groups.sort(DEMAND_UNIT_COLS), population_segments,
            )
        units = demand_groups.join(assignments, on=DEMAND_UNIT_COLS).with_columns(
            activity_profile_id=pl.col("allowed_activities").list.sort().list.join("|"),
        )
        keys = ["home_zone_id", "country", "csp", "activity_profile_id", "utility_profile_id"]
        first_columns = [column for column in units.columns if column not in keys + DEMAND_UNIT_COLS + ["n_persons"]]
        contexts = units.group_by(keys).agg(
            pl.col("n_persons").sum(),
            *[pl.col(column).first() for column in first_columns],
        )
        contexts, _ = StableKeyIndex(keys, "_score_context_id", first_new_id=0).extend(contexts, None)
        unit_map = units.join(contexts.select(keys + ["_score_context_id"]), on=keys).select(
            DEMAND_UNIT_COLS + ["_score_context_id"]
        )
        contexts = contexts.with_columns(
            demand_group_id=pl.col("_score_context_id"),
            demand_subgroup_id=pl.lit(0, dtype=pl.UInt32),
        )
        return cls(contexts, unit_map)

    @trace_simulation_step("compact scoring candidates")
    def collapse_step_rows(self, steps: pl.DataFrame | pl.LazyFrame) -> pl.DataFrame:
        """Retain one complete step per plan and scoring context."""
        columns = steps.collect_schema().names() if isinstance(steps, pl.LazyFrame) else steps.columns
        grouped = self._replace_unit_keys(steps)
        keys = PLAN_KEY_COLS + ["seq_step_index"]
        dates = CandidatePlanStepsAsset.RETENTION_COLUMNS
        first_columns = [column for column in columns if column not in keys + dates + ["iteration"]]
        history = [
            pl.col(column).min() if column == "first_seen_iteration" else pl.col(column).max()
            for column in dates if column in columns
        ]
        # Equivalent modeled plans can originate in different batches. Keep
        # the earliest generation step, as in the shared candidate pool.
        return grouped.group_by(keys).agg(
            *[pl.col(column).first() for column in first_columns],
            *([pl.col("iteration").min()] if "iteration" in columns else []),
            *history,
        ).select(columns).collect(engine="streaming")

    @trace_simulation_step("compact factored scoring candidates")
    def collapse_factored_step_rows(
        self,
        shared: ScoringCandidateSteps,
        shared_active: pl.DataFrame | None,
        previous: CompactCandidateSteps | None,
        current_plans: pl.DataFrame,
        *, context_filter: Callable[[pl.DataFrame], pl.DataFrame] | None = None,
        return_parts: bool = False,
        expand: bool = True,
    ) -> pl.DataFrame | tuple[pl.DataFrame | None, pl.DataFrame, pl.DataFrame] | None:
        """Group plan membership before attaching shared physical steps.

        Active plans have the same precedence as the row-based merge. Return
        None if a plan has different physical steps or membership data that
        cannot be represented once per plan. With expand=False, return the
        physical steps and memberships without joining them into context rows.
        """
        memberships = [shared.membership]
        physical = [shared.steps]
        if shared_active is not None and shared_active.height:
            if previous is None:
                logging.getLogger(__name__).debug("Factored scoring unavailable: active rows without compact history")
                return None
            active_members = shared_active.select(MEMBERSHIP_COLUMNS).unique()
            active_steps = shared_active.select(STEP_COLUMNS).unique()
            active_counts = shared_active.group_by(PLAN_KEY_COLS).len(name="actual")
            if (
                active_members.select(PLAN_KEY_COLS).n_unique() != active_members.height
                or shared_active.select(CandidatePlanStepsAsset.DEDUPE_COLUMNS).n_unique() != shared_active.height
            ):
                logging.getLogger(__name__).debug("Factored scoring unavailable: duplicate active plan memberships or steps")
                return None
            memberships.append(active_members.lazy())
            physical.append(active_steps)
        if previous is not None:
            active_members = previous.memberships_for_plans(current_plans.lazy()).collect(engine="streaming")
            active_steps = previous.physical_steps(active_members.lazy()).collect(engine="streaming")
            memberships.append(active_members.lazy())
            physical.append(active_steps)
        steps = pl.concat(physical, how="vertical_relaxed").unique()
        if steps.select(STEP_KEY_COLUMNS).n_unique() != steps.height:
            logging.getLogger(__name__).debug("Factored scoring unavailable: conflicting physical steps")
            return None
        step_counts = steps.group_by(PLAN_COLUMNS).len(name="expected")
        for table in physical:
            counts = table.group_by(PLAN_COLUMNS).len(name="actual")
            if counts.join(step_counts, on=PLAN_COLUMNS).filter(pl.col("actual") != pl.col("expected")).height:
                logging.getLogger(__name__).debug("Factored scoring unavailable: incomplete physical steps")
                return None
        if shared_active is not None and shared_active.height:
            if active_counts.join(step_counts, on=PLAN_COLUMNS).filter(pl.col("actual") != pl.col("expected")).height:
                logging.getLogger(__name__).debug("Factored scoring unavailable: incomplete active plan steps")
                return None
        members = pl.concat(memberships, how="vertical_relaxed").unique(
            subset=PLAN_KEY_COLS, keep="last"
        )
        # Match the row-based history merge without expanding physical steps.
        grouped = self._replace_unit_keys(members).group_by(PLAN_KEY_COLS).agg(
            pl.col("country").first(), pl.col("csp").first(), pl.col("iteration").min(),
            pl.col("country").n_unique().alias("_country_count"),
            pl.col("csp").n_unique().alias("_csp_count"),
            pl.col("first_seen_iteration").min(), pl.col("last_seen_iteration").max(),
            pl.col("last_active_iteration").max(),
        ).collect(engine="streaming")
        if grouped.filter(
            (pl.col("_country_count") > 1) | (pl.col("_csp_count") > 1)
        ).height:
            logging.getLogger(__name__).debug("Factored scoring unavailable: differing context membership data")
            return None
        if context_filter is not None:
            grouped = context_filter(grouped)
        grouped = grouped.drop("_country_count", "_csp_count")
        if not expand:
            return None, steps, grouped
        expanded = (
            grouped.join(steps, on=PLAN_COLUMNS, how="inner")
            .select(CandidatePlanStepsAsset.STRUCTURAL_COLUMNS + CandidatePlanStepsAsset.RETENTION_COLUMNS)
        )
        return (expanded, steps, grouped) if return_parts else expanded

    @trace_simulation_step("compact current allocations")
    def collapse_allocations(self, plans: pl.DataFrame) -> pl.DataFrame:
        """Aggregate origin populations without treating inner demand as behavior."""
        return self._replace_unit_keys(plans).group_by(PLAN_KEY_COLS).agg(
            pl.col("n_persons").sort().implode().list.sum(), pl.col("utility").first(),
        ).collect(engine="streaming")

    @trace_simulation_step("compact stay-home plans")
    def collapse_stay_home(self, steps: pl.DataFrame) -> pl.DataFrame:
        """Keep a stay-home alternative for every compact population group."""
        return self._replace_unit_keys(steps).group_by(DEMAND_UNIT_COLS).agg(
            pl.col("n_persons").sum(),
            *[pl.col(column).first() for column in steps.columns if column not in DEMAND_UNIT_COLS + ["n_persons"]],
        ).select(steps.columns).collect(engine="streaming")

    def expand_step_rows(self, steps: pl.DataFrame | pl.LazyFrame) -> pl.LazyFrame:
        """Expose a lazy compatibility view for the outer behavioral updater."""
        columns = steps.collect_schema().names() if isinstance(steps, pl.LazyFrame) else steps.columns
        return (
            steps.lazy().drop("demand_subgroup_id")
            .rename({"demand_group_id": "_score_context_id"})
            .join(self.unit_map.lazy(), on="_score_context_id")
            .select(columns)
        )

    def _replace_unit_keys(self, frame: pl.DataFrame | pl.LazyFrame) -> pl.LazyFrame:
        """Use compact context ids only inside the temporary scoring tables."""
        return frame.lazy().join(self.unit_map.lazy(), on=DEMAND_UNIT_COLS).drop(DEMAND_UNIT_COLS).rename(
            {"_score_context_id": "demand_group_id"}
        ).with_columns(demand_subgroup_id=pl.lit(0, dtype=pl.UInt32))
