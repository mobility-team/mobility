"""Group equivalent demand units before scoring and estimating inner demand."""

from dataclasses import dataclass

import polars as pl

from .candidate_plan_steps import CandidatePlanStepsAsset
from .demand_subgroups import DEMAND_UNIT_COLS
from .plan_ids import PLAN_KEY_COLS
from .stable_key_index import StableKeyIndex


@dataclass(frozen=True)
class ScoringContexts:
    """Compact population groups and the mapping back to original demand units."""

    demand_groups: pl.DataFrame
    unit_map: pl.DataFrame

    @classmethod
    def build(cls, demand_groups, transport_costs, population_segments):
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

    def collapse_step_rows(self, steps: pl.DataFrame | pl.LazyFrame) -> pl.DataFrame:
        """Retain one complete step per plan and scoring context."""
        columns = steps.collect_schema().names() if isinstance(steps, pl.LazyFrame) else steps.columns
        grouped = self._replace_unit_keys(steps)
        keys = PLAN_KEY_COLS + ["seq_step_index"]
        dates = CandidatePlanStepsAsset.RETENTION_COLUMNS
        first_columns = [column for column in columns if column not in keys + dates]
        history = [
            pl.col(column).min() if column == "first_seen_iteration" else pl.col(column).max()
            for column in dates if column in columns
        ]
        return grouped.group_by(keys).agg(
            *[pl.col(column).first() for column in first_columns],
            *history,
        ).select(columns).collect(engine="streaming")

    def collapse_allocations(self, plans: pl.DataFrame) -> pl.DataFrame:
        """Aggregate origin populations without treating inner demand as behavior."""
        return self._replace_unit_keys(plans).group_by(PLAN_KEY_COLS).agg(
            pl.col("n_persons").sum(), pl.col("utility").first(),
        ).collect(engine="streaming")

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
