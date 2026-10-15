import pathlib
from collections.abc import Callable

import polars as pl

from mobility.runtime.assets.cache_schema import read_cached_parquet
from mobility.runtime.assets.file_asset import FileAsset
from .activity_profiles import (
    get_schedules_by_activity_profile,
    get_survey_schedule_activities,
)
from .demand_subgroups import DEMAND_UNIT_COLS
from .plan_ids import PLAN_STEP_KEY_SCHEMA


class CandidatePlanStepsAsset(FileAsset):
    """Persisted candidate plan-step memory after one completed iteration."""

    REQUIRED_SCHEMA = {
        **PLAN_STEP_KEY_SCHEMA,
        "first_seen_iteration": pl.UInt16,
        "last_seen_iteration": pl.UInt16,
    }

    STRUCTURAL_COLUMNS = [
        "demand_group_id",
        "demand_subgroup_id",
        "country",
        "activity_seq_id",
        "time_seq_id",
        "dest_seq_id",
        "mode_seq_id",
        "seq_step_index",
        "activity",
        "from",
        "to",
        "mode",
        "duration_per_pers",
        "departure_time",
        "arrival_time",
        "next_departure_time",
        "iteration",
        "csp",
    ]

    RETENTION_COLUMNS = [
        "first_seen_iteration",
        "last_seen_iteration",
        "last_active_iteration",
    ]

    DEDUPE_COLUMNS = [
        "demand_group_id",
        "demand_subgroup_id",
        "activity_seq_id",
        "time_seq_id",
        "dest_seq_id",
        "mode_seq_id",
        "seq_step_index",
    ]

    def __init__(
        self,
        *,
        run_key: str,
        is_weekday: bool,
        iteration: int,
        base_folder: pathlib.Path,
        candidate_plan_steps: pl.DataFrame | None = None,
    ) -> None:
        self.candidate_plan_steps = candidate_plan_steps
        inputs = {
            "version": 3,
            "run_key": run_key,
            "is_weekday": is_weekday,
            "iteration": iteration,
        }
        cache_path = pathlib.Path(base_folder) / f"candidate_plan_steps_{iteration}.parquet"
        super().__init__(inputs, cache_path)

    def get_cached_asset(self) -> pl.DataFrame:
        return read_cached_parquet(
            self.cache_path,
            table_name="candidate_plan_steps",
            required_schema=self.REQUIRED_SCHEMA,
        )

    def create_and_get_asset(self) -> pl.DataFrame:
        if self.candidate_plan_steps is None:
            raise ValueError("Cannot save candidate plan steps without a dataframe.")
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        self.candidate_plan_steps.write_parquet(self.cache_path)
        return self.get_cached_asset()

    @classmethod
    def build_iteration_candidates(
        cls,
        *,
        destination_sequences,
        mode_sequences,
        survey_plan_steps: pl.DataFrame,
        demand_groups: pl.DataFrame,
        profile_assignments: pl.DataFrame,
        candidate_pruner: Callable[[pl.LazyFrame], pl.DataFrame] | None = None,
    ) -> pl.LazyFrame:
        """Build structural plan-step candidates generated at one iteration."""
        survey_plan_steps = (
            survey_plan_steps
            .select(
                [
                    "activity_seq_id",
                    "time_seq_id",
                    "seq_step_index",
                    "activity",
                    "duration_per_pers",
                    "departure_time",
                    "arrival_time",
                    "next_departure_time",
                ]
            )
        )
        mode_rows = mode_sequences.get()
        destination_rows = destination_sequences.get()
        return cls._attach_candidates_to_demand_units(
            mode_rows=mode_rows,
            destination_rows=destination_rows,
            survey_plan_steps=survey_plan_steps,
            demand_groups=demand_groups,
            profile_assignments=profile_assignments,
            candidate_pruner=candidate_pruner,
        )

    @classmethod
    def _attach_candidates_to_demand_units(
        cls,
        *,
        mode_rows: pl.DataFrame,
        destination_rows: pl.DataFrame,
        survey_plan_steps: pl.DataFrame,
        demand_groups: pl.DataFrame,
        profile_assignments: pl.DataFrame,
        candidate_pruner: Callable[[pl.LazyFrame], pl.DataFrame] | None,
    ) -> pl.LazyFrame:
        """Attach completed shared plans to eligible demand units."""
        if "allowed_activities" not in demand_groups.columns:
            raise ValueError(
                "Shared plan generation needs each demand unit's activity support set. "
                "Start a new run so the initial state includes allowed_activities."
            )

        units = (
            demand_groups
            .select(
                DEMAND_UNIT_COLS
                + ["country", "csp", "home_zone_id", "allowed_activities"]
            )
            .join(profile_assignments, on=DEMAND_UNIT_COLS)
            .with_columns(
                activity_profile_id=pl.col("allowed_activities").list.join("|")
            )
        )
        schedule_activities = get_survey_schedule_activities(survey_plan_steps)
        schedules = (
            destination_rows
            .select(["home_zone_id", "activity_seq_id", "time_seq_id"])
            .unique()
            .join(
                units.select(["country", "home_zone_id"]).unique(),
                on="home_zone_id",
                how="inner",
            )
            .with_columns(plan_weight_mass=pl.lit(1.0))
            .join(schedule_activities, on=["activity_seq_id", "time_seq_id"], how="inner")
        )
        eligible_schedules = get_schedules_by_activity_profile(
            schedules=schedules,
            profiles=units.select(
                ["country", "activity_profile_id", "allowed_activities"]
            ),
        ).select(["country", "activity_profile_id", "activity_seq_id", "time_seq_id"])

        # Score one compact row set per distinct CSP, activity, and utility context.
        # Give each compact utility context a temporary demand-unit key for scoring.
        scoring_contexts = (
            units
            .select(
                [
                    "country",
                    "csp",
                    "home_zone_id",
                    "activity_profile_id",
                    "utility_profile_id",
                ]
            )
            .unique()
            .sort(
                [
                    "country",
                    "csp",
                    "home_zone_id",
                    "activity_profile_id",
                    "utility_profile_id",
                ]
            )
            .with_row_index("_score_context_id")
            .with_columns(
                _score_context_id=pl.col("_score_context_id").cast(pl.UInt32),
                demand_group_id=pl.col("_score_context_id").cast(pl.UInt32),
                demand_subgroup_id=pl.lit(0, dtype=pl.UInt32),
            )
        )
        context_to_units = (
            units
            .join(
                scoring_contexts.select(
                    [
                        "country",
                        "csp",
                        "home_zone_id",
                        "activity_profile_id",
                        "utility_profile_id",
                        "_score_context_id",
                    ]
                ),
                on=[
                    "country",
                    "csp",
                    "home_zone_id",
                    "activity_profile_id",
                    "utility_profile_id",
                ],
                how="inner",
            )
            .select(["_score_context_id"] + DEMAND_UNIT_COLS)
            .unique()
        )

        plan_steps = (
            mode_rows.lazy()
            .join(
                destination_rows.lazy(),
                on=[
                    "home_zone_id",
                    "activity_seq_id",
                    "time_seq_id",
                    "dest_seq_id",
                    "seq_step_index",
                ],
            )
            .join(
                survey_plan_steps.lazy(),
                on=["activity_seq_id", "time_seq_id", "seq_step_index"],
            )
        )
        context_steps = (
            plan_steps
            .join(
                scoring_contexts.lazy(),
                on=["home_zone_id", "utility_profile_id"],
                how="inner",
            )
            .join(
                eligible_schedules.lazy(),
                on=["country", "activity_profile_id", "activity_seq_id", "time_seq_id"],
                how="inner",
            )
        )
        if candidate_pruner is not None:
            retained_context_plans = candidate_pruner(context_steps)
            context_steps = context_steps.join(
                retained_context_plans.lazy(),
                on=[
                    "_score_context_id",
                    "activity_seq_id",
                    "time_seq_id",
                    "dest_seq_id",
                    "mode_seq_id",
                ],
                how="inner",
            )

        return (
            context_steps
            .drop(DEMAND_UNIT_COLS)
            .join(
                context_to_units.lazy(),
                on="_score_context_id",
                how="inner",
            )
            .drop("_score_context_id")
            .select(cls.STRUCTURAL_COLUMNS)
        )

    @classmethod
    def build_candidate_memory(
        cls,
        *,
        destination_sequences,
        mode_sequences,
        survey_plan_steps: pl.DataFrame,
        demand_groups: pl.DataFrame,
        current_plans: pl.DataFrame,
        previous_candidate_plan_steps: pl.DataFrame | None,
        current_iteration: int,
        n_warmup_iterations: int,
        max_inactive_age: int,
        profile_assignments: pl.DataFrame,
        candidate_pruner: Callable[[pl.LazyFrame], pl.DataFrame] | None = None,
    ) -> pl.LazyFrame:
        """Merge new iteration candidates into the cumulative structural candidate memory."""
        candidate_sets = []
        if previous_candidate_plan_steps is not None:
            previous_candidates = previous_candidate_plan_steps.lazy()
            candidate_sets.append(
                previous_candidates
                .filter(pl.col("mode_seq_id") != 0)
                .select(cls.STRUCTURAL_COLUMNS + cls.RETENTION_COLUMNS)
            )
        if previous_candidate_plan_steps is not None:
            candidate_sets.append(
                previous_candidates
                .filter(pl.col("mode_seq_id") != 0)
                .join(
                    current_plans
                    .select(DEMAND_UNIT_COLS + ["activity_seq_id", "time_seq_id", "dest_seq_id", "mode_seq_id"])
                    .lazy(),
                    on=DEMAND_UNIT_COLS + ["activity_seq_id", "time_seq_id", "dest_seq_id", "mode_seq_id"],
                    how="inner",
                )
                .with_columns(
                    last_active_iteration=pl.lit(current_iteration).cast(pl.UInt16),
                )
                .select(cls.STRUCTURAL_COLUMNS + cls.RETENTION_COLUMNS)
            )
        candidate_sets.append(
            cls.build_iteration_candidates(
                destination_sequences=destination_sequences,
                mode_sequences=mode_sequences,
                survey_plan_steps=survey_plan_steps,
                demand_groups=demand_groups,
                profile_assignments=profile_assignments,
                candidate_pruner=candidate_pruner,
            )
            .with_columns(
                first_seen_iteration=pl.lit(current_iteration).cast(pl.UInt16),
                last_seen_iteration=pl.lit(current_iteration).cast(pl.UInt16),
                last_active_iteration=pl.lit(None, dtype=pl.UInt16),
            )
            .select(cls.STRUCTURAL_COLUMNS + cls.RETENTION_COLUMNS)
        )

        candidate_memory = (
            pl.concat(candidate_sets, how="vertical_relaxed")
            .group_by(cls.DEDUPE_COLUMNS)
            .agg(
                activity=pl.col("activity").sort_by("last_seen_iteration").last(),
                from_=pl.col("from").sort_by("last_seen_iteration").last(),
                to=pl.col("to").sort_by("last_seen_iteration").last(),
                mode=pl.col("mode").sort_by("last_seen_iteration").last(),
                duration_per_pers=pl.col("duration_per_pers").sort_by("last_seen_iteration").last(),
                departure_time=pl.col("departure_time").sort_by("last_seen_iteration").last(),
                arrival_time=pl.col("arrival_time").sort_by("last_seen_iteration").last(),
                next_departure_time=pl.col("next_departure_time").sort_by("last_seen_iteration").last(),
                iteration=pl.col("iteration").sort_by("last_seen_iteration").last(),
                country=pl.col("country").sort_by("last_seen_iteration").last(),
                csp=pl.col("csp").sort_by("last_seen_iteration").last(),
                first_seen_iteration=pl.col("first_seen_iteration").min(),
                last_seen_iteration=pl.col("last_seen_iteration").max(),
                last_active_iteration=pl.col("last_active_iteration").max(),
            )
            .rename({"from_": "from"})
        )
        if current_iteration <= n_warmup_iterations:
            return candidate_memory

        age_reference = pl.max_horizontal(
            pl.col("first_seen_iteration"),
            pl.col("last_seen_iteration"),
            pl.col("last_active_iteration").fill_null(0),
        )
        return candidate_memory.filter(
            (pl.lit(current_iteration, dtype=pl.UInt16) - age_reference) <= max_inactive_age
        )
