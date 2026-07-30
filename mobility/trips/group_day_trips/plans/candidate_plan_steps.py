import pathlib

import polars as pl

from mobility.runtime.assets.cache_schema import read_cached_parquet
from mobility.runtime.assets.file_asset import FileAsset
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
            "version": 2,
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
        return (
            mode_sequences.get_cached_asset().lazy()
            .join(
                destination_sequences.get_cached_asset().lazy(),
                on=DEMAND_UNIT_COLS + ["activity_seq_id", "time_seq_id", "dest_seq_id", "seq_step_index"],
            )
            .join(survey_plan_steps.lazy(), on=["activity_seq_id", "time_seq_id", "seq_step_index"])
            .join(
                demand_groups.select(DEMAND_UNIT_COLS + ["country", "csp"]).lazy(),
                on=DEMAND_UNIT_COLS,
            )
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
