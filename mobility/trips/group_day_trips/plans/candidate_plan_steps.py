import pathlib

import polars as pl

from mobility.runtime.assets.file_asset import FileAsset


class CandidatePlanStepsAsset(FileAsset):
    """Persisted candidate plan-step memory after one completed iteration."""

    STRUCTURAL_COLUMNS = [
        "demand_group_id",
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
        "last_active_iteration",
    ]

    DEDUPE_COLUMNS = [
        "demand_group_id",
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
            "version": 1,
            "run_key": run_key,
            "is_weekday": is_weekday,
            "iteration": iteration,
        }
        cache_path = pathlib.Path(base_folder) / f"candidate_plan_steps_{iteration}.parquet"
        super().__init__(inputs, cache_path)

    def get_cached_asset(self) -> pl.DataFrame:
        return pl.read_parquet(self.cache_path)

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
                on=["demand_group_id", "activity_seq_id", "time_seq_id", "dest_seq_id", "seq_step_index"],
            )
            .join(survey_plan_steps.lazy(), on=["activity_seq_id", "time_seq_id", "seq_step_index"])
            .join(demand_groups.select(["demand_group_id", "country", "csp"]).lazy(), on="demand_group_id")
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
            candidate_sets.append(
                previous_candidate_plan_steps.lazy()
                .filter(pl.col("mode_seq_id") != 0)
                .select(cls.STRUCTURAL_COLUMNS + cls.RETENTION_COLUMNS)
            )
        if previous_candidate_plan_steps is not None:
            candidate_sets.append(
                previous_candidate_plan_steps.lazy()
                .filter(pl.col("mode_seq_id") != 0)
                .join(
                    current_plans.select(["demand_group_id", "activity_seq_id", "time_seq_id", "dest_seq_id", "mode_seq_id"]).lazy(),
                    on=["demand_group_id", "activity_seq_id", "time_seq_id", "dest_seq_id", "mode_seq_id"],
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
                last_active_iteration=pl.lit(None, dtype=pl.UInt16),
            )
            .select(cls.STRUCTURAL_COLUMNS + cls.RETENTION_COLUMNS)
        )

        candidate_memory = (
            pl.concat(candidate_sets, how="vertical_relaxed")
            .group_by(cls.DEDUPE_COLUMNS)
            .agg(
                activity=pl.col("activity").first(),
                from_=pl.col("from").first(),
                to=pl.col("to").first(),
                mode=pl.col("mode").first(),
                duration_per_pers=pl.col("duration_per_pers").first(),
                departure_time=pl.col("departure_time").first(),
                arrival_time=pl.col("arrival_time").first(),
                next_departure_time=pl.col("next_departure_time").first(),
                iteration=pl.col("iteration").min(),
                country=pl.col("country").first(),
                csp=pl.col("csp").first(),
                first_seen_iteration=pl.col("first_seen_iteration").min(),
                last_active_iteration=pl.col("last_active_iteration").max(),
            )
            .rename({"from_": "from"})
        )
        if current_iteration <= n_warmup_iterations:
            return candidate_memory

        age_reference = pl.coalesce([pl.col("last_active_iteration"), pl.col("first_seen_iteration")])
        return candidate_memory.filter(
            (pl.lit(current_iteration, dtype=pl.UInt16) - age_reference) <= max_inactive_age
        )
