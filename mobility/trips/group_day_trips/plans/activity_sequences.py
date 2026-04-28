import pathlib
from typing import Any

import polars as pl

from mobility.runtime.assets.file_asset import FileAsset
from mobility.trips.group_day_trips.core.parameters import BehaviorChangeScope


class ActivitySequences(FileAsset):
    """Persist admitted timed activity-sequence seeds for one iteration."""

    OUTPUT_COLUMNS = [
        "demand_group_id",
        "activity_seq_id",
        "time_seq_id",
        "activity",
        "is_anchor",
        "seq_step_index",
        "departure_time",
        "arrival_time",
        "next_departure_time",
    ]

    def __init__(
        self,
        *,
        run_key: str,
        is_weekday: bool,
        iteration: int,
        base_folder: pathlib.Path,
        current_plans: pl.DataFrame | None = None,
        survey_plans: pl.DataFrame | None = None,
        survey_plan_steps: pl.DataFrame | None = None,
        demand_groups: pl.DataFrame | None = None,
        parameters: Any = None,
        seed: int | None = None,
    ) -> None:
        self.current_plans = current_plans
        self.survey_plans = survey_plans
        self.survey_plan_steps = survey_plan_steps
        self.demand_groups = demand_groups
        self.parameters = parameters
        self.seed = seed
        inputs = {
            "version": 2,
            "run_key": run_key,
            "is_weekday": is_weekday,
            "iteration": iteration,
        }
        cache_path = pathlib.Path(base_folder) / f"activity_sequences_{iteration}.parquet"
        super().__init__(inputs, cache_path)

    def get_cached_asset(self) -> pl.DataFrame:
        """Return cached admitted activity-sequence rows for one iteration."""
        return pl.read_parquet(self.cache_path)

    def create_and_get_asset(self) -> pl.DataFrame:
        """Compute and persist admitted activity-sequence rows for one iteration."""
        if self.current_plans is None:
            raise ValueError("Cannot build activity sequences without current plans.")
        if self.survey_plans is None:
            raise ValueError("Cannot build activity sequences without survey plans.")
        if self.survey_plan_steps is None:
            raise ValueError("Cannot build activity sequences without survey plan steps.")
        if self.demand_groups is None:
            raise ValueError("Cannot build activity sequences without demand groups.")
        if self.parameters is None:
            raise ValueError("Cannot build activity sequences without parameters.")
        if self.seed is None:
            raise ValueError("Cannot build activity sequences without a seed.")

        activity_sequences = self._build_activity_sequences_for_scope()
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        activity_sequences.write_parquet(self.cache_path)
        return self.get_cached_asset()

    def _build_activity_sequences_for_scope(self) -> pl.DataFrame:
        """Return admitted timed activity-sequence rows for this iteration."""
        scope = self.parameters.get_behavior_change_scope(self.iteration)

        if scope == BehaviorChangeScope.FULL_REPLANNING:
            return self._sample_all_activity_sequences()

        if scope in (BehaviorChangeScope.DESTINATION_REPLANNING, BehaviorChangeScope.MODE_REPLANNING):
            return self._select_active_activity_sequences()

        raise ValueError(f"Unsupported behavior change scope: {scope}")

    def _sample_all_activity_sequences(self) -> pl.DataFrame:
        """Sample timed survey activity-sequence seeds from raw survey plans."""
        weighted_survey_plans = (
            self.demand_groups
            .join(self.survey_plans, on=["country", "city_category", "csp", "n_cars"], how="inner")
            .filter(pl.col("time_seq_id") != 0)
        )

        k_activity_sequences = self.parameters.k_activity_sequences
        if k_activity_sequences is None:
            selected = weighted_survey_plans.select(["demand_group_id", "activity_seq_id", "time_seq_id"]).unique()
            return selected.join(
                self.survey_plan_steps,
                on=["activity_seq_id", "time_seq_id"],
                how="inner",
            ).select(self.OUTPUT_COLUMNS)

        activity_sequences = (
            weighted_survey_plans
            .select(["demand_group_id", "activity_seq_id", "time_seq_id", "p_plan"])
            .unique(subset=["demand_group_id", "activity_seq_id", "time_seq_id"], keep="first")
            .with_columns(
                sample_u=(
                    pl.struct(["demand_group_id", "activity_seq_id", "time_seq_id"])
                    .hash(seed=self.seed)
                    .cast(pl.Float64)
                    .truediv(pl.lit(18446744073709551616.0))
                    .clip(1e-18, 1.0 - 1e-18)
                )
            )
            .with_columns(
                sample_score=(pl.col("sample_u").log().neg() / pl.col("p_plan").clip(1e-18))
                + pl.col("time_seq_id").cast(pl.Float64) * 1e-18
            )
            .sort(["demand_group_id", "sample_score", "time_seq_id"])
            .with_columns(sample_rank=pl.col("time_seq_id").cum_count().over("demand_group_id"))
            .filter(pl.col("sample_rank") <= k_activity_sequences)
            .select(["demand_group_id", "activity_seq_id", "time_seq_id"])
        )

        return activity_sequences.join(
            self.survey_plan_steps,
            on=["activity_seq_id", "time_seq_id"],
            how="inner",
        ).select(self.OUTPUT_COLUMNS)

    def _select_active_activity_sequences(self) -> pl.DataFrame:
        """Return the currently occupied non-stay-home activity sequences."""
        active_activity_sequences = (
            self.current_plans
            .filter(pl.col("time_seq_id") != 0)
            .select(["demand_group_id", "activity_seq_id", "time_seq_id"])
            .unique()
        )

        if active_activity_sequences.height == 0:
            return self._empty_activity_sequences()

        return active_activity_sequences.join(
            self.survey_plan_steps,
            on=["activity_seq_id", "time_seq_id"],
            how="inner",
        ).select(self.OUTPUT_COLUMNS)

    def _empty_activity_sequences(self) -> pl.DataFrame:
        """Return an empty activity-sequences dataframe with the expected schema."""
        return pl.DataFrame(
            schema={
                "demand_group_id": pl.UInt32,
                "activity_seq_id": self.survey_plan_steps.schema["activity_seq_id"],
                "time_seq_id": self.survey_plan_steps.schema["time_seq_id"],
                "activity": self.survey_plan_steps.schema["activity"],
                "is_anchor": self.survey_plan_steps.schema["is_anchor"],
                "seq_step_index": self.survey_plan_steps.schema["seq_step_index"],
                "departure_time": self.survey_plan_steps.schema["departure_time"],
                "arrival_time": self.survey_plan_steps.schema["arrival_time"],
                "next_departure_time": self.survey_plan_steps.schema["next_departure_time"],
            }
        )
