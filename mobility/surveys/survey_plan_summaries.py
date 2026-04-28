from __future__ import annotations

import pathlib

import polars as pl

from mobility.runtime.assets.file_asset import FileAsset

from .survey_plan_steps import MobilitySurveyPlanSteps
from .survey_plans import MobilitySurveyPlans


class MobilitySurveyPlanSummaries(FileAsset):
    """Persist survey-plan summaries needed by downstream model logic.

    This asset derives compact weighted summary tables from the persisted
    survey plans and plan steps. Those summaries are intended to replace
    repeated recomputation of duration and demand statistics when the
    grouped day-trips model initializes survey-based priors.
    """

    def __init__(
        self,
        *,
        plans: MobilitySurveyPlans,
        plan_steps: MobilitySurveyPlanSteps,
    ) -> None:
        """Initialize the survey-plan summary asset.

        Args:
            plans: Plan-level survey asset containing ``p_plan``.
            plan_steps: Step-level survey-plan asset containing schedule
                and activity details.
        """
        self.plans = plans
        self.plan_steps = plan_steps
        folder_path = pathlib.Path(plan_steps.cache_path).parent
        cache_path = {
            "mean_activity_durations": folder_path / "group_day_trip_mean_activity_durations.parquet",
            "mean_home_night_durations": folder_path / "group_day_trip_mean_home_night_durations.parquet",
            "activity_demand_per_pers": folder_path / "group_day_trip_activity_demand_per_pers.parquet",
        }
        inputs = {
            "version": 2,
            "plans": plans,
            "plan_steps": plan_steps,
        }
        super().__init__(inputs, cache_path)

    def get_cached_asset(self) -> dict[str, pl.DataFrame]:
        """Read all cached summary tables from disk.

        Returns:
            A mapping from summary name to dataframe for the activity
            duration, home-night duration, and activity-demand outputs.
        """
        return {key: pl.read_parquet(path) for key, path in self.cache_path.items()}

    def create_and_get_asset(self) -> dict[str, pl.DataFrame]:
        """Build and cache all survey-plan summary tables.

        Returns:
            A mapping of summary names to weighted survey summary
            dataframes ready to be reused by the grouped day-trips model.
        """
        two_minutes = 120.0 / 3600.0
        plans = self.plans.get()
        plan_steps = self.plan_steps.get()

        weighted_steps = plan_steps.join(
            plans.select(
                [
                    "country",
                    "activity_seq_id",
                    "time_seq_id",
                    "is_weekday",
                    "city_category",
                    "csp",
                    "n_cars",
                    "p_plan",
                ]
            ),
            on=["activity_seq_id", "time_seq_id", "is_weekday", "city_category", "csp", "n_cars"],
            how="inner",
        )

        mean_activity_durations = (
            weighted_steps
            .filter(pl.col("seq_step_index") != pl.col("seq_step_index").max().over("time_seq_id"))
            .group_by(["country", "is_weekday", "csp", "activity"])
            .agg(mean_duration_per_pers=pl.max_horizontal(
                [
                    (pl.col("duration_per_pers") * pl.col("p_plan")).sum() / pl.col("p_plan").sum().clip(1e-18),
                    pl.lit(two_minutes),
                ]
            ))
        )

        mean_home_night_durations = (
            weighted_steps
            .group_by(["country", "is_weekday", "csp", "time_seq_id"])
            .agg(
                p_plan=pl.col("p_plan").first(),
                home_night_per_pers=24.0 - pl.col("duration_per_pers").sum(),
            )
            .group_by(["country", "is_weekday", "csp"])
            .agg(
                mean_home_night_per_pers=pl.max_horizontal(
                    [
                        (pl.col("home_night_per_pers") * pl.col("p_plan")).sum() / pl.col("p_plan").sum().clip(1e-18),
                        pl.lit(two_minutes),
                    ]
                )
            )
        )

        activity_demand_per_pers = (
            weighted_steps
            .group_by(["country", "is_weekday", "activity"])
            .agg(
                duration_per_pers=(pl.col("duration_per_pers") * pl.col("p_plan")).sum()
                / pl.col("p_plan").sum().clip(1e-18)
            )
        )

        self.cache_path["mean_activity_durations"].parent.mkdir(parents=True, exist_ok=True)
        mean_activity_durations.write_parquet(self.cache_path["mean_activity_durations"])
        mean_home_night_durations.write_parquet(self.cache_path["mean_home_night_durations"])
        activity_demand_per_pers.write_parquet(self.cache_path["activity_demand_per_pers"])
        return self.get_cached_asset()
