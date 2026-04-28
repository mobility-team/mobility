from __future__ import annotations

import pathlib
from typing import TYPE_CHECKING

import polars as pl

from mobility.runtime.assets.file_asset import FileAsset

if TYPE_CHECKING:
    from .survey_plan_steps import MobilitySurveyPlanSteps


class MobilitySurveyPlans(FileAsset):
    """Persist plan-level survey metadata and sampling probabilities.

    This asset collapses the step-level survey representation to one row
    per survey plan. It is the canonical place for plan-level probability
    mass and the segment keys used later to join survey plans to demand
    groups in the grouped day-trips model.
    """

    def __init__(
        self,
        *,
        plan_steps: MobilitySurveyPlanSteps,
    ) -> None:
        """Initialize the plan-level survey asset.

        Args:
            plan_steps: Step-level survey-plan asset for one survey.
        """
        self.plan_steps = plan_steps
        folder_path = pathlib.Path(plan_steps.cache_path).parent
        cache_path = folder_path / "group_day_trip_plans.parquet"
        inputs = {
            "version": 2,
            "plan_steps": plan_steps,
        }
        super().__init__(inputs, cache_path)

    def get_cached_asset(self) -> pl.DataFrame:
        """Read the cached plan-level survey table from disk.

        Returns:
            A one-row-per-plan dataframe containing survey metadata,
            sequence identifiers, segment keys, and ``p_plan``.
        """
        return pl.read_parquet(self.cache_path)

    def create_and_get_asset(self) -> pl.DataFrame:
        """Build and cache the plan-level survey table.

        Returns:
            A plan-level dataframe with one row per survey plan and a
            segment-specific sampling probability ``p_plan``.
        """
        plan_steps = self.plan_steps.get()
        survey_name = self.plan_steps.survey.inputs["parameters"].survey_name
        country = self.plan_steps.survey.inputs["parameters"].country

        plans = (
            plan_steps
            .select(
                [
                    "activity_seq_id",
                    "time_seq_id",
                    "is_weekday",
                    "city_category",
                    "csp",
                    "n_cars",
                ]
            )
            .unique()
            .with_columns(
                survey_name=pl.lit(survey_name),
                country=pl.lit(country),
            )
            .select(
                [
                    "survey_name",
                    "country",
                    "activity_seq_id",
                    "time_seq_id",
                    "is_weekday",
                    "city_category",
                    "csp",
                    "n_cars",
                ]
            )
            .sort(["activity_seq_id", "time_seq_id", "city_category", "csp", "n_cars", "is_weekday"])
        )

        plan_weights = (
            plan_steps
            .group_by(["activity_seq_id", "time_seq_id", "is_weekday", "city_category", "csp", "n_cars"])
            .agg(plan_weight_mass=pl.col("plan_weight_mass").first())
            .with_columns(
                p_plan=pl.col("plan_weight_mass")
                / pl.col("plan_weight_mass").sum().over(["is_weekday", "city_category", "csp", "n_cars"])
            )
            .select(["activity_seq_id", "time_seq_id", "is_weekday", "city_category", "csp", "n_cars", "p_plan"])
        )

        plans = plans.join(
            plan_weights,
            on=["activity_seq_id", "time_seq_id", "is_weekday", "city_category", "csp", "n_cars"],
            how="left",
        )

        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        plans.write_parquet(self.cache_path)
        return self.get_cached_asset()
