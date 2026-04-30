from __future__ import annotations

from typing import Any

import polars as pl

from mobility.runtime.assets.in_memory_asset import InMemoryAsset

from .survey_plan_steps import MobilitySurveyPlanSteps
from .survey_plans import MobilitySurveyPlans
from .survey_plan_summaries import MobilitySurveyPlanSummaries


class SurveyPlanAssets(InMemoryAsset):
    """Merge several per-survey plan assets behind one lazy in-memory interface."""

    def __init__(
        self,
        *,
        surveys: list[Any],
        activities: list[Any],
        modes: list[Any],
    ) -> None:
        self._plan_steps = None
        self._plans = None
        self._summary_tables = None
        self._mean_activity_durations = None
        self._mean_home_night_durations = None
        self._activity_demand_per_pers = None
        per_survey_assets = []
        for survey in surveys:
            plan_steps = MobilitySurveyPlanSteps(
                survey=survey,
                activities=activities,
                modes=modes,
            )
            plans = MobilitySurveyPlans(plan_steps=plan_steps)
            summaries = MobilitySurveyPlanSummaries(
                plans=plans,
                plan_steps=plan_steps,
            )
            per_survey_assets.append(
                {
                    "survey": survey,
                    "plan_steps": plan_steps,
                    "plans": plans,
                    "summaries": summaries,
                }
            )

        inputs = {
            "version": 1,
            "surveys": surveys,
            "activities": activities,
            "modes": modes,
            "per_survey_assets": per_survey_assets,
        }
        super().__init__(inputs)

    def _get_per_survey_assets(self) -> list[dict[str, Any]]:
        """Return the per-survey asset sets owned by this merged wrapper."""
        return self.inputs["per_survey_assets"]

    @staticmethod
    def _concat_frames(frames: list[pl.DataFrame]) -> pl.DataFrame:
        """Concatenate frames while tolerating an empty survey list."""
        if not frames:
            return pl.DataFrame()
        return pl.concat(frames, how="vertical_relaxed")

    def _get_cached_merged_table(
        self,
        name: str,
        frames: list[pl.DataFrame],
    ) -> pl.DataFrame:
        """Cache and return one merged dataframe built from per-survey tables."""
        cache_attr = f"_{name}"
        value = getattr(self, cache_attr)
        if value is None:
            value = self._concat_frames(frames)
            setattr(self, cache_attr, value)
        return value

    def _get_summary_tables(self) -> list[dict[str, pl.DataFrame]]:
        """Load and cache the per-survey summary mappings once."""
        if self._summary_tables is None:
            self._summary_tables = [
                assets["summaries"].get()
                for assets in self._get_per_survey_assets()
            ]
        return self._summary_tables

    def _get_merged_asset_table(self, name: str) -> pl.DataFrame:
        """Merge and cache one table loaded directly from a per-survey asset."""
        return self._get_cached_merged_table(
            name,
            [assets[name].get() for assets in self._get_per_survey_assets()],
        )

    def _get_merged_summary_table(self, name: str) -> pl.DataFrame:
        """Merge and cache one table extracted from per-survey summaries."""
        return self._get_cached_merged_table(
            name,
            [tables[name] for tables in self._get_summary_tables()],
        )

    def get_plan_steps(self) -> pl.DataFrame:
        """Return merged step-level survey plans."""
        return self._get_merged_asset_table("plan_steps")

    def get_plans(self) -> pl.DataFrame:
        """Return merged plan-level survey probabilities."""
        return self._get_merged_asset_table("plans")

    def get_mean_activity_durations(self) -> pl.DataFrame:
        """Return merged mean activity-duration summaries."""
        return self._get_merged_summary_table("mean_activity_durations")

    def get_mean_home_night_durations(self) -> pl.DataFrame:
        """Return merged mean home-night-duration summaries."""
        return self._get_merged_summary_table("mean_home_night_durations")

    def get_activity_demand_per_pers(self) -> pl.DataFrame:
        """Return merged per-person activity-demand summaries."""
        return self._get_merged_summary_table("activity_demand_per_pers")

    def get(self) -> dict[str, pl.DataFrame]:
        """SurveyPlanAssets has no single canonical value.

        Callers should request the specific merged table they need through the
        explicit getters on this class.
        """
        raise NotImplementedError(
            "SurveyPlanAssets has no single canonical `get()` value. "
            "Use one of: `get_plan_steps()`, `get_plans()`, "
            "`get_mean_activity_durations()`, `get_mean_home_night_durations()`, "
            "or `get_activity_demand_per_pers()`."
        )
