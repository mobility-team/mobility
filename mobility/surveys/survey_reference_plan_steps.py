from __future__ import annotations

import pathlib

import polars as pl

from mobility.runtime.assets.file_asset import FileAsset
from .survey_plan_steps import MobilitySurveyPlanSteps


class MobilitySurveyReferencePlanSteps(FileAsset):
    """Keep eligible survey days separate for reference metrics.

    Within one survey, weekday/weekend type, and demand segment (city category,
    CSP, and car ownership), two plans count as the same plan when their ordered
    activities and timings match. Timing matches when step index, activity,
    departure time, arrival time, and next departure time agree to the nearest
    second. Mode and distance do not affect that match. This asset keeps each
    eligible day's original mode, distance, and survey weight for reference
    metrics, even when another day has the same activities and timings.
    """

    def __init__(self, *, plan_steps: MobilitySurveyPlanSteps) -> None:
        self.plan_steps = plan_steps
        cache_path = pathlib.Path(plan_steps.cache_path).with_name(
            "group_day_trip_reference_steps.parquet"
        )
        super().__init__({"version": 2, "plan_steps": plan_steps}, cache_path)

    def get_cached_asset(self) -> pl.DataFrame:
        """Read the respondent-day survey steps."""
        return pl.read_parquet(self.cache_path)

    def create_and_get_asset(self) -> pl.DataFrame:
        """Build the reference from individual eligible days and their survey weights."""
        observations = self.plan_steps.get_reference_observations()
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        observations.write_parquet(self.cache_path)
        return self.get_cached_asset()
