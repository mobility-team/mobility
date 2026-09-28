from __future__ import annotations

from typing import Any

import polars as pl

from mobility.runtime.assets.in_memory_asset import InMemoryAsset

from .survey_plan_steps import MobilitySurveyPlanSteps
from .survey_plans import MobilitySurveyPlans
from .survey_plan_summaries import MobilitySurveyPlanSummaries
from .survey_reference_plan_steps import MobilitySurveyReferencePlanSteps
from .programme_pooling import build_donors, pool_programmes, ProgrammePoolingParameters


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
        self._reference_assets = None
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
            "version": 2,
            "surveys": surveys,
            # The merged survey wrapper changes only when one of its child
            # survey assets changes. Do not hash full activity or mode objects
            # here, because future scenario parameters are not survey logic.
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

    def get_reference_plan_assets_by_survey(self) -> list[dict[str, Any]]:
        """Return reference-step assets paired with their source surveys."""
        if self._reference_assets is None:
            # Keep these out of the wrapper inputs: they feed reference metrics,
            # while its existing inputs define the compact plans used for sampling.
            self._reference_assets = [
                {
                    "survey": assets["survey"],
                    "plan_steps": MobilitySurveyReferencePlanSteps(plan_steps=assets["plan_steps"]),
                }
                for assets in self._get_per_survey_assets()
            ]
        return self._reference_assets

    def get_plans(self) -> pl.DataFrame:
        """Return merged plan-level survey probabilities."""
        return self._get_merged_asset_table("plans")

    def get_programme_assignments(
        self, targets: pl.DataFrame, settings: ProgrammePoolingParameters,
    ) -> pl.DataFrame:
        """Return French EMP donor assignments for the requested demand segments.

        Reference steps and original model priors have no pooling dependency.
        The returned day table preserves donor provenance before schedules are
        compacted for the model's existing programme sampler. Legacy sampling
        includes repaired diaries and cannot be described by this donor table.
        """
        if not settings.enabled:
            raise ValueError("Enable programme pooling to inspect donor assignments; disabled runs use legacy survey plans.")
        tables = []
        for assets in self.get_reference_plan_assets_by_survey():
            parameters = assets["survey"].inputs["parameters"]
            if parameters.survey_name != "fr-EMP-2019":
                continue
            donors = build_donors(assets["plan_steps"].get(), parameters.survey_name)
            assignments = pool_programmes(
                donors, targets.filter(pl.col("country") == parameters.country), settings,
            ).with_columns(country=pl.lit(parameters.country), survey_name=pl.lit(parameters.survey_name))
            tables.append(assignments)
        return self._concat_frames(tables)

    def get_sampling_plans(
        self, targets: pl.DataFrame, settings: ProgrammePoolingParameters,
    ) -> pl.DataFrame:
        """Use pooled probabilities for EMP and existing probabilities elsewhere."""
        original = self.get_plans()
        if not settings.enabled:
            return original
        assignments = self.get_programme_assignments(targets, settings)
        if "p_sampling" not in assignments.columns:
            return original
        keys = ["survey_name", "country", "activity_seq_id", "time_seq_id",
                "is_weekday", "city_category", "csp", "n_cars"]
        # The existing sampler calls its probability p_plan. Only sampling mass
        # is combined for matching schedules; reference observations stay separate.
        pooled = assignments.filter(pl.col("p_sampling") > 0).group_by(keys).agg(p_plan=pl.col("p_sampling").sum())
        return pl.concat([
            original.filter(pl.col("survey_name") != "fr-EMP-2019"),
            pooled.select(original.columns),
        ], how="vertical_relaxed")

    def get_mean_activity_durations(self) -> pl.DataFrame:
        """Return unpooled model duration priors, not reference observations."""
        return self._get_merged_summary_table("mean_activity_durations")

    def get_mean_home_night_durations(self) -> pl.DataFrame:
        """Return unpooled model home-night priors, not reference observations."""
        return self._get_merged_summary_table("mean_home_night_durations")

    def get_activity_demand_per_pers(self) -> pl.DataFrame:
        """Return unpooled model opportunity priors, not reference observations."""
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
