from __future__ import annotations

import os
import pathlib
from typing import Any

import polars as pl

from mobility.runtime.assets.file_asset import FileAsset
from mobility.surveys import SurveyPlanAssets


def _get_demand_groups(population: Any) -> pl.DataFrame:
    """Return population weights grouped by the survey matching fields."""
    lau_to_city_category = pl.from_pandas(
        population.transport_zones.study_area.get()
        .drop("geometry", axis=1)[["local_admin_unit_id", "country", "urban_unit_category"]]
        .rename({"urban_unit_category": "city_category"}, axis=1)
    )
    return (
        pl.scan_parquet(population.get()["population_groups"])
        .rename(
            {
                "socio_pro_category": "csp",
                "transport_zone_id": "home_zone_id",
                "weight": "n_persons",
            }
        )
        .with_columns(home_zone_id=pl.col("home_zone_id").cast(pl.Int32))
        .join(lau_to_city_category.lazy(), on="local_admin_unit_id")
        .group_by(["country", "home_zone_id", "city_category", "csp", "n_cars"])
        .agg(pl.col("n_persons").sum())
        .collect(engine="streaming")
    )


def _weight_reference_observations(observations: pl.DataFrame) -> pl.DataFrame:
    """Normalize original respondent-day weights within each source segment."""
    segment_keys = ["survey_name", "country", "city_category", "csp", "n_cars"]
    if "is_weekday" in observations.columns:
        segment_keys.append("is_weekday")
    day_keys = [*segment_keys, "individual_id", "day_id"]
    # Count each respondent-day weight once, then apply it to every trip step.
    day_probabilities = (
        observations.group_by(day_keys)
        .agg(day_weight=pl.col("pondki").first())
        .with_columns(p_reference=pl.col("day_weight") / pl.col("day_weight").sum().over(segment_keys))
        .select(*day_keys, "p_reference")
    )
    return observations.join(day_probabilities, on=day_keys)


class PopulationWeightedPlanSteps(FileAsset):
    """Persist survey plan steps expanded and weighted by model demand groups."""

    def __init__(
        self,
        *,
        population: Any,
        survey_plan_assets: SurveyPlanAssets,
        is_weekday: bool,
    ) -> None:
        self.population = population
        self.survey_plan_assets = survey_plan_assets
        self.is_weekday = is_weekday
        project_folder = pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"])
        cache_path = (
            project_folder
            / "group_day_trips"
            / f"population_weighted_plan_steps_{'weekday' if is_weekday else 'weekend'}.parquet"
        )
        inputs = {
            "version": 7,
            "population": population,
            "survey_plan_assets": survey_plan_assets,
            "is_weekday": is_weekday,
        }
        super().__init__(inputs, cache_path)

    def get_cached_asset(self) -> pl.LazyFrame:
        """Return the cached weighted plan-step table as a lazy parquet scan."""
        return pl.scan_parquet(self.cache_path)

    def create_and_get_asset(self) -> pl.LazyFrame:
        """Build and cache the population-weighted survey plan-step table."""
        demand_groups = _get_demand_groups(self.population)

        countries = demand_groups["country"].unique().sort().to_list()
        survey_plan_steps = self.survey_plan_assets.get_plan_steps().select(
            [
                "activity_seq_id",
                "time_seq_id",
                "is_weekday",
                "city_category",
                "csp",
                "n_cars",
                "seq_step_index",
                "activity",
                "mode",
                "is_anchor",
                "departure_time",
                "arrival_time",
                "next_departure_time",
                "duration_per_pers",
                "travel_time",
                "distance",
            ]
        )
        survey_plans = self.survey_plan_assets.get_plans().select(
            [
                "country",
                "activity_seq_id",
                "time_seq_id",
                "city_category",
                "csp",
                "n_cars",
                "is_weekday",
                "p_plan",
            ]
        )

        def get_col_values(df1: pl.DataFrame, df2: pl.DataFrame, col: str) -> list[Any]:
            series = pl.concat([df1.select(col), df2.select(col)]).to_series()
            return series.unique().sort().to_list()

        city_category_values = get_col_values(demand_groups, survey_plans, "city_category")
        csp_values = get_col_values(demand_groups, survey_plans, "csp")
        n_cars_values = get_col_values(demand_groups, survey_plans, "n_cars")
        activity_values = survey_plan_steps["activity"].unique().sort().to_list()
        mode_values = survey_plan_steps["mode"].unique().sort().to_list()

        demand_groups = demand_groups.with_columns(
            country=pl.col("country").cast(pl.Enum(countries)),
            city_category=pl.col("city_category").cast(pl.Enum(city_category_values)),
            csp=pl.col("csp").cast(pl.Enum(csp_values)),
            n_cars=pl.col("n_cars").cast(pl.Enum(n_cars_values)),
        )
        survey_plans = survey_plans.with_columns(
            country=pl.col("country").cast(pl.Enum(countries)),
            city_category=pl.col("city_category").cast(pl.Enum(city_category_values)),
            csp=pl.col("csp").cast(pl.Enum(csp_values)),
            n_cars=pl.col("n_cars").cast(pl.Enum(n_cars_values)),
        )
        survey_plan_steps = survey_plan_steps.with_columns(
            city_category=pl.col("city_category").cast(pl.Enum(city_category_values)),
            csp=pl.col("csp").cast(pl.Enum(csp_values)),
            n_cars=pl.col("n_cars").cast(pl.Enum(n_cars_values)),
            activity=pl.col("activity").cast(pl.Enum(activity_values)),
            mode=pl.col("mode").cast(pl.Enum(mode_values)),
        )
        survey_plan_steps = survey_plan_steps.filter(pl.col("is_weekday") == self.is_weekday).drop("is_weekday")
        survey_plans = (
            survey_plans
            .filter(pl.col("is_weekday") == self.is_weekday)
            .drop("is_weekday")
        )

        population_weighted_plan_steps = (
            demand_groups.join(survey_plans, on=["country", "city_category", "csp", "n_cars"])
            .with_columns(n_persons=pl.col("n_persons") * pl.col("p_plan"))
            .join(
                survey_plan_steps,
                on=["activity_seq_id", "time_seq_id", "city_category", "csp", "n_cars"],
                how="inner",
            )
            .select(
                [
                    "country",
                    "home_zone_id",
                    "city_category",
                    "csp",
                    "n_cars",
                    "activity_seq_id",
                    "time_seq_id",
                    "seq_step_index",
                    "activity",
                    "mode",
                    "is_anchor",
                    "departure_time",
                    "arrival_time",
                    "next_departure_time",
                    "duration_per_pers",
                    "travel_time",
                    "distance",
                    "n_persons",
                ]
            )
        )

        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        population_weighted_plan_steps.write_parquet(self.cache_path)
        return self.get_cached_asset()


class PopulationWeightedSurveyReferenceSteps(FileAsset):
    """Expand individual survey-day steps to demand groups for reference metrics."""

    def __init__(
        self,
        *,
        population: Any,
        survey_plan_assets: SurveyPlanAssets,
        is_weekday: bool,
    ) -> None:
        self.population = population
        self.reference_plan_assets = survey_plan_assets.get_reference_plan_assets_by_survey()
        self.is_weekday = is_weekday
        project_folder = pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"])
        cache_path = (
            project_folder
            / "group_day_trips"
            / f"population_weighted_survey_reference_steps_{'weekday' if is_weekday else 'weekend'}.parquet"
        )
        super().__init__(
            {
                "version": 2,
                "population": population,
                "reference_plan_assets": self.reference_plan_assets,
                "is_weekday": is_weekday,
            },
            cache_path,
        )
        self.unsupported_demand_cache_path = self.cache_path.with_name(
            f"{self.cache_path.stem}_support.parquet"
        )

    def get_cached_asset(self) -> pl.LazyFrame:
        """Return the cached respondent-day reference steps."""
        return pl.scan_parquet(self.cache_path)

    def assets_missing(self) -> bool:
        """Rebuild when either the steps or their support summary is missing."""
        return super().assets_missing() or not self.unsupported_demand_cache_path.exists()

    @property
    def unsupported_demand_mass(self) -> float:
        """Return the population weight in demand groups without survey support."""
        self.get()
        return float(pl.read_parquet(self.unsupported_demand_cache_path)["n_persons"][0])

    def create_and_get_asset(self) -> pl.LazyFrame:
        """Weight each eligible respondent day within its survey segment."""
        demand_groups = _get_demand_groups(self.population)

        # Normalize each source survey independently, matching its p_plan values.
        observations = pl.concat(
            [
                item["plan_steps"].get()
                .filter(pl.col("is_weekday") == self.is_weekday)
                .with_columns(
                    country=pl.lit(item["survey"].inputs["parameters"].country),
                    survey_name=pl.lit(item["survey"].inputs["parameters"].survey_name),
                )
                for item in self.reference_plan_assets
            ],
            how="diagonal_relaxed",
        )
        weighted_observations = _weight_reference_observations(observations)
        segment_keys = ["country", "city_category", "csp", "n_cars"]
        supported_segments = weighted_observations.select(segment_keys).unique()
        unsupported_demand_mass = (
            demand_groups.join(supported_segments, on=segment_keys, how="anti")
            .select(pl.col("n_persons").sum())
            .with_columns(pl.col("n_persons").fill_null(0.0))
        )
        weighted = (
            demand_groups.join(
                weighted_observations,
                on=["country", "city_category", "csp", "n_cars"],
                how="inner",
            )
            .with_columns(n_persons=pl.col("n_persons") * pl.col("p_reference"))
            .select(
                [
                    "country",
                    "survey_name",
                    "p_reference",
                    "home_zone_id",
                    "city_category",
                    "csp",
                    "n_cars",
                    "activity_seq_id",
                    "time_seq_id",
                    "day_id",
                    "individual_id",
                    "seq_step_index",
                    "activity",
                    "mode",
                    "is_anchor",
                    "departure_time",
                    "arrival_time",
                    "next_departure_time",
                    "duration_per_pers",
                    "travel_time",
                    "distance",
                    "n_persons",
                ]
            )
        )
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        weighted.write_parquet(self.cache_path)
        unsupported_demand_mass.write_parquet(self.unsupported_demand_cache_path)
        return self.get_cached_asset()
