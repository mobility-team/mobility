from __future__ import annotations

import os
import pathlib
from typing import Any

import polars as pl

from mobility.runtime.assets.file_asset import FileAsset
from mobility.surveys import SurveyPlanAssets


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
            "version": 4,
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
        lau_to_city_cat = (
            pl.from_pandas(
                self.population.transport_zones.study_area.get()
                .drop("geometry", axis=1)[["local_admin_unit_id", "urban_unit_category"]]
                .rename({"urban_unit_category": "city_category"}, axis=1)
            ).with_columns(country=pl.col("local_admin_unit_id").str.slice(0, 2))
        )

        demand_groups = (
            pl.scan_parquet(self.population.get()["population_groups"])
            .rename(
                {
                    "socio_pro_category": "csp",
                    "transport_zone_id": "home_zone_id",
                    "weight": "n_persons",
                }
            )
            .with_columns(home_zone_id=pl.col("home_zone_id").cast(pl.Int32))
            .join(lau_to_city_cat.lazy(), on=["local_admin_unit_id"])
            .group_by(["country", "home_zone_id", "city_category", "csp", "n_cars"])
            .agg(pl.col("n_persons").sum())
            .collect(engine="streaming")
        )

        countries = demand_groups["country"].unique().sort().to_list()
        survey_plan_steps = self.survey_plan_assets.get_plan_steps().select(
            [
                "activity_seq_id",
                "time_seq_id",
                "seq_step_index",
                "activity",
                "mode",
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
            activity=pl.col("activity").cast(pl.Enum(activity_values)),
            mode=pl.col("mode").cast(pl.Enum(mode_values)),
        )
        survey_plan_steps = survey_plan_steps.unique(
            subset=["activity_seq_id", "time_seq_id", "seq_step_index"],
            keep="first",
        )
        survey_plans = (
            survey_plans
            .filter(pl.col("is_weekday") == self.is_weekday)
            .drop("is_weekday")
            .group_by(
                [
                    "country",
                    "city_category",
                    "csp",
                    "n_cars",
                    "activity_seq_id",
                    "time_seq_id",
                ]
            )
            .agg(p_plan=pl.col("p_plan").sum())
        )

        population_weighted_plan_steps = (
            demand_groups.join(survey_plans, on=["country", "city_category", "csp", "n_cars"])
            .with_columns(n_persons=pl.col("n_persons") * pl.col("p_plan"))
            .join(
                survey_plan_steps,
                on=["activity_seq_id", "time_seq_id"],
                how="inner",
            )
            .select(
                [
                    "country",
                    "home_zone_id",
                    "activity_seq_id",
                    "time_seq_id",
                    "activity",
                    "mode",
                    "travel_time",
                    "distance",
                    "n_persons",
                ]
            )
        )

        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        population_weighted_plan_steps.write_parquet(self.cache_path)
        return self.get_cached_asset()
