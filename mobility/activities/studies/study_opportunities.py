import logging
import os
import pathlib

import geopandas as gpd
import pandas as pd

from mobility.activities.opportunity_data import (
    missing_countries,
    normalize_local_admin_unit_ids,
)
from mobility.activities.studies.countries import available_study_data
from mobility.countries import normalize_country_codes
from mobility.runtime.assets.file_asset import FileAsset


class StudyOpportunities(FileAsset):
    """Study opportunities for the selected study area."""

    def __init__(
        self,
        countries: list[str] | tuple[str, ...] | None = None,
        local_admin_unit_ids: list[str] | tuple[str, ...] | None = None,
    ):
        countries = normalize_country_codes(countries)
        local_admin_unit_ids = normalize_local_admin_unit_ids(local_admin_unit_ids)
        study_by_country = available_study_data()
        selected_countries = countries or list(study_by_country)
        country_data = {
            country: study_by_country[country]
            for country in selected_countries
            if country in study_by_country
        }

        inputs = {
            "countries": countries,
            "local_admin_unit_ids": local_admin_unit_ids,
        }
        cache_path = pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / "study_opportunities.parquet"
        super().__init__(inputs, cache_path)
        self.country_data = country_data

    def get_cached_asset(self) -> pd.DataFrame:
        logging.info("School capacity already prepared. Reusing %s.", self.cache_path)
        schools = gpd.read_parquet(self.cache_path)
        schools = schools.set_crs(3035)
        return schools

    def create_and_get_asset(self) -> pd.DataFrame:
        countries = self.inputs["countries"] or list(available_study_data())
        unsupported_countries = missing_countries(countries, available_study_data())
        if unsupported_countries:
            raise ValueError(f"Study opportunities are not available for countries: {unsupported_countries}.")

        parts = []
        for country, country_data in self.country_data.items():
            schools = country_data.opportunities.filter_by_local_admin_unit_id(
                self.inputs["local_admin_unit_ids"]
            )
            self.validate_opportunities(country, schools)
            parts.append(schools)

        study_opportunities = pd.concat(parts, ignore_index=True)
        study_opportunities["school_type"] = study_opportunities["school_type"].astype(str)
        study_opportunities["local_admin_unit_id"] = study_opportunities["local_admin_unit_id"].astype(str)
        study_opportunities["n_students"] = pd.to_numeric(study_opportunities["n_students"], errors="coerce")
        study_opportunities = gpd.GeoDataFrame(study_opportunities, geometry="geometry", crs="EPSG:3035")

        study_opportunities.to_parquet(self.cache_path)
        return study_opportunities

    @staticmethod
    def validate_opportunities(country: str, schools: pd.DataFrame) -> None:
        """Fail when country study opportunities cannot be used by StudyActivity."""
        required_columns = ["local_admin_unit_id", "geometry", "school_type", "n_students"]
        missing = [column for column in required_columns if column not in schools.columns]
        messages = []
        if missing:
            messages.append(f"missing columns: {missing}")
        if isinstance(schools, gpd.GeoDataFrame) and schools.crs is None:
            messages.append("geometry CRS is missing")
        if messages:
            raise ValueError(
                f"Study opportunities for {country} are invalid. "
                + " ".join(messages)
                + " See docs/source/add_country.md#study-data."
            )
