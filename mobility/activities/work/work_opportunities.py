"""Work opportunities for the selected study area."""

import logging
import os
import pathlib

import pandas as pd

from mobility.activities.opportunity_data import (
    missing_countries,
    normalize_local_admin_unit_ids,
)
from mobility.activities.work.countries import available_work_data
from mobility.countries import normalize_country_codes
from mobility.runtime.assets.file_asset import FileAsset


class WorkOpportunities(FileAsset):
    """Work opportunities by local admin unit."""

    def __init__(
        self,
        countries: list[str] | tuple[str, ...] | None = None,
        local_admin_unit_ids: list[str] | tuple[str, ...] | None = None,
    ):
        countries = normalize_country_codes(countries)
        local_admin_unit_ids = normalize_local_admin_unit_ids(local_admin_unit_ids)
        work_by_country = available_work_data()
        selected_countries = countries or list(work_by_country)
        country_data = {
            country: work_by_country[country]
            for country in selected_countries
            if country in work_by_country
        }

        inputs = {
            "countries": countries,
            "local_admin_unit_ids": local_admin_unit_ids,
        }

        cache_path = {
            "active_population": (
                pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"])
                / "active_population.parquet"
            ),
            "jobs": pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / "jobs.parquet",
        }
        super().__init__(inputs, cache_path)
        self.country_data = country_data

    def get_cached_asset(self) -> tuple[pd.DataFrame, pd.DataFrame]:
        logging.info("Work opportunities already prepared. Reusing %s.", self.cache_path)
        jobs = pd.read_parquet(self.cache_path["jobs"])
        active_population = pd.read_parquet(self.cache_path["active_population"])
        return jobs, active_population

    def create_and_get_asset(self) -> tuple[pd.DataFrame, pd.DataFrame]:
        logging.info("Preparing work opportunities.")

        countries = self.inputs["countries"] or list(available_work_data())
        unsupported_countries = missing_countries(countries, available_work_data())
        if unsupported_countries:
            raise ValueError(f"Work opportunities are not available for countries: {unsupported_countries}.")

        jobs_parts = []
        active_population_parts = []
        for country, country_data in self.country_data.items():
            jobs, active_population = country_data.opportunities.filter_by_local_admin_unit_id(
                self.inputs["local_admin_unit_ids"]
            )
            self.validate_opportunities(country, jobs, active_population)
            jobs_parts.append(jobs)
            active_population_parts.append(active_population)

        jobs = pd.concat(jobs_parts)
        active_population = pd.concat(active_population_parts)

        jobs.to_parquet(self.cache_path["jobs"])
        active_population.to_parquet(self.cache_path["active_population"])
        return jobs, active_population

    @staticmethod
    def validate_opportunities(country: str, jobs: pd.DataFrame, active_population: pd.DataFrame) -> None:
        """Fail when country work opportunity tables cannot be used by WorkActivity."""
        messages = []
        if jobs.index.name != "local_admin_unit_id":
            messages.append("jobs index must be named local_admin_unit_id")
        if active_population.index.name != "local_admin_unit_id":
            messages.append("active population index must be named local_admin_unit_id")

        missing_jobs = [column for column in ["n_jobs_total"] if column not in jobs.columns]
        if missing_jobs:
            messages.append(f"jobs table is missing columns: {missing_jobs}")

        missing_active = [column for column in ["active_pop"] if column not in active_population.columns]
        if missing_active:
            messages.append(f"active population table is missing columns: {missing_active}")

        if messages:
            raise ValueError(
                f"Work opportunities for {country} are invalid. "
                + " ".join(messages)
                + " See docs/source/add_country.md#work-data."
            )
