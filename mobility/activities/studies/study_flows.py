import logging
import os
import pathlib

import pandas as pd

from mobility.activities.opportunity_data import (
    complete_country_priority_order,
    keep_first_flow_source,
    missing_countries,
    normalize_local_admin_unit_ids,
)
from mobility.activities.studies.countries import available_study_data
from mobility.countries import normalize_country_codes
from mobility.runtime.assets.file_asset import FileAsset


class StudyFlows(FileAsset):
    """Home-study flows for the selected study area."""

    def __init__(
        self,
        countries: list[str] | tuple[str, ...] | None = None,
        local_admin_unit_ids: list[str] | tuple[str, ...] | None = None,
        country_priority_order: list[str] | tuple[str, ...] | None = None,
    ):
        countries = normalize_country_codes(countries)
        local_admin_unit_ids = normalize_local_admin_unit_ids(local_admin_unit_ids)
        selected_countries = countries or ["fr"]
        study_by_country = available_study_data()
        country_priority_order = complete_country_priority_order(
            selected_countries,
            normalize_country_codes(country_priority_order) or selected_countries,
        )

        country_data = {
            country: study_by_country[country]
            for country in selected_countries
            if country in study_by_country
        }

        inputs = {
            "countries": countries,
            "local_admin_unit_ids": local_admin_unit_ids,
            "country_priority_order": country_priority_order,
        }
        cache_path = pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / "insee" / "study_flows.parquet"
        super().__init__(inputs, cache_path)
        self.country_data = country_data

    def get_cached_asset(self) -> pd.DataFrame:
        logging.info("Study flows already prepared. Reusing the file: " + str(self.cache_path))
        return pd.read_parquet(self.cache_path)

    def create_and_get_asset(self) -> pd.DataFrame:
        logging.info("Preparing study flows.")

        countries = self.inputs["countries"] or ["fr"]
        unsupported_countries = missing_countries(countries, available_study_data())
        if unsupported_countries:
            raise ValueError(f"School flow data is not available for countries: {unsupported_countries}.")

        parts = []
        for country, country_data in self.country_data.items():
            country_flows = country_data.flows.filter_by_local_admin_unit_id(
                self.inputs["local_admin_unit_ids"]
            ).copy()
            self.validate_flows(country, country_flows)
            country_flows["_flow_country"] = country
            parts.append(country_flows)
        flows = pd.concat(parts, ignore_index=True)

        # Some national files can contain the same cross-border flow. Keep the
        # row from the country preferred by the modeller.
        flows = keep_first_flow_source(
            flows,
            ["local_admin_unit_id_from", "local_admin_unit_id_to", "school_type"],
            self.inputs["country_priority_order"],
        )

        if self.inputs["local_admin_unit_ids"]:
            selected_ids = set(self.inputs["local_admin_unit_ids"])
            flows = flows[
                flows["local_admin_unit_id_from"].isin(selected_ids)
                | flows["local_admin_unit_id_to"].isin(selected_ids)
            ].copy()

        flows.to_parquet(self.cache_path)
        return flows

    @staticmethod
    def validate_flows(country: str, flows: pd.DataFrame) -> None:
        """Fail when country study flows cannot be used by StudyActivity."""
        required_columns = ["local_admin_unit_id_from", "local_admin_unit_id_to", "school_type", "n_students"]
        missing = [column for column in required_columns if column not in flows.columns]
        if missing:
            raise ValueError(
                f"Study flows for {country} are invalid. Missing columns: {missing}. "
                "See docs/source/add_country.md#study-data."
            )
