import os
import pathlib
import logging
import pandas as pd

from mobility.activities.opportunity_data import (
    complete_country_priority_order,
    keep_first_flow_source,
    missing_countries,
    normalize_local_admin_unit_ids,
)
from mobility.activities.work.countries import available_work_data
from mobility.runtime.assets.file_asset import FileAsset
from mobility.countries import normalize_country_codes


class WorkFlows(FileAsset):
    """Home-work flows for the selected study area."""

    def __init__(
        self,
        countries: list[str] | tuple[str, ...] | None = None,
        local_admin_unit_ids: list[str] | tuple[str, ...] | None = None,
        country_priority_order: list[str] | tuple[str, ...] | None = None,
    ):
        countries = normalize_country_codes(countries)
        local_admin_unit_ids = normalize_local_admin_unit_ids(local_admin_unit_ids)
        work_by_country = available_work_data()
        selected_countries = countries or list(work_by_country)
        country_priority_order = complete_country_priority_order(
            selected_countries,
            normalize_country_codes(country_priority_order) or selected_countries,
        )
        country_data = {
            country: work_by_country[country]
            for country in selected_countries
            if country in work_by_country
        }

        inputs = {
            "countries": countries,
            "local_admin_unit_ids": local_admin_unit_ids,
            "country_priority_order": country_priority_order,
        }
        cache_path = pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / "insee" / "work_flows.parquet"
        super().__init__(inputs, cache_path)
        self.country_data = country_data

    def get_cached_asset(self) -> pd.DataFrame:
        logging.info("Work flows already prepared. Reusing the file: " + str(self.cache_path))
        return pd.read_parquet(self.cache_path)

    def create_and_get_asset(self) -> pd.DataFrame:
        logging.info("Preparing work flows.")

        countries = self.inputs["countries"] or list(available_work_data())
        unsupported_countries = missing_countries(countries, available_work_data())
        if unsupported_countries:
            raise ValueError(f"Work flow data is not available for countries: {unsupported_countries}.")

        flows = []
        for country, country_data in self.country_data.items():
            country_flows = country_data.flows.filter_by_local_admin_unit_id(
                self.inputs["local_admin_unit_ids"]
            ).copy()
            self.validate_flows(country, country_flows)
            country_flows["_flow_country"] = country
            flows.append(country_flows)

        flows = pd.concat(flows, ignore_index=True)

        # Some national files can contain the same cross-border flow. Keep the
        # row from the country preferred by the modeller.
        flows = keep_first_flow_source(
            flows,
            ["local_admin_unit_id_from", "local_admin_unit_id_to", "mode"],
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
        """Fail when country work flows cannot be used by WorkActivity."""
        required_columns = ["local_admin_unit_id_from", "local_admin_unit_id_to", "mode", "ref_flow_volume"]
        missing = [column for column in required_columns if column not in flows.columns]
        if missing:
            raise ValueError(
                f"Work flows for {country} are invalid. Missing columns: {missing}. "
                "See docs/source/add_country.md#work-data."
            )
