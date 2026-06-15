import os
import pathlib
import logging
import pandas as pd

from mobility.activities.opportunity_data import missing_countries, normalize_local_admin_unit_ids
from mobility.activities.shopping.countries import available_shopping_data
from mobility.countries import normalize_country_codes
from mobility.runtime.assets.file_asset import FileAsset


class ShoppingOpportunities(FileAsset):
    """Shopping opportunities for the selected study area."""

    def __init__(
        self,
        countries: list[str] | tuple[str, ...] | None = None,
        local_admin_unit_ids: list[str] | tuple[str, ...] | None = None,
    ):
        countries = normalize_country_codes(countries)
        local_admin_unit_ids = normalize_local_admin_unit_ids(local_admin_unit_ids)
        shopping_by_country = available_shopping_data()
        selected_countries = countries or list(shopping_by_country)
        country_data = {
            country: shopping_by_country[country]
            for country in selected_countries
            if country in shopping_by_country
        }

        inputs = {
            "countries": countries,
            "local_admin_unit_ids": local_admin_unit_ids,
        }

        cache_path = {
            "shopping_opportunities": (
                pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"])
                / "insee"
                / "shopping_opportunities.parquet"
            )
        }

        super().__init__(inputs, cache_path)
        self.country_data = country_data

    def get_cached_asset(self) -> pd.DataFrame:
        """Reuse prepared shopping opportunities."""
        logging.info(f"Using cached shopping opportunities from: {self.cache_path['shopping_opportunities']}")

        return pd.read_parquet(self.cache_path["shopping_opportunities"])

    def create_and_get_asset(self) -> pd.DataFrame:
        """Prepare shopping opportunities for the selected local admin units."""
        countries = self.inputs["countries"] or list(available_shopping_data())
        unsupported_countries = missing_countries(countries, available_shopping_data())
        if unsupported_countries:
            raise ValueError(f"Shopping opportunities are not available for countries: {unsupported_countries}.")

        parts = []
        for country, country_data in self.country_data.items():
            country_shops = country_data.opportunities.filter_by_local_admin_unit_id(
                self.inputs["local_admin_unit_ids"],
            )
            self.validate_opportunities(country, country_shops)
            parts.append(country_shops)

        shopping_opportunities = pd.concat(parts)
        shopping_opportunities = shopping_opportunities.dropna(subset=["local_admin_unit_id"])
        if self.inputs["local_admin_unit_ids"]:
            shopping_opportunities = shopping_opportunities[
                shopping_opportunities["local_admin_unit_id"].isin(self.inputs["local_admin_unit_ids"])
            ].copy()
        shopping_opportunities.to_parquet(self.cache_path["shopping_opportunities"])

        return shopping_opportunities

    @staticmethod
    def validate_opportunities(country: str, shops: pd.DataFrame) -> None:
        """Fail when country shopping opportunities cannot be used by ShopActivity."""
        required_columns = ["local_admin_unit_id", "lon", "lat", "turnover"]
        missing = [column for column in required_columns if column not in shops.columns]
        if missing:
            raise ValueError(
                f"Shopping opportunities for {country} are invalid. Missing columns: {missing}. "
                "See docs/source/add_country.md#shopping-data."
            )
