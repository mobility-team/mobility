import logging
import os
import pathlib

import pandas as pd

from mobility.countries import normalize_country_codes
from mobility.runtime.assets.file_asset import FileAsset
from mobility.spatial.countries import available_local_admin_unit_categories


class LocalAdminUnitsCategories(FileAsset):
    """Urban unit categories for the countries in the study area."""

    def __init__(self, countries: list[str] | tuple[str, ...] | None = None):
        countries = normalize_country_codes(countries)
        categories_by_country = available_local_admin_unit_categories()
        selected_countries = countries or list(categories_by_country)
        selected_categories = {}
        for country in selected_countries:
            country_categories = categories_by_country.get(country)
            if country_categories is None:
                raise ValueError(f"Unsupported local admin unit category country: {country}.")
            selected_categories[country] = country_categories

        inputs = {
            "countries": countries,
            "categories_by_country": selected_categories,
        }
        cache_path = (
            pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"])
            / "local_admin_units_categories.parquet"
        )
        super().__init__(inputs, cache_path)

    def get_cached_asset(self) -> pd.DataFrame:
        logging.info("Local administrative unit categories already prepared. Reusing %s.", self.cache_path)
        return pd.read_parquet(self.cache_path)

    def get_by_ids(self, local_admin_unit_ids: list[str]) -> pd.DataFrame:
        """Return categories for the requested local admin units."""
        selected_ids = sorted(set(str(local_admin_unit_id) for local_admin_unit_id in local_admin_unit_ids))
        categories = []
        for country_categories in self.inputs["categories_by_country"].values():
            country_categories = country_categories.get_by_ids(selected_ids)
            if not country_categories.empty:
                categories.append(country_categories)

        if len(categories) == 0:
            return pd.DataFrame(columns=["local_admin_unit_id", "urban_unit_category"])

        return pd.concat(categories, ignore_index=True)

    def create_and_get_asset(self) -> pd.DataFrame:
        logging.info("Preparing local administrative unit categories.")

        categories = []
        for country_categories in self.inputs["categories_by_country"].values():
            categories.append(country_categories.get())

        local_admin_units = pd.concat(categories, ignore_index=True)
        local_admin_units.to_parquet(self.cache_path)
        return local_admin_units
