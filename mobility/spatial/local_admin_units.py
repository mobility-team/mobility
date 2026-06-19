from __future__ import annotations

import logging
import os
import pathlib
import warnings

import geopandas as gpd
import pandas as pd

from mobility.countries import normalize_country_codes
from mobility.runtime.assets.file_asset import FileAsset
from mobility.spatial.countries import available_admin_units
from mobility.spatial.local_admin_units_categories import LocalAdminUnitsCategories


class LocalAdminUnits(FileAsset):
    """Local administrative units for the countries in the study area.

    Each country admin-unit file should provide the full table, selected IDs,
    and selected bounds through get(), get_by_ids() and get_within_bounds().
    """

    def __init__(
        self,
        countries: list[str] | tuple[str, ...] | None = None,
        local_admin_unit_ids: list[str] | tuple[str, ...] | None = None,
        bounds: tuple[float, float, float, float] | None = None,
        center_local_admin_unit: "LocalAdminUnits" | None = None,
        radius: float | None = None,
    ):
        countries = normalize_country_codes(countries)
        local_admin_unit_ids = self.normalize_local_admin_unit_ids(local_admin_unit_ids)
        bounds = self.normalize_bounds(bounds)
        admin_units_by_country = available_admin_units()
        selected_countries = countries or list(admin_units_by_country)
        selected_admin_units = {}
        for country in selected_countries:
            country_admin_units = admin_units_by_country.get(country)
            if country_admin_units is None:
                raise ValueError(f"Unsupported local admin unit country: {country}.")
            admin_units, admin_unit_level = country_admin_units
            selected_admin_units[country] = admin_units(
                level=admin_unit_level
            )

        inputs = {
            "version": 2,
            "countries": countries,
            "local_admin_unit_ids": local_admin_unit_ids,
            "bounds": bounds,
            "center_local_admin_unit": center_local_admin_unit,
            "radius": radius,
            "admin_units_by_country": selected_admin_units,
            "categories": LocalAdminUnitsCategories(countries=countries or None),
        }

        cache_path = pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / "local_admin_units.parquet"
        super().__init__(inputs, cache_path)

    def get_cached_asset(self) -> pd.DataFrame:
        logging.info("Local administrative units already prepared. Reusing the file : %s", self.cache_path)
        return gpd.read_parquet(self.cache_path)

    def create_and_get_asset(self) -> pd.DataFrame:
        logging.info("Preparing local administrative units.")

        bounds = self.inputs["bounds"]
        if self.inputs["center_local_admin_unit"] is not None:
            center_local_admin_unit = self.inputs["center_local_admin_unit"].get()
            bounds = self.get_radius_bounds(
                center_local_admin_unit,
                self.inputs["radius"],
            )

        parts = []
        for admin_units in self.inputs["admin_units_by_country"].values():
            if self.inputs["local_admin_unit_ids"]:
                country_admin_units = admin_units.get_by_ids(self.inputs["local_admin_unit_ids"])
            elif bounds is not None:
                country_admin_units = admin_units.get_within_bounds(bounds)
            else:
                country_admin_units = admin_units.get()

            if not country_admin_units.empty:
                parts.append(self.format_local_admin_units(country_admin_units))

        if len(parts) == 0:
            if self.inputs["local_admin_unit_ids"]:
                raise ValueError(
                    "No local admin unit found for: "
                    f"{self.inputs['local_admin_unit_ids']}."
                )
            raise ValueError("No local admin unit found within the study area bounds.")

        local_admin_units = pd.concat(parts, ignore_index=True)
        crs = local_admin_units.crs
        local_admin_unit_ids = local_admin_units["local_admin_unit_id"].tolist()
        local_admin_units = pd.merge(
            local_admin_units,
            self.inputs["categories"].get_by_ids(local_admin_unit_ids),
            on="local_admin_unit_id",
            how="left",
        )
        local_admin_units = gpd.GeoDataFrame(local_admin_units, geometry="geometry", crs=crs)
        missing_categories = local_admin_units[
            local_admin_units["urban_unit_category"].isna()
        ]["local_admin_unit_id"].tolist()
        if missing_categories:
            local_admin_units["urban_unit_category"] = local_admin_units["urban_unit_category"].fillna("R")
            warnings.warn(
                "No urban unit category found for local admin units, setting them to rural: "
                f"{sorted(missing_categories)}."
            )

        if self.inputs["local_admin_unit_ids"]:
            found_ids = set(local_admin_units["local_admin_unit_id"])
            missing_ids = sorted(set(self.inputs["local_admin_unit_ids"]) - found_ids)
            if missing_ids:
                raise ValueError(f"No local admin unit found for: {missing_ids}.")

        local_admin_units.to_parquet(self.cache_path)
        return local_admin_units

    @staticmethod
    def format_local_admin_units(admin_units):
        """Return admin units with the historical LocalAdminUnits columns."""
        admin_units = admin_units[["admin_id", "admin_name", "country", "geometry"]].copy()
        admin_units.columns = [
            "local_admin_unit_id",
            "local_admin_unit_name",
            "country",
            "geometry",
        ]
        return admin_units

    @staticmethod
    def normalize_local_admin_unit_ids(
        local_admin_unit_ids: list[str] | tuple[str, ...] | None,
    ) -> list[str]:
        """Return a stable list of requested local admin unit IDs."""
        if local_admin_unit_ids is None:
            return []
        return sorted(set(str(local_admin_unit_id) for local_admin_unit_id in local_admin_unit_ids))

    @staticmethod
    def normalize_bounds(
        bounds: tuple[float, float, float, float] | None,
    ) -> tuple[float, float, float, float] | None:
        """Return stable lon-lat bounds, or no bounds."""
        if bounds is None:
            return None
        if len(bounds) != 4:
            raise ValueError("Bounds should contain minx, miny, maxx and maxy.")
        return tuple(float(value) for value in bounds)

    @staticmethod
    def get_radius_bounds(
        center_local_admin_unit: gpd.GeoDataFrame,
        radius: float | None,
    ) -> tuple[float, float, float, float]:
        """Return lon-lat bounds around the centre local admin unit."""
        if center_local_admin_unit.empty:
            raise ValueError("No centre local admin unit found.")
        if radius is None:
            raise ValueError("A radius is needed to select nearby local admin units.")

        buffer = center_local_admin_unit.centroid.buffer(radius * 1000).iloc[0]
        bounds = gpd.GeoSeries([buffer], crs=center_local_admin_unit.crs).to_crs(4326).total_bounds
        return tuple(float(value) for value in bounds)
