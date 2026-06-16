from __future__ import annotations

import logging
import os
import pathlib
import zipfile

import geopandas as gpd
import numpy as np
import pandas as pd

from mobility.runtime.assets.file_asset import FileAsset
from mobility.runtime.io.download_file import download_file
from mobility.spatial.admin_units import FrenchAdminUnits


class FrenchCityLegalPopulation(FileAsset):
    """French legal population by local admin unit."""

    def __init__(self):
        inputs = {}
        cache_path = (
            pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"])
            / "insee"
            / "legal_populations"
            / "french_city_legal_population.parquet"
        )
        super().__init__(inputs, cache_path)

    def get_cached_asset(self) -> pd.DataFrame:
        logging.info("French legal populations already prepared. Reusing the file : " + str(self.cache_path))
        return pd.read_parquet(self.cache_path)

    def create_and_get_asset(self) -> pd.DataFrame:
        url = "https://www.data.gouv.fr/fr/datasets/r/1443e7dc-3e22-4961-aad6-84fdb2c9d429"
        folder = pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / "insee" / "legal_populations"
        folder.mkdir(parents=True, exist_ok=True)
        path = folder / "city_legal_populations.zip"
        download_file(url, path)

        with zipfile.ZipFile(path, "r") as zip_ref:
            zip_ref.extractall(folder)

        pop = pd.read_csv(
            folder / "donnees_communes.csv",
            sep=";",
            usecols=["COM", "PTOT"],
            dtype={"COM": str, "PTOT": np.int32},
        )
        pop.columns = ["local_admin_unit_id", "legal_population"]
        pop["local_admin_unit_id"] = "fr-" + pop["local_admin_unit_id"]
        pop.to_parquet(self.cache_path)
        return pop


class FrenchPopulationGroups:
    """French population groups used to sample individuals."""

    def __init__(self, census_localized_individuals):
        self.census_localized_individuals = census_localized_individuals

    def build(
        self,
        transport_zones,
        legal_pop_by_city: pd.DataFrame,
        lau_to_tz_coeff: pd.DataFrame,
    ) -> pd.DataFrame:
        regions = FrenchAdminUnits.get_population_region_boundaries()
        transport_zones = transport_zones[transport_zones["country"] == "fr"]
        transport_zones = gpd.sjoin(transport_zones, regions[["INSEE_REG", "geometry"]], predicate="intersects")
        transport_zones_regions = transport_zones["INSEE_REG"].drop_duplicates().tolist()

        cantons = FrenchAdminUnits.get_population_commune_boundaries()
        cantons = cantons[["INSEE_COM", "INSEE_CAN"]]
        cantons.columns = ["local_admin_unit_id", "CANTVILLE"]

        census_data = [
            self.census_localized_individuals(tz_region).get()
            for tz_region in transport_zones_regions
        ]
        census_data = pd.concat(census_data)
        census_data.set_index(["CANTVILLE"], inplace=True)
        census_data["pop_group_share"] = census_data["weight"] / census_data.groupby("CANTVILLE")["weight"].transform("sum")
        census_data = census_data.reset_index().drop(["weight"], axis=1)

        pop_groups = pd.merge(transport_zones, lau_to_tz_coeff, on=["transport_zone_id", "local_admin_unit_id"])
        pop_groups = pd.merge(pop_groups, cantons, on="local_admin_unit_id")
        pop_groups = pd.merge(pop_groups, census_data, on="CANTVILLE")
        pop_groups = pd.merge(pop_groups, legal_pop_by_city, on="local_admin_unit_id")
        pop_groups["weight"] = (
            pop_groups["legal_population"]
            * pop_groups["lau_to_tz_coeff"]
            * pop_groups["pop_group_share"]
        )

        pop_groups = pop_groups[
            [
                "transport_zone_id",
                "local_admin_unit_id",
                "age",
                "socio_pro_category",
                "ref_pers_socio_pro_category",
                "n_pers_household",
                "n_cars",
                "weight",
            ]
        ]
        pop_groups["country"] = "fr"
        return pop_groups
