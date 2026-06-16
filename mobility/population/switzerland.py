from __future__ import annotations

import logging
import os
import pathlib

import pandas as pd

from mobility.runtime.assets.file_asset import FileAsset
from mobility.runtime.io.download_file import download_file


class SwissCityLegalPopulation(FileAsset):
    """Swiss legal population by local admin unit."""

    def __init__(self):
        inputs = {}
        cache_path = (
            pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"])
            / "bfs"
            / "legal_populations"
            / "swiss_city_legal_population.parquet"
        )
        super().__init__(inputs, cache_path)

    def get_cached_asset(self) -> pd.DataFrame:
        logging.info("Swiss legal populations already prepared. Reusing the file : " + str(self.cache_path))
        return pd.read_parquet(self.cache_path)

    def create_and_get_asset(self) -> pd.DataFrame:
        url = "https://www.data.gouv.fr/fr/datasets/r/5529f7f8-7a00-4890-b453-0d215c7a5726"
        file_path = pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / "bfs" / "je-f-21.03.01.xlsx"

        download_file(url, file_path)

        pop = pd.read_excel(file_path)
        pop = pop.iloc[8:2180, [0, 2]]
        pop.columns = ["local_admin_unit_id", "n_pop_total"]
        pop["local_admin_unit_id"] = "ch-" + pop["local_admin_unit_id"].astype(int).astype(str)
        pop.columns = ["local_admin_unit_id", "legal_population"]
        pop.to_parquet(self.cache_path)
        return pop


class SwissPopulationGroups:
    """Swiss population groups used to sample individuals."""

    def __init__(self, switzerland_census):
        self.switzerland_census = switzerland_census

    def build(
        self,
        transport_zones,
        legal_pop_by_city: pd.DataFrame,
        lau_to_tz_coeff: pd.DataFrame,
    ) -> pd.DataFrame:
        if self.switzerland_census is None:
            raise ValueError(
                "Some transport zones are in Switzerland and no parser for the swiss census dataset was provided."
            )

        transport_zones = transport_zones[transport_zones["country"] == "ch"]

        census_data = self.switzerland_census.get()
        census_data = census_data.set_index("local_admin_unit_id")
        census_data = census_data.loc[transport_zones["local_admin_unit_id"]]
        census_data["pop_group_share"] = census_data["weight"] / census_data.groupby("local_admin_unit_id")["weight"].transform("sum")
        census_data = census_data.reset_index().drop(["individual_id", "weight"], axis=1)

        pop_groups = pd.merge(transport_zones, lau_to_tz_coeff, on=["transport_zone_id", "local_admin_unit_id"])
        pop_groups = pd.merge(pop_groups, census_data, on="local_admin_unit_id")
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
        pop_groups["country"] = "ch"
        return pop_groups
