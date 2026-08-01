from __future__ import annotations

import numpy as np
import pandas as pd
import os
import pathlib

from mobility.runtime.assets.file_asset import FileAsset
from mobility.runtime.io.download_file import download_file
from mobility.spatial.admin_units import GermanAdminUnits


class GermanCityLegalPopulation(FileAsset):
    """German legal population by local admin unit."""


    def __init__(self):
        inputs = {}
        cache_path = (
            pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"])
            / "bkg"
            / "legal_populations"
            / "german_city_legal_population.parquet"
        )
        super().__init__(inputs, cache_path)

    def create_and_get_asset(self) -> pd.DataFrame:
        folder = "data/germany"

        pop = pd.read_csv(
            f"{folder}/legal_population_germany.csv",
            sep=",",
            usecols=["COM", "PTOT"],
            dtype={"COM": str, "PTOT": np.int32},
        )
        pop.columns = ["local_admin_unit_id", "legal_population"]
        pop["local_admin_unit_id"] = "de-" + pop["local_admin_unit_id"]
        pop.to_parquet(self.cache_path)
        return pop

    def get_cached_asset(self) -> pd.DataFrame:
        return pd.read_parquet(self.cache_path)

class GermanPopulationGroups:
    """German population groups used to sample individuals."""

    
    def __init__(self):
        # self.census_localized_individuals = census_localized_individuals
        return

    def build(
        self,
        transport_zones,
        legal_pop_by_city: pd.DataFrame,
        lau_to_tz_coeff: pd.DataFrame,
    ) -> pd.DataFrame:
        folder = "data/germany"
        pop = pd.read_csv(
            f"{folder}/synthetic_population.csv",
            sep=",",
            usecols=["COM", "age", "socio_pro_category"],
            dtype={"COM": str, "age": np.int32, "socio_pro_category": np.int32},
        )
        
        n = len(pop)
        rand = np.random.default_rng(seed=42)

        german_transport_zone = transport_zones[transport_zones["country"] == "de"],
            
        pop_groups = pd.DataFrame({
            "local_admin_unit_id": "de-" + pop["COM"],
            "age": pop["age"],
            "socio_pro_category": pop["socio_pro_category"],
            "ref_pers_socio_pro_category": pop["socio_pro_category"],
            "n_pers_household": rand.integers(1, 5, size=n),  # 1 à 4
            "n_cars": rand.integers(0, 2, size=n),            # 0 ou 1
            "weight": [1.0]*n,
            "country": ["de"]*n,
        })
        pop_groups = pd.merge(pop_groups, legal_pop_by_city, on="local_admin_unit_id")
        pop_groups = pd.merge(pop_groups, german_transport_zone, on="local_admin_unit_id")
        return pop_groups
