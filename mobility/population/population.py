from __future__ import annotations

import logging
import os
import pathlib

import numpy as np
import pandas as pd
import shortuuid
from pydantic import BaseModel, ConfigDict, Field
from typing import Annotated

from mobility.countries import normalize_country_codes
from mobility.population.census_localized_individuals import CensusLocalizedIndividuals
from mobility.population.city_legal_population import CityLegalPopulation
from mobility.population.countries import available_population_groups
from mobility.runtime.assets.file_asset import FileAsset
from mobility.spatial.admin_units import FrenchAdminUnits


class Population(FileAsset):
    """Sample a synthetic population for the area."""

    def __init__(
        self,
        transport_zones,
        sample_size: int | None = None,
        switzerland_census: CensusLocalizedIndividuals = None,
        parameters: "PopulationParameters" | None = None,
    ):
        parameters = self.prepare_parameters(
            parameters=parameters,
            parameters_cls=PopulationParameters,
            explicit_args={"sample_size": sample_size},
            required_fields=["sample_size"],
            owner_name="Population",
        )

        inputs = {
            "transport_zones": transport_zones,
            "parameters": parameters,
            "switzerland_census": switzerland_census,
        }
        cache_path = {
            "individuals": pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"]) / "individuals.parquet",
            "population_groups": pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"]) / "population_groups.parquet",
        }
        super().__init__(inputs, cache_path)

    def get_cached_asset(self) -> pd.DataFrame:
        logging.info("Population already prepared. Reusing the files : " + str(self.cache_path))
        return self.cache_path

    def create_and_get_asset(self) -> pd.DataFrame:
        transport_zones = self.inputs["transport_zones"].get()
        if "country" not in transport_zones.columns:
            if hasattr(self.inputs["transport_zones"], "study_area"):
                study_area = self.inputs["transport_zones"].study_area.get()[["local_admin_unit_id", "country"]]
                transport_zones = transport_zones.merge(study_area, on="local_admin_unit_id", how="left")
            else:
                raise ValueError("Population requires a transport_zones table with a country column.")
        if transport_zones["country"].isna().any():
            raise ValueError("Population transport zones should contain a country for every row.")

        country_codes = normalize_country_codes(self.inputs["transport_zones"].countries)
        if not country_codes:
            raise ValueError("Population requires at least one country in the transport zones.")
        legal_pop_by_city = CityLegalPopulation(country_codes).get()

        lau_to_tz_coeff = (
            transport_zones[["transport_zone_id", "local_admin_unit_id", "weight"]]
            .rename({"weight": "lau_to_tz_coeff"}, axis=1)
        )

        pop_groups = []
        population_groups_by_country = available_population_groups(self, CensusLocalizedIndividuals)
        for country in country_codes:
            population_groups = population_groups_by_country.get(country)
            if population_groups is None:
                raise ValueError(f"Population does not know how to build country {country}.")
            pop_groups.append(
                population_groups.build(
                    transport_zones,
                    legal_pop_by_city,
                    lau_to_tz_coeff,
                )
            )

        pop_groups = pd.concat(pop_groups)

        sample_sizes = self.get_sample_sizes(
            pop_groups=pop_groups,
            sample_size=self.inputs["parameters"].sample_size,
        )
        sample_sizes = sample_sizes.set_index("transport_zone_id")["n_persons"].to_dict()

        individuals = (
            pop_groups.groupby("transport_zone_id", group_keys=False)
            .apply(
                lambda g: (
                    g.assign(transport_zone_id=g.name)
                    .sample(n=sample_sizes[g.name], weights="weight")
                ),
                include_groups=False,
            )
            .reset_index(drop=True)
        )

        individuals["individual_id"] = [shortuuid.uuid() for _ in range(individuals.shape[0])]
        individuals = individuals[
            [
                "individual_id",
                "transport_zone_id",
                "age",
                "socio_pro_category",
                "ref_pers_socio_pro_category",
                "n_pers_household",
                "country",
                "n_cars",
            ]
        ]

        individuals.to_parquet(self.cache_path["individuals"])
        pop_groups.to_parquet(self.cache_path["population_groups"])
        return self.cache_path

    def get_sample_sizes(
        self,
        pop_groups: pd.DataFrame | None = None,
        sample_size: int | None = None,
        lau_to_tz_coeff: pd.DataFrame | None = None,
    ):
        logging.info("Computing the number of individuals in each transport zone given the global sample size...")
        if sample_size is None:
            raise ValueError("sample_size is required.")

        if pop_groups is None:
            if lau_to_tz_coeff is None:
                raise ValueError("pop_groups or lau_to_tz_coeff is required.")
            legal_pop_by_city = CityLegalPopulation().get()
            population = pd.merge(
                lau_to_tz_coeff,
                legal_pop_by_city,
                on="local_admin_unit_id",
                how="left",
            )
            population["legal_population"] = population["legal_population"].fillna(0)
            population = population.groupby("transport_zone_id", as_index=False).agg(
                {
                    "local_admin_unit_id": "first",
                    "legal_population": "sum",
                }
            )
        else:
            population = pop_groups.groupby("transport_zone_id", as_index=False)["weight"].sum()
            population.rename({"weight": "legal_population"}, axis=1, inplace=True)
            population["local_admin_unit_id"] = population["transport_zone_id"]

        if population.empty:
            raise ValueError("No population rows are available to sample.")

        total_legal_population = population["legal_population"].sum()
        if total_legal_population <= 0:
            population["n_persons"] = 1
            return population

        population["n_persons"] = (
            sample_size
            * population["legal_population"].pow(0.5)
            / population["legal_population"].pow(0.5).sum()
        )
        population["n_persons"] = np.ceil(population["n_persons"])
        population["n_persons"] = population["n_persons"].astype(int)
        population["n_persons"] = np.maximum(population["n_persons"], 1)
        sampling_rate = population["n_persons"].sum() / total_legal_population
        logging.info("Global sampling rate : " + str(round(10000 * sampling_rate) / 10000) + " %.")
        return population

class PopulationParameters(BaseModel):
    """Parameters controlling population sampling."""

    model_config = ConfigDict(extra="forbid")

    sample_size: Annotated[
        int,
        Field(
            ge=1,
            title="Population sample size",
            description="Number of inhabitants to sample within the selected transport zones.",
        ),
    ]
