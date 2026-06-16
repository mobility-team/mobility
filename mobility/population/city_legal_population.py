import os
import pathlib
import logging

import pandas as pd
from mobility.runtime.assets.file_asset import FileAsset
from mobility.countries import normalize_country_codes
from mobility.population.countries import available_legal_population


class CityLegalPopulation(FileAsset):
    """Legal population by local admin unit."""

    def __init__(self, countries: list[str] | tuple[str, ...] | None = None):
        inputs = {"countries": normalize_country_codes(countries)}
        file_name = "insee_city_legal_population.parquet"
        cache_path = pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / "insee" / "legal_populations" / file_name
        super().__init__(inputs, cache_path)

    def get_cached_asset(self) -> pd.DataFrame:
        logging.info("Legal populations already prepared. Reusing the file : " + str(self.cache_path))
        return pd.read_parquet(self.cache_path)

    def create_and_get_asset(self) -> pd.DataFrame:
        legal_population_by_country = available_legal_population()
        countries = self.inputs["countries"] or list(legal_population_by_country)
        populations = []
        for country in countries:
            legal_population = legal_population_by_country.get(country)
            if legal_population is None:
                raise ValueError(f"Unsupported legal population country: {country}.")
            populations.append(legal_population().get())

        pop = pd.concat(populations)
        pop.to_parquet(self.cache_path)
        return pop
