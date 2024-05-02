import os
import pathlib
import logging
import pandas as pd
import numpy as np
import geopandas as gpd

from mobility.asset import Asset
from mobility.parsers import CityLegalPopulation

class Population(Asset):
    
    def __init__(self, transport_zones: gpd.GeoDataFrame, sample_size: int):
        
        legal_pop_by_city = CityLegalPopulation()
        
        inputs = {"transport_zones": transport_zones, "legal_pop_by_city": legal_pop_by_city, "sample_size": sample_size}

        file_name = "population.parquet"
        cache_path = pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"]) / file_name

        super().__init__(inputs, cache_path)
        
        
    def get_cached_asset(self) -> pd.DataFrame:

        logging.info("Population already prepared. Reusing the file : " + str(self.cache_path))
        population = pd.read_parquet(self.cache_path)

        return population
    
    def create_and_get_asset(self) -> pd.DataFrame:

        transport_zones = self.inputs["transport_zones"].get()
        legal_pop_by_city = self.inputs["legal_pop_by_city"].get()
        sample_size = self.inputs["sample_size"]
        
        population = pd.merge(
            transport_zones,
            legal_pop_by_city,
            left_on="admin_id",
            right_on="insee_city_id",
            how="left"
        )
        
        if population["legal_population"].isnull().any():
            logging.info(
                """
                    Could not associate legal populations to some of the 
                    transport zones (different INSEE COG versions ?). 
                    The population count of these transport zones will be set
                    to zero.
                """
            )
            population["legal_population"].fillna(0, inplace=True)
            
        
        population["n_persons"] = sample_size*population["legal_population"]/population["legal_population"].sum()
        population["n_persons"] = np.ceil(population["n_persons"])
        population["n_persons"] = population["n_persons"].astype(int)
        population["n_persons"] = np.maximum(population["n_persons"], 1)
        
        population.to_parquet(self.cache_path)

        return population