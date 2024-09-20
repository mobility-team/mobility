import os
import pathlib
import logging
import pandas as pd
import geopandas as gpd

from typing import List

from mobility.asset import Asset
from mobility.transport_modes.transport_mode import TransportMode

class MultiModalTravelCosts(Asset):
    """
    Class to compute the travel costs for the covered modes using dedicated classes:
        - TravelCosts for modes car, walk and bicycle
        - PublicTransportTravelCosts for public transport
        - CarpoolTravelCosts for carpool (2, 3 or 4 persons in the car)
    
    This class is responsible for creating, caching, and retrieving multimodal travel costs based on specified transport zones and travel modes.

    """
    
    def __init__(self, modes: List[TransportMode]):
        """
        Retrieves travel costs for the covered modes if they already exist for these transport zones and parameters,
        otherwise calculates them.
        
        Expected running time : between a few seconds and a few minutes.
        

        """
        
        inputs = {mode.name + "_travel_costs": mode.travel_costs for mode in modes}
        file_name = "multimodal_travel_costs.parquet"
        cache_path = pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"]) / file_name

        super().__init__(inputs, cache_path)
        
        
    def get_cached_asset(self) -> pd.DataFrame:

        logging.info("Multimodal travel costs prepared. Reusing the file : " + str(self.cache_path))
        prob = pd.read_parquet(self.cache_path)

        return prob
    
    def create_and_get_asset(self) -> pd.DataFrame:
        
        logging.info("Aggregating travel costs...")
        
        costs = {k: travel_costs.get() for k, travel_costs in self.inputs.items() if "_travel_costs" in k}
        costs = pd.concat([v for v in costs.values()])
        costs["from"] = costs["from"].astype(int)
        costs["to"] = costs["to"].astype(int)
        
        costs.to_parquet(self.cache_path)

        return costs
    