import os
import pathlib
import logging
import pandas as pd
import geopandas as gpd

from mobility.asset import Asset

from mobility.travel_costs import TravelCosts
from mobility.public_transport_travel_costs import PublicTransportTravelCosts

class MultimodalTravelCosts(Asset):
    
    def __init__(
            self,
            transport_zones: gpd.GeoDataFrame,
            public_transport_start_time_min: float = 6.5,
            public_transport_start_time_max: float = 7.5,
            public_transport_max_traveltime: float = 1.0,
            public_transport_additional_gtfs_files: list = None
        ):
        
        car_travel_costs = TravelCosts(transport_zones, "car")
        walk_travel_costs = TravelCosts(transport_zones, "walk")
        bicycle_travel_costs = TravelCosts(transport_zones, "bicycle")
        
        pub_trans_travel_costs = PublicTransportTravelCosts(
            transport_zones,
            public_transport_start_time_min,
            public_transport_start_time_max,
            public_transport_max_traveltime,
            public_transport_additional_gtfs_files
        )
        
        inputs = {
            "car_travel_costs": car_travel_costs,
            "walk_travel_costs": walk_travel_costs,
            "bicycle_travel_costs": bicycle_travel_costs,
            "pub_trans_travel_costs": pub_trans_travel_costs
        }

        file_name = "multimodal_travel_costs.parquet"
        cache_path = pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"]) / file_name

        super().__init__(inputs, cache_path)
        
        
    def get_cached_asset(self) -> pd.DataFrame:

        logging.info("Multimodal travel costs prepared. Reusing the file : " + str(self.cache_path))
        prob = pd.read_parquet(self.cache_path)

        return prob
    
    def create_and_get_asset(self) -> pd.DataFrame:
        
        logging.info("Aggregating travel costs...")
        
        car = self.inputs["car_travel_costs"].get()
        walk = self.inputs["walk_travel_costs"].get()
        bicycle = self.inputs["bicycle_travel_costs"].get()
        pub_trans = self.inputs["pub_trans_travel_costs"].get()
        
        costs = self.aggregate_travel_costs(car, walk, bicycle, pub_trans)
        costs.to_parquet(self.cache_path)

        return costs
    
    
    def aggregate_travel_costs(
            self, car: pd.DataFrame, walk: pd.DataFrame, 
            bicycle: pd.DataFrame, pub_trans: pd.DataFrame
        ):
        
        logging.info("Aggregating travel costs between transport zones...")
        
        car["mode"] = "car"
        walk["mode"] = "walk"
        bicycle["mode"] = "bicycle"
        
        # BUG
        # Fix public transport times to only have one row per OD pair
        # (should be fixed in PublicTransportTravelCosts !)
        pub_trans = pub_trans.sort_values(["from", "to", "time"])
        pub_trans = pub_trans.groupby(["from", "to"], as_index=False).first()
        
        costs = pd.concat([
            car,
            walk,
            bicycle,
            pub_trans
        ])
        
        # BUG
        # Remove null costs that might occur
        # (should be fixed in TravelCosts !)
        costs = costs[(~costs["time"].isnull()) & (~costs["distance"].isnull())]
        
        costs["from"] = costs["from"].astype(int)
        costs["to"] = costs["to"].astype(int)
        
        return costs
        