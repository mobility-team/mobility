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
    
    def __init__(
            self,
            transport_zones: gpd.GeoDataFrame,
            modes: List[TransportMode]
        ):
        """
        Retrieves travel costs for the covered modes if they already exist for these transport zones and parameters,
        otherwise calculates them.
        
        Expected running time : between a few seconds and a few minutes.
        
        Args:
            transport_zones (gpd.GeoDataFrame): GeoDataFrame containing transport zone geometries.

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
        
        costs = self.aggregate_travel_costs()
        
        # BUG (distances underestimated for mode car for intra-zones travels)
        # Force it to be higher or equal to the the distance by feet and keep the same speed
        if "car_travel_costs" in self.inputs.keys() and "walk_travel_costs" in self.inputs.keys():
            car = costs[costs["mode"] == "car"]
            walk = costs[costs["mode"] == "walk"]
            for orig in list(set(car["from"])):
                mask_car = (car["from"] == orig) & (car["to"] == orig)
                mask_walk = (walk["from"] == orig) & (walk["to"] == orig)
                car_speed = car.loc[mask_car, "distance"]/car.loc[mask_car, "time"]
                dist_walk = walk.loc[mask_walk, "distance"]
                if not dist_walk.empty:
                    car.loc[mask_car, "distance"] = car.loc[mask_car, "distance"].apply(lambda x: max(dist_walk.iloc[0], x))
                    car.loc[mask_car, "time"] = car.loc[mask_car, "distance"]/car_speed
        
        costs.to_parquet(self.cache_path)

        return costs
    
    
    def aggregate_travel_costs(self):
        
        logging.info("Aggregating travel costs between transport zones...")
        
        costs = {k: travel_costs.get() for k, travel_costs in self.inputs.items() if "_travel_costs" in k}
        
        # BUG (check if still there now that gtfs_router has been improved ?)
        # Fix public transport times to only have one row per OD pair
        # (should be fixed in PublicTransportTravelCosts !)
        if "public_transport" in costs.keys():
            pub_trans = costs["public_transport_travel_costs"]
            pub_trans = pub_trans.sort_values(["from", "to", "time"])
            pub_trans = pub_trans.groupby(["from", "to"], as_index=False).first()
            costs["public_transport_travel_costs"] = pub_trans
        
        costs = pd.concat([v for v in costs.values()])
        
        # BUG (check if still there now that we use cpprouting ?)
        # Remove null costs that might occur
        # (should be fixed in TravelCosts !)
        costs = costs[(~costs["time"].isnull()) & (~costs["distance"].isnull())]
        
        costs["from"] = costs["from"].astype(int)
        costs["to"] = costs["to"].astype(int)
        
        return costs
        