import os
import pathlib
import logging
import pandas as pd
import geopandas as gpd

from mobility.asset import Asset

from mobility.travel_costs import TravelCosts
from mobility.public_transport_travel_costs import PublicTransportTravelCosts
from mobility.carpool_travel_costs import CarpoolTravelCosts

class MultimodalTravelCosts(Asset):
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
            public_transport_start_time_min: float = 6.5,
            public_transport_start_time_max: float = 7.5,
            public_transport_max_traveltime: float = 1.0,
            public_transport_additional_gtfs_files: list = None
        ):
        """
        Retrieves travel costs for the covered modes if they already exist for these transport zones and parameters,
        otherwise calculates them.
        
        Expected running time : between a few seconds and a few minutes.
        
        Args:
            transport_zones (gpd.GeoDataFrame): GeoDataFrame containing transport zone geometries.
            public_transport_start_time_min : float containing the start hour to consider for public transport cost determination
            start_time_max : float containing the end hour to consider for public transport cost determination, should be superior to start_time_min
            public_transport_start_time_max : float with the maximum travel time to consider for public transport, in hours
            public_transport_max_traveltime : list of additional GTFS files to include in the calculations

        """        
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

        carpool2_travel_costs = CarpoolTravelCosts(car_travel_costs, 2)
        carpool3_travel_costs = CarpoolTravelCosts(car_travel_costs, 3)
        carpool4_travel_costs = CarpoolTravelCosts(car_travel_costs, 4)

        inputs = {
            "car_travel_costs": car_travel_costs,
            "walk_travel_costs": walk_travel_costs,
            "bicycle_travel_costs": bicycle_travel_costs,
            "pub_trans_travel_costs": pub_trans_travel_costs,
            "carpool2_travel_costs": carpool2_travel_costs,
            "carpool3_travel_costs": carpool3_travel_costs,
            "carpool4_travel_costs": carpool4_travel_costs
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
        carpool2 = self.inputs["carpool2_travel_costs"].get()
        carpool3 = self.inputs["carpool3_travel_costs"].get()
        carpool4 = self.inputs["carpool4_travel_costs"].get()
        
        # BUG (distances underestimated for mode car for intra-zones travels)
        # Force it to be higher or equal to the the distance by feet and keep the same speed
        for orig in list(set(car["from"])):
            mask_car = (car["from"] == orig) & (car["to"] == orig)
            mask_walk = (walk["from"] == orig) & (walk["to"] == orig)
            car_speed = car.loc[mask_car, "distance"]/car.loc[mask_car, "time"]
            dist_walk = walk.loc[mask_walk, "distance"]
            if not dist_walk.empty:
                car.loc[mask_car, "distance"] = car.loc[mask_car, "distance"].apply(lambda x: max(dist_walk.iloc[0], x))
                car.loc[mask_car, "time"] = car.loc[mask_car, "distance"]/car_speed


        costs = self.aggregate_travel_costs(car, walk, bicycle, pub_trans, carpool2, carpool3, carpool4)
        costs.to_parquet(self.cache_path)

        return costs
    
    
    def aggregate_travel_costs(
            self, car: pd.DataFrame, walk: pd.DataFrame, 
            bicycle: pd.DataFrame, pub_trans: pd.DataFrame,
            carpool2: pd.DataFrame, carpool3: pd.DataFrame,
            carpool4: pd.DataFrame
        ):
        
        logging.info("Aggregating travel costs between transport zones...")
        
        car["mode"] = "car"
        walk["mode"] = "walk"
        bicycle["mode"] = "bicycle"
        carpool2["mode"] = "carpool2"
        carpool3["mode"] = "carpool3"
        carpool4["mode"] = "carpool4"
        
        # BUG (check if still there now that gtfs_router has been improved ?)
        # Fix public transport times to only have one row per OD pair
        # (should be fixed in PublicTransportTravelCosts !)
        pub_trans = pub_trans.sort_values(["from", "to", "time"])
        pub_trans = pub_trans.groupby(["from", "to"], as_index=False).first()
        
        costs = pd.concat([
            car,
            walk,
            bicycle,
            pub_trans,
            carpool2,
            carpool3,
            carpool4
        ])
        
        # BUG (check if still there now that we use cpprouting ?)
        # Remove null costs that might occur
        # (should be fixed in TravelCosts !)
        costs = costs[(~costs["time"].isnull()) & (~costs["distance"].isnull())]
        
        costs["from"] = costs["from"].astype(int)
        costs["to"] = costs["to"].astype(int)
        
        return costs
        