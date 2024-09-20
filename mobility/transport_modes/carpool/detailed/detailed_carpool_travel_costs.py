import pathlib
import os
import logging
import pandas as pd
import numpy as np

from importlib import resources

from mobility.asset import Asset
from mobility.r_utils.r_script import RScript
from mobility.path_travel_costs import PathTravelCosts
from mobility.transport_modes.carpool.detailed.detailed_carpool_parameters import DetailedCarpoolParameters

class DetailedCarpoolTravelCosts(Asset):

    def __init__(
            self,
            car_travel_costs: PathTravelCosts,
            parameters: DetailedCarpoolParameters
        ):

        inputs = {
            "car_travel_costs": car_travel_costs,
            "parameters": parameters
        }

        file_name = "detailed_carpool" + str(parameters.number_persons) + "_travel_costs.parquet"
        cache_path = pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"]) / file_name

        super().__init__(inputs, cache_path)

    def get_cached_asset(self) -> pd.DataFrame:

        logging.info("Travel costs already prepared. Reusing the file : " + str(self.cache_path))
        costs = pd.read_parquet(self.cache_path)
        costs["mode"] = self.inputs["parameters"].name

        return costs

    def create_and_get_asset(self) -> pd.DataFrame:
        
        logging.info("Preparing carpool travel costs for " + str(self.inputs["parameters"].number_persons) + " occupants...")
        
        costs = self.compute_travel_costs()
        costs.to_parquet(self.cache_path)
        costs["mode"] = self.inputs["parameters"].name

        return costs

    def compute_travel_costs(self) -> pd.DataFrame:
        
        costs = self.inputs["car_travel_costs"].get()
        params = self.inputs["parameters"]
        transport_zones = self.inputs["car_travel_costs"].inputs["transport_zones"].get()
        
        logging.info("Computing carpool travel costs for " + str(params.number_persons) + " occupants...")
        
        script = RScript(resources.files('mobility.transport_modes.carpool').joinpath('compute_carpool_travel_costs.R'))
        
        script.run(
            args=[
                str(transport_zones.cache_path),
                self.car_travel_costs.graph,
                params.parking_locations,
                str(self.cache_path)
            ]
        )

        costs = pd.read_parquet(self.cache_path)
        
        costs = pd.merge(
            costs,
            transport_zones[["transport_zone_id", "local_admin_unit_id", "country"]].rename({"transport_zone_id": "from"}, axis=1).set_index("from"),
            on="from"
        )
        
        costs = pd.merge(
            costs,
            transport_zones[["transport_zone_id", "local_admin_unit_id", "country"]].rename({"transport_zone_id": "to"}, axis=1).set_index("to"),
            on="to",
            suffixes=["_from", "_to"]
        )
        
        # Compute the cost of time based on travelled distance
        ct = params.cost_of_time_c0_short
        ct = np.where(costs["distance"] > 5, params.cost_of_time_c0 + params.cost_of_time_c1*costs["distance"], ct)
        ct = np.where(costs["distance"] > 20, 30.2 + 0.017*costs["distance"], ct)
        ct = np.where(costs["distance"] > 80, 37.0, ct)
        ct *= 1.17 # Inflation coeff
        
        # Apply coefficients by country of origin
        ct = np.where(costs["country_from"] == "fr", ct*params.cost_of_time_country_coeff_fr, ct)
        ct = np.where(costs["country_to"] == "ch", ct*params.cost_of_time_country_coeff_ch, ct)
        
        # Compute revenues        
        revenues_distance = np.where(
            costs["local_admin_unit_id_from"].isin(params.revenue_distance_local_admin_units_ids),
            params.revenue_distance_r0 + params.revenue_distance_r1*costs["distance"],
            0.0
        )
        revenues_distance = np.minimum(revenues_distance, params.revenue_distance_max)
        
        revenues_passenger = np.where(
            costs["local_admin_unit_id_from"].isin(params.revenue_passengers_local_admin_units_ids),
            params.revenue_passengers_r1*params.number_persons,
            0.0
        )
        
        # Add all cost and revenues components
        costs["cost"] = ct*costs["time"]*2
        costs["cost"] += params.cost_of_distance*costs["distance"]*2
        costs["cost"] += params.cost_constant
        costs["cost"] -= revenues_distance + revenues_passenger
        
        return costs