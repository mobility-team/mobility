import pathlib
import os
import logging
import pandas as pd
import numpy as np

from mobility.asset import Asset
from mobility.path_travel_costs import PathTravelCosts
from mobility.transport_modes.carpool.carpool_parameters import CarpoolParameters

class CarpoolTravelCosts(Asset):
    """
    A class for computing carpooling travel cost using car travel costs, inheriting from the Asset class.

    This class is responsible for creating, caching, and retrieving carpool travel costs based on specified transport zones and a number of persons in the vehicule.

    Attributes:
        car_travel_costs (MultimodalTravelCosts): The travel costs for the different modes (excluding )
        number_persons (int): number of persons in the vehicule.
        absolute_delay_per_passenger (int): absolute delay per supplementary passenger, in minutes. Default: 5
        relative_delay_per_passenger (float) : relative delay per supplementary passenger in proportion of the total travel time. Default: 0.05
        absolute_extra_distance_per_passenger (float): absolute extra distance per supplementary passenger, in km. Default: 1
        relative_extra_distance_per_passenger (flaot) : relative extra distance per supplementary passenger in proportion of the total distance. Default: 0.05
        

    Methods:
        get_cached_asset: Retrieve a cached DataFrame of travel costs.
        create_and_get_asset: Calculate and retrieve travel costs based on the current inputs.
    """

    def __init__(
            self,
            car_travel_costs: PathTravelCosts,
            parameters: CarpoolParameters
        ):
        """
        Initializes a CarpoolTravelCosts object with the given transport zones, travel mode and parameters.

        """

        inputs = {
            "car_travel_costs": car_travel_costs,
            "parameters": parameters
        }

        file_name = "carpool" + str(parameters.number_persons) + "_travel_costs.parquet"
        cache_path = pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"]) / file_name

        super().__init__(inputs, cache_path)

    def get_cached_asset(self) -> pd.DataFrame:
        """
        Retrieves the travel costs DataFrame from the cache.

        Returns:
            pd.DataFrame: The cached DataFrame of travel costs.
        """

        logging.info("Travel costs already prepared. Reusing the file : " + str(self.cache_path))
        costs = pd.read_parquet(self.cache_path)
        costs["mode"] = self.inputs["parameters"].name

        return costs

    def create_and_get_asset(self) -> pd.DataFrame:
        """
        Creates and retrieves carpool travel costs based on the current inputs.

        Returns:
            pd.DataFrame: A DataFrame of calculated carpool travel costs.
        """ 
        
        logging.info("Preparing carpool travel costs for " + str(self.inputs["parameters"].number_persons) + " occupants...")
        
        costs = self.compute_travel_costs()
        costs.to_parquet(self.cache_path)
        costs["mode"] = self.inputs["parameters"].name

        return costs

    def compute_travel_costs(self) -> pd.DataFrame:
        """
        Calculates carpool travel costs for the specified number of occupants in the vehicule.

        Args:
            car_travel_costs (MultimodalTravelCosts): The travel costs for the different modes (excluding )
            number_persons: number of persons in the vehicule.

        Returns:
            pd.DataFrame: A DataFrame containing calculated travel costs.
        """
        
        costs = self.inputs["car_travel_costs"].get()
        params = self.inputs["parameters"]
        transport_zones = self.inputs["car_travel_costs"].inputs["transport_zones"].get()
        
        logging.info("Computing carpool travel costs for " + str(params.number_persons) + " occupants...")
        print(costs)
        
        costs = pd.merge(
            costs,
            transport_zones[["transport_zone_id", "local_admin_unit_id", "country"]].rename({"transport_zone_id": "from"}, axis=1).set_index("from"),
            on="from"
            )
        print(costs)
        
        costs = pd.merge(
            costs,
            transport_zones[["transport_zone_id", "local_admin_unit_id", "country"]].rename({"transport_zone_id": "to"}, axis=1).set_index("to"),
            on="to",
            suffixes=["_from", "_to"]
        )
        
        # Adding a delay of 5min (1km) and 5% of the travel time (resp. distance) per passenger
        costs["time"] += (params.number_persons-1)*(params.relative_delay_per_passenger*costs["time"] + params.absolute_delay_per_passenger/60)
        costs["distance"] += (params.number_persons-1)*(params.relative_extra_distance_per_passenger*costs["distance"] + params.absolute_extra_distance_per_passenger)
        
        # Compute the cost of time based on travelled distance
        ct = params.cost_of_time_c0_short
        ct = np.where(costs["distance"] > 5, params.cost_of_time_c0 + params.cost_of_time_c1*costs["distance"], ct)
        ct = np.where(costs["distance"] > 20, 30.2 + 0.017*costs["distance"], ct)
        ct = np.where(costs["distance"] > 80, 37.0, ct)
        ct *= 1.17 # Inflation coeff
        
        # Apply coefficients by country of origin
        ct = np.where(costs["country_from"] == "fr", ct*params.cost_of_time_country_coeff_fr, ct)
        ct = np.where(costs["country_to"] == "ch", ct*params.cost_of_time_country_coeff_ch, ct)
        
        # Apply coefficients by OD
        for ct_od_coeffs in params.cost_of_time_od_coeffs:
            ct = np.where(
                (costs["local_admin_unit_id_from"].isin(ct_od_coeffs["local_admin_unit_id_from"])) &
                (costs["local_admin_unit_id_to"].isin(ct_od_coeffs["local_admin_unit_id_to"])),
                ct*ct_od_coeffs["coeff"],
                ct
            )
        
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