import pathlib
import os
import logging
import pandas as pd

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
            name: str,
            parameters: CarpoolParameters
        ):
        """
        Initializes a CarpoolTravelCosts object with the given transport zones, travel mode and parameters.

        """
        
        self.name = name

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
        costs["mode"] = self.name

        return costs

    def create_and_get_asset(self) -> pd.DataFrame:
        """
        Creates and retrieves carpool travel costs based on the current inputs.

        Returns:
            pd.DataFrame: A DataFrame of calculated carpool travel costs.
        """ 
        
        logging.info("Preparing carpool travel costs for " + str(self.inputs["parameters"].number_persons) + " occupants...")
        
        costs = self.compute_carpool_costs(
            self.inputs["car_travel_costs"].get(),
            self.inputs["parameters"].number_persons,
            self.inputs["parameters"].absolute_delay_per_passenger,
            self.inputs["parameters"].relative_delay_per_passenger,
            self.inputs["parameters"].absolute_delay_per_passenger,
            self.inputs["parameters"].relative_extra_distance_per_passenger
        )
        
        costs.to_parquet(self.cache_path)
        
        costs["mode"] = self.name

        return costs

    def compute_carpool_costs(
            self,
            car_travel_costs: pd.DataFrame,
            number_persons: int,
            absolute_delay_per_passenger: int,
            relative_delay_per_passenger: float,
            absolute_extra_distance_per_passenger: float,
            relative_extra_distance_per_passenger: float
        ) -> pd.DataFrame:
        """
        Calculates carpool travel costs for the specified number of occupants in the vehicule.

        Args:
            car_travel_costs (MultimodalTravelCosts): The travel costs for the different modes (excluding )
            number_persons: number of persons in the vehicule.

        Returns:
            pd.DataFrame: A DataFrame containing calculated travel costs.
        """

        logging.info("Computing carpool travel costs for " + str(number_persons) + " occupants...")
        
        costs = car_travel_costs.copy()
        
        # Adding a delay of 5min (1km) and 5% of the travel time (resp. distance) per passenger
        costs["time"] += (number_persons-1)*(relative_delay_per_passenger*costs["time"] + absolute_delay_per_passenger/60)
        costs["distance"] += (number_persons-1)*(relative_extra_distance_per_passenger*costs["distance"] + absolute_extra_distance_per_passenger)

        return costs