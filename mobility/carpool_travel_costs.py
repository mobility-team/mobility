import os
import pathlib
import logging
import pandas as pd

from mobility.asset import Asset
from mobility.travel_costs import TravelCosts

class CarpoolTravelCosts(Asset):
    """
    A class for computing carpooling travel cost using car travel costs, inheriting from the Asset class.

    This class is responsible for creating, caching, and retrieving carpool travel costs based on specified transport zones and a number of persons in the vehicule.

    Attributes:
        car_travel_costs (MultimodalTravelCosts): The travel costs for the different modes (excluding )
        nb_occupant: number of persons in the vehicule.

    Methods:
        get_cached_asset: Retrieve a cached DataFrame of travel costs.
        create_and_get_asset: Calculate and retrieve travel costs based on the current inputs.
    """

    def __init__(self, car_travel_costs: TravelCosts, nb_occupant: int):
        """
        Initializes a CarpoolTravelCosts object with the given transport zones and travel mode.

        """

        inputs = {"car_travel_costs": car_travel_costs, "nb_occupant": nb_occupant}

        file_name = "carpool" + str(nb_occupant) + "_travel_costs.parquet"
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

        return costs

    def create_and_get_asset(self) -> pd.DataFrame:
        """
        Creates and retrieves carpool travel costs based on the current inputs.

        Returns:
            pd.DataFrame: A DataFrame of calculated carpool travel costs.
        """
        car_travel_costs = self.inputs["car_travel_costs"].get()
        nb_occupant = self.inputs["nb_occupant"]
        
        logging.info("Preparing carpool travel costs for " + str(nb_occupant) + " occupants...")
        
        costs = self.compute_carpool_costs(car_travel_costs, nb_occupant)
        
        costs.to_parquet(self.cache_path)

        return costs

    def compute_carpool_costs(self, car_travel_costs: pd.DataFrame, nb_occupant: int) -> pd.DataFrame:
        """
        Calculates carpool travel costs for the specified number of occupants in the vehicule.

        Args:
            car_travel_costs (MultimodalTravelCosts): The travel costs for the different modes (excluding )
            nb_occupant: number of persons in the vehicule.

        Returns:
            pd.DataFrame: A DataFrame containing calculated travel costs.
        """

        logging.info("Computing carpool travel costs for " + str(nb_occupant) + " occupants...")
        
        costs = car_travel_costs.copy()
        
        # Adding a delay of 5min (1km) and 5% of the travel time (resp. distance) per passenger
        costs["time"] += (nb_occupant-1)*(0.05*costs["time"] + 5/60)
        costs["distance"] += (nb_occupant-1)*(0.05*costs["distance"] + 1)

        return costs
    
