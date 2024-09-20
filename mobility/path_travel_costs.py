import os
import pathlib
import logging
import pandas as pd
import numpy as np
import geopandas as gpd

from importlib import resources
from mobility.path_graph import PathGraph
from mobility.asset import Asset
from mobility.r_utils.r_script import RScript
from mobility.parameters import ModeParameters
from mobility.transport_zones import TransportZones

class PathTravelCosts(Asset):
    """
    A class for managing travel cost calculations for certain modes using OpenStreetMap (OSM) data, inheriting from the Asset class.

    This class is responsible for creating, caching, and retrieving travel costs for modes car, walk, and bicycle,
    based on specified transport zones and travel modes.

    Attributes:
        dodgr_modes (dict): Mapping of general travel modes to specific dodgr package modes.
        transport_zones (gpd.GeoDataFrame): The geographical areas for which travel costs are calculated.
        mode (str): The mode of transportation used for calculating travel costs.
        gtfs (GTFS): GTFS object containing data about public transport routes and schedules.

    Methods:
        get_cached_asset: Retrieve a cached DataFrame of travel costs.
        create_and_get_asset: Calculate and retrieve travel costs based on the current inputs.
        dodgr_graph: Create a routable graph for the specified mode of transportation.
        dodgr_costs: Calculate travel costs using the generated graph.
    """

    def __init__(self, transport_zones: gpd.GeoDataFrame, mode_parameters: ModeParameters):
        """
        Initializes a TravelCosts object with the given transport zones and travel mode.

        Args:
            transport_zones (gpd.GeoDataFrame): GeoDataFrame defining the transport zones.
            mode (str): Mode of transportation for calculating travel costs.
        """

        path_graph = PathGraph(transport_zones, mode_parameters)
        
        inputs = {
            "transport_zones": transport_zones,
            "mode_parameters": mode_parameters,
            "simplified_path_graph": path_graph.simplified,
            "contracted_path_graph": path_graph.contracted
        }

        file_name = "dodgr_travel_costs_" + mode_parameters.name + ".parquet"
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
        costs["mode"] = self.mode_parameters.name

        return costs

    def create_and_get_asset(self) -> pd.DataFrame:
        """
        Creates and retrieves travel costs based on the current inputs.

        Returns:
            pd.DataFrame: A DataFrame of calculated travel costs.
        """
        
        mode = self.mode_parameters.name
        
        logging.info("Preparing travel costs for mode " + mode)
        
        self.transport_zones.get()
        self.contracted_path_graph.get()
        
        costs = self.compute_costs_by_OD(self.transport_zones, self.contracted_path_graph)
        costs["mode"] = mode
        
        costs.to_parquet(self.cache_path)

        return costs



    def compute_costs_by_OD(self, transport_zones: TransportZones, path_graph: PathGraph) -> pd.DataFrame:
        """
        Calculates travel costs for the specified mode of transportation using the created graph.

        Args:
            transport_zones (gpd.GeoDataFrame): GeoDataFrame containing transport zone geometries.
            graph (str): Path to the routable graph file.

        Returns:
            pd.DataFrame: A DataFrame containing calculated travel costs.
        """

        logging.info("Computing travel times and distances by OD...")
        
        script = RScript(resources.files('mobility.r_utils').joinpath('prepare_dodgr_costs.R'))
        script.run(
            args=[
                str(transport_zones.cache_path),
                str(path_graph.cache_path),
                str(self.mode_parameters.routing_max_speed),
                str(self.mode_parameters.routing_max_time),
                str(self.cache_path)
            ]
        )

        costs = pd.read_parquet(self.cache_path)
        
        params = self.mode_parameters
        
        logging.info("Computing generalized cost by OD...")
        
        # Compute the cost of time based on travelled distance
        ct = params.cost_of_time_c0_short
        ct = np.where(costs["distance"] > 5, params.cost_of_time_c0 + params.cost_of_time_c1*costs["distance"], ct)
        ct = np.where(costs["distance"] > 20, 30.2 + 0.017*costs["distance"], ct)
        ct = np.where(costs["distance"] > 80, 37.0, ct)
        ct *= 1.17 # Inflation coeff
           
        # Add all cost and revenues components
        costs["cost"] = ct*costs["time"]*2
        costs["cost"] += params.cost_of_distance*costs["distance"]*2
        costs["cost"] += params.cost_constant

        return costs
    
    
