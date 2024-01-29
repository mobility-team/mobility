import os
import pathlib
import logging
import pandas as pd
import geopandas as gpd

from mobility.parsers.osm import OSMData
from mobility.asset import Asset
from mobility.r_script import RScript

class TravelCosts(Asset):
    """
    A class for managing travel cost calculations using OpenStreetMap (OSM) data, inheriting from the Asset class.

    This class is responsible for creating, caching, and retrieving travel costs based on specified transport zones and travel modes.

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

    def __init__(self, transport_zones: gpd.GeoDataFrame, mode: str):
        """
        Initializes a TravelCosts object with the given transport zones and travel mode.

        Args:
            transport_zones (gpd.GeoDataFrame): GeoDataFrame defining the transport zones.
            mode (str): Mode of transportation for calculating travel costs.
        """

        self.dodgr_modes = {"car": "motorcar", "bicycle": "bicycle", "walk": "foot"}
        
        available_modes = list(self.dodgr_modes.keys())
        if mode not in available_modes:
            raise ValueError(
                "Mode '" + mode + "' is not available. Available options are : " \
                + ", ".join(available_modes) + "."
            )

        osm = OSMData(transport_zones, list(self.dodgr_modes.values()))
        inputs = {"transport_zones": transport_zones, "osm": osm, "mode": mode}

        file_name = "dodgr_travel_costs_" + mode + ".parquet"
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
        Creates and retrieves travel costs based on the current inputs.

        Returns:
            pd.DataFrame: A DataFrame of calculated travel costs.
        """
        transport_zones = self.inputs["transport_zones"]
        mode = self.inputs["mode"]
        
        osm = self.inputs["osm"]
        graph = self.dodgr_graph(transport_zones, osm, mode)
        costs = self.dodgr_costs(transport_zones, graph)

        return costs

    def dodgr_graph(self, transport_zones: gpd.GeoDataFrame, osm: str, mode: str) -> str:
        """
        Creates a routable graph for the specified mode of transportation using dodgr.

        Args:
            transport_zones (gpd.GeoDataFrame): GeoDataFrame containing transport zone geometries.
            osm (str): Path to the OSM data file.
            mode (str): Mode of transportation for which the graph is created.

        Returns:
            str: The file path to the saved routable graph.
        """

        dodgr_mode = self.dodgr_modes[mode]

        output_file_name = "dodgr_graph_" + dodgr_mode + ".rds"
        output_file_path = pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"]) / output_file_name

        logging.info("Creating a routable graph with dodgr, this might take a while...")

        script = RScript(pathlib.Path(__file__).parent / "prepare_dodgr_graph.R")

        script.run(args=[str(transport_zones.cache_path), str(osm.cache_path), dodgr_mode, output_file_path])

        return output_file_path

    def dodgr_costs(self, transport_zones: gpd.GeoDataFrame, graph: str) -> pd.DataFrame:
        """
        Calculates travel costs for the specified mode of transportation using the created graph.

        Args:
            transport_zones (gpd.GeoDataFrame): GeoDataFrame containing transport zone geometries.
            graph (str): Path to the routable graph file.

        Returns:
            pd.DataFrame: A DataFrame containing calculated travel costs.
        """

        logging.info("Computing travel costs...")

        script = RScript(pathlib.Path(__file__).parent / "prepare_dodgr_costs.R")

        script.run(args=[str(transport_zones.cache_path), graph, str(self.cache_path)])

        costs = pd.read_parquet(self.cache_path)

        return costs
    
