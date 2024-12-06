import os
import pathlib
import logging
import json
import pandas as pd
import geopandas as gpd
import numpy as np

from importlib import resources
from dataclasses import asdict

from mobility.file_asset import FileAsset
from mobility.r_utils.r_script import RScript
from mobility.transport_zones import TransportZones
from mobility.transport_modes.public_transport.gtfs_router import GTFSRouter
from mobility.transport_modes.public_transport.public_transport_routing_parameters import PublicTransportRoutingParameters
from mobility.path_travel_costs import PathTravelCosts
from mobility.transport_modes import TransportMode
from mobility.transport_modes.modal_shift import ModalShift

class PublicTransportGraph(FileAsset):
    """
    A class for managing public transport travel costs calculations using GTFS files, inheriting from the Asset class.

    This class is responsible for creating, caching, and retrieving public transport travel costs 
    based on specified transport zones and travel modes.
    
    Uses GTFS files that have been prepared by TransportZones, but a list of additional GTFS files
    (representing a project for instance) can be provided.
    """

    def __init__(
            self,
            transport_zones: TransportZones,
            parameters: PublicTransportRoutingParameters = PublicTransportRoutingParameters()
    ):
        """
        Retrieves public transport travel costs if they already exist for these transport zones and parameters,
        otherwise calculates them.
        
        Expected running time : between a few seconds and a few minutes.
        
        Args:
            transport_zones (gpd.GeoDataFrame): GeoDataFrame containing transport zone geometries.
            gtfs_router : GTFSRouter object containing data about public transport routes and schedules.
            start_time_min : float containing the start hour to consider for cost determination
            start_time_max : float containing the end hour to consider for cost determination, should be superior to start_time_min
            max_traveltime : float with the maximum travel time to consider for public transport, in hours
            additional_gtfs_files : list of additional GTFS files to include in the calculations

        """
        
        gtfs_router = GTFSRouter(transport_zones, parameters.additional_gtfs_files)

        inputs = {
            "transport_zones": transport_zones,
            "gtfs_router": gtfs_router,
            "parameters": parameters
        }

        file_name = "public_transport_graph/simplified/done"
        cache_path = pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"]) / file_name

        super().__init__(inputs, cache_path)

    def get_cached_asset(self) -> pd.DataFrame:
        logging.info("Graph already prepared. Reusing the file : " + str(self.cache_path))
        return self.cache_path

    def create_and_get_asset(self) -> pd.DataFrame:
        
        self.gtfs_graph(
            self.inputs["transport_zones"],
            self.inputs["gtfs_router"],
            self.inputs["parameters"]
        )

        return self.cache_path

    
    def gtfs_graph(
            self,
            transport_zones: TransportZones,
            gtfs_router: GTFSRouter,
            parameters: PublicTransportRoutingParameters
        ) -> pd.DataFrame:
        """
        Calculates travel costs for public transport between transport zones.

        Args:
            transport_zones (gpd.GeoDataFrame): GeoDataFrame containing transport zone geometries.
            gtfs_router : GTFSRouter object containing data about public transport routes and schedules.
            start_time_min : float containing the start hour to consider for cost determination
            start_time_max : float containing the end hour to consider for cost determination, should be superior to start_time_min
            max_traveltime : float with the maximum travel time to consider for public transport, in hours

        Returns:
            pd.DataFrame: A DataFrame containing calculated public transport travel costs.
        """

        logging.info("Computing public transport travel costs...")
        
        script = RScript(resources.files('mobility.transport_modes.public_transport').joinpath('prepare_public_transport_graph.R'))
        
        script.run(
            args=[
                str(transport_zones.cache_path),
                str(gtfs_router.get()),
                json.dumps(asdict(parameters)),
                str(self.cache_path)
            ]
        )

        return None
    
