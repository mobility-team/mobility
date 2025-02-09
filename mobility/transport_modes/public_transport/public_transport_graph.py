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
from .gtfs.gtfs_router import GTFSRouter
from mobility.transport_modes.public_transport.public_transport_routing_parameters import PublicTransportRoutingParameters
from mobility.path_travel_costs import PathTravelCosts
from mobility.transport_modes import TransportMode
from mobility.transport_modes.modal_transfer import IntermodalTransfer

class PublicTransportGraph(FileAsset):
    """
    A class for managing public transport travel costs calculations using GTFS files, inheriting from the FileAsset class.

    This class is responsible for creating, caching, and retrieving public transport travel costs 
    based on specified transport zones and travel modes, using a R script managed by the gtfs_graph method.
    
    Uses GTFS files that have been prepared by TransportZones, but a list of additional GTFS files
    (representing a project for instance) can be provided.
    
    Args:
        transport_zones (gpd.GeoDataFrame): GeoDataFrame containing transport zone geometries.
        parameters: PublicTransportRoutingParameters. Will use standard parameters by default
    """

    def __init__(
            self,
            transport_zones: TransportZones,
            parameters: PublicTransportRoutingParameters = PublicTransportRoutingParameters()
    ):
        
        gtfs_router = GTFSRouter(
            transport_zones,
            parameters.additional_gtfs_files,
            parameters.expected_agencies
        )

        inputs = {
            "transport_zones": transport_zones,
            "gtfs_router": gtfs_router,
            "parameters": parameters
        }

        file_name = "public_transport_graph/simplified/public-transport-graph"
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
        Calculates travel costs for public transport between transport zones using the R script prepare_public_transport_graph.R

        Args:
            transport_zones (gpd.GeoDataFrame): GeoDataFrame containing transport zone geometries.
            gtfs_router : GTFSRouter object containing data about public transport routes and schedules.
            parameters: PublicTransportRoutingParameters

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
    

    
    
