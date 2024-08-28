import os
import pathlib
import logging
import pandas as pd
import geopandas as gpd

from importlib import resources
from mobility.asset import Asset
from mobility.r_utils.r_script import RScript
from mobility.transport_zones import TransportZones
from mobility.transport_modes.public_transport.gtfs_router import GTFSRouter
from mobility.transport_modes.public_transport.public_transport_parameters import PublicTransportParameters

class PublicTransportTravelCosts(Asset):
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
            parameters: PublicTransportParameters
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

        file_name = "public_transport_travel_costs.parquet"
        cache_path = pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"]) / file_name

        super().__init__(inputs, cache_path)

    def get_cached_asset(self) -> pd.DataFrame:
        
        logging.info("Travel costs already prepared. Reusing the file : " + str(self.cache_path))
        costs = pd.read_parquet(self.cache_path)

        return costs

    def create_and_get_asset(self) -> pd.DataFrame:
        
        costs = self.gtfs_router_costs(
            self.inputs["transport_zones"],
            self.inputs["gtfs_router"],
            self.inputs["parameters"]
        )

        return costs

    
    def gtfs_router_costs(
            self,
            transport_zones: gpd.GeoDataFrame,
            gtfs_router: GTFSRouter,
            parameters: PublicTransportParameters
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
        
        script = RScript(resources.files('mobility.r_utils').joinpath('prepare_public_transport_costs.R'))
        
        gtfs_route_types_path = resources.files("mobility").joinpath('data/gtfs/gtfs_route_types.xlsx')
        
        gtfs_router.get()
        
        script.run(
            args=[
                str(transport_zones.cache_path),
                gtfs_router.cache_path,
                str(gtfs_route_types_path),
                str(parameters.start_time_min),
                str(parameters.start_time_max),
                str(parameters.max_traveltime),
                str(self.cache_path)
            ]
        )

        costs = pd.read_parquet(self.cache_path)

        return costs
    
