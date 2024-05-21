import os
import pathlib
import logging
import pandas as pd
import geopandas as gpd

from importlib import resources
from mobility.asset import Asset
from mobility.r_script import RScript
from mobility.gtfs_router import GTFSRouter
from mobility.transport_zones import TransportZones

class PublicTransportTravelCosts(Asset):


    def __init__(self, transport_zones: TransportZones):
        
        gtfs_router = GTFSRouter(transport_zones)

        inputs = {"transport_zones": transport_zones, "gtfs_router": gtfs_router}

        file_name = "public_transport_travel_costs.parquet"
        cache_path = pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"]) / file_name

        super().__init__(inputs, cache_path)

    def get_cached_asset(self) -> pd.DataFrame:
        
        logging.info("Travel costs already prepared. Reusing the file : " + str(self.cache_path))
        costs = pd.read_parquet(self.cache_path)

        return costs

    def create_and_get_asset(self) -> pd.DataFrame:

        transport_zones = self.inputs["transport_zones"]
        gtfs_router = self.inputs["gtfs_router"]
        costs = self.gtfs_router_costs(transport_zones, gtfs_router)

        return costs

    
    def gtfs_router_costs(self, transport_zones: gpd.GeoDataFrame, gtfs_router: GTFSRouter) -> pd.DataFrame:
        """
        Calculates travel costs for public transport between transport zones.

        Args:
            transport_zones (gpd.GeoDataFrame): GeoDataFrame containing transport zone geometries.
            gtfs (GTFS): GTFS object containing data about public transport routes and schedules.

        Returns:
            pd.DataFrame: A DataFrame containing calculated public transport travel costs.
        """

        logging.info("Computing public transport travel costs...")
        
        script = RScript(resources.files('mobility.R').joinpath('prepare_public_transport_costs.R'))
        
        gtfs_route_types_path = resources.files("mobility").joinpath('data/gtfs/gtfs_route_types.xlsx')
        
        gtfs_router.get()
        
        script.run(
            args=[
                str(transport_zones.cache_path),
                gtfs_router.cache_path,
                str(gtfs_route_types_path),
                str(self.cache_path)
            ]
        )

        costs = pd.read_parquet(self.cache_path)

        return costs
    
