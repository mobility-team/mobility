import os
import pathlib
import logging
import json
import pandas as pd

from importlib import resources
from dataclasses import asdict

from mobility.file_asset import FileAsset
from mobility.r_utils.r_script import RScript
from mobility.transport_zones import TransportZones
from mobility.transport_modes.public_transport.public_transport_graph import PublicTransportGraph
from mobility.transport_modes.public_transport.public_transport_routing_parameters import PublicTransportRoutingParameters
from mobility.transport_modes import TransportMode
from mobility.transport_modes.modal_shift import ModalShift
from mobility.path_graph import SimplifiedPathGraph

class PublicTransportTravelCosts(FileAsset):
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
            parameters: PublicTransportRoutingParameters,
            first_leg_mode: TransportMode,
            last_leg_mode: TransportMode,
            first_modal_shift: ModalShift = None,
            last_modal_shift: ModalShift = None
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
        

        public_transport_graph = PublicTransportGraph(transport_zones, parameters)

        inputs = {
            "transport_zones": transport_zones,
            "public_transport_graph": public_transport_graph,
            "first_leg_graph": first_leg_mode.travel_costs.contracted_path_graph,
            "last_leg_graph": last_leg_mode.travel_costs.contracted_path_graph,
            "first_modal_shift": first_modal_shift,
            "last_modal_shift": last_modal_shift,
            "parameters": parameters
        }
        
        self.first_leg_mode = first_leg_mode
        self.last_leg_mode = last_leg_mode

        file_name = "public_transport_travel_costs.parquet"
        cache_path = pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"]) / file_name

        super().__init__(inputs, cache_path)

    def get_cached_asset(self) -> pd.DataFrame:
        
        logging.info("Travel costs already prepared. Reusing the file : " + str(self.cache_path))
        costs = pd.read_parquet(self.cache_path)

        return costs

    def create_and_get_asset(self) -> pd.DataFrame:
        
        costs = self.compute_travel_costs(
            self.inputs["transport_zones"],
            self.inputs["public_transport_graph"],
            self.inputs["first_leg_graph"],
            self.inputs["last_leg_graph"],
            self.inputs["first_modal_shift"],
            self.inputs["last_modal_shift"],
            self.inputs["parameters"]
        )
        
        costs.to_parquet(self.cache_path)

        return costs

    
    def  compute_travel_costs(
            self,
            transport_zones: TransportZones,
            public_transport_graph: PublicTransportGraph,
            first_leg_graph: SimplifiedPathGraph,
            last_leg_graph: SimplifiedPathGraph,
            first_modal_shift: ModalShift,
            last_modal_shift: ModalShift,
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
        
        script = RScript(resources.files('mobility.transport_modes.public_transport').joinpath('compute_intermodal_public_transport_travel_costs.R'))
        
        script.run(
            args=[
                str(transport_zones.cache_path),
                str(public_transport_graph.get()),
                str(first_leg_graph.get()),
                str(last_leg_graph.get()),
                json.dumps(asdict(first_modal_shift)),
                json.dumps(asdict(last_modal_shift)),
                str(self.cache_path)
            ]
        )

        costs = pd.read_parquet(self.cache_path)

        return costs
    
