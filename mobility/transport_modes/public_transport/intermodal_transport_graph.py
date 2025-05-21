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
from mobility.transport_modes.public_transport.public_transport_routing_parameters import PublicTransportRoutingParameters
from mobility.transport_modes.transport_mode import TransportMode
from mobility.transport_modes.modal_transfer import IntermodalTransfer
from mobility.transport_modes.public_transport.public_transport_graph import PublicTransportGraph
from mobility.transport_graphs.path_graph import ContractedPathGraph

class IntermodalTransportGraph(FileAsset):
    """
    A class for managing intermodal transport travel costs calculations using GTFS files, inheriting from the FileAsset class.
    A first leg mode (like walk or car) enables to reach the public transport network.
    A last leg mode enables to reach destination from the public transport network. 

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
            first_modal_transfer: IntermodalTransfer = None,
            last_modal_transfer: IntermodalTransfer = None
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
            "first_modal_transfer": first_modal_transfer,
            "last_modal_transfer": last_modal_transfer,
            "parameters": parameters
        }
        
        self.first_leg_mode = first_leg_mode
        self.last_leg_mode = last_leg_mode

        file_name = first_leg_mode.name + "_public_transport_" + last_leg_mode.name + "_intermodal_transport_graph/simplified/done"
        cache_path = pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"]) / file_name

        super().__init__(inputs, cache_path)

    def get_cached_asset(self) -> pd.DataFrame:
        logging.info("Intermodal graph already created. Reusing the file : " + str(self.cache_path))
        return self.cache_path

    def create_and_get_asset(self) -> pd.DataFrame:
        
        self.prepare_intermodal_graph(
            self.inputs["transport_zones"],
            self.inputs["public_transport_graph"],
            self.inputs["first_leg_graph"],
            self.inputs["last_leg_graph"],
            self.inputs["first_modal_transfer"],
            self.inputs["last_modal_transfer"],
            self.inputs["parameters"]
        )

        return self.cache_path

    
    def prepare_intermodal_graph(
            self,
            transport_zones: TransportZones,
            public_transport_graph: PublicTransportGraph,
            first_leg_graph: ContractedPathGraph,
            last_leg_graph: ContractedPathGraph,
            first_modal_transfer: IntermodalTransfer,
            last_modal_transfer: IntermodalTransfer,
            parameters: PublicTransportRoutingParameters
        ) -> pd.DataFrame:
        """
        Calculates intermodal travel costs between transport zones. Uses the R script called prepare_intermodal_public_transport_graph.R

        Args:
            transport_zones (gpd.GeoDataFrame): GeoDataFrame containing transport zone geometries.
            public_transport_graph (PublicTransportGraph): public transport part of the intermodal graph
            first_leg_graph: graph for the first leg mode
            last_leg_graph: graph for the last leg mode
            first_modal_transfer: transfer parameters between the first mode and public transport
            last_modal_transfer: transfer parameters between public transport and the last mode
            parameters: PublicTransportRoutingParameters


        Returns:
            None, but a file is prepared
        """

        logging.info("Computing public transport travel costs...")
        
        script = RScript(resources.files('mobility.transport_modes.public_transport').joinpath('prepare_intermodal_public_transport_graph.R'))
        
        script.run(
            args=[
                str(transport_zones.cache_path),
                str(public_transport_graph.get()),
                str(first_leg_graph.get()),
                str(last_leg_graph.get()),
                json.dumps(asdict(first_modal_transfer)),
                json.dumps(asdict(last_modal_transfer)),
                json.dumps(asdict(parameters)),
                str(self.cache_path)
            ]
        )

        return None
    

    def update(self):
        
        self.create_and_get_asset()
