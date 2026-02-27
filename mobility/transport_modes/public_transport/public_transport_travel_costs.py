import os
import pathlib
import logging
import json
import pandas as pd

from importlib import resources

from mobility.file_asset import FileAsset
from mobility.r_utils.r_script import RScript
from mobility.transport_zones import TransportZones
from mobility.transport_modes.public_transport.public_transport_graph import PublicTransportGraph, PublicTransportRoutingParameters
from mobility.transport_modes.public_transport.intermodal_transport_graph import IntermodalTransportGraph
from mobility.transport_modes.transport_mode import TransportMode
from mobility.transport_modes.modal_transfer import IntermodalTransfer
from mobility.transport_graphs import SimplifiedPathGraph, ContractedPathGraph

class PublicTransportTravelCosts(FileAsset):
    """
    A class for managing public transport travel costs calculations using GTFS files, inheriting from the FileAsset class.

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
        if first_modal_transfer is None or last_modal_transfer is None:
            raise ValueError(
                "PublicTransportTravelCosts requires both `first_modal_transfer` and `last_modal_transfer`."
            )

        intermodal_graph = IntermodalTransportGraph(
            transport_zones,
            parameters,
            first_leg_mode,
            last_leg_mode,
            first_modal_transfer,
            last_modal_transfer
        )

        inputs = {
            "intermodal_graph": intermodal_graph,
            "transport_zones": transport_zones,
            "first_modal_transfer": first_modal_transfer,
            "last_modal_transfer": last_modal_transfer,
            "parameters": parameters
        }

        file_name = (
            first_leg_mode.inputs["parameters"].name
            + "_public_transport_"
            + last_leg_mode.inputs["parameters"].name
            + "_travel_costs.parquet"
        )
        cache_path = pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"]) / file_name

        super().__init__(inputs, cache_path)

    def get_cached_asset(self, congestion: bool = False) -> pd.DataFrame:
        
        logging.info("Travel costs already prepared. Reusing the file : " + str(self.cache_path))
        costs = pd.read_parquet(self.cache_path)

        return costs

    def create_and_get_asset(self, congestion: bool = False) -> pd.DataFrame:
        
        costs = self.compute_travel_costs(
            self.inputs["transport_zones"],
            self.inputs["intermodal_graph"],
            self.inputs["first_modal_transfer"],
            self.inputs["last_modal_transfer"],
            self.inputs["parameters"]
        )
        
        costs.to_parquet(self.cache_path)

        return costs

    
    def compute_travel_costs(
            self,
            transport_zones: TransportZones,
            intermodal_graph: IntermodalTransportGraph,
            first_modal_transfer: IntermodalTransfer,
            last_modal_transfer: IntermodalTransfer,
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
                str(intermodal_graph.get()),
                json.dumps(first_modal_transfer.model_dump(mode="json")),
                json.dumps(last_modal_transfer.model_dump(mode="json")),
                json.dumps(parameters.model_dump(mode="json")),
                str(self.cache_path)
            ]
        )

        costs = pd.read_parquet(self.cache_path)

        return costs
    
    
    def update(self, od_flows):
        
        self.inputs["intermodal_graph"].update()
        self.create_and_get_asset()
        
    def audit_gtfs(self):
        return self.inputs["intermodal_graph"].audit_gtfs()
