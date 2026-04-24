import os
import pathlib
import logging
import json
import pandas as pd

from importlib import resources

from mobility.runtime.assets.file_asset import FileAsset
from mobility.runtime.r_integration.r_script_runner import RScriptRunner
from mobility.spatial.transport_zones import TransportZones
from mobility.transport.modes.core.modal_transfer import IntermodalTransfer
from mobility.transport.modes.public_transport.public_transport_graph import PublicTransportGraph, PublicTransportRoutingParameters
from mobility.transport.graphs.contracted.contracted_path_graph import ContractedPathGraph
from mobility.spatial.osm import OSMData

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
            first_leg_travel_costs,
            last_leg_travel_costs,
            first_leg_mode_name: str,
            last_leg_mode_name: str,
            first_modal_transfer: IntermodalTransfer = None,
            last_modal_transfer: IntermodalTransfer = None,
            parkings_geofabrik_extract_date: str = "250101"
    ):
        """Build the intermodal PT graph from leg graphs and PT routing inputs."""
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
            parkings_geofabrik_extract_date:

        """
        if first_modal_transfer is None or last_modal_transfer is None:
            raise ValueError(
                "IntermodalTransportGraph requires both `first_modal_transfer` and `last_modal_transfer`."
            )

        public_transport_graph = PublicTransportGraph(transport_zones, parameters)

        inputs = {
            "transport_zones": transport_zones,
            "public_transport_graph": public_transport_graph,
            # PT only needs the contracted leg graphs here, not the full leg
            # mode objects that were previously threaded through this constructor.
            "first_leg_graph": first_leg_travel_costs.contracted_path_graph,
            "last_leg_graph": last_leg_travel_costs.contracted_path_graph,
            "first_modal_transfer": first_modal_transfer,
            "last_modal_transfer": last_modal_transfer,
            "parameters": parameters
        }

        # Parking supply is only relevant when access to PT starts by car.
        if first_leg_mode_name == "car":
            inputs["osm_parkings"] = OSMData(
                transport_zones.study_area,
                object_type="a",
                key="parking",
                boundary_buffer=0.0,
                geofabrik_extract_date=parkings_geofabrik_extract_date
            )
        
        file_name = (
            first_leg_mode_name
            + "_public_transport_"
            + last_leg_mode_name
            + "_intermodal_transport_graph/simplified/done"
        )
        cache_path = pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"]) / file_name

        super().__init__(inputs, cache_path)

    def get_cached_asset(self) -> pd.DataFrame:
        """Return the persisted intermodal graph marker path."""
        logging.info("Intermodal graph already created. Reusing the file : " + str(self.cache_path))
        return self.cache_path

    def create_and_get_asset(self) -> pd.DataFrame:
        """Build and persist the intermodal graph marker path."""
        self.prepare_intermodal_graph(**self.inputs)
        return self.cache_path

    
    def prepare_intermodal_graph(
            self,
            transport_zones: TransportZones,
            public_transport_graph: PublicTransportGraph,
            first_leg_graph: ContractedPathGraph,
            last_leg_graph: ContractedPathGraph,
            first_modal_transfer: IntermodalTransfer,
            last_modal_transfer: IntermodalTransfer,
            parameters: PublicTransportRoutingParameters,
            osm_parkings: OSMData = None
        ) -> pd.DataFrame:
        """Build the routable intermodal PT graph on disk."""

        logging.info("Computing public transport travel costs...")
        
        script = RScriptRunner(resources.files('mobility.transport.modes.public_transport').joinpath('prepare_intermodal_public_transport_graph.R'))
        
        script.run(
            args=[
                str(transport_zones.cache_path),
                str(public_transport_graph.get()),
                str(first_leg_graph.get()),
                str(last_leg_graph.get()),
                json.dumps(first_modal_transfer.model_dump(mode="json")),
                json.dumps(last_modal_transfer.model_dump(mode="json")),
                "" if osm_parkings is None else str(osm_parkings.get()),
                json.dumps(parameters.model_dump(mode="json")),
                str(self.cache_path)
            ]
        )

        return None
    

    def update(self):
        """Refresh the persisted intermodal graph."""
        
        self.create_and_get_asset()

    def audit_gtfs(self):
        """Expose GTFS audit information from the PT subgraph."""
        return self.inputs["public_transport_graph"].audit_gtfs()
