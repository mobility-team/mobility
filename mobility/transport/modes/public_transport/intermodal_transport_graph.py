import os
import pathlib
import logging
import json
import pandas as pd

from importlib import resources

from mobility.runtime.assets.file_asset import FileAsset
from mobility.runtime.r_integration.r_script_runner import RScriptRunner
from mobility.spatial.transport_zones import TransportZones
from mobility.transport.graphs.core.graph_cache_cleanup import graph_cache_paths
from mobility.transport.modes.core.modal_transfer import IntermodalTransfer
from mobility.transport.modes.public_transport.public_transport_graph import PublicTransportGraph, PublicTransportRoutingParameters
from mobility.spatial.osm import OSMData

class IntermodalTransportGraph(FileAsset):
    """Build the road-access + timetable + road-egress PT routing graph."""

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
            parkings_geofabrik_extract_date: str = "260101"
    ):
        if first_modal_transfer is None or last_modal_transfer is None:
            raise ValueError(
                "IntermodalTransportGraph requires both `first_modal_transfer` and `last_modal_transfer`."
            )

        public_transport_graph = PublicTransportGraph(transport_zones, parameters)

        inputs = {
            "transport_zones": transport_zones,
            "public_transport_graph": public_transport_graph,
            "first_leg_graph": first_leg_travel_costs.active_routing_graph,
            "first_leg_backend": first_leg_travel_costs.active_routing_backend,
            "first_leg_cch_graph": first_leg_travel_costs.active_cch_graph,
            "last_leg_graph": last_leg_travel_costs.active_routing_graph,
            "last_leg_backend": last_leg_travel_costs.active_routing_backend,
            "last_leg_cch_graph": last_leg_travel_costs.active_cch_graph,
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
        logging.debug("Intermodal graph already created. Reusing the file : " + str(self.cache_path))
        return self.cache_path

    def _cache_paths_to_remove(self):
        return graph_cache_paths(self.cache_path, self.hash_path)

    def create_and_get_asset(self) -> pd.DataFrame:
        """Build and persist the intermodal graph marker path."""
        self.prepare_intermodal_graph(**self.inputs)
        return self.cache_path

    
    def prepare_intermodal_graph(
            self,
            transport_zones: TransportZones,
            public_transport_graph: PublicTransportGraph,
            first_leg_graph,
            first_leg_backend: str,
            first_leg_cch_graph,
            last_leg_graph,
            last_leg_backend: str,
            last_leg_cch_graph,
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
                first_leg_backend,
                "" if first_leg_cch_graph is None else str(first_leg_cch_graph.get()),
                str(last_leg_graph.get()),
                last_leg_backend,
                "" if last_leg_cch_graph is None else str(last_leg_cch_graph.get()),
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
