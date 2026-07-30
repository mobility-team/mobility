import os
import pathlib
import logging

from importlib import resources
from mobility.runtime.assets.file_asset import FileAsset
from mobility.runtime.r_integration.r_script_runner import RScriptRunner
from mobility.transport.costs.od_flows_asset import VehicleODFlowsAsset
from mobility.transport.graphs.modified.modified_path_graph import ModifiedPathGraph
from mobility.transport.graphs.cch.cch_path_graph import CCHPathGraph
from mobility.transport.graphs.core.graph_cache_cleanup import graph_cache_paths
from mobility.spatial.transport_zones import TransportZones

class CongestedPathGraph(FileAsset):

    def __init__(
            self,
            modified_graph: ModifiedPathGraph,
            cch_graph: CCHPathGraph,
            transport_zones: TransportZones,
            handles_congestion: bool = False,
            congestion_flows_scaling_factor: float = 1.0,
            target_max_vehicles_per_od_endpoint: float = 1000.0,
            congestion_assignment_max_iterations: int = 10,
            congestion_assignment_max_gap: float = 0.05,
            congestion_assignment_retained_volume_share: float = 0.95,
            vehicle_flows: VehicleODFlowsAsset | None = None,
        ):
        
        inputs = {
            "version": "1",
            "mode_name": modified_graph.mode_name,
            "modified_graph": modified_graph,
            "cch_graph": cch_graph,
            "transport_zones": transport_zones,
            "vehicle_flows": vehicle_flows,
            "handles_congestion": handles_congestion,
            "congestion_flows_scaling_factor": congestion_flows_scaling_factor,
            "target_max_vehicles_per_od_endpoint": target_max_vehicles_per_od_endpoint,
            "congestion_assignment_max_iterations": congestion_assignment_max_iterations,
            "congestion_assignment_max_gap": congestion_assignment_max_gap,
            "congestion_assignment_retained_volume_share": congestion_assignment_retained_volume_share,
        }
        
        mode_name = modified_graph.mode_name
        folder_path = pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"])
        file_name = pathlib.Path("path_graph_" + mode_name) / "congested" / (mode_name + "-congested-path-graph")
        cache_path = folder_path / file_name

        self.flows_file_path = folder_path / ("path_graph_" + mode_name) / "simplified" / "flows.parquet"

        super().__init__(inputs, cache_path)

    def get_cached_asset(self) -> pathlib.Path:
        
        logging.debug("Congested graph already prepared. Reusing the files in : " + str(self.cache_path.parent))
         
        return self.cache_path

    def _cache_paths_to_remove(self):
        return graph_cache_paths(self.cache_path, self.hash_path)

    def create_and_get_asset(self) -> pathlib.Path:
        
        logging.info("Loading graph with traffic...")
        vehicle_flows = self.inputs["vehicle_flows"]
        if vehicle_flows is None:
            flows_file_path = self.flows_file_path
            cch_graph_path = ""
            enable_congestion = False
        else:
            vehicle_flows.get()
            flows_file_path = vehicle_flows.cache_path
            cch_graph_path = self.inputs["cch_graph"].get()
            enable_congestion = True

        self.load_graph(
            self.inputs["modified_graph"].get(),
            self.inputs["transport_zones"].cache_path,
            enable_congestion,
            flows_file_path,
            cch_graph_path,
            self.inputs["congestion_flows_scaling_factor"],
            self.inputs["target_max_vehicles_per_od_endpoint"],
            self.inputs["congestion_assignment_max_iterations"],
            self.inputs["congestion_assignment_max_gap"],
            self.inputs["congestion_assignment_retained_volume_share"],
        )

        return self.cache_path

    def load_graph(
            self,
            simplified_graph_path: pathlib.Path,
            transport_zones_path: pathlib.Path,
            enable_congestion: bool,
            flows_file_path: pathlib.Path,
            cch_graph_path: pathlib.Path,
            congestion_flows_scaling_factor: float,
            target_max_vehicles_per_od_endpoint: float,
            congestion_assignment_max_iterations: int,
            congestion_assignment_max_gap: float,
            congestion_assignment_retained_volume_share: float,
        ) -> None:
         
        script = RScriptRunner(resources.files('mobility.transport.graphs.congested').joinpath('load_path_graph.R'))

        script.run(
            args=[
                str(simplified_graph_path),
                str(transport_zones_path),
                str(enable_congestion),
                str(flows_file_path),
                str(cch_graph_path),
                str(congestion_flows_scaling_factor),
                str(target_max_vehicles_per_od_endpoint),
                str(congestion_assignment_max_iterations),
                str(congestion_assignment_max_gap),
                str(congestion_assignment_retained_volume_share),
                str(self.cache_path)
            ]
        )

        return None
    

