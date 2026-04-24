import os
import pathlib
import logging

from importlib import resources
from mobility.runtime.assets.file_asset import FileAsset
from mobility.runtime.r_integration.r_script_runner import RScriptRunner
from mobility.transport.costs.od_flows_asset import VehicleODFlowsAsset
from mobility.transport.graphs.modified.modified_path_graph import ModifiedPathGraph
from mobility.spatial.transport_zones import TransportZones

class CongestedPathGraph(FileAsset):

    def __init__(
            self,
            modified_graph: ModifiedPathGraph,
            transport_zones: TransportZones,
            handles_congestion: bool = False,
            congestion_flows_scaling_factor: float = 1.0,
            target_max_vehicles_per_od_endpoint: float = 1000.0,
            vehicle_flows: VehicleODFlowsAsset | None = None,
        ):
        
        inputs = {
            "version": "1",
            "mode_name": modified_graph.mode_name,
            "modified_graph": modified_graph,
            "transport_zones": transport_zones,
            "vehicle_flows": vehicle_flows,
            "handles_congestion": handles_congestion,
            "congestion_flows_scaling_factor": congestion_flows_scaling_factor,
            "target_max_vehicles_per_od_endpoint": target_max_vehicles_per_od_endpoint,
        }
        
        mode_name = modified_graph.mode_name
        folder_path = pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"])
        file_name = pathlib.Path("path_graph_" + mode_name) / "congested" / (mode_name + "-congested-path-graph")
        cache_path = folder_path / file_name

        self.flows_file_path = folder_path / ("path_graph_" + mode_name) / "simplified" / "flows.parquet"

        super().__init__(inputs, cache_path)

    def get_cached_asset(self) -> pathlib.Path:
        
        logging.info("Congested graph already prepared. Reusing the files in : " + str(self.cache_path.parent))
         
        return self.cache_path

    def create_and_get_asset(self) -> pathlib.Path:
        
        logging.info("Loading graph with traffic...")
        vehicle_flows = self.inputs["vehicle_flows"]
        if vehicle_flows is None:
            flows_file_path = self.flows_file_path
            enable_congestion = False
        else:
            vehicle_flows.get()
            flows_file_path = vehicle_flows.cache_path
            enable_congestion = True

        self.load_graph(
            self.inputs["modified_graph"].get(),
            self.inputs["transport_zones"].cache_path,
            enable_congestion,
            flows_file_path,
            self.inputs["congestion_flows_scaling_factor"],
            self.inputs["target_max_vehicles_per_od_endpoint"],
        )

        return self.cache_path

    def load_graph(
            self,
            simplified_graph_path: pathlib.Path,
            transport_zones_path: pathlib.Path,
            enable_congestion: bool,
            flows_file_path: pathlib.Path,
            congestion_flows_scaling_factor: float,
            target_max_vehicles_per_od_endpoint: float,
        ) -> None:
         
        script = RScriptRunner(resources.files('mobility.transport.graphs.congested').joinpath('load_path_graph.R'))

        script.run(
            args=[
                str(simplified_graph_path),
                str(transport_zones_path),
                str(enable_congestion),
                str(flows_file_path),
                str(congestion_flows_scaling_factor),
                str(target_max_vehicles_per_od_endpoint),
                str(self.cache_path)
            ]
        )

        return None
    
    def asset_for_iteration(self, run, iteration: int) -> "CongestedPathGraph":
        """Return the graph instance corresponding to the congestion active at one iteration."""
        if iteration < 1:
            raise ValueError("Iteration should be >= 1.")
        if iteration > int(run.parameters.n_iterations):
            raise ValueError(
                f"Iteration should be <= {int(run.parameters.n_iterations)} for this run."
            )

        flow_asset = self.get_flow_asset_for_iteration(run, iteration)
        if flow_asset is None:
            return self

        return CongestedPathGraph(
            modified_graph=self.inputs["modified_graph"],
            transport_zones=self.inputs["transport_zones"],
            vehicle_flows=flow_asset,
            handles_congestion=self.inputs["handles_congestion"],
            congestion_flows_scaling_factor=self.inputs["congestion_flows_scaling_factor"],
            target_max_vehicles_per_od_endpoint=self.inputs["target_max_vehicles_per_od_endpoint"],
        )

    def get_for_iteration(self, run, iteration: int):
        """Materialize the graph payload corresponding to one simulation iteration."""
        return self.asset_for_iteration(run, iteration).get()

    def get_flow_asset_for_iteration(self, run, iteration: int) -> VehicleODFlowsAsset | None:
        """Return the persisted flow asset backing the congestion active at one iteration."""
        if self.inputs["handles_congestion"] is False:
            return None

        cost_update_interval = int(run.parameters.n_iter_per_cost_update)
        if cost_update_interval <= 0 or iteration <= 1:
            return None

        source_iteration = max(
            update_iteration
            for update_iteration in range(1, int(iteration))
            if (update_iteration - 1) % cost_update_interval == 0
        )

        flow_asset = VehicleODFlowsAsset.from_inputs(
            run_key=str(run.inputs_hash),
            is_weekday=bool(run.is_weekday),
            iteration=int(source_iteration),
            mode_name=str(self.inputs["mode_name"]),
        )
        if flow_asset.cache_path.exists() is False:
            raise RuntimeError(
                "Missing persisted congestion flow asset for "
                f"run_key={run.inputs_hash}, is_weekday={run.is_weekday}, "
                f"mode={self.inputs['mode_name']}, source_iteration={source_iteration}. "
                "Rerun the simulation from scratch."
            )
        return flow_asset


