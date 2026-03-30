import os
import pathlib
import logging

from importlib import resources

from mobility.runtime.assets.file_asset import FileAsset
from mobility.runtime.r_integration.r_script_runner import RScriptRunner
from mobility.transport.graphs.modified.modified_path_graph import ModifiedPathGraph
from mobility.spatial.transport_zones import TransportZones
from mobility.transport.costs.od_flows_asset import VehicleODFlowsAsset


class CongestedPathGraphSnapshot(FileAsset):
    """A per-run/iteration congested graph snapshot.

    This is the "variant" layer: it depends on a stable modified graph and a
    VehicleODFlowsAsset, so different seeds/iterations produce distinct cache
    files without invalidating upstream base graphs.
    """

    def __init__(
        self,
        modified_graph: ModifiedPathGraph,
        transport_zones: TransportZones,
        vehicle_flows: VehicleODFlowsAsset,
        congestion_flows_scaling_factor: float,
    ):
        inputs = {
            "mode_name": modified_graph.mode_name,
            "modified_graph": modified_graph,
            "transport_zones": transport_zones,
            "vehicle_flows": vehicle_flows,
            "congestion_flows_scaling_factor": float(congestion_flows_scaling_factor),
            "schema_version": 1,
        }

        mode_name = modified_graph.mode_name
        folder_path = pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"])
        file_name = pathlib.Path("path_graph_" + mode_name) / "congested" / (mode_name + "-congested-path-graph")
        cache_path = folder_path / file_name

        super().__init__(inputs, cache_path)

    def get_cached_asset(self) -> pathlib.Path:
        logging.info("Congested snapshot graph already prepared. Reusing: " + str(self.cache_path))
        return self.cache_path

    def create_and_get_asset(self) -> pathlib.Path:
        vehicle_flows: VehicleODFlowsAsset = self.inputs["vehicle_flows"]
        logging.info("Building congested snapshot graph...")
        vehicle_flows.get()  # ensure parquet exists

        script = RScriptRunner(resources.files('mobility.transport.graphs.congested').joinpath('load_path_graph.R'))
        script.run(
            args=[
                str(self.inputs["modified_graph"].get()),
                str(self.inputs["transport_zones"].cache_path),
                "True",
                str(vehicle_flows.cache_path),
                str(self.inputs["congestion_flows_scaling_factor"]),
                str(self.cache_path),
            ]
        )

        return self.cache_path
