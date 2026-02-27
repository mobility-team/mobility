import os
import pathlib
import logging

from importlib import resources

from mobility.file_asset import FileAsset
from mobility.r_utils.r_script import RScript
from mobility.transport_graphs.modified_path_graph import ModifiedPathGraph
from mobility.transport_zones import TransportZones
from mobility.transport_costs.od_flows_asset import VehicleODFlowsAsset


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
        if os.environ.get("MOBILITY_DEBUG_CONGESTION") == "1":
            vf: VehicleODFlowsAsset = self.inputs["vehicle_flows"]
            logging.info(
                "Congested snapshot graph cache hit: inputs_hash=%s mode=%s flows_hash=%s path=%s",
                self.inputs_hash,
                self.inputs["mode_name"],
                vf.get_cached_hash(),
                str(self.cache_path),
            )
        else:
            logging.info("Congested snapshot graph already prepared. Reusing: " + str(self.cache_path))
        return self.cache_path

    def create_and_get_asset(self) -> pathlib.Path:
        vehicle_flows: VehicleODFlowsAsset = self.inputs["vehicle_flows"]
        if os.environ.get("MOBILITY_DEBUG_CONGESTION") == "1":
            logging.info(
                "Building congested snapshot graph: inputs_hash=%s mode=%s flows_hash=%s flows_path=%s out=%s",
                self.inputs_hash,
                self.inputs["mode_name"],
                vehicle_flows.get_cached_hash(),
                str(vehicle_flows.cache_path),
                str(self.cache_path),
            )
        else:
            logging.info("Building congested snapshot graph...")
        vehicle_flows.get()  # ensure parquet exists

        script = RScript(resources.files('mobility.transport_graphs').joinpath('load_path_graph.R'))
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
