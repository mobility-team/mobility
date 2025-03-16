import os
import pathlib
import logging
import dataclasses
import json

from importlib import resources
from mobility.file_asset import FileAsset
from mobility.r_utils.r_script import RScript
from mobility.transport_graphs.modified_path_graph import ModifiedPathGraph
from mobility.transport_zones import TransportZones

from typing import List

class CongestedPathGraph(FileAsset):

    def __init__(
            self,
            modified_graph: ModifiedPathGraph,
            transport_zones: TransportZones,
            handles_congestion: bool = False,
            congestion_flows_scaling_factor: float = 1.0
        ):
        
        inputs = {
            "mode_name": modified_graph.mode_name,
            "modified_graph": modified_graph,
            "transport_zones": transport_zones,
            "handles_congestion": handles_congestion,
            "congestion_flows_scaling_factor": congestion_flows_scaling_factor
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

    def create_and_get_asset(self, enable_congestion: bool = False) -> pathlib.Path:
        
        logging.info("Modifying graph...")

        self.load_graph(
            self.modified_graph.get(),
            self.transport_zones.cache_path,
            enable_congestion,
            self.flows_file_path,
            self.congestion_flows_scaling_factor,
        )

        return self.cache_path

    def load_graph(
            self,
            simplified_graph_path: pathlib.Path,
            transport_zones_path: pathlib.Path,
            enable_congestion: bool,
            flows_file_path: pathlib.Path,
            congestion_flows_scaling_factor: float,
        ) -> None:
         
        script = RScript(resources.files('mobility.transport_graphs').joinpath('load_path_graph.R'))

        script.run(
            args=[
                str(simplified_graph_path),
                str(transport_zones_path),
                str(enable_congestion),
                str(flows_file_path),
                str(congestion_flows_scaling_factor),
                str(self.cache_path)
            ]
        )

        return None
    

    def update(self, od_flows):
        
        if self.handles_congestion is True:
            
            od_flows.write_parquet(self.flows_file_path)
            self.create_and_get_asset(enable_congestion=True)


