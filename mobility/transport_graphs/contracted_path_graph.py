import os
import pathlib
import logging

from importlib import resources
from mobility.file_asset import FileAsset
from mobility.r_utils.r_script import RScript
from mobility.transport_graphs.congested_path_graph import CongestedPathGraph
from mobility.transport_zones import TransportZones

class ContractedPathGraph(FileAsset):

    def __init__(
            self,
            congested_graph: CongestedPathGraph
        ):
        
        inputs = {
            "congested_graph": congested_graph
        }
        
        mode_name = congested_graph.mode_name
        folder_path = pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"])
        file_name = pathlib.Path("path_graph_" + mode_name) / "contracted" / (mode_name + "-contracted-path-graph")
        cache_path = folder_path / file_name

        super().__init__(inputs, cache_path)

    def get_cached_asset(self) -> pathlib.Path:
        
        logging.info("Contracted graph already prepared. Reusing the files in : " + str(self.cache_path.parent))
         
        return self.cache_path

    def create_and_get_asset(self) -> pathlib.Path:
        
        logging.info("Contracting graph...")

        self.contract_graph(
            self.congested_graph.get(),
            self.cache_path
        )

        return self.cache_path

    def contract_graph(
            self,
            congested_graph_path: pathlib.Path,
            output_file_path: pathlib.Path
        ) -> None:
         
        script = RScript(resources.files('mobility.transport_graphs').joinpath('contract_path_graph.R'))

        script.run(
            args=[
                str(congested_graph_path),
                str(output_file_path)
            ]
        )

        return None
    
    def update(self, od_flows, flow_asset=None):
        
        if self.congested_graph.handles_congestion is True:
            
            logging.info("Rebuilding contracted graph given OD flows and congestion...")

            self.congested_graph.update(od_flows, flow_asset=flow_asset)
            self.create_and_get_asset()


