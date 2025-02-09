import os
import pathlib
import logging

from importlib import resources
from mobility.file_asset import FileAsset
from mobility.r_utils.r_script import RScript
from mobility.simplified_path_graph import SimplifiedPathGraph
from mobility.transport_zones import TransportZones

class ContractedPathGraph(FileAsset):

    def __init__(
            self,
            simplified_graph: SimplifiedPathGraph,
            transport_zones: TransportZones,
            handles_congestion: bool = False
        ):
        
        inputs = {
            "transport_zones": transport_zones,
            "simplified_graph": simplified_graph,
            "handles_congestion": handles_congestion
        }
        
        file_name = pathlib.Path("path_graph_" + simplified_graph.mode_name) / "contracted" / (simplified_graph.mode_name + "-contracted-path-graph")
        cache_path = pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"]) / file_name
        
        self.flows_file_path = pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"]) / ("path_graph_" + simplified_graph.mode_name) / "simplified" / "flows.parquet"

        super().__init__(inputs, cache_path)

    def get_cached_asset(self) -> pathlib.Path:
        
        logging.info("Contracted graph already prepared. Reusing the files in : " + str(self.cache_path.parent))
         
        return self.cache_path

    def create_and_get_asset(self, enable_congestion: bool = False) -> pathlib.Path:
        
        logging.info("Contracting graph...")
        
        self.transport_zones.get()

        self.contract_graph(
            self.simplified_graph.get(),
            self.transport_zones.cache_path,
            enable_congestion,
            self.flows_file_path,
            self.cache_path
        )

        return self.cache_path

    def contract_graph(
            self,
            simplified_graph_path: pathlib.Path,
            transport_zones_path: pathlib.Path,
            enable_congestion: bool,
            flows_file_path: pathlib.Path,
            output_file_path: pathlib.Path
        ) -> None:
         
        script = RScript(resources.files('mobility.r_utils').joinpath('contract_path_graph.R'))

        script.run(
            args=[
                str(simplified_graph_path),
                str(transport_zones_path),
                str(enable_congestion),
                str(flows_file_path),
                str(output_file_path)
            ]
        )

        return None
    
    def update(self, od_flows):
        
        if self.handles_congestion is True:
            
            logging.info("Rebuilding contracted graph given OD flows and congestion...")
            
            od_flows.write_parquet(self.flows_file_path)
            self.create_and_get_asset(enable_congestion=True)


