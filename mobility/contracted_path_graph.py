import os
import pathlib
import logging

from importlib import resources
from mobility.file_asset import FileAsset
from mobility.r_utils.r_script import RScript
from mobility.simplified_path_graph import SimplifiedPathGraph

class ContractedPathGraph(FileAsset):

    def __init__(self, simplified_graph: SimplifiedPathGraph):
        
        inputs = {"simplified_graph": simplified_graph}
        
        file_name = pathlib.Path("path_graph_" + simplified_graph.mode_name) / "contracted" / "done"
        cache_path = pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"]) / file_name

        super().__init__(inputs, cache_path)

    def get_cached_asset(self) -> pathlib.Path:
        
        logging.info("Contracted graph already prepared. Reusing the files in : " + str(self.cache_path.parent))
         
        return self.cache_path.parent

    def create_and_get_asset(self) -> pathlib.Path:
        
        logging.info("Contracting graph...")

        self.contract_graph(
            self.simplified_graph.get(),
            self.cache_path
        )

        return self.cache_path.parent

    def contract_graph(
            self,
            simplified_graph_path: pathlib.Path,
            output_file_path: pathlib.Path
        ) -> None:
         
        script = RScript(resources.files('mobility.r_utils').joinpath('contract_path_graph.R'))

        script.run(
            args=[
                str(simplified_graph_path),
                str(output_file_path)
            ]
        )

        return None


