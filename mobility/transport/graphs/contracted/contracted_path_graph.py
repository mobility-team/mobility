import os
import pathlib
import logging

from importlib import resources
from mobility.runtime.assets.file_asset import FileAsset
from mobility.runtime.r_integration.r_script_runner import RScriptRunner
from mobility.transport.graphs.core.graph_cache_cleanup import graph_cache_paths
from mobility.transport.graphs.modified.modified_path_graph import ModifiedPathGraph

class ContractedPathGraph(FileAsset):

    def __init__(
            self,
            modified_graph: ModifiedPathGraph
        ):
        
        inputs = {
            "modified_graph": modified_graph
        }
        
        mode_name = modified_graph.mode_name
        folder_path = pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"])
        file_name = pathlib.Path("path_graph_" + mode_name) / "contracted" / (mode_name + "-contracted-path-graph")
        cache_path = folder_path / file_name

        super().__init__(inputs, cache_path)

    def get_cached_asset(self) -> pathlib.Path:
        
        logging.debug("Contracted graph already prepared. Reusing the files in : " + str(self.cache_path.parent))
         
        return self.cache_path

    def _cache_paths_to_remove(self):
        return graph_cache_paths(self.cache_path, self.hash_path)

    def create_and_get_asset(self) -> pathlib.Path:
        
        logging.info("Contracting graph...")

        self.contract_graph(
            self.modified_graph.get(),
            self.cache_path
        )

        return self.cache_path

    def contract_graph(
            self,
            modified_graph_path: pathlib.Path,
            output_file_path: pathlib.Path
        ) -> None:
         
        script = RScriptRunner(resources.files('mobility.transport.graphs.contracted').joinpath('contract_path_graph.R'))

        script.run(
            args=[
                str(modified_graph_path),
                str(output_file_path)
            ]
        )

        return None


