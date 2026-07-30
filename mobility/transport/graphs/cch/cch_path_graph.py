import logging
import os
import pathlib

from importlib import resources
from mobility.runtime.assets.file_asset import FileAsset
from mobility.runtime.r_integration.r_script_runner import RScriptRunner
from mobility.transport.graphs.core.graph_cache_cleanup import graph_cache_paths
from mobility.transport.graphs.modified.modified_path_graph import ModifiedPathGraph


class CCHPathGraph(FileAsset):
    """Reusable CCH topology prepared from the modified path graph."""

    def __init__(self, modified_graph: ModifiedPathGraph, mode_name: str | None = None):
        inputs = {
            "version": "1",
            "modified_graph": modified_graph,
        }

        mode_name = mode_name or modified_graph.mode_name
        folder_path = pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"])
        file_name = pathlib.Path("path_graph_" + mode_name) / "cch" / (mode_name + "-cch-path-graph")
        cache_path = folder_path / file_name

        super().__init__(inputs, cache_path)

    def get_cached_asset(self) -> pathlib.Path:
        logging.debug("CCH graph already prepared. Reusing the files in: " + str(self.cache_path.parent))
        return self.cache_path

    def _cache_paths_to_remove(self):
        return graph_cache_paths(self.cache_path, self.hash_path)

    def create_and_get_asset(self) -> pathlib.Path:
        logging.info("Preparing CCH graph...")
        self.prepare_cch_graph(
            self.inputs["modified_graph"].get(),
            self.cache_path,
        )
        return self.cache_path

    def prepare_cch_graph(
        self,
        modified_graph_path: pathlib.Path,
        output_file_path: pathlib.Path,
    ) -> None:
        script = RScriptRunner(resources.files("mobility.transport.graphs.cch").joinpath("prepare_cch_path_graph.R"))

        script.run(
            args=[
                str(modified_graph_path),
                str(output_file_path),
            ]
        )
