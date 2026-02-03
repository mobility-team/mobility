import os
import pathlib
import logging

from importlib import resources

from mobility.file_asset import FileAsset
from mobility.r_utils.r_script import RScript
from mobility.transport_graphs.congested_path_graph_snapshot import CongestedPathGraphSnapshot


class ContractedPathGraphSnapshot(FileAsset):
    """A per-run/iteration contracted graph derived from a congested snapshot."""

    def __init__(self, congested_graph: CongestedPathGraphSnapshot):
        inputs = {"congested_graph": congested_graph, "schema_version": 1}

        mode_name = congested_graph.inputs["mode_name"]
        folder_path = pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"])
        file_name = pathlib.Path("path_graph_" + mode_name) / "contracted" / (mode_name + "-contracted-path-graph")
        cache_path = folder_path / file_name

        super().__init__(inputs, cache_path)

    def get_cached_asset(self) -> pathlib.Path:
        logging.info("Contracted snapshot graph already prepared. Reusing: " + str(self.cache_path))
        return self.cache_path

    def create_and_get_asset(self) -> pathlib.Path:
        logging.info("Contracting snapshot graph...")

        congested_graph_path = self.inputs["congested_graph"].get()
        script = RScript(resources.files('mobility.transport_graphs').joinpath('contract_path_graph.R'))
        script.run(args=[str(congested_graph_path), str(self.cache_path)])

        return self.cache_path

