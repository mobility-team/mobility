import os
import pathlib
import logging
import dataclasses
import json
import pandas as pd
import geopandas as gpd
from shapely.geometry import LineString

from importlib import resources
from mobility.file_asset import FileAsset
from mobility.r_utils.r_script import RScript
from mobility.transport_graphs.simplified_path_graph import SimplifiedPathGraph
from mobility.transport_graphs.speed_modifier import SpeedModifier
from mobility.transport_graphs.graph_gpkg_exporter import GraphGPKGExporter

from typing import List

class ModifiedPathGraph(FileAsset):

    def __init__(
            self,
            simplified_graph: SimplifiedPathGraph,
            speed_modifiers: List[SpeedModifier] = []
        ):
        
        inputs = {
            "mode_name": simplified_graph.mode_name,
            "simplified_graph": simplified_graph,
            "speed_modifiers": speed_modifiers
        }
        
        mode_name = simplified_graph.mode_name
        folder_path = pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"])
        file_name = pathlib.Path("path_graph_" + mode_name) / "modified" / (mode_name + "-modified-path-graph")
        cache_path = folder_path / file_name

        super().__init__(inputs, cache_path)

    def get_cached_asset(self) -> pathlib.Path:
        
        logging.info("Modified graph already prepared. Reusing the files in : " + str(self.cache_path.parent))
         
        return self.cache_path

    def create_and_get_asset(self, enable_congestion: bool = False) -> pathlib.Path:
        
        logging.info("Modifying graph...")

        self.modify_graph(
            self.simplified_graph.get(),
            self.speed_modifiers
        )

        return self.cache_path

    def modify_graph(
            self,
            simplified_graph_path: pathlib.Path,
            speed_modifiers: List[SpeedModifier]
        ) -> None:
         
        script = RScript(resources.files('mobility.transport_graphs').joinpath('modify_path_graph.R'))
        
        speed_modifiers = [sm.get() for sm in speed_modifiers]

        script.run(
            args=[
                str(simplified_graph_path),
                json.dumps(speed_modifiers),
                str(self.cache_path)
            ]
        )

        return None
    

    def convert_to_gpkg(self):
        gpkg_fp = GraphGPKGExporter().export(self)
        return gpkg_fp
    
