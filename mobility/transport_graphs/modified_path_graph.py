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

        gpkg_fp = dict_fp = self.cache_path.parent / (self.inputs_hash + "-graph.gpkg")
        
        data_fp = self.cache_path.parent / (self.inputs_hash + "data.parquet")
        dict_fp = self.cache_path.parent / (self.inputs_hash + "dict.parquet")
        vertices_fp = self.cache_path.parents[1] / (self.inputs_hash + "-vertices.parquet")

        if data_fp.exists() and dict_fp.exists() and vertices_fp.exists():

            graph_data = pd.read_parquet(data_fp)
            graph_dict = pd.read_parquet(dict_fp)
            graph_vertices = pd.read_parquet(vertices_fp)

            graph_data = pd.merge(graph_data, graph_dict, left_on="from", right_on="id")
            graph_data = pd.merge(graph_data, graph_dict, left_on="to", right_on="id", suffixes=["_from", "_to"])

            graph_data = pd.merge(graph_data, graph_vertices, left_on="ref_from", right_on="vertex_id")
            graph_data = pd.merge(graph_data, graph_vertices, left_on="ref_to", right_on="vertex_id", suffixes=["_from", "_to"])

            gdf = gpd.GeoDataFrame(
                graph_data, 
                geometry=[
                    LineString([(x1, y1), (x2, y2)]) 
                    for x1, y1, x2, y2 in zip(
                        graph_data['x_from'],
                        graph_data['y_from'],
                        graph_data['x_to'],
                        graph_data['y_to']
                    )
                ],
                crs="EPSG:3035"
            )

            gdf.to_file(gpkg_fp, driver="GPKG")

        else:

            raise ValueError(
                """
                Cannot convert the graph, at least one of the data / dict / 
                vertices parquet files is missing"
                """
            )
        
        return gpkg_fp
    
