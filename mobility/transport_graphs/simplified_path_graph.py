import os
import pathlib
import logging
import dataclasses
import json
import geopandas as gpd

from importlib import resources
from mobility.parsers.osm import OSMData
from mobility.file_asset import FileAsset
from mobility.r_utils.r_script import RScript
from mobility.transport_modes.osm_capacity_parameters import OSMCapacityParameters
from mobility.transport_zones import TransportZones
from mobility.transport_graphs.graph_gpkg_exporter import GraphGPKGExporter

class SimplifiedPathGraph(FileAsset):

    def __init__(
            self,
            mode_name: str,
            transport_zones: TransportZones,
            osm_capacity_parameters: OSMCapacityParameters
        ):
        
        available_modes = ["car", "bicycle", "walk"]
        if mode_name not in available_modes:
            raise ValueError(
                "Cannot compute travel costs for mode : '" + mode_name + "'. Available options are : " \
                + ", ".join(available_modes) + "."
            )

        osm = OSMData(
            transport_zones.study_area,
            object_type="w",
            key="highway",
            tags = osm_capacity_parameters.get_highway_tags(),
            geofabrik_extract_date="250101",
            file_format="osm"
        )
        
        inputs = {
            "transport_zones": transport_zones,
            "osm": osm,
            "osm_capacity_parameters": osm_capacity_parameters,
            "mode_name": mode_name
        }
        
        file_name = pathlib.Path("path_graph_" + mode_name) / "simplified" / (mode_name + "-simplified-path-graph")
        cache_path = pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"]) / file_name

        super().__init__(inputs, cache_path)

    def get_cached_asset(self) -> pathlib.Path:
        
        logging.info("Path graph already prepared. Reusing the graph in : " + str(self.cache_path.parent))
        
        return self.cache_path

    def create_and_get_asset(self) -> pathlib.Path:
        
        logging.info("Preparing travel costs for mode " + self.mode_name)

        self.prepare_path_graph(
            self.transport_zones,
            self.osm.get(),
            self.mode_name,
            self.osm_capacity_parameters,
            self.cache_path
        )

        return self.cache_path

    def prepare_path_graph(
            self,
            transport_zones: gpd.GeoDataFrame,
            osm_data_path: pathlib.Path,
            mode: str,
            osm_capacity_parameters: OSMCapacityParameters,
            output_file_path: pathlib.Path
        ) -> None:

    
        logging.info("Creating a routable graph with dodgr and cpp routing this might take a while...")
         
        script = RScript(resources.files('mobility.transport_graphs').joinpath('prepare_path_graph.R'))

        script.run(
            args=[
                str(transport_zones.cache_path),
                str(osm_data_path),
                mode,
                json.dumps(dataclasses.asdict(osm_capacity_parameters)),
                str(output_file_path)
            ]
        )

        return None


    def convert_to_gpkg(self):
        gpkg_fp = GraphGPKGExporter().export(self)
        return gpkg_fp