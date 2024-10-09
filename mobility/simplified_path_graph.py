import os
import pathlib
import logging
import geopandas as gpd

from importlib import resources
from mobility.parsers.osm import OSMData
from mobility.file_asset import FileAsset
from mobility.r_utils.r_script import RScript

class SimplifiedPathGraph(FileAsset):

    def __init__(
            self,
            mode_name: str,
            transport_zones: gpd.GeoDataFrame
        ):
        
        available_modes = ["car", "bicycle", "walk"]
        if mode_name not in available_modes:
            raise ValueError(
                "Cannot compute travel costs for mode : '" + mode_name + "'. Available options are : " \
                + ", ".join(available_modes) + "."
            )

        osm = OSMData(
            transport_zones,
            object_type="w",
            key="highway",
            tags = [
                "primary", "secondary", "tertiary", "unclassified", "residential",
                "service", "track", "cycleway", "path", "steps", "ferry",
                "living_street", "bridleway", "footway", "pedestrian",
                "primary_link", "secondary_link", "tertiary_link"
            ],
            geofabrik_extract_date="240101",
            file_format="osm"
        )
        
        inputs = {
            "transport_zones": transport_zones,
            "osm": osm,
            "mode_name": mode_name
        }
        
        file_name = pathlib.Path("path_graph_" + mode_name) / "simplified" / "done"
        cache_path = pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"]) / file_name

        super().__init__(inputs, cache_path)

    def get_cached_asset(self) -> pathlib.Path:
        
        logging.info("Path graph already prepared. Reusing the graph in : " + str(self.cache_path.parent))
        
        return self.cache_path.parent

    def create_and_get_asset(self) -> pathlib.Path:
        
        logging.info("Preparing travel costs for mode " + self.mode_name)

        self.prepare_path_graph(
            self.transport_zones,
            self.osm.get(),
            self.mode_name,
            self.cache_path
        )

        return self.cache_path.parent

    def prepare_path_graph(
            self,
            transport_zones: gpd.GeoDataFrame,
            osm_data_path: pathlib.Path,
            mode: str,
            output_file_path: pathlib.Path
        ) -> None:

    
        logging.info("Creating a routable graph with dodgr and cpp routing this might take a while...")
         
        script = RScript(resources.files('mobility.r_utils').joinpath('prepare_path_graph.R'))

        script.run(
            args=[
                str(transport_zones.cache_path),
                str(osm_data_path),
                mode,
                output_file_path
            ]
        )

        return None


