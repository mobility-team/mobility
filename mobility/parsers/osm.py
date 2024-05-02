import os
import pathlib
import subprocess
import geojson
import logging
import shapely
import geopandas as gpd

from typing import Tuple, List

from mobility.parsers.download_file import download_file
from mobility.parsers.admin_boundaries import get_french_old_regions_boundaries
from mobility.asset import Asset

class OSMData(Asset):
    """
    A class for managing OpenStreetMap (OSM) data, inheriting from the Asset class.

    This class handles the downloading, processing, and caching of OSM data 
    based on specified transport zones and modes.

    Attributes:
        transport_zones (gpd.GeoDataFrame): The geographical areas for which OSM data is needed.
        dodgr_modes (list): List of modes used for routing in the dodgr package.

    Methods:
        get_cached_asset: Retrieve a cached OSM data file path.
        create_and_get_asset: Download, process, and cache OSM data, then return the file path.
        create_transport_zones_boundary: Create a boundary polygon for the transport zones.
        get_osm_regions: Identify and download OSM region files intersecting with transport zones.
        crop_region: Crop OSM region files to the transport zones boundary.
        filter_region: Filter OSM region files based on specified tags.
        merge_regions: Merge multiple OSM region files into a single file.
        get_dodgr_modes_osm_tags: Retrieve OSM tags relevant to the specified dodgr modes.
        get_dodgr_mode_osm_tags: Retrieve OSM tags for a specific dodgr mode.
    """
    
    def __init__(self, transport_zones: gpd.GeoDataFrame, dodgr_modes: list):
        """
        Initializes an OSMData object with the given transport zones and dodgr modes.

        Args:
            transport_zones (gpd.GeoDataFrame): GeoDataFrame defining the transport zones.
            dodgr_modes (list): List of modes to be used for routing in dodgr.
        """
        
        inputs = {
            "transport_zones": transport_zones,
            "dodgr_modes": dodgr_modes
        }
        
        file_name = "osm_data.osm"
        cache_path = pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"]) / file_name
        
        super().__init__(inputs, cache_path)
        
        
    def get_cached_asset(self) -> str:
        """
        Retrieves the path of the cached OSM data file.

        Returns:
            str: The file path of the cached OSM data.
        """
        logging.info("OSM data already prepared. Reusing the file : " + str(self.cache_path))
        return self.cache_path
    

    def create_and_get_asset(self) -> str:
        """
        Creates a combined boundary polygon for all transport zones and saves it as a GeoJSON file.

        Args:
            transport_zones (gpd.GeoDataFrame): The GeoDataFrame containing transport zone geometries.

        Returns:
            Tuple[shapely.Polygon, str]: A tuple containing the combined boundary polygon and the path to the saved GeoJSON file.
        """
        
        logging.info("Downloading and pre-processing OSM data.")
        
        transport_zones = self.inputs["transport_zones"].get()
    
        tz_boundary, tz_boundary_path = self.create_transport_zones_boundary(transport_zones)
        regions_paths = self.get_osm_regions(tz_boundary)
        osm_tags = self.get_dodgr_modes_osm_tags(self.inputs["dodgr_modes"])
        
        filtered_regions_paths = []
        
        for region_path in regions_paths:
            
            cropped_region_path = self.crop_region(region_path, tz_boundary_path)
            filtered_region_path = self.filter_region(cropped_region_path, osm_tags)
            filtered_regions_paths.append(filtered_region_path)
            
        merged_regions_path = self.merge_regions(filtered_regions_paths)
    
        return merged_regions_path
    
    
    def create_transport_zones_boundary(
            self, transport_zones: gpd.GeoDataFrame
        ) -> Tuple[shapely.Polygon, str]:
        """
        Creates a combined boundary polygon for all transport zones and saves it as a GeoJSON file.

        This method merges the geometries of all provided transport zones into a single polygon, 
        which represents the combined boundary of these zones. It then saves this boundary as a 
        GeoJSON file to be used in subsequent operations.

        Args:
            transport_zones (gpd.GeoDataFrame): A GeoDataFrame containing the geometries of transport zones.

        Returns:
            Tuple[shapely.geometry.Polygon, str]: A tuple containing the combined boundary polygon 
            and the path to the saved GeoJSON file.
        """
        
        # Merge all transport zones into one polygon
        transport_zones_boundary = transport_zones.to_crs(4326).unary_union.buffer(0.1)
        
        # Store the boundary as a temporary geojson file
        tz_boundary_geojson = geojson.Feature(geometry=transport_zones_boundary, properties={})
        tz_boundary_path = pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"]) / "transport_zones_boundary.geojson"
        
        with open(tz_boundary_path, "w") as f:
            geojson.dump(tz_boundary_geojson, f)
            
        return transport_zones_boundary, tz_boundary_path
    
    
    def get_osm_regions(self, transport_zones_boundary: shapely.Polygon) -> List[pathlib.Path]:
        """
        Downloads OpenStreetMap (OSM) data for French regions intersecting with the specified transport zones boundary.

        Args:
            transport_zones_boundary (shapely.Polygon): The boundary polygon encompassing all transport zones.

        Returns:
            List[pathlib.Path]: A list of file paths where the OSM data for each relevant region is saved.
        """
        
        # Find which geofabrik regions are included in the transport zone 
        regions = get_french_old_regions_boundaries()
        regions = regions[regions.intersects(transport_zones_boundary)]
        
        osm_regions = []
        
        for geofabrik_region_name in regions["geofabrik_name"].values:
            
            url = f"https://download.geofabrik.de/europe/france/{geofabrik_region_name}-latest.osm.pbf"
            path = pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / "osm" / f"{geofabrik_region_name}-latest.osm.pbf"
            download_file(url, path)
                    
            osm_regions.append(path)
            
        return osm_regions
    
    
    def crop_region(self, osm_region: pathlib.Path, tz_boundary_path: pathlib.Path) -> pathlib.Path:
        """
        Crops an OSM region file to the area defined by the transport zones boundary.

        Args:
            osm_region (pathlib.Path): The file path of the OSM region data.
            tz_boundary_path (pathlib.Path): The file path of the transport zones boundary GeoJSON.

        Returns:
            pathlib.Path: The file path of the cropped OSM region.
        """
        
        logging.info("Cropping OSM extracts")
        cropped_region_name = "cropped-" + osm_region.name
        cropped_region_path = pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"]) / cropped_region_name
        cropped_region_path_str = str(cropped_region_path)
        command = f"osmium extract --polygon {tz_boundary_path} {osm_region} --overwrite --strategy complete_ways -o {cropped_region_path_str}"
        subprocess.run(command, shell=True)
        
        return cropped_region_path
        
        
    def filter_region(self, cropped_region_path: pathlib.Path, osm_tags: List[str]) -> pathlib.Path:
        """
        Filters the cropped OSM region file based on specified OSM tags.

        Args:
            cropped_region_path (pathlib.Path): The file path of the cropped OSM region data.
            osm_tags (List[str]): A list of OSM tags to filter the data.

        Returns:
            pathlib.Path: The file path of the filtered OSM region.
        """
        
        logging.info("Subsetting OSM extracts")
        osm_tags = ",".join(osm_tags)
        filtered_region_name = "filtered-" + cropped_region_path.name
        filtered_region_path = pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"]) / filtered_region_name
        command = f"osmium tags-filter --overwrite -o {filtered_region_path} {cropped_region_path} w/highway={osm_tags}"
        subprocess.run(command, shell=True)
        
        return filtered_region_path
    
    
    def merge_regions(self, filtered_regions_paths: List[pathlib.Path]):
        """
        Merges multiple filtered OSM region files into a single file.

        Args:
            filtered_regions_paths (List[pathlib.Path]): A list of file paths of filtered OSM regions.

        Returns:
            None: The result is saved to the instance's cache path.
        """
        
        filtered_regions_paths = [str(path) for path in filtered_regions_paths]
        filtered_regions_paths = " ".join(filtered_regions_paths)
        
        logging.info("Merging OSM extracts")
        command = f"osmium merge {filtered_regions_paths} --overwrite -o {self.cache_path}"
        subprocess.run(command, shell=True)
    
    
    def get_dodgr_modes_osm_tags(self, dodgr_modes: list) -> list:
        """
        Retrieves the OSM tags relevant to the specified dodgr modes for routing.

        Args:
            dodgr_modes (list): A list of modes used for routing in the dodgr package.

        Returns:
            list: A list of OSM tags related to the specified dodgr modes.
        """
        
        osm_tags = []
        for dodgr_mode in dodgr_modes:
            tags = self.get_dodgr_mode_osm_tags(dodgr_mode)
            tags = tags.split(",")
            osm_tags.extend(tags)
        osm_tags = list(set(osm_tags))
        
        return osm_tags


    def get_dodgr_mode_osm_tags(self, dodgr_mode: str) -> list:
        """
        Retrieves OSM tags relevant to a specific dodgr mode.

        Args:
            dodgr_mode (str): A mode used for routing in the dodgr package.

        Returns:
            list: A list of OSM tags associated with the specified dodgr mode.
        """
        
        # Get OSM highway tags that are valid for the given mode
        # (= tags of ways that dodgr uses for routing for this mode)
        path_to_r_script = pathlib.Path(__file__).parents[1] / "get_dodgr_osm_tags.R"
        cmd = ["Rscript", path_to_r_script, pathlib.Path(__file__).parents[1], dodgr_mode]
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        output, error = process.communicate()
        
        osm_tags = output.decode().strip()
        
        return osm_tags