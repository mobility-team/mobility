import os
import pathlib
import subprocess
import geojson
import logging
import shapely
import hashlib
import psutil
import json
import math
import geopandas as gpd

from rich.progress import Progress
from typing import Tuple, List
from concurrent.futures import ThreadPoolExecutor, as_completed

from mobility.parsers.osm.geofabrik_extract import GeofabrikExtract
from mobility.parsers.osm.geofabrik_regions import GeofabrikRegions
from mobility.file_asset import FileAsset
from mobility.study_area import StudyArea

class OSMData(FileAsset):
    """
    A class for managing OpenStreetMap (OSM) data, inheriting from the Asset class.

    This class handles the downloading, processing, and caching of specific OSM data within the study area.

        Parameters
        ----------
        study_area : StudyArea
            The area for which OSM data is needed.
        object_type : str
            OSM types to query. "a" for areas, "n" for nodes, "w" for ways, "r" for relations, "nwr" for everything.
            See https://docs.osmcode.org/osmium/latest/osmium-tags-filter.html for the precise doc
        key : str
            OSM key to query, for instance "building".
            See https://docs.osmcode.org/osmium/latest/osmium-tags-filter.html for the precise doc
        tags : List[str], default=None
            OSM tag(s) to query.
            See https://docs.osmcode.org/osmium/latest/osmium-tags-filter.html for the precise doc
        split_local_admin_units : bool, default=False
            To describe.
        geofabrik_extract_date : str, default="240101"
            Reference date of the OSM data to use (date at which it has been extracted on Geofabrik).
        file_format : str, default="pbf"
            To describe.

    Methods
    -------
        get_cached_asset: Retrieve a cached OSM data file path.
        create_and_get_asset: Download, process, and cache OSM data, then return the file path.
        create_transport_zones_boundary: Create a boundary polygon for the transport zones.
        get_osm_regions: Identify and download OSM region files intersecting with transport zones.
        crop_region: Crop OSM region files to the transport zones boundary.
        filter_region: Filter OSM region files based on specified tags.
        merge_regions: Merge multiple OSM region files into a single file.
    """
    
    def __init__(
            self,
            study_area: StudyArea,
            object_type: str,
            key: str,
            tags: List[str] = None,
            boundary_buffer: float = 10000.0,
            split_local_admin_units: bool = False,
            geofabrik_extract_date: str = "240101",
            file_format: str = "pbf"
        ):
        if tags is None:
            tags = []
        
        inputs = {
            "study_area": study_area,
            "object_type": object_type,
            "key": key,
            "tags": tags,
            "boundary_buffer": boundary_buffer,
            "split_local_admin_units": split_local_admin_units,
            "geofabrik_extract_date": geofabrik_extract_date,
            "file_format": file_format
        }
        
        if split_local_admin_units is False:
            file_name = key + "-osm_data." + file_format
        else:
            file_name = pathlib.Path(key + "-osm_data") / "done"
        
        cache_path = pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"]) / file_name
        
        super().__init__(inputs, cache_path)
        
        
    def get_cached_asset(self) -> str:
        """
        Retrieves the path of the cached OSM data file.

        Returns:
            str: The file path of the cached OSM data.
        """
        logging.info("OSM data already prepared. Reusing the file : " + str(self.cache_path))
        
        if self.split_local_admin_units is False:
            return self.cache_path
        else:
            return self.cache_path.parent
    

    def create_and_get_asset(self) -> str:
        """
        Creates a combined boundary polygon for all transport zones and saves it as a GeoJSON file.

        Args:
            transport_zones (gpd.GeoDataFrame): The GeoDataFrame containing transport zone geometries.

        Returns:
            Tuple[shapely.Polygon, str]: A tuple containing the combined boundary polygon and the path to the saved GeoJSON file.
        """
        
        logging.info("Downloading and pre-processing OSM data.")
        
        study_area = self.inputs["study_area"].get()
        
        # Create a buffered version of the boundary
        boundary_path = self.inputs["study_area"].cache_path["boundary"]
        boundary = gpd.read_file(boundary_path).to_crs(3035)
        boundary_buffered = boundary.buffer(self.inputs["boundary_buffer"]).to_crs(4326).geometry[0]
        boundary_buffered_geojson = geojson.Feature(geometry=boundary_buffered, properties={})
        boundary_buffered_path = self.cache_path.parents[1] / (self.key + "-study-area-buffered-boundary.geojson")
        
        with open(boundary_buffered_path, "w") as f:
            geojson.dump(boundary_buffered_geojson, f)
    
        regions_paths = self.get_osm_regions(boundary_buffered)
        
        filtered_regions_paths = []
        
        for region_path in regions_paths:
            
            cropped_region_path = self.crop_region(region_path, boundary_buffered_path)
            filtered_region_path = self.filter_region(cropped_region_path, self.object_type, self.key, self.tags)
            filtered_regions_paths.append(filtered_region_path)
            
        
        if self.split_local_admin_units is True:
            result_path = self.merge_regions(filtered_regions_paths, self.cache_path.parents[1] / (self.key + "merged-filtered-cropped.pbf"))
            result_path = self.crop_by_local_admin_unit(result_path, study_area, self.cache_path.parent)
        else:
            result_path = self.merge_regions(filtered_regions_paths, self.cache_path)
    
        if self.split_local_admin_units is False:
            return self.cache_path
        else:
            return self.cache_path.parent
    
    
    def get_osm_regions(self, transport_zones_boundary: shapely.Polygon) -> List[pathlib.Path]:
        """
        Downloads OpenStreetMap (OSM) data for French regions intersecting with the specified transport zones boundary.

        Args:
            transport_zones_boundary (shapely.Polygon): The boundary polygon encompassing all transport zones.

        Returns:
            List[pathlib.Path]: A list of file paths where the OSM data for each relevant region is saved.
        """
        
        # Find which geofabrik regions are included in the transport zone 
        regions = GeofabrikRegions(extract_date=self.inputs["geofabrik_extract_date"]).get()
        regions = regions[regions.intersects(transport_zones_boundary)]
        
        osm_regions = []
        
        for index, region in regions.iterrows():
            
            logging.info("Downloading Geofabrik data : " + region.url)               
            osm_regions.append(GeofabrikExtract(region.url).get())
            
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

        command = [
            "osmium", "extract",
            "--polygon", tz_boundary_path,
            osm_region,
            "--overwrite",
            "--strategy", "complete_ways",
            "-o", cropped_region_path_str
        ]
        
        subprocess.run(command)
        
        return cropped_region_path
        
        
    def filter_region(
            self,
            cropped_region_path: pathlib.Path,
            object_type: str,
            key: str,
            tags: List[str]
        ) -> pathlib.Path:
        """
        Filters the cropped OSM region file based on specified OSM tags.

        Args:
            cropped_region_path (pathlib.Path): The file path of the cropped OSM region data.
            object_type (str): OSM types to query. "a" for areas, "n" for nodes, "w" for ways, "r" for relations, "nwr" for everything
            key (str): OSM key to query, for instance "building"
            tags (list of str): OSM tag(s) to query
                For the parameters "object_type", "key" and "tags", see https://docs.osmcode.org/osmium/latest/osmium-tags-filter.html for the precise doc            

        Returns:
            pathlib.Path: The file path of the filtered OSM region.
        """
        
        logging.info("Subsetting OSM extracts")
        
        tags = "=" + ",".join(tags) if len(tags) > 0 else ""
        query = object_type + "/" + key + tags
        query_hash = hashlib.md5(query.encode("utf-8")).hexdigest()
        
        filtered_region_name = query_hash + "-filtered-" + cropped_region_path.name
        filtered_region_path = pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"]) / filtered_region_name
        
        command = [
            "osmium", "tags-filter",
            "--overwrite",
            "-o", filtered_region_path,
            cropped_region_path,
            query
        ]
        
        subprocess.run(command)
        
        return filtered_region_path
    
    
    def merge_regions(self, filtered_regions_paths: List[pathlib.Path], result_path: pathlib.Path):
        """
        Merges multiple filtered OSM region files into a single file.

        Args:
            filtered_regions_paths (List[pathlib.Path]): A list of file paths of filtered OSM regions.

        Returns:
            None: The result is saved to the instance's cache path.
        """
        
        filtered_regions_paths = [str(path) for path in filtered_regions_paths]
        
        logging.info("Merging OSM extracts")
        
        command = [
            "osmium", "merge",
            *filtered_regions_paths,
            "--overwrite",
            "-o", str(result_path)
        ]
        
        subprocess.run(command)
        
        return result_path

        
    def crop_by_local_admin_unit(
            self,
            osm_region: pathlib.Path,
            study_area: gpd.GeoDataFrame,
            result_folder: pathlib.Path
        ) -> pathlib.Path:
        
        logging.info("Splitting OSM data by local admin unit...")
        
        study_area = study_area.to_crs(4326)
        
        # Create a config file for osmium specifiying multiple extracts
        # (one for each local admin unit)
        extracts = []
        
        for row in study_area.itertuples():
                
            boundary = geojson.Feature(geometry=row.geometry, properties={})
            
            lau_fp = result_folder / row.local_admin_unit_id
            boundary_fp = lau_fp / "boundary.geojson"
            result_fp = lau_fp / (self.key + ".pbf")
            
            os.makedirs(lau_fp, exist_ok=True)
            
            if result_fp.exists() is False:
            
                with open(boundary_fp, "w") as f:
                    geojson.dump(boundary, f)
                    
                extracts.append({
                    "output": row.local_admin_unit_id + "/" + self.key + ".pbf",
                    "polygon": {
                        "file_name": str(boundary_fp),
                        "file_type": "geojson"
                    }
                })
                
        n_extracts_max = 20
        
        with Progress() as progress:
            
            task = progress.add_task("[green]Extracting buildings in each local admin unit..", total=math.ceil(len(extracts)/n_extracts_max))
                
            for i in range(0, len(extracts), n_extracts_max):
                
                extract_batch = extracts[i:i + n_extracts_max]
                    
                config = {
                    "extracts": extract_batch,
                    "directory": str(result_folder)
                }
                
                config_fp = result_folder / "config.json"
                with open(config_fp, "w") as f:
                    json.dump(config, f)
                
                # Run osmium extract
                command = [
                    "osmium", "extract",
                    "--config", str(config_fp),
                    str(osm_region),
                    "--overwrite",
                    "--strategy", "complete_ways"
                ]
            
                subprocess.run(command)
                
                progress.update(task, advance=1)
        
        # Write the flag to indicate all osmium commands ran
        with open(self.cache_path, "w"):
            pass
        
        return result_folder
