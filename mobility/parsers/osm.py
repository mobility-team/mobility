import os
import pathlib
import subprocess
import geojson
import re
import logging

from mobility.parsers.download_file import download_file
from mobility.parsers.admin_boundaries import get_french_old_regions_boundaries

def prepare_osm(transport_zones, force=False):
    """
        Prepares OSM data for the area of the transport zones.
        - Downloads OSM data from geofabrik.
        - Crops data to the area.
        - Subsets data to keep only relevant ways and nodes for routing with dodgr.
        - Merges all OSM data into one file.
    """
    
    # File path for the final result
    merged_path = pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / "merged.osm"
    
    if merged_path.exists() is False or force is True:
        
        logging.info("Downloading and pre-processing OSM data.")
    
        # Merge all transport zones into one polygon
        transport_zones_boundary = transport_zones.to_crs(4326).unary_union.buffer(0.1)
              
        # Find which geofabrik regions are included in the transport zone 
        regions = get_french_old_regions_boundaries()
        regions = regions[regions.intersects(transport_zones_boundary)]
        
    
        osm_file_paths = []
        
        for geofabrik_region_name in regions["geofabrik_name"].values:
            
            url = f"https://download.geofabrik.de/europe/france/{geofabrik_region_name}-latest.osm.pbf"
            path = pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / "osm" / f"{geofabrik_region_name}-latest.osm.pbf"
            download_file(url, path)
                    
            osm_file_paths.append(path)
            
            
        # Store the boundary as a temporary geojson file
        tz_boundary_geojson = geojson.Feature(geometry=transport_zones_boundary, properties={})
        tz_boundary_path = pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / "transport_zones_boundary.geojson"
        
        with open(tz_boundary_path, "w") as f:
            geojson.dump(tz_boundary_geojson, f)
            
        tz_boundary_path_wsl = windows_path_to_wsl(tz_boundary_path)
        
        filter_paths_wsl = []
        
        for osm_path in osm_file_paths:
            
            logging.info("Cropping OSM extracts")
            
            osm_path_wsl = windows_path_to_wsl(osm_path)
    
            subset_name = "subset-" + osm_path.name
            subset_path = pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / subset_name
            subset_path_wsl = windows_path_to_wsl(subset_path)
            
            command = f"wsl osmium extract --polygon {tz_boundary_path_wsl} {osm_path_wsl} --overwrite --strategy complete_ways -o {subset_path_wsl}"
            subprocess.run(command, shell=True)
            
            logging.info("Subsetting OSM extracts")
            
            filter_name = "filter-" + osm_path.name
            filter_path = pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / filter_name
            filter_path_wsl = windows_path_to_wsl(filter_path)
            
            highway_tags = [
                "motorway", "trunk", "primary", "secondary", "tertiary",
                "unclassified", "residential", "service", "living_street",
                "motorway_link", "trunk_link", "primary_link", "secondary_link",
                "tertiary_link"
            ]
            highway_tags = ",".join(highway_tags)
            
            command = f"wsl osmium tags-filter --overwrite -o {filter_path_wsl} {subset_path_wsl} w/highway={highway_tags}"
            subprocess.run(command, shell=True)
            
            filter_paths_wsl.append(filter_path_wsl)
            
            
        logging.info("Merging OSM extracts")
            
        filter_paths_wsl = " ".join(filter_paths_wsl)
        merged_path_wsl = windows_path_to_wsl(merged_path)
        
        command = f"wsl osmium merge {filter_paths_wsl} --overwrite -o {merged_path_wsl}"
        subprocess.run(command, shell=True)
        
    else:
        
        logging.info("OSM data already pre-processed. Reusing the file " + str(merged_path))

    return merged_path
    
    
def windows_path_to_wsl(path):
    # Convert Path object to string for regex operations
    path_str = str(path)

    # This regex pattern will match the drive letter and colon (e.g., 'C:')
    pattern = r"^[a-zA-Z]:"

    # Check if the path matches the Windows drive pattern
    if re.match(pattern, path_str):
        # Replace backslashes with forward slashes and drive letter
        path_str = re.sub(pattern, lambda x: f"/mnt/{x.group().lower()[0]}", path_str)
        path_str = path_str.replace("\\", "/")

    return path_str


def wsl_path_to_windows(path):
    # This regex pattern will match the WSL path format (e.g., '/mnt/c/')
    pattern = r"^/mnt/[a-zA-Z]/"

    # Check if the path matches the WSL path pattern
    if re.match(pattern, path):
        # Replace the '/mnt/' part with the drive letter and a colon
        path = re.sub(pattern, lambda x: f"{x.group()[5].upper()}:\\", path)

        # Replace forward slashes with backslashes
        path = path.replace("/", "\\")

    return path