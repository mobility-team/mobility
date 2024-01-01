import os
import pathlib
import subprocess
import geojson
import logging

from mobility.parsers.download_file import download_file
from mobility.parsers.admin_boundaries import get_french_old_regions_boundaries
from mobility.dodgr_modes import get_dodgr_mode

def prepare_osm(transport_zones, mode, update_needed):
    """
        Prepares OSM data for the area of the transport zones.
        - Downloads OSM data from geofabrik.
        - Crops data to the area.
        - Subsets data to keep only relevant ways and nodes for routing with dodgr.
        - Merges all OSM data into one file.
    """
    
    dodgr_mode = get_dodgr_mode(mode)
    
    # File path for the final result
    output_file_name = "osm_data_" + dodgr_mode + ".osm"
    output_file_path = pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"]) / output_file_name
    
    if update_needed is True:
        
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
        tz_boundary_path = pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"]) / "transport_zones_boundary.geojson"
        
        with open(tz_boundary_path, "w") as f:
            geojson.dump(tz_boundary_geojson, f)
        
        filter_paths = []
        
        for osm_path in osm_file_paths:
            
            logging.info("Cropping OSM extracts")
            subset_name = "subset-" + osm_path.name
            subset_path = str(pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"]) / subset_name)
            command = f"osmium extract --polygon {tz_boundary_path} {osm_path} --overwrite --strategy complete_ways -o {subset_path}"
            subprocess.run(command, shell=True)
            
            logging.info("Subsetting OSM extracts")
            filter_name = "filter-" + osm_path.name
            filter_path = str(pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"]) / filter_name)
            osm_tags = get_dodgr_osm_tags(dodgr_mode)
            command = f"osmium tags-filter --overwrite -o {filter_path} {subset_path} w/highway={osm_tags}"
            subprocess.run(command, shell=True)
            
            filter_paths.append(filter_path)
            
        filter_paths = " ".join(filter_paths)
            
        logging.info("Merging OSM extracts")
        command = f"osmium merge {filter_paths} --overwrite -o {output_file_path}"
        subprocess.run(command, shell=True)
        
    else:
        
        logging.info("OSM data already pre-processed. Reusing the file " + str(output_file_path))

    return output_file_path


def get_dodgr_osm_tags(dodgr_mode):
    
    # Get OSM highway tags that are valid for the given mode
    # (= tags of ways that dodgr uses for routing for this mode)
    path_to_r_script = pathlib.Path(__file__).parents[1] / "get_dodgr_osm_tags.R"
    cmd = ["Rscript", path_to_r_script, "-d", dodgr_mode]
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output, error = process.communicate()
    
    osm_tags = output.decode().strip()
    
    return osm_tags