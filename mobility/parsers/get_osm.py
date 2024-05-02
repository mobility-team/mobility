import os
import pathlib
import geopandas as gpd
import requests
import subprocess
import shapely
import geojson
import re

from mobility.parsers.old_regions import prepare_old_regions


def get_osm(transport_zones_boundary, proxies={}, verify=None):
    
    data_folder_path = (
        pathlib.Path.home() / ".mobility/data"
    )

    if data_folder_path.exists() is False:
        os.makedirs(data_folder_path)
        
        
    # Select regions to download
    path = data_folder_path / "france-geojson/regions-avant-redecoupage-2015.geojson"

    if path.exists() is False:
        prepare_old_regions()
        
    regions = gpd.read_file(path)
    
    regions = regions[regions.intersects(transport_zones_boundary)]
    
    
    region_names = {
        "11": "ile-de-france",
        "21": "champagne-ardenne",
        "22": "picardie",
        "23": "haute-normandie",
        "24": "centre",
        "25": "basse-normandie",
        "26": "bourgogne",
        "31": "nord-pas-de-calais",
        "41": "lorraine",
        "42": "alsace",
        "43": "franche-comte",
        "52": "pays-de-la-loire",
        "53": "bretagne",
        "54": "poitou-charentes",
        "72": "aquitaine",
        "73": "midi-pyrenees",
        "74": "limousin",
        "82": "rhone-alpes",
        "83": "auvergne",
        "91": "languedoc-roussillon",
        "93": "provence-alpes-cote-d-azur",
    }
    
    osm_file_paths = []
    
    for region_code in regions["code"].values:
        
        url = "https://download.geofabrik.de/europe/france/{region_name}-latest.osm.pbf".format(region_name=region_names[region_code])
        file_name = pathlib.Path(url).name

        # Download OSM geofabrik extracts
        if (data_folder_path / "osm").exists() is False:
            os.makedirs(data_folder_path / "osm")
        
        path = data_folder_path / "osm" / file_name
        
        if path.exists() is False:
            
            print("Downloading OSM extracts")
            
            r = requests.get(
                url=url,
                proxies=proxies,
                verify=verify
            )
            with open(path, "wb") as file:
                file.write(r.content)
                
        osm_file_paths.append(path)
        
        
    # Store the boundary as a temporary geojson file
    if (data_folder_path / "tmp").exists() is False:
        os.makedirs(data_folder_path / "tmp")
    
    tz_boundary_geojson = geojson.Feature(geometry=transport_zones_boundary, properties={})
    
    tz_boundary_path = data_folder_path / "tmp" / "transport_zones_boundary.geojson"
    
    with open(tz_boundary_path, "w") as f:
        geojson.dump(tz_boundary_geojson, f)
        
    tz_boundary_path_wsl = windows_path_to_wsl(tz_boundary_path)
    
    filter_paths_wsl = []
    
    for osm_path in osm_file_paths:
        
        print("Cropping OSM extracts")
        
        osm_path_wsl = windows_path_to_wsl(osm_path)

        subset_name = "subset-" + osm_path.name
        subset_path_wsl = windows_path_to_wsl(data_folder_path / "tmp" / subset_name)
        
        command = f"wsl osmium extract --polygon {tz_boundary_path_wsl} {osm_path_wsl} --overwrite -o {subset_path_wsl}"
        subprocess.run(command, shell=True)
        
        print("Subsetting OSM extracts")
        
        osm_tags = {
            "car": [
                "motorway", "trunk", "primary", "secondary", "tertiary",
                "unclassified", "residential", "service", "living_street", 
                "motorway_link", "trunk_link", "primary_link", "secondary_link",
                "tertiary_link"
            ]
        }
        
        tags_cmd = "nw/highway=" + ",".join(osm_tags["car"])
        
        filter_name = "filter-" + osm_path.name
        filter_path_wsl = windows_path_to_wsl(data_folder_path / "tmp" / filter_name)
        
        command = f"wsl osmium tags-filter --overwrite -o {filter_path_wsl} {subset_path_wsl} {tags_cmd}"
        subprocess.run(command, shell=True)
        
        filter_paths_wsl.append(filter_path_wsl)
        
    if len(filter_paths_wsl) > 1:
        
        print("Merging OSM extracts")
        
        filter_paths_wsl = " ".join(filter_paths_wsl)
        merged_path_wsl = windows_path_to_wsl(data_folder_path / "tmp" / "merged.osm.pbf")
        
        command = f"wsl osmium merge {filter_paths_wsl} --overwrite -o {merged_path_wsl}"
        subprocess.run(command, shell=True)
        
    else:
        
        merged_path_wsl = filter_paths_wsl[0]
    
        
    merged_path = wsl_path_to_windows(merged_path_wsl)

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