import os
import pathlib
import requests
import geopandas as gpd

def prepare_old_regions(proxies={}):
    
    data_folder_path = (
        pathlib.Path.home() / ".mobility/data/france-geojson"
    )

    if data_folder_path.exists() is False:
        os.makedirs(data_folder_path)

    # Download the IGN data from data.gouv.fr if needed
    path = data_folder_path / "regions-avant-redecoupage-2015.geojson"
    
    if path.exists() is False:
        
        print("Downloading old regions boundaries.")
        
        # Download the zip file
        r = requests.get(
            url="https://raw.githubusercontent.com/gregoiredavid/france-geojson/master/regions-avant-redecoupage-2015.geojson",
            proxies=proxies
        )
        with open(path, "wb") as file:
            file.write(r.content)
            
    