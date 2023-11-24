import os
import pathlib
import requests
import py7zr
import geopandas as gpd

def prepare_ign(proxies={}):
    
    data_folder_path = (
        pathlib.Path.home() / ".mobility/data/ign/admin-express"
    )

    if data_folder_path.exists() is False:
        os.makedirs(data_folder_path)

    # Download the IGN data from data.gouv.fr if needed
    path = data_folder_path / "ADMIN-EXPRESS-COG-CARTO_3-2__SHP_LAMB93_FXX_2023-05-03.7z"
    
    if path.exists() is False:
        
        print("Downloading IGN data (Admin Express)")
        
        # Download the zip file
        r = requests.get(
            url="https://data.cquest.org/ign/adminexpress/ADMIN-EXPRESS-COG-CARTO_3-2__SHP_LAMB93_FXX_2023-05-03.7z",
            proxies=proxies
        )
        with open(path, "wb") as file:
            file.write(r.content)

        # Unzip the content    
        with py7zr.SevenZipFile(path, "r") as z:
            z.extractall(data_folder_path)
            
            
    # Convert to geoparquet
    path = data_folder_path / "ADMIN-EXPRESS-COG-CARTO_3-2__SHP_LAMB93_FXX_2023-05-03" / \
     "ADMIN-EXPRESS-COG-CARTO" / "1_DONNEES_LIVRAISON_2023-05-03" / "ADECOGC_3-2_SHP_LAMB93_FXX"
    
    for shp_file in ["COMMUNE.shp", "EPCI.shp", "REGION.SHP"]:
            
         df = gpd.read_file(path / shp_file)
         parquet_file = pathlib.Path(shp_file).stem + ".parquet"
         df.to_parquet(data_folder_path / parquet_file)
         
    
         
         