import os
import pathlib
import py7zr
import logging
import pandas as pd
import geopandas as gpd

from mobility.parsers.download_file import download_file


def prepare_french_admin_boundaries():
    
    logging.info("Preparing french city limits...")
    
    url = "https://data.cquest.org/ign/adminexpress/ADMIN-EXPRESS-COG-CARTO_3-2__SHP_LAMB93_FXX_2023-05-03.7z"
    path = pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / "ign/admin-express/ADMIN-EXPRESS-COG-CARTO_3-2__SHP_LAMB93_FXX_2023-05-03.7z"
    download_file(url, path)
    
    with py7zr.SevenZipFile(path, "r") as z:
        z.extractall(path.parent)
            
    # Convert to geoparquet
    path = path.parent / "ADMIN-EXPRESS-COG-CARTO_3-2__SHP_LAMB93_FXX_2023-05-03" / \
     "ADMIN-EXPRESS-COG-CARTO" / "1_DONNEES_LIVRAISON_2023-05-03" / "ADECOGC_3-2_SHP_LAMB93_FXX"
    
    for shp_file in ["ARRONDISSEMENT_MUNICIPAL.shp", "COMMUNE.shp", "EPCI.shp", "REGION.shp"]:
            
         df = gpd.read_file(path / shp_file)
         parquet_file = pathlib.Path(shp_file).stem + ".parquet"
         output_path = pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / "ign/admin-express" / parquet_file
         df.to_parquet(output_path)
        
    # Replace Paris / Lyon / Marseille cities with their constituting arrondissements
    arrond = gpd.read_parquet(pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / "ign/admin-express" / "ARRONDISSEMENT_MUNICIPAL.parquet")
    cities = gpd.read_parquet(pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / "ign/admin-express" / "COMMUNE.parquet")
    
    cities.loc[cities["INSEE_CAN"] == "NR", "INSEE_CAN"] = "ZZ"
    cities["INSEE_CAN"] = cities["INSEE_DEP"] + cities["INSEE_CAN"]
    
    # Lyon : 69ZZ, Paris : 75ZZ, Marseille : 1398
    arrond["INSEE_CAN"] = arrond["INSEE_ARM"].str[0:2] + "ZZ"
    arrond.loc[arrond["INSEE_ARM"].str[0:2] == "13", "INSEE_CAN"] = "1398"
    
    cities = cities[["INSEE_COM", "INSEE_CAN", "NOM", "SIREN_EPCI", "geometry"]]
    arrond = arrond[["INSEE_COM", "INSEE_CAN", "INSEE_ARM", "NOM", "geometry"]]
    
    arrond = pd.merge(
        arrond,
        pd.DataFrame(cities.drop(columns='geometry'))[["INSEE_COM", "SIREN_EPCI"]],
        on="INSEE_COM"
    )
    
    cities = cities[~cities["INSEE_COM"].isin(arrond["INSEE_COM"])]
    
    arrond["INSEE_COM"] = arrond["INSEE_ARM"]
    
    cities = pd.concat([
        cities,
        arrond[["INSEE_COM", "INSEE_CAN", "NOM", "SIREN_EPCI", "geometry"]]
    ])
    
    cities.to_parquet(pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / "ign/admin-express" / "COMMUNE_mod.parquet")
    
         
    
def get_french_cities_boundaries():
    
    path = pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / "ign/admin-express/COMMUNE_mod.parquet"
    
    if path.exists() is False:
        prepare_french_admin_boundaries()
    
    cities = gpd.read_parquet(path, columns=["INSEE_COM", "INSEE_CAN", "SIREN_EPCI", "NOM", "geometry"])
    
    # Fix the Grand Paris EPCI ids
    cities.loc[cities["SIREN_EPCI"].str[0:9] == "200054781", "SIREN_EPCI"] = "200054781"
    
    return cities
         

def get_french_epci_boundaries():
    
    path = pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / "ign/admin-express/EPCI.parquet"
    
    if path.exists() is False:
        prepare_french_admin_boundaries()
    
    epci = gpd.read_parquet(path)
    
    return epci


def get_french_regions_boundaries():
    
    path = pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / "ign/admin-express/REGION.parquet"
    
    if path.exists() is False:
        prepare_french_admin_boundaries()
    
    regions = gpd.read_parquet(path)
    
    return regions



    