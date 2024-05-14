import os
import pathlib
import logging
import zipfile
import py7zr
import shapely
import geopandas as gpd
import pandas as pd

from mobility.asset import Asset
from mobility.parsers.download_file import download_file

class LocalAdminUnits(Asset):
    
    def __init__(self):
        
        inputs = {}
        cache_path = pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / "local_admin_units.parquet"
        super().__init__(inputs, cache_path)
        
    def get_cached_asset(self) -> pd.DataFrame:

        logging.info("Local administrative units already prepared. Reusing the file : " + str(self.cache_path))
        local_admin_units = gpd.read_parquet(self.cache_path)

        return local_admin_units
    
    
    def create_and_get_asset(self) -> pd.DataFrame:
        
        logging.info("Preparing local administrative units.")
        
        local_admin_units_fr = self.prepare_french_local_admin_units()
        local_admin_units_ch = self.prepare_swiss_local_admin_units()
        
        local_admin_units = pd.concat([local_admin_units_fr, local_admin_units_ch])
        local_admin_units.to_parquet(self.cache_path)

        return local_admin_units
    
    
    def prepare_french_local_admin_units(self):
        
        logging.info("Preparing french city limits...")
        
        url = "https://data.cquest.org/ign/adminexpress/ADMIN-EXPRESS-COG-CARTO_3-2__SHP_LAMB93_FXX_2023-05-03.7z"
        path = pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / "ign/admin-express/ADMIN-EXPRESS-COG-CARTO_3-2__SHP_LAMB93_FXX_2023-05-03.7z"
        download_file(url, path)
        
        with py7zr.SevenZipFile(path, "r") as z:
            z.extractall(path.parent)
                
        # Convert to geoparquet
        path = path.parent / "ADMIN-EXPRESS-COG-CARTO_3-2__SHP_LAMB93_FXX_2023-05-03" / \
         "ADMIN-EXPRESS-COG-CARTO" / "1_DONNEES_LIVRAISON_2023-05-03" / "ADECOGC_3-2_SHP_LAMB93_FXX"
        
        # Replace Paris / Lyon / Marseille cities with their constituting arrondissements
        arrond = gpd.read_file(path / "ARRONDISSEMENT_MUNICIPAL.shp")
        cities = gpd.read_file(path / "COMMUNE.shp")
        
        cities = cities[["INSEE_COM", "NOM", "geometry"]]
        arrond = arrond[["INSEE_COM", "INSEE_ARM", "NOM", "geometry"]]
        
        arrond["INSEE_COM"] = arrond["INSEE_ARM"]
        arrond = arrond[["INSEE_COM", "NOM", "geometry"]]
        
        cities = cities[~cities["INSEE_COM"].isin(arrond["INSEE_COM"])]
        
        cities = pd.concat([
            cities,
            arrond
        ])
        
        cities.columns = ["local_admin_id", "local_admin_name", "geometry"]
        
        cities["local_admin_id"] = "fr-" + cities["local_admin_id"]
        
        cities = cities.to_crs(3035)
        
        return cities
    
    
    def prepare_swiss_local_admin_units(self):
        
        url = "https://data.geo.admin.ch/ch.swisstopo.swissboundaries3d/swissboundaries3d_2024-01/swissboundaries3d_2024-01_2056_5728.gpkg.zip"
        
        data_folder = pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / "swisstopo"
        zip_path = data_folder / "swissboundaries3d_2024-01_2056_5728.gpkg.zip"
        file_path = data_folder / "swissBOUNDARIES3D_1_5_LV95_LN02.gpkg"
        
        download_file(url, zip_path)
        
        # Unzip the content
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(data_folder)
        
        
        cities = gpd.read_file(file_path, layer="tlm_hoheitsgebiet")
        
        cities = cities[["bfs_nummer", "name", "geometry"]].copy()
        cities.columns = ["local_admin_id", "local_admin_name", "geometry"]
        
        cities["local_admin_id"] = "ch-" + cities["local_admin_id"].astype(str)
        
        cities["geometry"] = shapely.wkb.loads(shapely.wkb.dumps(cities["geometry"], output_dimension=2))
        cities = cities.to_crs(3035)
        
        return cities
