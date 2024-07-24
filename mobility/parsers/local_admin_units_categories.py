import os
import pathlib
import logging
import zipfile
import pandas as pd
import numpy as np

from mobility.asset import Asset
from mobility.parsers.download_file import download_file

class LocalAdminUnitsCategories(Asset):
    
    def __init__(self):
        
        inputs = {}
        cache_path = pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / "local_admin_units_categories.parquet"
        super().__init__(inputs, cache_path)
        
    def get_cached_asset(self) -> pd.DataFrame:

        logging.info("Local administrative units categories already prepared. Reusing the file : " + str(self.cache_path))
        local_admin_units_categories = pd.read_parquet(self.cache_path)

        return local_admin_units_categories
    
    
    def create_and_get_asset(self) -> pd.DataFrame:
        
        logging.info("Preparing local administrative units.")
        
        local_admin_units_categories_fr = self.prepare_french_local_admin_units_categories()
        local_admin_units_categories_ch = self.prepare_swiss_local_admin_units_categories()
        
        local_admin_units = pd.concat([local_admin_units_categories_fr, local_admin_units_categories_ch])
        local_admin_units.to_parquet(self.cache_path)

        return local_admin_units
    
    
    def prepare_french_local_admin_units_categories(self):

        url = "https://www.data.gouv.fr/fr/datasets/r/c59f74bb-8095-4e41-9627-5fecca95668d"
        path = pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / "insee/UU2020_au_01-01-2023.zip"
        download_file(url, path)

        # Unzip the content
        with zipfile.ZipFile(path, "r") as zip_ref:
            zip_ref.extractall(path.parent)
       
        path = pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / "insee/UU2020_au_01-01-2023.xlsx"
        
        categories = pd.read_excel(
            path,
            sheet_name="Composition_communale",
            skiprows=5
        )
        
        categories = categories.iloc[:, [0, 5]]
        categories.columns = ["local_admin_unit_id", "urban_unit_category"]
        
        # Add Paris Lyon Marseille arrondissements
        arr_categories = pd.concat([
            pd.DataFrame({"local_admin_unit_id": np.arange(75101, 75121, 1).astype(str), "urban_unit_category": "C"}),
            pd.DataFrame({"local_admin_unit_id": np.arange(69381, 69390, 1).astype(str), "urban_unit_category": "C"}),
            pd.DataFrame({"local_admin_unit_id": np.arange(13201, 13218, 1).astype(str), "urban_unit_category": "C"})
        ])
        
        categories = categories[~categories["local_admin_unit_id"].isin(["69123", "75056", "13055"])]
        
        categories = pd.concat([categories, arr_categories])
        
        categories["local_admin_unit_id"] = "fr-" + categories["local_admin_unit_id"].astype(str)
        
        categories["urban_unit_category"] = np.where(
            categories["urban_unit_category"] != "H",
            categories["urban_unit_category"],
            "R"
        )
        
        return categories
    
    
    def prepare_swiss_local_admin_units_categories(self):
        
        url = "https://www.data.gouv.fr/fr/datasets/r/c776c9fe-5405-4568-b456-65209387035b"
        
        data_folder = pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / "bfs"
        file_path = data_folder / "BFS - Typologie des communes 2020 en 9 catégories.xlsx"
        
        file_path = download_file(url, file_path)
        
        
        categories = pd.read_excel(file_path, skiprows=4, skipfooter=11)
        categories = categories.iloc[:, [0, 2]]
        categories.columns = ["local_admin_unit_id", "urban_unit_category"]
        
        categories["local_admin_unit_id"] = "ch-" + categories["local_admin_unit_id"].astype(str)
        
        categories["urban_unit_category"] = categories["urban_unit_category"].map({
            "Commune urbaine d’une grande agglomération (11)": "C",
            "Commune urbaine d'une agglomération moyenne (12)": "C",
            "Commune urbaine d’une petite ou hors agglomération (13)": "I",
            "Commune périurbaine de forte densité (21)": "B",
            "Commune périurbaine de moyenne densité (22)": "B",
            "Commune périurbaine de faible densité (23)": "B",
            "Commune d’un centre rural (31)": "R",
            "Commune rurale en situation centrale (32)": "R",
            "Commune rurale périphérique (33)": "R"
        })
        
        return categories
