import logging
import numpy as np
import pandas as pd
import os
from pathlib import Path
import pathlib

import requests
import zipfile

from mobility.file_asset import FileAsset
from mobility.parsers.download_file import download_file


#script
import mobility



class WorkHomeFlows_fr(FileAsset):

    
    def __init__(self, year="2021"):
        
        inputs = {"year": year}

        file_name = f"insee_mobpro_{year}.parquet"
        cache_path = pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / "insee" / "flows" / file_name

        super().__init__(inputs, cache_path)
        
    def get_cached_asset(self) -> pd.DataFrame:

        logging.info("French home-work flows already prepared. Reusing the file : " + str(self.cache_path))
        flows = pd.read_parquet(self.cache_path)

        return flows
    
    
    def create_and_get_asset(self) -> pd.DataFrame:
        """
        Parse and format french home-work flows for the given year.
        
        Returns:
            A pandas.DataFrame giving the french home-work flows
        """

        urls ={
        "2021" : "https://www.insee.fr/fr/statistiques/fichier/8201899/base-flux-mobilite-domicile-lieu-travail-2021-csv.zip",
        "2020" : "https://www.insee.fr/fr/statistiques/fichier/7630376/base-flux-mobilite-domicile-lieu-travail-2020-csv.zip",
        "2019" : "https://www.insee.fr/fr/statistiques/fichier/6454112/base-csv-flux-mobilite-domicile-lieu-travail-2019.zip",
        "2018" : "https://www.insee.fr/fr/statistiques/fichier/5393835/base-csv-flux-mobilite-domicile-lieu-travail-2018.zip"
        }
        
        year = self.year
        
        folder = pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / "insee" / "flows"
        if folder.exists() is False:
            os.mkdir(folder)
        path = folder / f"insee_mobpro_{year}.zip"
        download_file(urls[year], path)
        
        with zipfile.ZipFile(path, "r") as zip_ref:
            zip_ref.extractall(folder)
            
        match self.year:
            case "2021":
                file_name= "base-flux-mobilite-domicile-lieu-travail-2021.csv"
                col_name = "NBFLUX_C21_ACTOCC15P"
            case "2020":
                file_name= "base-flux-mobilite-domicile-lieu-travail-2020.csv"
                col_name = "NBFLUX_C20_ACTOCC15P"
            case "2019":
                file_name= "base-flux-mobilite-domicile-lieu-travail-2019.csv"
                col_name = "NBFLUX_C19_ACTOCC15P"
            case "2018":
                file_name= "base-flux-mobilite-domicile-lieu-travail-2018.csv"
                col_name = "NBFLUX_C18_ACTOCC15P"                
                
        flows = pd.read_csv(
            folder / file_name,
            sep=";",
            usecols=["CODGEO", "DCLT", col_name],
            dtype={"CODGEO": str, "DCLT": str, col_name: np.float32}
        )
        flows.columns = ["local_admin_unit_id_from", "local_admin_unit_id_to","insee_flows"]
        
        flows["local_admin_unit_id_from"] = "fr-" + flows["local_admin_unit_id_from"]
        flows["local_admin_unit_id_to"] = "fr-" + flows["local_admin_unit_id_to"]
 
        flows.to_parquet(self.cache_path)
        
        return flows
