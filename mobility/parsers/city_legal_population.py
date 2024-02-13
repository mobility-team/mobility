import os
import pathlib
import logging
import zipfile
import pandas as pd
import numpy as np

from mobility.asset import Asset
from mobility.parsers.download_file import download_file

class CityLegalPopulation(Asset):
    
    def __init__(self):
        
        inputs = {}

        file_name = "insee_city_legal_population.parquet"
        cache_path = pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / file_name

        super().__init__(inputs, cache_path)
        
    def get_cached_asset(self) -> pd.DataFrame:

        logging.info("Legal populations already prepared. Reusing the file : " + str(self.cache_path))
        legal_populations = pd.read_parquet(self.cache_path)

        return legal_populations
    
    
    def create_and_get_asset(self) -> pd.DataFrame:
        
        url = "https://www.data.gouv.fr/fr/datasets/r/1443e7dc-3e22-4961-aad6-84fdb2c9d429"
        folder = pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / "insee" / "legal_populations"
        if folder.exists() is False:
            os.mkdir(folder)
        path = folder / "city_legal_populations.zip"
        download_file(url, path)
        
        with zipfile.ZipFile(path, "r") as zip_ref:
            zip_ref.extractall(folder)
            
        legal_populations = pd.read_csv(
            folder / "donnees_communes.csv",
            sep=";",
            usecols=["COM", "PTOT"],
            dtype={"COM": str, "PTOT": np.int32}
        )
        legal_populations.columns = ["insee_city_id", "legal_population"]
        
        legal_populations.to_parquet(self.cache_path)
        
        return legal_populations