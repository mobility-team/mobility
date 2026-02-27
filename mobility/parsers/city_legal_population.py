import os
import pathlib
import logging
import zipfile
import pandas as pd
import numpy as np

from mobility.file_asset import FileAsset
from mobility.parsers.download_file import download_file

class CityLegalPopulation(FileAsset):
    
    def __init__(self):
        
        inputs = {}

        file_name = "insee_city_legal_population.parquet"
        cache_path = pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / "insee" / "legal_populations" / file_name

        super().__init__(inputs, cache_path)
        
    def get_cached_asset(self) -> pd.DataFrame:

        logging.info("Legal populations already prepared. Reusing the file : " + str(self.cache_path))
        legal_populations = pd.read_parquet(self.cache_path)

        return legal_populations
    
    
    def create_and_get_asset(self) -> pd.DataFrame:
        """
        Parse, format and concatenate the french and swiss legal population
        databases.
        
        Returns:
            A pandas.DataFrame giving the legal population of each city in
            France and Switzerland.
        """
        
        pop_fr = self.prepare_french_cities_legal_populations()
        pop_ch = self.prepare_swiss_cities_legal_populations()
        
        pop = pd.concat([pop_fr, pop_ch])
        pop.to_parquet(self.cache_path)
        
        return pop
        
        
    def prepare_french_cities_legal_populations(self) -> pd.DataFrame:
        """
        Parse, format and concatenate the french legal population database.
        
        Returns:
            A pandas.DataFrame giving the legal population of each city in
            France.
        """
        
        url = "https://www.data.gouv.fr/fr/datasets/r/1443e7dc-3e22-4961-aad6-84fdb2c9d429"
        folder = pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / "insee" / "legal_populations"
        if folder.exists() is False:
            os.mkdir(folder)
        path = folder / "city_legal_populations.zip"
        download_file(url, path)
        
        with zipfile.ZipFile(path, "r") as zip_ref:
            zip_ref.extractall(folder)
            
        pop = pd.read_csv(
            folder / "donnees_communes.csv",
            sep=";",
            usecols=["COM", "PTOT"],
            dtype={"COM": str, "PTOT": np.int32}
        )
        pop.columns = ["local_admin_unit_id", "legal_population"]
        
        pop["local_admin_unit_id"] = "fr-" + pop["local_admin_unit_id"]
        
        return pop
    
    
    def prepare_swiss_cities_legal_populations(self) -> pd.DataFrame:
        """
        Parse, format and concatenate the swiss legal population database.
        
        Returns:
            A pandas.DataFrame giving the legal population of each city in
            Switzerland.
        """
        
        url = "https://www.data.gouv.fr/fr/datasets/r/5529f7f8-7a00-4890-b453-0d215c7a5726"
        file_path = pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / "bfs" / "je-f-21.03.01.xlsx"
        download_file(url, file_path)
        
        pop = pd.read_excel(file_path)
        pop = pop.iloc[8:2180, [0, 2]]
        pop.columns = ["local_admin_unit_id", "n_pop_total"]
        pop["local_admin_unit_id"] = "ch-" + pop["local_admin_unit_id"].astype(int).astype(str)
            
        pop.columns = ["local_admin_unit_id", "legal_population"]
        
        return pop