import os
import pathlib
import logging
import zipfile
import pandas as pd
import numpy as np

from mobility.file_asset import FileAsset
from mobility.parsers.download_file import download_file

class CensusLocalizedIndividuals(FileAsset):
    
    def __init__(self, region: str):

        inputs = {"region": region}

        file_name = "census_localized_individuals_" + region + ".parquet"
        cache_path = pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / "insee" / "census_localized_individuals" / file_name

        super().__init__(inputs, cache_path)
        
    def get_cached_asset(self) -> pd.DataFrame:

        logging.info("Census individual data already prepared. Reusing the file : " + str(self.cache_path))
        individuals = pd.read_parquet(self.cache_path)

        return individuals
    
    
    def create_and_get_asset(self) -> pd.DataFrame:
        
        region = self.inputs["region"]
        
        if region == "11":
            zone = "A"
            url = "https://www.data.gouv.fr/fr/datasets/r/30f90960-2839-4bc6-8f20-d5679832793d"
        elif region in ["24", "27", "28", "32"]:
            zone = "B"
            url = "https://www.data.gouv.fr/fr/datasets/r/e6e042cc-9780-458d-b732-888c21433496"
        elif region in ["44", "52", "53"]:
            zone = "C"
            url = "https://www.data.gouv.fr/fr/datasets/r/41797024-a56d-4f50-a0dc-07d5e92bcb53"
        elif region in ["75", "76"]:
            zone = "D"
            url = "https://www.data.gouv.fr/fr/datasets/r/8a325743-2ad5-4ebc-8574-529e3b9cfd5e"
        else:
            zone = "E"
            url = "https://www.data.gouv.fr/fr/datasets/r/8b6cb882-5f1b-4f0d-890c-d78de3b4efa0"

    
        folder = self.cache_path.parent
        if folder.exists() is False:
            os.mkdir(folder)
        
        filename = f"RP2019_INDCVIZ{zone}_csv.zip"
        output_path = self.cache_path.parent / filename
            
        download_file(url, output_path)
        
        with zipfile.ZipFile(output_path, "r") as zip_ref:
            zip_ref.extractall(folder)
            
        unzipped_output_path = self.cache_path.parent / f"FD_INDCVIZ{zone}_2019.csv"
        
        cols = {
            "NUMMI": str,
            "CANTVILLE": str,
            "ARM": str,
            "LPRM": str, 
            "NPERR": str, 
            "CS1": str, 
            "AGEREV": int, 
            "VOIT": str,
            "IPONDI": float
        }
            
        individuals = pd.read_csv(
            unzipped_output_path, 
            sep=";", 
            usecols=list(cols.keys()),
            dtype=cols,
        )
        
        individuals.rename({
            "NUMMI": "household_number",
            "LPRM": "link_ref_pers_household", 
            "NPERR": "n_pers_household", 
            "CS1": "socio_pro_category", 
            "AGEREV": "age", 
            "VOIT": "n_cars",
            "IPONDI": "weight"
        }, axis=1, inplace=True)
        
        
        individuals["household_id"] = individuals["CANTVILLE"] + "-" + individuals["household_number"]
        
        # Split the CSP 8 into 2 groups : inf and sup 15 years old
        conditions = [
            (individuals["socio_pro_category"] == "8") & (individuals["age"] < 15),
            (individuals["socio_pro_category"] == "8") & (individuals["age"] >= 15),
        ]
        choices = ["8a", "8b"]
        
        individuals["socio_pro_category"] = np.select(
            conditions, choices, default=individuals["socio_pro_category"]
        )
        
        individuals["socio_pro_category"] = "fr-" + individuals["socio_pro_category"]
        
        
        # Handle individuals living outside of households
        individuals_in_hh = individuals[individuals["link_ref_pers_household"] != "Z"].copy()
        individuals_out_hh = individuals[individuals["link_ref_pers_household"] == "Z"].copy()
        
        ref_pers_spc = individuals.loc[individuals["link_ref_pers_household"] == "1", ["household_id", "socio_pro_category"]]
        ref_pers_spc.columns = ["household_id", "ref_pers_socio_pro_category"]
        ref_pers_spc.reset_index(inplace=True, drop=True)
        
        individuals_in_hh = pd.merge(individuals_in_hh, ref_pers_spc, on="household_id")
        individuals_in_hh.drop("household_id", axis=1, inplace=True)
        
        individuals_out_hh["n_pers_household"] = "1"
        individuals_out_hh["n_cars"] = "0"
        individuals_out_hh["ref_pers_socio_pro_category"] = individuals_out_hh["socio_pro_category"]
        
        cols = [
            "CANTVILLE",
            "age", "n_pers_household", "n_cars", "socio_pro_category", 
            "ref_pers_socio_pro_category", "weight"
        ]
        
        individuals = pd.concat([
            individuals_in_hh[cols],
            individuals_out_hh[cols]
        ])
        
        individuals["n_pers_household"] = individuals["n_pers_household"].astype(int)
        
        individuals["n_cars"] = individuals["n_cars"].astype(int)
        individuals["n_cars"] = np.where(individuals["n_cars"] < 2, individuals["n_cars"].astype(str), "2+")
        
        individuals.to_parquet(self.cache_path)
        
        os.unlink(output_path)
        os.unlink(unzipped_output_path)
        
        return individuals