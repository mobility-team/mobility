import os
import pathlib
import logging
import zipfile
import pandas as pd

from mobility.asset import Asset
from mobility.parsers.download_file import download_file

class JobsActivePopulationDistribution(Asset):
    
    def __init__(self):
        
        inputs = {}
        
        cache_path = {
            "active_population": pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / "insee" / "active_population.parquet",
            "jobs": pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / "insee" / "jobs.parquet"
        }

        super().__init__(inputs, cache_path)
        
    def get_cached_asset(self) -> pd.DataFrame:

        logging.info("Jobs and active population spatial distribution already prepared. Reusing the files : " + str(self.cache_path))
        
        active_population = pd.read_parquet(self.cache_path["active_population"])
        jobs = pd.read_parquet(self.cache_path["jobs"])

        return active_population, jobs
    
    
    def create_and_get_asset(self) -> pd.DataFrame:
        
        url = "https://www.data.gouv.fr/fr/datasets/r/02653cc4-76c0-4c3a-bc17-d5485c7ea2b9"
        
        data_folder = pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / "insee"
        zip_path = data_folder / "base-cc-emploi-pop-active-2019.zip"
        csv_path = data_folder / "base-cc-emploi-pop-active-2019.csv"
    
        download_file(url, zip_path)
        
        logging.info("Zip downloaded." if zip_path.exists() else "Zip download failed.")
        
        # Unzip the content
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            logging.info("csv in zip." if "base-cc-emploi-pop-active-2019.csv" in zip_ref.namelist() else "csv not in zip.")
            zip_ref.extractall(data_folder)
            
        logging.info("csv extracted." if csv_path.exists() else "csv not extracted.")
        
        # Informations about jobs and active population for each city
        jobs_active_population = pd.read_csv(
            csv_path,
            sep=";",
            usecols=[
                "CODGEO",
                "P19_ACT15P",
                "C19_ACT1564_CS1",
                "C19_ACT1564_CS2",
                "C19_ACT1564_CS3",
                "C19_ACT1564_CS4",
                "C19_ACT1564_CS5",
                "C19_ACT1564_CS6",
                "P19_EMPLT",
                "C19_EMPLT_CS1",
                "C19_EMPLT_CS2",
                "C19_EMPLT_CS3",
                "C19_EMPLT_CS4",
                "C19_EMPLT_CS5",
                "C19_EMPLT_CS6",
            ],
            dtype={"CODGEO": str}
        )

        jobs = jobs_active_population.loc[
            :,
            [
                "CODGEO",
                "P19_EMPLT",
                "C19_EMPLT_CS1",
                "C19_EMPLT_CS2",
                "C19_EMPLT_CS3",
                "C19_EMPLT_CS4",
                "C19_EMPLT_CS5",
                "C19_EMPLT_CS6",
            ],
        ]
        jobs.set_index("CODGEO", inplace=True)
        jobs.rename(
            columns={
                "P19_EMPLT": "n_jobs_total",
                "C19_EMPLT_CS1": "n_jobs_CS1",
                "C19_EMPLT_CS2": "n_jobs_CS2",
                "C19_EMPLT_CS3": "n_jobs_CS3",
                "C19_EMPLT_CS4": "n_jobs_CS4",
                "C19_EMPLT_CS5": "n_jobs_CS5",
                "C19_EMPLT_CS6": "n_jobs_CS6",
            },
            inplace=True
        )

        active_population = jobs_active_population.loc[
            :,
            [
                "CODGEO",
                "P19_ACT15P",
                "C19_ACT1564_CS1",
                "C19_ACT1564_CS2",
                "C19_ACT1564_CS3",
                "C19_ACT1564_CS4",
                "C19_ACT1564_CS5",
                "C19_ACT1564_CS6",
            ],
        ]
        active_population.set_index("CODGEO", inplace=True)
        active_population.rename(
            columns={
                "P19_ACT15P": "active_pop",
                "C19_ACT1564_CS1": "active_pop_CS1",
                "C19_ACT1564_CS2": "active_pop_CS2",
                "C19_ACT1564_CS3": "active_pop_CS3",
                "C19_ACT1564_CS4": "active_pop_CS4",
                "C19_ACT1564_CS5": "active_pop_CS5",
                "C19_ACT1564_CS6": "active_pop_CS6",
            },
            inplace=True
        )
        
        active_population.to_parquet(self.cache_path["active_population"])
        jobs.to_parquet(self.cache_path["jobs"])
        
        os.unlink(zip_path)
        os.unlink(csv_path)

        return active_population, jobs