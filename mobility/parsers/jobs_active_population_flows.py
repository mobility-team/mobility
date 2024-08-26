import os
import pathlib
import logging
import zipfile
import pandas as pd

from mobility.asset import Asset
from mobility.parsers.download_file import download_file

class JobsActivePopulationFlows(Asset):
    
    def __init__(self):
        
        inputs = {}
        cache_path = pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / "insee" / "jobs_active_population_flows.parquet"
        super().__init__(inputs, cache_path)
        
    def get_cached_asset(self) -> pd.DataFrame:

        logging.info("Active population <-> jobs flows already prepared. Reusing the file : " + str(self.cache_path))
        flows = pd.read_parquet(self.cache_path)

        return flows
    
    
    def create_and_get_asset(self) -> pd.DataFrame:
        
        logging.info("Preparing active population <-> jobs flows.")
        
        url = "https://www.data.gouv.fr/fr/datasets/r/f3f22487-22d0-45f4-b250-af36fc56ccd0"
        
        data_folder = pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / "insee"
        zip_path = data_folder / "rp2019-mobpro-csv.zip"
        csv_path = data_folder / "FD_MOBPRO_2019.csv"
    
        download_file(url, zip_path)
                
        # Unzip the content
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(data_folder)
        
        flows = pd.read_csv(
            csv_path,
            sep=";",
            usecols=["COMMUNE", "ARM", "DCFLT", "DCLT", "IPONDI"],
            dtype={"COMMUNE": str, "ARM": str, "DCFLT": str, "DCLT": str, "IPONDI": float}
        )
        
        flows.loc[flows["ARM"] != "ZZZZZ", "COMMUNE"] = flows.loc[flows["ARM"] != "ZZZZZ", "ARM"]
        flows.rename({"COMMUNE": "local_admin_unit_id_from", "DCLT": "local_admin_unit_id_to", "IPONDI": "ref_flow_volume"}, axis=1, inplace=True)
        
        flows["local_admin_unit_id_from"] = "fr-" + flows["local_admin_unit_id_from"]
        flows["local_admin_unit_id_to"] = "fr-" + flows["local_admin_unit_id_to"]
        
        flows = flows.groupby(["local_admin_unit_id_from", "local_admin_unit_id_to"], as_index=False)[["ref_flow_volume"]].sum()
        
        flows.to_parquet(self.cache_path)
        
        os.unlink(zip_path)
        os.unlink(csv_path)

        return flows