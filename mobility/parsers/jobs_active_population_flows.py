import os
import pathlib
import logging
import zipfile
import pandas as pd
import numpy as np

from mobility.file_asset import FileAsset
from mobility.parsers.download_file import download_file
from mobility.parsers.local_admin_units import LocalAdminUnits
from mobility.parsers.jobs_active_population_distribution import JobsActivePopulationDistribution

class JobsActivePopulationFlows(FileAsset):
    
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
        
        local_admin_units = LocalAdminUnits().get().drop(columns="geometry")
        
        fr_flows = self.prepare_jobs_active_population_flows_fr(local_admin_units)
        ch_flows = self.prepare_jobs_active_population_flows_ch(local_admin_units)
        
        flows = pd.concat([fr_flows, ch_flows])
        
        flows.to_parquet(self.cache_path)

        return flows
    
    
    def prepare_jobs_active_population_flows_fr(self, local_admin_units):
        
        url = "https://www.data.gouv.fr/fr/datasets/r/f3f22487-22d0-45f4-b250-af36fc56ccd0"
        
        data_folder = pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / "insee"
        zip_path = data_folder / "rp2019-mobpro-csv.zip"
        csv_path = data_folder / "FD_MOBPRO_2019.csv"
    
        download_file(url, zip_path)
                
        # Unzip the content
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(data_folder)
            
        # Map INSEE ids to BFS ids
        url = "https://www.data.gouv.fr/fr/datasets/r/a3643a84-3190-44ad-b933-84fb25459dce"
        data_folder = pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / "insee"
        file_path = data_folder / "insee_bfs_mapping.xlsx"
        download_file(url, file_path)
        
        mapping = pd.read_excel(file_path, dtype=str)

        # Load the flows
        flows = pd.read_csv(
            csv_path,
            sep=";",
            usecols=["COMMUNE", "ARM", "DCFLT", "DCLT", "IPONDI"],
            dtype={"COMMUNE": str, "ARM": str, "DCFLT": str, "DCLT": str, "IPONDI": float}
        )
        
        flows["DCFLT"] = flows["DCFLT"].str.replace(".", "")
        flows.loc[flows["ARM"] != "ZZZZZ", "COMMUNE"] = flows.loc[flows["ARM"] != "ZZZZZ", "ARM"]
        
        # Origins
        flows["local_admin_unit_id_from"] = "fr-" + flows["COMMUNE"]
        
        # Destinations
        flows = pd.merge(
            flows,
            mapping,
            left_on="DCFLT",
            right_on="insee_id",
            how="left"
        )
        
        flows["local_admin_unit_id_to"] = np.where(
            flows["DCFLT"] == "99999",
            "fr-" + flows["DCLT"],
            "ch-" + flows["bfs_id"]
        )
        
        # Volume
        flows.rename({"IPONDI": "ref_flow_volume"}, axis=1, inplace=True)

        flows = flows.groupby(["local_admin_unit_id_from", "local_admin_unit_id_to"], as_index=False)[["ref_flow_volume"]].sum()
        
        os.unlink(zip_path)
        os.unlink(csv_path)
        
        return flows
    
    
    
    def prepare_jobs_active_population_flows_ch(self, local_admin_units):
        
        url = "https://www.data.gouv.fr/fr/datasets/r/9376a647-69d5-4f3d-b2ac-e9421425608d"
        
        data_folder = pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / "bfs"
        file_path = data_folder / "je-f-11.04.04.05.xlsx"
        
        download_file(url, file_path)
        
        flows = pd.read_excel(file_path, skiprows=4, nrows=95406)
        flows = flows.iloc[:, [2, 6, 10]]
        flows.columns = ["local_admin_unit_id_from", "local_admin_unit_id_to", "ref_flow_volume"]
        
        flows["local_admin_unit_id_from"] = "ch-" + flows["local_admin_unit_id_from"].astype(int).astype(str)
        flows["local_admin_unit_id_to"] = "ch-" + flows["local_admin_unit_id_to"].astype(int).astype(str)
        flows["ref_flow_volume"] = pd.to_numeric(flows["ref_flow_volume"], errors="coerce")
        
        flows = flows[~flows["ref_flow_volume"].isnull()]
        
        flows = flows[flows["local_admin_unit_id_from"].isin(local_admin_units["local_admin_unit_id"])]
        flows = flows[flows["local_admin_unit_id_to"].isin(local_admin_units["local_admin_unit_id"])]
        
        # BUG ?
        # The number of active persons in each city computed from the flows is not the 
        # same than the one computed based on the 15 - 64 years population count.
        # So we adjust the flows proportionnaly to make the two source match.
        jobs, act = JobsActivePopulationDistribution().get()
        
        act = act[["active_pop"]].reset_index()
        act_flows = flows.groupby("local_admin_unit_id_from", as_index=False)["ref_flow_volume"].sum()
        
        correction = pd.merge(act_flows, act, left_on="local_admin_unit_id_from", right_on="local_admin_unit_id")
        correction["k"] = correction["active_pop"]/correction["ref_flow_volume"]
        
        flows = pd.merge(flows, correction[["local_admin_unit_id_from", "k"]], on="local_admin_unit_id_from")
        flows["ref_flow_volume"] *= flows["k"]
        
        del flows["k"]
        
        return flows
        
        
    
    
