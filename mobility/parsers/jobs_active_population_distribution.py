import os
import pathlib
import logging
import zipfile
import pandas as pd
import numpy as np

from mobility.file_asset import FileAsset
from mobility.parsers.download_file import download_file

class JobsActivePopulationDistribution(FileAsset):
    
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

        return jobs, active_population
    
    
    def create_and_get_asset(self) -> pd.DataFrame:
        
        jobs_fr, act_fr = self.prepare_french_jobs_active_population_distribution()
        jobs_ch, act_ch = self.prepare_swiss_jobs_active_population_distribution()
        
        jobs = pd.concat([jobs_fr, jobs_ch])
        act = pd.concat([act_fr, act_ch])
        
        jobs.to_parquet(self.cache_path["jobs"])
        act.to_parquet(self.cache_path["active_population"])
        
        return jobs, act
        
    
    def prepare_french_jobs_active_population_distribution(self):
        
        url = "https://www.data.gouv.fr/fr/datasets/r/02653cc4-76c0-4c3a-bc17-d5485c7ea2b9"
        
        data_folder = pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / "insee"
        zip_path = data_folder / "base-cc-emploi-pop-active-2019.zip"
        csv_path = data_folder / "base-cc-emploi-pop-active-2019.CSV"
    
        download_file(url, zip_path)
                
        # Unzip the content
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(data_folder)
            
        # Informations about jobs and active population for each city
        jobs_active_population = pd.read_csv(
            csv_path,
            sep=";",
            usecols=[
                "CODGEO",
                "P19_ACTOCC",
                "C19_ACTOCC1564_CS1",
                "C19_ACTOCC1564_CS2",
                "C19_ACTOCC1564_CS3",
                "C19_ACTOCC1564_CS4",
                "C19_ACTOCC1564_CS5",
                "C19_ACTOCC1564_CS6",
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
        
        jobs_active_population.rename({"CODGEO": "local_admin_unit_id"}, axis=1, inplace=True)
        jobs_active_population["local_admin_unit_id"] = "fr-" + jobs_active_population["local_admin_unit_id"]

        jobs = jobs_active_population.loc[
            :,
            [
                "local_admin_unit_id",
                "P19_EMPLT",
                "C19_EMPLT_CS1",
                "C19_EMPLT_CS2",
                "C19_EMPLT_CS3",
                "C19_EMPLT_CS4",
                "C19_EMPLT_CS5",
                "C19_EMPLT_CS6",
            ],
        ]
        jobs.set_index("local_admin_unit_id", inplace=True)
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
                "local_admin_unit_id",
                "P19_ACTOCC",
                "C19_ACTOCC1564_CS1",
                "C19_ACTOCC1564_CS2",
                "C19_ACTOCC1564_CS3",
                "C19_ACTOCC1564_CS4",
                "C19_ACTOCC1564_CS5",
                "C19_ACTOCC1564_CS6",
            ],
        ]
        active_population.set_index("local_admin_unit_id", inplace=True)
        active_population.rename(
            columns={
                "P19_ACTOCC": "active_pop",
                "C19_ACTOCC1564_CS1": "active_pop_CS1",
                "C19_ACTOCC1564_CS2": "active_pop_CS2",
                "C19_ACTOCC1564_CS3": "active_pop_CS3",
                "C19_ACTOCC1564_CS4": "active_pop_CS4",
                "C19_ACTOCC1564_CS5": "active_pop_CS5",
                "C19_ACTOCC1564_CS6": "active_pop_CS6",
            },
            inplace=True
        )
        
        os.unlink(zip_path)
        os.unlink(csv_path)

        return jobs, active_population
    
    
    def prepare_swiss_jobs_active_population_distribution(self):
        
        url = "https://www.data.gouv.fr/fr/datasets/r/5529f7f8-7a00-4890-b453-0d215c7a5726"
        file_path = pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / "bfs" / "je-f-21.03.01.xlsx"
        download_file(url, file_path)
        
        jobs_act = pd.read_excel(file_path)
        jobs_act = jobs_act.iloc[8:2180, [0, 2, 6, 7, 8, 22]]
        jobs_act.columns = ["local_admin_unit_id", "n_pop_total", "share_pop_inf_19", "share_pop_20_64", "share_pop_sup_65", "n_jobs_total"]
        jobs_act["local_admin_unit_id"] = "ch-" + jobs_act["local_admin_unit_id"].astype(int).astype(str)
        
        # Compute the active population count based on a global hypothesis of 79%
        # (could be more precise by using BFS data at canton or city level)
        jobs_act["active_pop"] = 0.79*jobs_act["n_pop_total"]*(0.25*jobs_act["share_pop_inf_19"] + jobs_act["share_pop_20_64"])/100
        
        # Compute the number of jobs when the data is missing, based on the average act pop / jobs ratio in Switzerland
        jobs_act["n_jobs_total"] = pd.to_numeric(jobs_act["n_jobs_total"], errors="coerce")
        
        n_jobs = jobs_act.loc[~jobs_act["n_jobs_total"].isnull(), "n_jobs_total"].sum()
        n_act = jobs_act.loc[~jobs_act["active_pop"].isnull(), "active_pop"].sum()
        ratio = n_jobs/n_act
        
        jobs_act["n_jobs_total"] = np.where(
            jobs_act["n_jobs_total"].isnull(),
            ratio*jobs_act["active_pop"],
            jobs_act["n_jobs_total"]
        )
        
        # Merge cities 2021 -> 2024
        url = "https://www.data.gouv.fr/fr/datasets/r/9f51fe8f-3e07-40cf-8c75-00bdfa01ceaf"
        file_path = pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / "bfs" / "bfs_mutations_communes.csv"
        download_file(url, file_path)
        
        bfs_mutations = pd.read_csv(file_path)
        bfs_mutations = bfs_mutations.iloc[:, [0, 5, 7, 8]]
        bfs_mutations.columns = ["mutation_id", "bfs_id", "radiation", "inscription"]
    
        from_ids = []
        to_ids = []
        
        for i, mutation in bfs_mutations.groupby("mutation_id"):
            
            if (mutation["radiation"] == "Radiation").any() and (mutation["inscription"] == "Création").any() and ((mutation["inscription"] == "Création").sum() == 1):
                
                for row in mutation.to_dict(orient="records"):
                    
                    if row["radiation"] == "Radiation":
                        from_ids.append(row["bfs_id"])
                    elif row["inscription"] == "Création":
                        to_ids += [row["bfs_id"]]*(mutation.shape[0]-1)
        
        bfs_mutations = pd.DataFrame({"from_bfs_id": from_ids, "to_bfs_id": to_ids})
        bfs_mutations = bfs_mutations.groupby("from_bfs_id", as_index=False).last()
        
        bfs_mutations["from_bfs_id"] = "ch-" + bfs_mutations["from_bfs_id"].astype(int).astype(str)
        bfs_mutations["to_bfs_id"] = "ch-" + bfs_mutations["to_bfs_id"].astype(int).astype(str)
        
        jobs_act = pd.merge(jobs_act, bfs_mutations, left_on="local_admin_unit_id", right_on="from_bfs_id", how = "left")
        
        jobs_act["local_admin_unit_id"] = np.where(
            ~jobs_act["to_bfs_id"].isnull(),
            jobs_act["to_bfs_id"],
            jobs_act["local_admin_unit_id"]
        )
        
        jobs_act = jobs_act.groupby(["local_admin_unit_id"], as_index=False)[["n_jobs_total", "active_pop"]].sum()
        
        jobs = jobs_act[["local_admin_unit_id", "n_jobs_total"]].copy()
        act = jobs_act[["local_admin_unit_id", "active_pop"]].copy()
        
        jobs.set_index("local_admin_unit_id", inplace=True)
        act.set_index("local_admin_unit_id", inplace=True)

        return jobs, act