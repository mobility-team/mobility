import logging
import os
import pathlib
import zipfile

import numpy as np
import pandas as pd

from mobility.runtime.assets.file_asset import FileAsset
from mobility.runtime.io.download_file import download_file


class FrenchWorkOpportunities(FileAsset):
    """French work opportunities by local admin unit."""

    def __init__(self):
        cache_path = {
            "active_population": (
                pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"])
                / "insee"
                / "active_population_fr.parquet"
            ),
            "jobs": (
                pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"])
                / "insee"
                / "jobs_fr.parquet"
            ),
        }
        super().__init__({}, cache_path)

    def get_cached_asset(self):
        logging.info("French jobs and active population already prepared. Reusing %s.", self.cache_path)
        jobs = pd.read_parquet(self.cache_path["jobs"])
        active_population = pd.read_parquet(self.cache_path["active_population"])
        return jobs, active_population

    def filter_by_local_admin_unit_id(self, local_admin_unit_ids):
        """Keep rows in the selected local admin units."""
        jobs, active_population = self.get()
        if not local_admin_unit_ids:
            return jobs, active_population
        selected_ids = jobs.index.intersection(local_admin_unit_ids)
        return jobs.loc[selected_ids], active_population.loc[selected_ids]

    def create_and_get_asset(self):
        url = "https://www.data.gouv.fr/fr/datasets/r/02653cc4-76c0-4c3a-bc17-d5485c7ea2b9"

        data_folder = pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / "insee"
        zip_path = data_folder / "base-cc-emploi-pop-active-2019.zip"
        csv_path = data_folder / "base-cc-emploi-pop-active-2019.CSV"

        download_file(url, zip_path)

        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(data_folder)

        work_data = pd.read_csv(
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
            dtype={"CODGEO": str},
        )

        work_data.rename({"CODGEO": "local_admin_unit_id"}, axis=1, inplace=True)
        work_data["local_admin_unit_id"] = (
            "fr-" + work_data["local_admin_unit_id"]
        )

        jobs = work_data[
            [
                "local_admin_unit_id",
                "P19_EMPLT",
                "C19_EMPLT_CS1",
                "C19_EMPLT_CS2",
                "C19_EMPLT_CS3",
                "C19_EMPLT_CS4",
                "C19_EMPLT_CS5",
                "C19_EMPLT_CS6",
            ]
        ].copy()
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
            inplace=True,
        )

        active_population = work_data[
            [
                "local_admin_unit_id",
                "P19_ACTOCC",
                "C19_ACTOCC1564_CS1",
                "C19_ACTOCC1564_CS2",
                "C19_ACTOCC1564_CS3",
                "C19_ACTOCC1564_CS4",
                "C19_ACTOCC1564_CS5",
                "C19_ACTOCC1564_CS6",
            ]
        ].copy()
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
            inplace=True,
        )

        os.unlink(zip_path)
        os.unlink(csv_path)

        jobs.to_parquet(self.cache_path["jobs"])
        active_population.to_parquet(self.cache_path["active_population"])
        return jobs, active_population


class FrenchWorkFlows(FileAsset):
    """French home-work flows by local admin unit."""

    def __init__(self):
        cache_path = (
            pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"])
            / "insee"
            / "work_flows_fr.parquet"
        )
        super().__init__({}, cache_path)

    def get_cached_asset(self) -> pd.DataFrame:
        logging.info("French home-work flows already prepared. Reusing %s.", self.cache_path)
        return pd.read_parquet(self.cache_path)

    def filter_by_local_admin_unit_id(self, local_admin_unit_ids) -> pd.DataFrame:
        """Keep flows where origin or destination is in the selected local admin units."""
        flows = self.get()
        if not local_admin_unit_ids:
            return flows
        selected_ids = set(local_admin_unit_ids)
        return flows[
            flows["local_admin_unit_id_from"].isin(selected_ids)
            | flows["local_admin_unit_id_to"].isin(selected_ids)
        ].copy()

    def create_and_get_asset(self) -> pd.DataFrame:
        url = "https://www.data.gouv.fr/fr/datasets/r/f3f22487-22d0-45f4-b250-af36fc56ccd0"

        data_folder = pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / "insee"
        zip_path = data_folder / "rp2019-mobpro-csv.zip"
        csv_path = data_folder / "FD_MOBPRO_2019.csv"

        download_file(url, zip_path)

        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(data_folder)

        url = "https://www.data.gouv.fr/fr/datasets/r/a3643a84-3190-44ad-b933-84fb25459dce"
        mapping_path = data_folder / "insee_bfs_mapping.xlsx"
        download_file(url, mapping_path)
        mapping = pd.read_excel(mapping_path, dtype=str)

        flows = pd.read_csv(
            csv_path,
            sep=";",
            usecols=["COMMUNE", "ARM", "DCFLT", "DCLT", "TRANS", "IPONDI"],
            dtype={"COMMUNE": str, "ARM": str, "DCFLT": str, "DCLT": str, "TRANS": str, "IPONDI": float},
        )

        flows["DCFLT"] = flows["DCFLT"].str.replace(".", "")
        flows.loc[flows["ARM"] != "ZZZZZ", "COMMUNE"] = flows.loc[flows["ARM"] != "ZZZZZ", "ARM"]
        flows["local_admin_unit_id_from"] = "fr-" + flows["COMMUNE"]

        flows = pd.merge(
            flows,
            mapping,
            left_on="DCFLT",
            right_on="insee_id",
            how="left",
        )
        flows["local_admin_unit_id_to"] = np.where(
            flows["DCFLT"] == "99999",
            "fr-" + flows["DCLT"],
            "ch-" + flows["bfs_id"],
        )

        flows.rename({"IPONDI": "ref_flow_volume", "TRANS": "mode"}, axis=1, inplace=True)
        flows["mode"] = flows["mode"].replace(
            {
                "1": "no-transport",
                "2": "walk",
                "3": "bicycle",
                "4": "motorcycle",
                "5": "car",
                "6": "public-transport",
            }
        )

        flows = flows.groupby(
            ["local_admin_unit_id_from", "local_admin_unit_id_to", "mode"],
            as_index=False,
        )[["ref_flow_volume"]].sum()

        os.unlink(zip_path)
        os.unlink(csv_path)

        flows.to_parquet(self.cache_path)
        return flows


class FrenchWork:
    """French work inputs."""

    country = "fr"

    @property
    def opportunities(self):
        return FrenchWorkOpportunities()

    @property
    def flows(self):
        return FrenchWorkFlows()
