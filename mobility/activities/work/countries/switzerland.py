import logging
import os
import pathlib

import numpy as np
import pandas as pd

from mobility.runtime.assets.file_asset import FileAsset
from mobility.runtime.io.download_file import download_file


class SwissWorkOpportunities(FileAsset):
    """Swiss work opportunities by local admin unit."""

    def __init__(self):
        cache_path = {
            "active_population": (
                pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"])
                / "bfs"
                / "active_population_ch.parquet"
            ),
            "jobs": (
                pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"])
                / "bfs"
                / "jobs_ch.parquet"
            ),
        }
        super().__init__({}, cache_path)

    def get_cached_asset(self):
        logging.info("Swiss jobs and active population already prepared. Reusing %s.", self.cache_path)
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
        url = "https://www.data.gouv.fr/fr/datasets/r/5529f7f8-7a00-4890-b453-0d215c7a5726"
        file_path = pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / "bfs" / "je-f-21.03.01.xlsx"
        download_file(url, file_path)

        jobs_act = pd.read_excel(file_path)
        jobs_act = jobs_act.iloc[8:2180, [0, 2, 6, 7, 8, 22]]
        jobs_act.columns = [
            "local_admin_unit_id",
            "n_pop_total",
            "share_pop_inf_19",
            "share_pop_20_64",
            "share_pop_sup_65",
            "n_jobs_total",
        ]
        jobs_act["local_admin_unit_id"] = "ch-" + jobs_act["local_admin_unit_id"].astype(int).astype(str)
        jobs_act["active_pop"] = (
            0.79
            * jobs_act["n_pop_total"]
            * (0.25 * jobs_act["share_pop_inf_19"] + jobs_act["share_pop_20_64"])
            / 100
        )

        jobs_act["n_jobs_total"] = pd.to_numeric(jobs_act["n_jobs_total"], errors="coerce")
        n_jobs = jobs_act.loc[~jobs_act["n_jobs_total"].isnull(), "n_jobs_total"].sum()
        n_act = jobs_act.loc[~jobs_act["active_pop"].isnull(), "active_pop"].sum()
        ratio = n_jobs / n_act
        jobs_act["n_jobs_total"] = np.where(
            jobs_act["n_jobs_total"].isnull(),
            ratio * jobs_act["active_pop"],
            jobs_act["n_jobs_total"],
        )

        url = "https://www.data.gouv.fr/fr/datasets/r/9f51fe8f-3e07-40cf-8c75-00bdfa01ceaf"
        mutation_path = pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / "bfs" / "bfs_mutations_communes.csv"
        download_file(url, mutation_path)

        bfs_mutations = pd.read_csv(mutation_path)
        bfs_mutations = bfs_mutations.iloc[:, [0, 5, 7, 8]]
        bfs_mutations.columns = ["mutation_id", "bfs_id", "radiation", "inscription"]

        from_ids = []
        to_ids = []
        for _, mutation in bfs_mutations.groupby("mutation_id"):
            one_creation = (mutation["inscription"] == "Création").sum() == 1
            if (mutation["radiation"] == "Radiation").any() and one_creation:
                for row in mutation.to_dict(orient="records"):
                    if row["radiation"] == "Radiation":
                        from_ids.append(row["bfs_id"])
                    elif row["inscription"] == "Création":
                        to_ids += [row["bfs_id"]] * (mutation.shape[0] - 1)

        bfs_mutations = pd.DataFrame({"from_bfs_id": from_ids, "to_bfs_id": to_ids})
        bfs_mutations = bfs_mutations.groupby("from_bfs_id", as_index=False).last()
        bfs_mutations["from_bfs_id"] = "ch-" + bfs_mutations["from_bfs_id"].astype(int).astype(str)
        bfs_mutations["to_bfs_id"] = "ch-" + bfs_mutations["to_bfs_id"].astype(int).astype(str)

        jobs_act = pd.merge(
            jobs_act,
            bfs_mutations,
            left_on="local_admin_unit_id",
            right_on="from_bfs_id",
            how="left",
        )
        jobs_act["local_admin_unit_id"] = np.where(
            ~jobs_act["to_bfs_id"].isnull(),
            jobs_act["to_bfs_id"],
            jobs_act["local_admin_unit_id"],
        )

        jobs_act = jobs_act.groupby(["local_admin_unit_id"], as_index=False)[
            ["n_jobs_total", "active_pop"]
        ].sum()

        jobs = jobs_act[["local_admin_unit_id", "n_jobs_total"]].copy()
        active_population = jobs_act[["local_admin_unit_id", "active_pop"]].copy()
        jobs.set_index("local_admin_unit_id", inplace=True)
        active_population.set_index("local_admin_unit_id", inplace=True)

        jobs.to_parquet(self.cache_path["jobs"])
        active_population.to_parquet(self.cache_path["active_population"])
        return jobs, active_population


class SwissWorkFlows(FileAsset):
    """Swiss home-work flows by local admin unit."""

    def __init__(self):
        cache_path = (
            pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"])
            / "bfs"
            / "work_flows_ch.parquet"
        )
        super().__init__({}, cache_path)
        self.opportunities = SwissWorkOpportunities()

    def get_cached_asset(self) -> pd.DataFrame:
        logging.info("Swiss home-work flows already prepared. Reusing %s.", self.cache_path)
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

        flows["mode"] = "car"

        # Match the flow totals to the active population reference table.
        _, active_population = self.opportunities.filter_by_local_admin_unit_id(None)
        active_population = active_population[["active_pop"]].reset_index()
        active_flows = flows.groupby("local_admin_unit_id_from", as_index=False)["ref_flow_volume"].sum()

        correction = pd.merge(
            active_flows,
            active_population,
            left_on="local_admin_unit_id_from",
            right_on="local_admin_unit_id",
        )
        correction["k"] = correction["active_pop"] / correction["ref_flow_volume"]

        flows = pd.merge(flows, correction[["local_admin_unit_id_from", "k"]], on="local_admin_unit_id_from")
        flows["ref_flow_volume"] *= flows["k"]
        del flows["k"]

        flows.to_parquet(self.cache_path)
        return flows


class SwissWork:
    """Swiss work inputs."""

    country = "ch"

    @property
    def opportunities(self):
        return SwissWorkOpportunities()

    @property
    def flows(self):
        return SwissWorkFlows()
