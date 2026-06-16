import logging
import os
import pathlib
import zipfile

import geopandas as gpd
import pandas as pd

from mobility.runtime.assets.file_asset import FileAsset
from mobility.runtime.io.download_file import download_file


class FrenchStudyOpportunities(FileAsset):
    """French study opportunities by local admin unit."""

    def __init__(self):
        cache_path = (
            pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"])
            / "insee"
            / "study_opportunities_fr.parquet"
        )
        super().__init__({}, cache_path)

    def filter_by_local_admin_unit_id(self, local_admin_unit_ids):
        """Keep rows in the selected local admin units."""
        schools = self.get()
        if not local_admin_unit_ids:
            return schools
        return schools[schools["local_admin_unit_id"].isin(local_admin_unit_ids)].copy()

    def get_cached_asset(self):
        logging.info("French school capacity already prepared. Reusing %s.", self.cache_path)
        schools = gpd.read_parquet(self.cache_path)
        schools = schools.set_crs(3035)
        return schools

    def create_and_get_asset(self):
        data_folder = pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / "insee" / "schools"
        data_folder.mkdir(parents=True, exist_ok=True)

        url = "https://www.data.gouv.fr/api/1/datasets/r/6ebc938c-af7a-4faa-b10b-7b2757b50404"
        csv_path = data_folder / "fr-en-annuaire-education.csv"
        download_file(url, csv_path)

        schools = pd.read_csv(
            csv_path,
            sep=";",
            usecols=[
                "Code_commune",
                "Type_etablissement",
                "Nombre_d_eleves",
                "latitude",
                "longitude",
            ],
            dtype={
                "Code_commune": str,
                "Type_etablissement": str,
            },
        )
        schools.rename(
            columns={
                "Code_commune": "local_admin_unit_id",
                "Type_etablissement": "school_type",
                "Nombre_d_eleves": "n_students",
                "latitude": "lat",
                "longitude": "lon",
            },
            inplace=True,
        )
        schools = schools[schools["school_type"].isin(["Ecole", "Collège", "Lycée"])]
        schools["school_type"] = schools["school_type"].replace({"Ecole": 1, "Collège": 2, "Lycée": 3})
        schools = schools.dropna(subset=["n_students"])
        schools = schools.groupby(
            ["local_admin_unit_id", "school_type", "lon", "lat"],
            as_index=False,
        )["n_students"].sum()

        url = "https://www.data.gouv.fr/fr/datasets/r/b10fd6c8-6bc9-41fc-bdfd-e0ac898c674a"
        csv_path = data_folder / "fr-esr-atlas_regional-effectifs-d-etudiants-inscrits-detail_etablissements.csv"
        download_file(url, csv_path)

        higher = pd.read_csv(
            csv_path,
            sep=";",
            usecols=[
                "Rentrée universitaire",
                "code commune",
                "gps",
                "nombre total d’étudiants inscrits hors doubles inscriptions université/CPGE",
            ],
            dtype={
                "Rentrée universitaire": int,
                "code commune": str,
                "gps": str,
                "nombre total d’étudiants inscrits hors doubles inscriptions université/CPGE": int,
            },
        )
        higher.columns = ["year", "local_admin_unit_id", "coords", "n_students"]
        higher[["lat", "lon"]] = higher["coords"].str.strip().str.split(",", expand=True).astype(float)
        higher = higher[higher["year"] == 2023]
        higher = higher.groupby(["local_admin_unit_id", "lon", "lat"], as_index=False)["n_students"].sum()
        higher["school_type"] = 4

        schools = pd.concat([schools, higher], ignore_index=True)
        schools["local_admin_unit_id"] = "fr-" + schools["local_admin_unit_id"]

        schools = gpd.GeoDataFrame(
            schools,
            geometry=gpd.points_from_xy(schools["lon"], schools["lat"]),
            crs="EPSG:4326",
        )
        schools = schools[["school_type", "local_admin_unit_id", "geometry", "n_students"]]
        schools.to_crs(3035, inplace=True)
        schools.to_parquet(self.cache_path)
        return schools


class FrenchStudyFlows(FileAsset):
    """French home-study flows by local admin unit."""

    def __init__(self):
        cache_path = (
            pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"])
            / "insee"
            / "study_flows_fr.parquet"
        )
        super().__init__({}, cache_path)

    def get_cached_asset(self) -> pd.DataFrame:
        logging.info("French home-study flows already prepared. Reusing %s.", self.cache_path)
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
        data_folder = pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / "insee" / "schools"
        data_folder.mkdir(parents=True, exist_ok=True)

        url = "https://www.insee.fr/fr/statistiques/fichier/7637665/RP2020_mobsco_csv.zip"
        zip_path = data_folder / "RP2020_mobsco_csv.zip"
        csv_path = data_folder / "FD_MOBSCO_2020.csv"

        download_file(url, zip_path)

        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(data_folder)

        flows = pd.read_csv(
            csv_path,
            sep=";",
            usecols=["COMMUNE", "DCETUF", "AGEREV10", "IPONDI"],
            dtype={"COMMUNE": str, "DCETUF": str},
        )

        flows.rename(
            columns={
                "COMMUNE": "local_admin_unit_id_from",
                "DCETUF": "local_admin_unit_id_to",
                "AGEREV10": "age_group",
                "IPONDI": "n_students",
            },
            inplace=True,
        )

        age_school_types = {
            "1": list(range(2, 7)),
            "2": [11],
            "3": [15],
            "4": [18, 25, 30],
        }
        age_school_types = {
            code: school_type
            for school_type, age_codes in age_school_types.items()
            for code in age_codes
        }
        flows["school_type"] = flows["age_group"].replace(age_school_types)

        flows = flows.groupby(
            ["local_admin_unit_id_from", "local_admin_unit_id_to", "school_type"],
            as_index=False,
        )["n_students"].sum()
        flows["local_admin_unit_id_from"] = "fr-" + flows["local_admin_unit_id_from"]
        flows["local_admin_unit_id_to"] = "fr-" + flows["local_admin_unit_id_to"]

        flows.to_parquet(self.cache_path)
        return flows


class FrenchStudy:
    """French study inputs."""

    country = "fr"

    @property
    def opportunities(self):
        return FrenchStudyOpportunities()

    @property
    def flows(self):
        return FrenchStudyFlows()
