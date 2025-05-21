import os
import pathlib
import logging
import pandas as pd

from mobility.file_asset import FileAsset
from mobility.parsers.download_file import download_file


class SchoolsCapacityDistribution(FileAsset):

    def __init__(self):

        inputs = {}

        cache_path = pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / "insee" / "schools_capacity.parquet"
        
        
        super().__init__(inputs, cache_path)

    def get_cached_asset(self) -> pd.DataFrame:

        logging.info("School capacity spatial distribution already prepared. Reusing the file: " + str(self.cache_path))

        schools = pd.read_parquet(self.cache_path)

        return schools

    def create_and_get_asset(self) -> pd.DataFrame:

        schools_fr = self.prepare_french_schools_capacity_distribution()
        schools_fr.to_parquet(self.cache_path)

        return schools_fr

    def prepare_french_schools_capacity_distribution(self):

        data_folder = pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / "insee" / "schools"
        data_folder.mkdir(parents=True, exist_ok=True)

        # ---------------------------------------------------------------------
        # Primary and secondary schools (Ecole, Collège, Lycée)
        # ---------------------------------------------------------------------

        url = (
            "https://data.education.gouv.fr/api/explore/v2.1/catalog/datasets/"
            "fr-en-annuaire-education/exports/csv?lang=fr&timezone=Europe%2FBerlin&"
            "use_labels=true&delimiter=%3B"
        )
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
            ["local_admin_unit_id", "school_type", "lon", "lat"], as_index=False
        )["n_students"].sum()

        # ---------------------------------------------------------------------
        # Higher education institutions
        # ---------------------------------------------------------------------

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

        higher = higher.groupby(
            ["local_admin_unit_id", "lon", "lat"], as_index=False
        )["n_students"].sum()
        higher["school_type"] = 4

        # ---------------------------------------------------------------------
        # Concatenate and return
        # ---------------------------------------------------------------------

        schools = pd.concat([schools, higher], ignore_index=True)

        schools["local_admin_unit_id"] = "fr-" + schools["local_admin_unit_id"]

        return schools


