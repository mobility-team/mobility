import os
import pathlib
import logging
import zipfile
import pandas as pd

from mobility.file_asset import FileAsset
from mobility.parsers.download_file import download_file
from mobility.parsers.local_admin_units import LocalAdminUnits


class SchoolStudentsFlows(FileAsset):

    def __init__(self):

        inputs = {}

        cache_path = pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / "insee" / "school_students_flows.parquet"

        super().__init__(inputs, cache_path)

    def get_cached_asset(self) -> pd.DataFrame:

        logging.info("Students <-> school flows already prepared. Reusing the file: " + str(self.cache_path))

        flows = pd.read_parquet(self.cache_path)

        return flows

    def create_and_get_asset(self) -> pd.DataFrame:

        logging.info("Preparing students <-> school flows.")

        local_admin_units = LocalAdminUnits().get().drop(columns="geometry")

        flows_fr = self.prepare_french_school_students_flows(local_admin_units)
        flows_fr.to_parquet(self.cache_path)

        return flows_fr

    def prepare_french_school_students_flows(self, local_admin_units: pd.DataFrame) -> pd.DataFrame:

        data_folder = pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / "insee" / "schools"
        data_folder.mkdir(parents=True, exist_ok=True)

        # ---------------------------------------------------------------------
        # Student origin-destination flows (RP 2020 - INSEE)
        # ---------------------------------------------------------------------

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

        # ---------------------------------------------------------------------
        # Mapping age groups to school type categories
        # ---------------------------------------------------------------------

        age_schools_categories = {
            "1": list(range(2, 7)),        # Ecole
            "2": [11],                     # Collège
            "3": [15],                     # Lycée
            "4": [18, 25, 30]              # Etudes sup
        }

        age_schools_categories = {
            code: category
            for category, codes in age_schools_categories.items()
            for code in codes
        }

        flows["school_type"] = flows["age_group"].replace(age_schools_categories)

        flows = flows.groupby(
            ["local_admin_unit_id_from", "local_admin_unit_id_to", "school_type"], as_index=False
        )["n_students"].sum()
        
        flows["local_admin_unit_id_from"] = "fr-" + flows["local_admin_unit_id_from"]
        flows["local_admin_unit_id_to"] = "fr-" + flows["local_admin_unit_id_to"]


        return flows
