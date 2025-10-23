import os
import pathlib
import logging
import zipfile
import pandas as pd
import numpy as np

from mobility.parsers.mobility_survey import MobilitySurvey
from mobility.parsers.download_file import download_file

class EMPMobilitySurvey(MobilitySurvey):
    """
    A class for managing and processing mobility survey data for the EMP-2019 and ENTD-2008 surveys.

    Attributes:
        source (str): The source of the mobility survey data (e.g., "EMP-2019" or "ENTD-2008").
        cache_path (dict): A dictionary mapping data identifiers to their file paths in the cache.

    Methods:
        get_cached_asset: Returns the cached asset data as a dictionary of pandas DataFrames.
        create_and_get_asset: Prepares mobility survey data from scratch, caches it, and returns it.
        download_survey_data: Downloads survey data zip files from specified URLs.
        prepare_survey_data: Prepares survey data by calling specific methods based on the source.
        prepare_survey_data_ENTD_2008: Processes and formats ENTD-2008 survey data.
        prepare_survey_data_EMP_2019: Processes and formats EMP-2019 survey data.
    """

    def __init__(self, seq_prob_cutoff: float = 0.5):
        inputs = {
            "version": "1",
            "survey_name": "fr-EMP-2019",
            "country": "fr"
        }
        super().__init__(inputs, seq_prob_cutoff)


    def create_and_get_asset(self) -> dict[str, pd.DataFrame]:
        """
        Prepares mobility survey data by downloading, processing, and caching it, then returns the cached data.

        Returns:
            dict: A dictionary where keys are data identifiers and values are pandas DataFrames of the prepared and cached data.
        """

        logging.info("Preparing EMP survey data...")

        dataset_path =  (
            pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) /
            "mobility_surveys" / "EMP-2019" / "emp-2019.zip"
        )

        self.download_survey_data(dataset_path)
        self.parse_survey_data(dataset_path)

        return self.get_cached_asset()


    def download_survey_data(self, dataset_path: pathlib.Path) -> None:
        """
        Downloads the survey data zip file from a specified URL and extracts it to a designated folder.

        Args:
            source (str): The source identifier of the mobility survey data (e.g., "EMP-2019" or "ENTD-2008").

        Returns:
            None
        """

        if dataset_path.exists() is False:

            download_file(
                "https://www.data.gouv.fr/fr/datasets/r/4a97ce1d-4e7f-43fd-bac3-60a9cc2bc1b5",
                dataset_path
            )

            with zipfile.ZipFile(dataset_path, "r") as zip_ref:
                zip_ref.extractall(dataset_path.parent)

        return None



    def parse_survey_data(self, dataset_path: pathlib.Path) -> None:
        """
        Processes and formats the EMP-2019 mobility survey data, then saves the data to parquet files in the cache directory.

        Args:
            data_folder_path (pathlib.Path): The path to the folder containing the extracted EMP-2019 survey data files.

        Returns:
            None
        """

        data_folder_path = dataset_path.parent

        # Info about the individuals (CSP, city category...)
        cols = {
            "ident_men": int,
            "ident_ind": int,
            "CS24": str,
            "AGE": int
        }
        
        indiv = pd.read_csv(
            data_folder_path / "tcm_ind_kish_public_V2.csv",
            encoding="latin-1",
            sep=";",
            dtype=cols,
            usecols=list(cols.keys()),
        )
        
        indiv.rename({
            "ident_men": "IDENT_MEN",
            "ident_ind": "IDENT_IND"
        }, inplace=True, axis=1)

        indiv["csp"] = indiv["CS24"].str.slice(0, 1).fillna("8")
        
        # Some individuals have CSP 00, which corresponds to nothing in the 
        # survey metadata. We replace this CSP by the most likely given the
        # age of the individual.
        # TO DO : use the weights to get unbiased most likely value.
        age_to_csp = ( 
            indiv[~indiv["csp"].isnull()]
            .groupby(["AGE", "csp"])["IDENT_IND"]
            .count()
            .sort_values(ascending=False)
            .groupby(["AGE"])
            .head(1)
            .reset_index()
            [["AGE", "csp"]]
        )
        age_to_csp.columns = ["AGE", "csp_age"]
        
        indiv = pd.merge(
            indiv,
            age_to_csp,
            on="AGE"
        )
        
        indiv["csp"] = np.where(
            indiv["csp"] == "0",
            indiv["csp_age"],
            indiv["csp"]
        )
        
        
        # Split the CSP 8 into 2 groups : inf and sup 15 years old
        conditions = [
            (indiv["csp"] == "8") & (indiv["AGE"] < 15),
            (indiv["csp"] == "8") & (indiv["AGE"] >= 15),
        ]
        choices = ["8a", "8b"]
        
        indiv["csp"] = np.select(
            conditions, choices, default=indiv["csp"]
        )
        
        indiv["csp"] = "fr-" + indiv["csp"]

        # Info about households
        cols = {
            "ident_men": int,
            "STATUTCOM_UU_RES": str,
            "CS24PR": str,
            "NPERS": str,
            "AGEPR": int
        }
        
        hh = pd.read_csv(
            data_folder_path / "tcm_men_public_V2.csv",
            encoding="latin-1",
            sep=";",
            dtype=cols,
            usecols=list(cols.keys()),
        )
        hh.rename({
            "ident_men": "IDENT_MEN",
            "STATUTCOM_UU_RES": "city_category",
            "CS24PR": "csp",
            "NPERS": "n_pers",
            "AGEPR": "AGE"
        }, inplace=True, axis=1)

        # the R category of the ENTD correspond to the H category of the EMP 2019
        hh.loc[hh["city_category"] == "H", "city_category"] = "R"

        hh["csp"] = hh["csp"].str.slice(0, 1)
        hh["csp_household"] = hh["csp"]
        hh["n_pers"] = hh["n_pers"].astype(int)
        
        hh = pd.merge(
            hh,
            age_to_csp,
            on="AGE"
        )
        
        hh["csp"] = np.where(
            hh["csp"] == "0",
            hh["csp_age"],
            hh["csp"]
        )
        
        # Split the CSP 8 into 2 groups : inf and sup 15 years old
        conditions = [
            (hh["csp"] == "8") & (hh["AGE"] < 15),
            (hh["csp"] == "8") & (hh["AGE"] >= 15),
        ]
        choices = ["8_under_15y", "8_15y_and_over"]
        
        hh["csp"] = np.select(
            conditions, choices, default=hh["csp"]
        )
        

        # Number of cars in each household
        cars = pd.read_csv(
            data_folder_path / "q_menage_public_V2.csv",
            encoding="latin-1",
            sep=";",
            dtype={
                "IDENTMEN": int,
                "JNBVEH": str,
                "BLOGDIST": str
            },
            usecols=["IDENT_MEN", "JNBVEH", "BLOGDIST"],
        )

        cars["n_cars"] = "0"
        cars.loc[cars["JNBVEH"].astype(int) == 1, "n_cars"] = "1"
        cars.loc[cars["JNBVEH"].astype(int) > 1, "n_cars"] = "2+"

        cars = cars[["IDENT_MEN", "n_cars", "BLOGDIST"]]

        # Infos about the individuals (weights, immobility)
        k_indiv = pd.read_csv(
            data_folder_path / "k_individu_public_V2.csv",
            encoding="latin-1",
            sep=";",
            dtype={"IDENT_IND": int, "pond_indC": float},
            usecols=[
                "IDENT_IND",
                "pond_indC",
                "IMMODEP_A",
                "IMMODEP_B",
                "IMMODEP_C",
                "IMMODEP_D",
                "IMMODEP_E",
                "IMMODEP_F",
                "IMMODEP_G",
                "MDATE_jour",
                "MDATE_delai",
            ],
        )

        # ------------------------------------------
        # Trips dataset
        df = pd.read_csv(
            data_folder_path / "k_deploc_public_V2.csv",
            encoding="latin-1",
            sep=";",
            dtype={
                "IDENT_DEP": int,
                "IDENT_IND": int,
                "mobloc": str,
                "TYPEJOUR": str,
                "MMOTIFDES": str,
                "MOTPREC": str,
                "mtp": str,
                "MDATE_jour": str,
                "MDATE_mois": str,
                "MORIHDEP": str,
                "MDESHARR": str
            },
            usecols=[
                "IDENT_DEP",
                "IDENT_IND",
                "POND_JOUR",
                "TYPEJOUR",
                "mobloc",
                "MMOTIFDES",
                "MOTPREC",
                "MDISTTOT_fin",
                "mtp",
                "MACCOMPM",
                "MACCOMPHM",
                "MDATE_jour",
                "MDATE_mois",
                "MORIHDEP",
                "MDESHARR"
            ]
        )

        df["daily_trip_index"] = df.groupby("IDENT_IND").cumcount() + 1

        df["POND_JOUR"] = df["POND_JOUR"].astype(float)
        df["MDISTTOT_fin"] = df["MDISTTOT_fin"].astype(float)
        df.loc[df["MACCOMPM"].isnull(), "MACCOMPM"] = 0
        df.loc[df["MACCOMPHM"].isnull(), "MACCOMPHM"] = 0
        df["n_other_passengers"] = df["MACCOMPM"].astype(int) + df["MACCOMPHM"].astype(int)
        df["weekday"] = np.where(df["TYPEJOUR"] == "1", True, False)

        df["departure_time"] = pd.to_timedelta(df["MORIHDEP"]).astype('timedelta64[s]').astype(int)
        df["arrival_time"] = pd.to_timedelta(df["MDESHARR"]).astype('timedelta64[s]').astype(int)

        # the day weight is divided by 5 if it's a weekday and 2 otherwise in order to be consistent with the ENTD
        df["POND_JOUR"] = np.where(df["weekday"], df["POND_JOUR"] / 5, df["POND_JOUR"] / 2)

        # Remove long distance trips (> 80 km from home)
        df = df[df["mobloc"] == "1"]

        # Remove trips with an unknown or zero distance
        df = df[(df["MDISTTOT_fin"] > 0.0) | (~df["MDISTTOT_fin"].isnull())]

        # Map months and day of week to integers
        df["MDATE_mois"] = df["MDATE_mois"].replace({
            'janvier': "1",
            'février': "2",
            'mars': "3",
            'avril': "4",
            'mai': "5",
            'juin': "6",
            'juillet': "7",
            'août': "8",
            'septembre': "9",
            'octobre': "10",
            'novembre': "11",
            'décembre': "12"
        }).astype(int)

        df["MDATE_jour"] = df["MDATE_jour"].replace({
            'lundi': "0",
            'mardi': "1",
            'mercredi': "2",
            'jeudi': "3",
            'vendredi': "4",
            'samedi': "5",
            'dimanche': "6"
        }).astype(int)

        # Convert the mode id from the EMP terminology to the ENTD one
        data = np.array(
            [
                ["1.1", "1.10"],
                ["1.2", "1.11"],
                ["1.3", "1.12"],
                ["1.4", "1.13"],
                ["2.1", "2.20"],
                ["2.2", "2.20"],
                ["2.3", "2.22"],
                ["2.4", "2.23"],
                ["2.5", "2.24"],
                ["2.6", "2.25"],
                ["2.7", "2.29"],
                ["3.1", "3.30"],
                ["3.2", "3.32"],
                ["3.3", "3.33"],
                ["3.4", "3.39"],
                ["4.1", "4.40"],
                ["4.2", "4.41"],
                ["4.3", "4.42"],
                ["4.4", "4.43"],
                ["5.1", "5.50"],
                ["5.2", "5.51"],
                ["5.3", "5.52"],
                ["5.4", "5.53"],
                ["5.5", "5.54"],
                ["5.6", "5.55"],
                ["5.7", "5.56"],
                ["5.8", "5.57"],
                ["5.9", "5.58"],
                ["5.10", "5.59"],
                ["6.1", "6.60"],
                ["6.2", "6.61"],
                ["6.3", "6.62"],
                ["6.4", "6.63"],
                ["6.5", "6.69"],
                ["7.1", "7.70"],
                ["7.2", "7.70"],
                ["7.3", "7.70"],
                ["8.1", "8.80"],
                ["9.1", "9.90"],
            ]
        )
        emp_modes_to_entd_modes = pd.DataFrame(
            data, columns=["emp_mode_id", "entd_mode_id"]
        )

        emp_modes_to_entd_modes.columns = ["mtp", "entd_mode_id"]
        df = pd.merge(df, emp_modes_to_entd_modes, on="mtp")
        df.drop(columns="mtp", inplace=True)
        df.rename(columns={"entd_mode_id": "mtp"}, inplace=True)

        # Convert the motive id from the EMP terminology to the ENTD one
        data = np.array(
            [
                ["1.1", "1.1"],
                ["1.2", "1.2"],
                ["1.3", "1.3"],
                ["1.4", "1.11"],
                ["1.5", "1.12"],
                ["2.1", "2.20"],
                ["2.2", "2.21"],
                ["3.1", "3.31"],
                ["4.1", "4.41"],
                ["4.12", "4.41"],
                ["5.1", "5.51"],
                ["5.2", "5.52"],
                ["6.1", "6.61"],
                ["6.2", "6.62"],
                ["6.3", "6.63"],
                ["6.4", "6.64"],
                ["7.1", "7.71"],
                ["7.2", "7.72"],
                ["7.3", "7.73"],
                ["7.4", "7.74"],
                ["7.5", "7.75"],
                ["7.6", "7.76"],
                ["7.7", "7.77"],
                ["7.8", "7.78"],
                ["8.1", "8.80"],
                ["8.2", "8.81"],
                ["8.3", "8.82"],
                ["8.4", "8.89"],
                ["9.1", "9.91"],
                ["9.2", "9.92"],
                ["9.3", "9.94"],
                ["9.4", "9.95"],
                ["9.5", "9.96"],
            ]
        )
        emp_motives_to_entd_motives = pd.DataFrame(
            data, columns=["emp_motive_id", "entd_motive_id"]
        )

        emp_motives_to_entd_motives.columns = ["MOTPREC", "entd_motive_id_ori"]
        df = pd.merge(df, emp_motives_to_entd_motives, on="MOTPREC")

        emp_motives_to_entd_motives.columns = ["MMOTIFDES", "entd_motive_id_des"]
        df = pd.merge(df, emp_motives_to_entd_motives, on="MMOTIFDES")

        df.drop(columns=["MOTPREC", "MMOTIFDES"], inplace=True)
        df.rename(
            columns={"entd_motive_id_ori": "MOTPREC", "entd_motive_id_des": "MMOTIFDES"},
            inplace=True,
        )

        # Merge the trips dataframe with the data about individuals and household cars
        df = pd.merge(df, indiv, on="IDENT_IND")
        df = pd.merge(
            df, hh[["city_category", "IDENT_MEN", "csp_household"]], on="IDENT_MEN"
        )
        df = pd.merge(df, cars, on="IDENT_MEN")

        # Transform the deplacement id into a day id
        df["IDENT_DEP"] = df["IDENT_DEP"].astype(str).str.slice(0, 14).astype(int)

        # Data base of days trip : group the trips by days
        days_trip = df[
            [
                "IDENT_DEP",
                "weekday",
                "city_category",
                "csp",
                "n_cars",
                "POND_JOUR",
                "MDATE_mois",
                "MDATE_jour"
            ]
        ].copy()

        days_trip.columns = [
            "day_id",
            "weekday",
            "city_category",
            "csp",
            "n_cars",
            "pondki",
            "month",
            "day_of_week"
        ]

        # Keep only the first trip of each day to have one row per day
        days_trip = days_trip.groupby("day_id").first()
        days_trip.reset_index(inplace=True)
        days_trip.set_index(["csp", "n_cars", "weekday", "city_category"], inplace=True)

        # Filter and format the columns
        df = df[
            [
                "IDENT_IND",
                "IDENT_DEP",
                "daily_trip_index",
                "weekday",
                "departure_time",
                "arrival_time",
                "city_category",
                "csp",
                "n_cars",
                "BLOGDIST",
                "MOTPREC",
                "MMOTIFDES",
                "mtp",
                "MDISTTOT_fin",
                "n_other_passengers",
                "POND_JOUR"
            ]
        ]
        df.columns = [
            "individual_id",
            "day_id",
            "daily_trip_index",
            "weekday",
            "departure_time",
            "arrival_time",
            "city_category",
            "csp",
            "n_cars",
            "BLOGDIST",
            "previous_motive",
            "motive",
            "mode_id",
            "distance",
            "n_other_passengers",
            "pondki"
        ]
        df.set_index(["day_id"], inplace=True)


        # ------------------------------------------
        # Long distance trips dataset
        df_long = pd.read_csv(
            data_folder_path / "k_voy_depdet_public_V2.csv",
            encoding="latin-1",
            sep=";",
            dtype={
                "IDENTIND": int,
                "IDENT_VOY": str,
                "OLDVMH": str,
                "OLDMOT": str,
                "OLDKM_fin": str,
                "mtp": str,
                "nbaccomp": str,
                "STATUTCOM_UU_DES": str,
                "poids_annuel": str,
                "NBJOURS_DEP": str,
                "NUITEE_DEST_DEP": str
            },
            usecols=[
                "IDENT_IND",
                "IDENT_VOY",
                "OLDVMH",
                "OLDMOT",
                "OLDKM_fin",
                "mtp",
                "nbaccomp",
                "STATUTCOM_UU_DES",
                "poids_annuel",
                "NBJOURS_DEP",
                "NUITEE_DEST_DEP"
            ],
        )

        df_long["poids_annuel"] = df_long["poids_annuel"].astype(float)
        df_long["OLDVMH"] = df_long["OLDVMH"].astype(float)
        df_long["OLDKM_fin"] = df_long["OLDKM_fin"].astype(float)
        df_long["n_other_passengers"] = df_long["nbaccomp"].astype(int)
        df_long["NBJOURS_DEP"] = df_long["NBJOURS_DEP"].astype(float)
        df_long["NUITEE_DEST_DEP"] = df_long["NUITEE_DEST_DEP"].astype(float)

        # the R category of the ENTD corresponds to the H category of the EMP 2019
        df_long.loc[df_long["STATUTCOM_UU_DES"] == "H", "STATUTCOM_UU_DES"] = "R"

        # Convert the mode id from the EMP terminology to the ENTD one
        df_long = pd.merge(df_long, emp_modes_to_entd_modes, on="mtp")
        df_long.drop(columns="mtp", inplace=True)
        df_long.rename(columns={"entd_mode_id": "mtp"}, inplace=True)

        # Convert the motive id from the EMP terminology to the ENTD one
        emp_motives_to_entd_motives.columns = ["OLDMOT", "entd_motive_id_des"]
        df_long = pd.merge(df_long, emp_motives_to_entd_motives, on="OLDMOT")
        df_long.drop(columns="OLDMOT", inplace=True)
        df_long.rename(columns={"entd_motive_id_des": "OLDMOT"}, inplace=True)

        # Merge with the data about individuals and household cars
        df_long = pd.merge(df_long, indiv, on="IDENT_IND")
        df_long = pd.merge(df_long, k_indiv[["IDENT_IND", "pond_indC"]], on="IDENT_IND")
        df_long = pd.merge(
            df_long, hh[["city_category", "IDENT_MEN", "csp_household"]], on="IDENT_MEN"
        )
        df_long = pd.merge(df_long, cars, on="IDENT_MEN")

        # If the city category of the destination is not available
        # the home's city category is used
        df_long.loc[df_long["STATUTCOM_UU_DES"].isna(), "STATUTCOM_UU_DES"] = df_long.loc[
            df_long["STATUTCOM_UU_DES"].isna(), "city_category"
        ]

        # Filter and format the columns
        df_long = df_long[
            [
                "IDENT_IND",
                "IDENT_VOY",
                "city_category",
                "STATUTCOM_UU_DES",
                "csp",
                "n_cars",
                "OLDVMH",
                "OLDMOT",
                "mtp",
                "OLDKM_fin",
                "n_other_passengers",
                "poids_annuel",
                "NBJOURS_DEP",
                "NUITEE_DEST_DEP"
            ]
        ]
        df_long.columns = [
            "individual_id",
            "travel_id",
            "city_category",
            "destination_city_category",
            "csp",
            "n_cars",
            "n_nights",
            "motive",
            "mode_id",
            "distance",
            "n_other_passengers",
            "pondki",
            "n_days_in_travel",
            "n_nights_at_destination"
        ]

        df_long.set_index("travel_id", inplace=True)
        df_long["previous_motive"] = np.nan
        df_long.drop(
            ["n_nights", "individual_id", "destination_city_category"], axis=1, inplace=True
        )

        # ------------------------------------------
        # Travels dataset
        travels = pd.read_csv(
            data_folder_path / "k_voyage_public_V2.csv",
            encoding="latin-1",
            sep=";",
            dtype={
                "IDENT_IND": int,
                "IDENT_VOY": str,
                "OLDVMH": str,
                "OLDMOT": str,
                "mtp": str,
                "STATUTCOM_UU_VOY_DES": str,
                "poids_annuel": str,
                "OLDDEBJ_mois": str,
                "OLDDEBJ_jour": str
            },
            usecols=[
                "IDENT_IND",
                "IDENT_VOY",
                "OLDVMH",
                "OLDMOT",
                "mtp",
                "STATUTCOM_UU_VOY_DES",
                "poids_annuel",
                "OLDDEBJ_mois",
                "OLDDEBJ_jour"
            ],
        )

        travels["poids_annuel"] = travels["poids_annuel"].astype(float)
        travels["OLDVMH"] = travels["OLDVMH"].astype(float)

        # the R category of the ENTD correspond to the H category of the EMP 2019
        travels.loc[travels["STATUTCOM_UU_VOY_DES"] == "H", "STATUTCOM_UU_VOY_DES"] = "R"

        # Convert the mode id from the EMP terminology to the ENTD one
        travels = pd.merge(travels, emp_modes_to_entd_modes, on="mtp")
        travels.drop(columns="mtp", inplace=True)
        travels.rename(columns={"entd_mode_id": "mtp"}, inplace=True)

        # Convert the motive id from the EMP terminology to the ENTD one
        emp_motives_to_entd_motives.columns = ["OLDMOT", "entd_motive_id_des"]
        travels = pd.merge(travels, emp_motives_to_entd_motives, on="OLDMOT")
        travels.drop(columns="OLDMOT", inplace=True)
        travels.rename(columns={"entd_motive_id_des": "OLDMOT"}, inplace=True)

        # Merge with the data about individuals and household cars
        travels = pd.merge(travels, indiv, on="IDENT_IND")
        travels = pd.merge(travels, k_indiv[["IDENT_IND", "pond_indC"]], on="IDENT_IND")
        travels = pd.merge(
            travels, hh[["city_category", "IDENT_MEN", "csp_household"]], on="IDENT_MEN"
        )
        travels = pd.merge(travels, cars, on="IDENT_MEN")

        # If the city category of the destination is not available
        # the home's city category is used
        travels.loc[
            travels["STATUTCOM_UU_VOY_DES"].isna(), "STATUTCOM_UU_VOY_DES"
        ] = travels.loc[travels["STATUTCOM_UU_VOY_DES"].isna(), "city_category"]

        # Map months and day of week to integers
        travels["OLDDEBJ_mois"] = travels["OLDDEBJ_mois"].replace({
            'janvier': "1",
            'février': "2",
            'mars': "3",
            'avril': "4",
            'mai': "5",
            'juin': "6",
            'juillet': "7",
            'août': "8",
            'septembre': "9",
            'octobre': "10",
            'novembre': "11",
            'décembre': "12"
        }).astype(int)

        travels["OLDDEBJ_jour"] = travels["OLDDEBJ_jour"].replace({
            'lundi': "0",
            'mardi': "1",
            'mercredi': "2",
            'jeudi': "3",
            'vendredi': "4",
            'samedi': "5",
            'dimanche': "6"
        }).astype(int)

        travels = travels.loc[
            :,
            [
                "IDENT_VOY",
                "city_category",
                "STATUTCOM_UU_VOY_DES",
                "csp",
                "n_cars",
                "OLDVMH",
                "OLDMOT",
                "poids_annuel",
                "OLDDEBJ_mois",
                "OLDDEBJ_jour"
            ],
        ]
        travels.columns = [
            "travel_id",
            "city_category",
            "destination_city_category",
            "csp",
            "n_cars",
            "n_nights",
            "motive",
            "pondki",
            "month",
            "weekday"
        ]

        travels.set_index(["csp", "n_cars", "city_category"], inplace=True)


        # ------------------------------------------
        # Population by csp in 2019 from the weigths in the data base k_individu
        # These weights have been computed to be representative of the french population (6 years old and older) = 59.482e6 individuals
        k_indiv = pd.merge(k_indiv, indiv, on="IDENT_IND")
        csp_pop_2019 = k_indiv.groupby("csp")["pond_indC"].sum()
        csp_pop_2019.name = "n_pop"
        csp_pop_2019 = pd.DataFrame(csp_pop_2019)

        # ------------------------------------------
        # Number of travels in a 4 week period, given the CSP
        travel_csp_pop = travels.groupby(["csp"])["pondki"].sum()
        travel_csp_pop = pd.merge(
            travel_csp_pop, csp_pop_2019, left_index=True, right_index=True
        )
        travel_csp_pop["n_travel_by_csp"] = (
            travel_csp_pop["pondki"] / travel_csp_pop["n_pop"]
        )

        # Compute the number of travels per year
        n_travel_by_csp = travel_csp_pop["n_travel_by_csp"]

        # ------------------------------------------
        # Probability of being immobile during a weekday or a week-end day given the CSP

        indiv_mob = k_indiv.loc[
            :,
            [
                "IMMODEP_A",
                "IMMODEP_B",
                "IMMODEP_C",
                "IMMODEP_D",
                "IMMODEP_E",
                "IMMODEP_F",
                "IMMODEP_G",
                "pond_indC",
                "csp",
                "MDATE_jour",
                "MDATE_delai",
            ],
        ]

        indiv_mob["IMMODEP_A"] = indiv_mob["IMMODEP_A"] * indiv_mob["pond_indC"]
        indiv_mob["IMMODEP_B"] = indiv_mob["IMMODEP_B"] * indiv_mob["pond_indC"]
        indiv_mob["IMMODEP_C"] = indiv_mob["IMMODEP_C"] * indiv_mob["pond_indC"]
        indiv_mob["IMMODEP_D"] = indiv_mob["IMMODEP_D"] * indiv_mob["pond_indC"]
        indiv_mob["IMMODEP_E"] = indiv_mob["IMMODEP_E"] * indiv_mob["pond_indC"]
        indiv_mob["IMMODEP_F"] = indiv_mob["IMMODEP_F"] * indiv_mob["pond_indC"]
        indiv_mob["IMMODEP_G"] = indiv_mob["IMMODEP_G"] * indiv_mob["pond_indC"]

        # Determine the day of the week (from 0 to 6 corresponding to monday to sunday)
        # of the day of the visit from the ref day (MDATE_jour) and the delay between
        # the ref day and the day of the vist (MDATE_delai)
        num_weekday = pd.DataFrame(
            {
                "day": [
                    "lundi",
                    "mardi",
                    "mercredi",
                    "jeudi",
                    "vendredi",
                    "samedi",
                    "dimanche",
                ],
                "num_day": [0, 1, 2, 3, 4, 5, 6],
            }
        )
        indiv_mob = pd.merge(indiv_mob, num_weekday, left_on="MDATE_jour", right_on="day")
        # The delay between the ref day and the day of the visit is added to get the day of the visit
        indiv_mob["num_day_visit"] = indiv_mob["num_day"] + indiv_mob["MDATE_delai"]
        indiv_mob["num_day_visit"] = np.where(
            indiv_mob["num_day_visit"] == -1, 6, indiv_mob["num_day_visit"]
        )
        indiv_mob["num_day_visit"] = np.where(
            indiv_mob["num_day_visit"] == -2, 5, indiv_mob["num_day_visit"]
        )
        indiv_mob["num_day_visit"] = np.where(
            indiv_mob["num_day_visit"] == -3, 4, indiv_mob["num_day_visit"]
        )
        indiv_mob["num_day_visit"] = np.where(
            indiv_mob["num_day_visit"] == -4, 3, indiv_mob["num_day_visit"]
        )
        indiv_mob["num_day_visit"] = np.where(
            indiv_mob["num_day_visit"] == -5, 2, indiv_mob["num_day_visit"]
        )
        indiv_mob["num_day_visit"] = np.where(
            indiv_mob["num_day_visit"] == -6, 1, indiv_mob["num_day_visit"]
        )
        indiv_mob["num_day_visit"] = np.where(
            indiv_mob["num_day_visit"] == -7, 0, indiv_mob["num_day_visit"]
        )

        # Determine the day of the week (from 0 to 6 corresponding to monday to sunday)
        # for each surveyed day : day A (one day before the visit), day B (2 days before the visit),
        # ... day G (7 days before the visit)
        indiv_mob["weekday_A"] = np.where(
            indiv_mob["num_day_visit"] == 0, 6, indiv_mob["num_day_visit"] - 1
        )
        indiv_mob["weekday_B"] = np.where(
            indiv_mob["weekday_A"] == 0, 6, indiv_mob["weekday_A"] - 1
        )
        indiv_mob["weekday_C"] = np.where(
            indiv_mob["weekday_B"] == 0, 6, indiv_mob["weekday_B"] - 1
        )
        indiv_mob["weekday_D"] = np.where(
            indiv_mob["weekday_C"] == 0, 6, indiv_mob["weekday_C"] - 1
        )
        indiv_mob["weekday_E"] = np.where(
            indiv_mob["weekday_D"] == 0, 6, indiv_mob["weekday_D"] - 1
        )
        indiv_mob["weekday_F"] = np.where(
            indiv_mob["weekday_E"] == 0, 6, indiv_mob["weekday_E"] - 1
        )
        indiv_mob["weekday_G"] = np.where(
            indiv_mob["weekday_F"] == 0, 6, indiv_mob["weekday_F"] - 1
        )

        # Determine if the day A, B ... G is a weekday (weekday_X=True) or a week-end day (weekday_X=False)
        indiv_mob[
            [
                "weekday_A",
                "weekday_B",
                "weekday_C",
                "weekday_D",
                "weekday_E",
                "weekday_F",
                "weekday_G",
            ]
        ] = (
            indiv_mob[
                [
                    "weekday_A",
                    "weekday_B",
                    "weekday_C",
                    "weekday_D",
                    "weekday_E",
                    "weekday_F",
                    "weekday_G",
                ]
            ]
            < 5
        )

        # Compute the number of immobility days during the week (sum only on the weekdays)
        indiv_mob["immobility_weekday"] = np.where(
            indiv_mob["weekday_A"], indiv_mob["IMMODEP_A"], 0
        )
        indiv_mob["immobility_weekday"] += np.where(
            indiv_mob["weekday_B"], indiv_mob["IMMODEP_B"], 0
        )
        indiv_mob["immobility_weekday"] += np.where(
            indiv_mob["weekday_C"], indiv_mob["IMMODEP_C"], 0
        )
        indiv_mob["immobility_weekday"] += np.where(
            indiv_mob["weekday_D"], indiv_mob["IMMODEP_D"], 0
        )
        indiv_mob["immobility_weekday"] += np.where(
            indiv_mob["weekday_E"], indiv_mob["IMMODEP_E"], 0
        )
        indiv_mob["immobility_weekday"] += np.where(
            indiv_mob["weekday_F"], indiv_mob["IMMODEP_F"], 0
        )
        indiv_mob["immobility_weekday"] += np.where(
            indiv_mob["weekday_G"], indiv_mob["IMMODEP_G"], 0
        )

        # Compute the number of immobility days during the week-end
        indiv_mob["immobility_weekend"] = np.where(
            indiv_mob["weekday_A"], 0, indiv_mob["IMMODEP_A"]
        )
        indiv_mob["immobility_weekend"] += np.where(
            indiv_mob["weekday_B"], 0, indiv_mob["IMMODEP_B"]
        )
        indiv_mob["immobility_weekend"] += np.where(
            indiv_mob["weekday_C"], 0, indiv_mob["IMMODEP_C"]
        )
        indiv_mob["immobility_weekend"] += np.where(
            indiv_mob["weekday_D"], 0, indiv_mob["IMMODEP_D"]
        )
        indiv_mob["immobility_weekend"] += np.where(
            indiv_mob["weekday_E"], 0, indiv_mob["IMMODEP_E"]
        )
        indiv_mob["immobility_weekend"] += np.where(
            indiv_mob["weekday_F"], 0, indiv_mob["IMMODEP_F"]
        )
        indiv_mob["immobility_weekend"] += np.where(
            indiv_mob["weekday_G"], 0, indiv_mob["IMMODEP_G"]
        )

        # Sum on all the indivuduals grouped by csp
        indiv_mob = indiv_mob.groupby("csp").sum()

        # Compute the probability of being immobile during a weekday and a week-end day given the csp
        indiv_mob["immobility_weekday"] = (
            indiv_mob["immobility_weekday"] / indiv_mob["pond_indC"] / 5
        )
        indiv_mob["immobility_weekend"] = (
            indiv_mob["immobility_weekend"] / indiv_mob["pond_indC"] / 2
        )
        p_immobility = indiv_mob[["immobility_weekday", "immobility_weekend"]]

        # ------------------------------------------
        # Probability of owning a car given the city category, the CSP of the ref person
        # and the number of persons in the household
        p_car = pd.merge(hh, cars, on="IDENT_MEN")

        p_car["n_pers"] = np.where(p_car["n_pers"] < 3, p_car["n_pers"].astype(str), "3+")

        p_car = p_car.groupby(["city_category", "csp_household", "n_cars", "n_pers"])[
            "n_cars"
        ].count()
        p_car = p_car / p_car.groupby(["city_category", "csp_household", "n_pers"]).sum()

        # ------------------------------------------
        # Probability of detailed public transport modes and two wheels vehicles (bikes, motorcycles)
        # given city category, distance travelled
        p_det_mode = df.copy()
        p_det_mode = p_det_mode[
            p_det_mode["mode_id"].isin(
                [
                    "2.20",
                    "2.22",
                    "2.23",
                    "2.24",
                    "2.25",
                    "2.29",
                    "5.50",
                    "5.51",
                    "5.52",
                    "5.53",
                    "5.54",
                    "5.55",
                    "5.56",
                    "5.57",
                    "5.58",
                    "5.59",
                ]
            )
        ]

        p_det_mode["mode_group"] = "2"
        p_det_mode.loc[
            p_det_mode["mode_id"].isin(
                [
                    "5.50",
                    "5.51",
                    "5.52",
                    "5.53",
                    "5.54",
                    "5.55",
                    "5.56",
                    "5.57",
                    "5.58",
                    "5.59",
                ]
            ),
            "mode_group",
        ] = "5"

        p_det_mode["dist_bin"] = pd.qcut(p_det_mode["distance"].values, 4)
        p_det_mode["dist_bin_left"] = p_det_mode["dist_bin"].apply(lambda x: x.left)
        p_det_mode["dist_bin_right"] = p_det_mode["dist_bin"].apply(lambda x: x.right)

        p_det_mode["dist_bin_left"] = p_det_mode["dist_bin_left"].astype(float)
        p_det_mode["dist_bin_right"] = p_det_mode["dist_bin_right"].astype(float)

        p_det_mode = p_det_mode.groupby(
            ["city_category", "dist_bin_left", "dist_bin_right", "mode_group", "mode_id"]
        )["pondki"].sum()
        p_det_mode_tot = p_det_mode.groupby(
            ["city_category", "mode_group", "dist_bin_left", "dist_bin_right"]
        ).sum()

        p_det_mode = p_det_mode / p_det_mode_tot
        p_det_mode.dropna(inplace=True)



        df = df.drop("pondki", axis=1)

        # ------------------------------------------
        # Write datasets to parquet files

        files = {
            "short_trips": df,
            "days_trip": days_trip,
            "p_immobility": p_immobility,
            "long_trips": df_long,
            "travels": travels,
            "n_travels": n_travel_by_csp.to_frame(),
            "p_car": p_car.to_frame(),
            "p_det_mode": p_det_mode.to_frame()
        }

        for name, df in files.items():
            df.to_parquet(self.cache_path[name])

        return None
