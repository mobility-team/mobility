import os
import pathlib
import logging
import zipfile
import pandas as pd
from pyaxis import pyaxis

from mobility.file_asset import FileAsset
from mobility.parsers.download_file import download_file


class StudentsDistribution(FileAsset):
    """
    Prepares and caches the spatial distribution of students by combining French
    and Swiss student data from various sources.
    """

    def __init__(self):
        """
        Initialize the StudentsDistribution asset with an empty inputs dictionary
        and a cache path.
        """
        inputs = {}
        cache_path = (
            pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"])
            / "insee"
            / "students.parquet"
        )
        super().__init__(inputs, cache_path)

    def get_cached_asset(self) -> pd.DataFrame:
        """
        Retrieve the cached student distribution DataFrame.

        Returns:
            pd.DataFrame: Cached student distribution data.
        """
        logging.info(
            "Students spatial distribution already prepared. Reusing the file: %s",
            str(self.cache_path),
        )
        students = pd.read_parquet(self.cache_path)
        return students

    def create_and_get_asset(self) -> pd.DataFrame:
        """
        Create the student distribution dataset by preparing both French and Swiss data,
        combining them, saving the combined result to cache, and returning the DataFrame.

        Returns:
            pd.DataFrame: Combined French and Swiss student distribution data.
        """
        students_fr = self.prepare_french_students_distribution()
        students_ch = self.prepare_swiss_students_distribution()

        # Combine the French and Swiss datasets and save to cache.
        students = pd.concat([students_fr, students_ch], ignore_index=True)
        students.to_parquet(self.cache_path)

        return students

    def prepare_french_students_distribution(self) -> pd.DataFrame:
        """
        Prepare French student distribution data using INSEE census data.

        This method downloads and extracts a ZIP file containing French education data,
        reads the CSV file, transforms it from wide to long format, maps age groups
        to school type categories, aggregates the student counts by local administrative
        unit, and prefixes the unit IDs with 'fr-' to denote French data.

        Returns:
            pd.DataFrame: French student distribution data.
        """
        # Define data folder and ensure it exists.
        data_folder = (
            pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"])
            / "insee"
            / "schools"
        )
        data_folder.mkdir(parents=True, exist_ok=True)

        # ---------------------------------------------------------------------
        # Download and extract INSEE census data for student population by age group.
        # ---------------------------------------------------------------------
        insee_url = (
            "https://www.insee.fr/fr/statistiques/fichier/6454124/"
            "base-ccc-diplomes-formation-2019.zip"
        )
        zip_path = data_folder / "base-ccc-diplomes-formation-2019.zip"
        csv_path = data_folder / "base-cc-diplomes-formation-2019.csv"

        download_file(insee_url, zip_path)

        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(data_folder)

        # Read the CSV file with selected columns.
        students = pd.read_csv(
            csv_path,
            sep=";",
            usecols=[
                "CODGEO",
                "P19_SCOL0205",
                "P19_SCOL0610",
                "P19_SCOL1114",
                "P19_SCOL1517",
                "P19_SCOL1824",
                "P19_SCOL2529",
                "P19_SCOL30P",
            ],
            dtype={"CODGEO": str},
        )

        # Transform from wide to long format.
        students = pd.melt(
            students,
            id_vars="CODGEO",
            var_name="age_group",
            value_name="n_students",
        )

        # ---------------------------------------------------------------------
        # Map age groups to school type categories.
        # ---------------------------------------------------------------------
        age_schools_categories = {
            "primary_school_fr": ["P19_SCOL0205", "P19_SCOL0610"],  # Ecole
            "secondary_school_fr": ["P19_SCOL1114"],                   # Collège
            "high_school_fr": ["P19_SCOL1517"],                        # Lycée
            "university_fr": ["P19_SCOL1824", "P19_SCOL2529", "P19_SCOL30P"],  # Etudes sup
        }

        # Create a mapping from each age group code to its corresponding school type.
        age_schools_categories = {
            code: category
            for category, codes in age_schools_categories.items()
            for code in codes
        }
        students["school_type"] = students["age_group"].replace(age_schools_categories)

        # Rename geographic code column.
        students.rename(columns={"CODGEO": "local_admin_unit_id"}, inplace=True)

        # Aggregate student counts by local administrative unit and school type.
        students = students.groupby(
            ["local_admin_unit_id", "school_type"], as_index=False
        )["n_students"].sum()

        # Prefix local admin unit IDs with "fr-" to indicate French data.
        students["local_admin_unit_id"] = "fr-" + students["local_admin_unit_id"]

        return students

    def prepare_swiss_students_distribution(self) -> pd.DataFrame:
        """
        Prepare Swiss student distribution data using BFS census data.

        This method downloads an Excel file containing Swiss student data, processes it to:
          - Extract commune codes and names from the "Région" column.
          - Transform the data into long format.
          - Merge with BFS enrollment rate data by age to compute student numbers.
          - Aggregate the student counts by local administrative unit and school type.
          - Prefix the unit IDs with 'ch-' to indicate Swiss data.

        Returns:
            pd.DataFrame: Swiss student distribution data.
        """
        data_folder = (
            pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"])
            / "bfs"
            / "schools"
        )
        data_folder.mkdir(parents=True, exist_ok=True)

        # ---------------------------------------------------------------------
        # Download and process the BFS Excel file with student data.
        # ---------------------------------------------------------------------
        bfs_url = "https://dam-api.bfs.admin.ch/hub/api/dam/assets/32229174/master"
        xlsx_path = data_folder / "su-f-01.02.03.06.xlsx"

        download_file(bfs_url, xlsx_path)

        # Read the Excel file, skipping the first row.
        students_ch = pd.read_excel(xlsx_path, skiprows=1)
        # Keep rows where the "Région" column starts with "......"
        students_ch = students_ch[
            students_ch["Région"].str.startswith("......", na=False)
        ]
        # Extract commune code using regex.
        students_ch["local_admin_unit_id"] = students_ch["Région"].str.extract(
            r"^......(\d+)\s"
        )
        # Extract commune name by removing the code and leading dots.
        students_ch["local_admin_unit"] = students_ch["Région"].str.replace(
            r"^......\d+\s+", "", regex=True
        )

        # Transform the data into long format.
        students_ch = pd.melt(
            students_ch,
            id_vars=["local_admin_unit_id", "local_admin_unit"],
            var_name="age",
            value_name="n_individuals",
        )
        # Remove header or total rows.
        students_ch = students_ch[~students_ch["age"].isin(["Région", "Total"])]

        # ---------------------------------------------------------------------
        # Map age groups to Swiss school type categories.
        # ---------------------------------------------------------------------
        age_schools_categories_ch = {
            "primary_ch": ["4", "5", "6", "7", "8", "9", "10", "11"],     # Primary
            "secondary_1_ch": ["12", "13", "14", "15"],                    # Secondary I
            "secondary_2_ch": ["15", "16", "17", "18", "19"],               # Secondary II
            "university_ch": [str(i) for i in range(18, 32)],               # Higher education
        }
        age_group_rows = [
            {"age": int(age), "school_type": group}
            for group, ages in age_schools_categories_ch.items()
            for age in ages
        ]
        age_groups_ch = pd.DataFrame(age_group_rows)

        # ---------------------------------------------------------------------
        # Get BFS enrollment rate data by age.
        # ---------------------------------------------------------------------
        enrollment_rate_data = {
            "age": list(range(3, 32)),
            "mandatory_school": [
                0.02, 0.48, 0.98, 0.99, 0.99, 0.99, 0.99, 0.99, 0.99,
                0.99, 0.99, 0.98, 0.64, 0.11, 0.02, 0.00, 0.00, 0.00,
                0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00,
                0.00, 0.00,
            ],
            "secondary": [
                0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00,
                0.00, 0.00, 0.01, 0.32, 0.80, 0.88, 0.75, 0.46, 0.24,
                0.14, 0.09, 0.07, 0.05, 0.03, 0.03, 0.02, 0.01, 0.01,
                0.01, 0.01,
            ],
            "tertiary": [
                0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00,
                0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.05, 0.14, 0.24,
                0.31, 0.34, 0.33, 0.29, 0.24, 0.20, 0.16, 0.12, 0.10,
                0.08, 0.07,
            ],
        }
        enrollment_rate_df = pd.DataFrame(enrollment_rate_data)
        enrollment_rate_df = enrollment_rate_df.melt(
            id_vars="age", var_name="level", value_name="share"
        )

        # Merge enrollment rates with age group mapping.
        student_shares_ch = enrollment_rate_df.merge(
            age_groups_ch, on="age", how="left"
        )
        # Drop rows with no matching school type or where share equals 0.
        student_shares_ch = student_shares_ch.dropna(subset=["school_type"])
        student_shares_ch = student_shares_ch[student_shares_ch["share"] != 0]

        # Merge the computed shares with the student data.
        students_ch = pd.merge(students_ch, student_shares_ch, on="age")
        # Calculate the number of students based on the share.
        students_ch["n_students"] = (
            students_ch["n_individuals"] * students_ch["share"]
        )
        # Filter out rows with zero students.
        students_ch = students_ch[students_ch["n_students"] != 0]

        # Aggregate student counts by local administrative unit and school type.
        students_ch = students_ch.groupby(
            ["local_admin_unit_id", "school_type"], as_index=False
        )["n_students"].sum()

        # Prefix local admin unit IDs with "ch-" to indicate Swiss data.
        students_ch["local_admin_unit_id"] = "ch-" + students_ch["local_admin_unit_id"]

        return students_ch
