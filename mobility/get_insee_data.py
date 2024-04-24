import pandas as pd

import os
from pathlib import Path

from mobility.parsers.job_active_population import prepare_job_active_population
from mobility.parsers.permanent_db_facilities import prepare_facilities
<<<<<<< Updated upstream
=======
from mobility.parsers.school_attendance import prepare_school_attendance
from mobility.parsers.student_attendance import prepare_student_attendance
>>>>>>> Stashed changes


def get_insee_data(test=False):
    """
    Loads the parquet files corresponding to the INSEE data
    (downloads and writes them first if needed).
    The INSEE data contains:
        - the repartition of the active population,
        - the repartion of jobs
        - the repartion of shops
        - the repartion of schools
        - the repartion of administration facilities
        - the repartion of sport facilities
        - the repartion of care facilities
        - the repartion of show facilities
        - the repartion of museum
        - the repartion of restaurants

    Returns
    -------
    dict:
        keys (list of str):
            ['jobs', 'active_population', 'malls', 'shops', 'schools',
             'admin', 'sport', 'care', 'show', 'museum', 'restaurants']
        values (list of pd.DataFrame):
            The corresponding dataframes wich have the following structure:
                Index:
                    DEPCOM (str): city geographic code
                Columns:
                    sink_volume (int): weight of the corresponding facilities
    """
    data_folder_path = Path(os.path.dirname(__file__)) / "data/insee"

    # Check if the parquet files already exist, if not writes them calling the corresponding funtion
    check_files = (data_folder_path / "work/jobs.parquet").exists()
    check_files = (
        check_files and (data_folder_path / "work/active_population.parquet").exists()
    )
    check_files = (
        check_files and (data_folder_path / "facilities/malls.parquet").exists()
    )
    check_files = (
        check_files and (data_folder_path / "facilities/shops.parquet").exists()
    )
    check_files = (
<<<<<<< Updated upstream
        check_files and (data_folder_path / "facilities/schools.parquet").exists()
=======
        check_files and (data_folder_path / "schools/schools.parquet").exists()
    )
    check_files = (
        check_files and (data_folder_path / "schools/students.parquet").exists()
>>>>>>> Stashed changes
    )
    check_files = (
        check_files
        and (data_folder_path / "facilities/admin_facilities.parquet").exists()
    )
    check_files = (
        check_files
        and (data_folder_path / "facilities/sport_facilities.parquet").exists()
    )
    check_files = (
        check_files
        and (data_folder_path / "facilities/care_facilities.parquet").exists()
    )
    check_files = (
        check_files
        and (data_folder_path / "facilities/show_facilities.parquet").exists()
    )
    check_files = (
        check_files and (data_folder_path / "facilities/museum.parquet").exists()
    )
    check_files = (
        check_files and (data_folder_path / "facilities/restaurants.parquet").exists()
    )

    if not (check_files):  # ie all the files are not here
        print("Writing the INSEE parquet files.")
        prepare_job_active_population(test=test)
<<<<<<< Updated upstream
=======
        prepare_school_attendance(test=test)
        prepare_student_attendance(test=test)
>>>>>>> Stashed changes
        prepare_facilities()

    # Load the dataframes into a dict
    insee_data = {}

    jobs = pd.read_parquet(data_folder_path / "work/jobs.parquet")
    active_population = pd.read_parquet(
        data_folder_path / "work/active_population.parquet"
    )
    shops = pd.read_parquet(data_folder_path / "facilities/shops.parquet")
    schools = pd.read_parquet(data_folder_path / "facilities/schools.parquet")
    admin = pd.read_parquet(data_folder_path / "facilities/admin_facilities.parquet")
    sport = pd.read_parquet(data_folder_path / "facilities/sport_facilities.parquet")
    care = pd.read_parquet(data_folder_path / "facilities/care_facilities.parquet")
    show = pd.read_parquet(data_folder_path / "facilities/show_facilities.parquet")
    museum = pd.read_parquet(data_folder_path / "facilities/museum.parquet")
    restaurant = pd.read_parquet(data_folder_path / "facilities/restaurants.parquet")

    insee_data["jobs"] = jobs
    insee_data["active_population"] = active_population
    insee_data["shops"] = shops
    insee_data["schools"] = schools
<<<<<<< Updated upstream
=======
    insee_data["students"] = students
>>>>>>> Stashed changes
    insee_data["admin"] = admin
    insee_data["sport"] = sport
    insee_data["care"] = care
    insee_data["show"] = show
    insee_data["museum"] = museum
    insee_data["restaurant"] = restaurant

    return insee_data
