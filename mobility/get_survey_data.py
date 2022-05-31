import numpy as np
import pandas as pd

import os
from pathlib import Path

from parsers import prepare_entd_2008, prepare_emp_2019

def get_survey_data(source="EMP-2019"):
    """
    This function transforms raw survey data into dataframes needed for the sampling procedures.
    
    Args:
        source (str) : The source of the travels and trips data ("ENTD-2008" or "EMP-2019", the default).

    Returns:
        survey_data (dict) : a dict of dataframes.
            "short_trips" (pd.DataFrame)
            "days_trip" (pd.DataFrame)
            "long_trips" (pd.DataFrame)
            "travels" (pd.DataFrame)
            "n_travels" (pd.DataFrame)
            "p_immobility" (pd.DataFrame)
            "car_ownership_probability" (pd.DataFrame)
            "p_det_mode" (pd.DataFrame)
    """
    
    # Tester si les fichiers parquet existent déjà pour la source demandée
    # Si oui, charger les parquet dans un dict
    # Si non, utiliser les fonctions de préparation pour les créer avant de les charger dans un dict
    
    data_folder_path = Path(os.path.dirname(__file__)).parent / "data"
    
    if source == "ENTD-2008":
        path = data_folder_path / "surveys/entd_2008"
    elif source == "EMP-2019":
        path = data_folder_path / "surveys/emp-2019" 
    else:
        print("The source specified doesn't exist. The EMP 2019 is used by default")
        source = "EMD-2018-2019"
        path = data_folder_path / "surveys/emp-2019"
    
    # Check if the parquet files already exist, if not writes them calling the corresponding funtion
    check_files = (path / "short_dist_trips.parquet").exists()
    check_files = check_files and (path / "days_trip.parquet").exists()
    check_files = check_files and (path / "immobility_probability.parquet").exists()
    check_files = check_files and (path / "long_dist_trips.parquet").exists()
    check_files = check_files and (path / "travels.parquet").exists()
    check_files = check_files and (path / "long_dist_travel_number.parquet").exists()
    check_files = check_files and (path / "car_ownership_probability.parquet").exists()
    check_files = check_files and (path / "insee_modes_to_entd_modes.parquet").exists()

    if not(check_files) : # ie all the files are not here
        print("Writing the parquet files")
        if source == "ENTD-2008":
            prepare_entd_2008()
        else :
            prepare_emp_2019()
    
    # Load the files into a dict
    survey_data = {}

    df = pd.read_parquet(path / "short_dist_trips.parquet")
    days_trip = pd.read_parquet(path / "days_trip.parquet")
    df_long = pd.read_parquet(path / "long_dist_trips.parquet")
    travels = pd.read_parquet(path / "travels.parquet")
    n_travel_cs1 = pd.read_parquet(path / "long_dist_travel_number.parquet")
    p_immobility = pd.read_parquet(path / "immobility_probability.parquet")
    p_car = pd.read_parquet(path / "car_ownership_probability.parquet")
    p_det_mode = pd.read_parquet(path / "insee_modes_to_entd_modes.parquet")
    
    survey_data["short_trips"] = df
    survey_data["days_trip"] = days_trip
    survey_data["long_trips"] = df_long
    survey_data["travels"] = travels
    survey_data["n_travels"] = n_travel_cs1
    survey_data["p_immobility"] = p_immobility
    survey_data["p_car"] = p_car
    survey_data["p_det_mode"] = p_det_mode
    
    return survey_data

