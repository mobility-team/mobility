import numpy as np
import pandas as pd

import os
from pathlib import Path

def prepare_entd_2008():
    """
    This function loads the raw survey data from the survey ENTD 2008 stored in ../data/input/sdes/entd_2008
    filtered the data we need and writhes these data bases into parquet files
    """

    # Préparer la dataframe des déplacements locaux
    # On va essayer de changer de logique d'échantillonage :
    # Au lieu d'échantilloner des déplacements, on va échantilloner des journées entières de déplacements
    # On aura de cette manière plus d'informations sur les chaines de déplacements, qui sont déterminantes dans le choix de modes notamment
    # On évite aussi d'avoir à calculer un nombre de déplacements par jour

    # Préparer la dataframe des voyages

    # Préparer la dataframe du nombre de voyages par année
    
    data_folder_path = Path(os.path.dirname(__file__)).parent / "data"
    
    # AF : importer K_mobilite pour pondki et avoir la pop par CSP
    # AF : importer K_voyage pour avoir le nb de nuités pro/perso selon la CSP

    # Info about the individuals (CSP, city category...)
    indiv = pd.read_csv(
        data_folder_path / "input/sdes/entd_2008/Q_tcm_individu.csv",
        encoding="latin-1",
        sep=";",
        dtype=str,
        usecols=["IDENT_MEN", "IDENT_IND", "CS24"]
    )
    
    indiv["cs1"] = indiv["CS24"].str.slice(0, 1)
    indiv.loc[indiv["cs1"].isnull(), "cs1"] = "no_csp"
    
    # Info about households
    hh = pd.read_csv(
        data_folder_path / "input/sdes/entd_2008/Q_tcm_menage_0.csv",
        encoding="latin-1",
        sep=";",
        dtype=str,
        usecols=["idENT_MEN", "numcom_UU2010", "NPERS", "CS24PR"]
    )
    hh.columns = ["IDENT_MEN", "cs1", "n_pers", "city_category"]
    
    hh["cs1"] = hh["cs1"].str.slice(0, 1)
    hh["cs1_ref_pers"] = hh["cs1"]
    hh["n_pers"] = hh["n_pers"].astype(int)
    
    # Number of cars in each household
    cars = pd.read_csv(
        data_folder_path / "input/sdes/entd_2008/Q_menage.csv",
        encoding="latin-1",
        sep=";",
        dtype=str,
        usecols=["idENT_MEN", "V1_JNBVEH"]
    )
    
    cars["n_cars"] = "0"
    cars.loc[cars["V1_JNBVEH"].astype(int) == 1, "n_cars"] = "1"
    cars.loc[cars["V1_JNBVEH"].astype(int) > 1, "n_cars"] = "2+"
    
    cars = cars[["idENT_MEN", "n_cars"]]
    cars.columns = ["IDENT_MEN", "n_cars"]

    # ------------------------------------------
    # Trips dataset
    df = pd.read_csv(
        data_folder_path / "input/sdes/entd_2008/K_deploc.csv",
        encoding="latin-1",
        sep=";",
        dtype=str,
        usecols=["IDENT_IND", "IDENT_JOUR", "PONDKI",
                 "V2_TYPJOUR", "V2_DLOCAL", "V2_MMOTIFDES", "V2_MMOTIFORI",
                 "V2_MDISTTOT", "V2_MTP", "V2_MACCOMPM", "V2_MACCOMPHM"]
    )
    df["V2_MDISTTOT"] = df["V2_MDISTTOT"].astype(float)
    df["PONDKI"] = df["PONDKI"].astype(float)
    df["n_trip_companions"] = df["V2_MACCOMPM"].astype(int) + df["V2_MACCOMPHM"].astype(int)    
    df["weekday"] = np.where(df["V2_TYPJOUR"] == "1", True, False)
    
    # Remove long distance trips (> 80 km from home)
    df = df[df["V2_DLOCAL"] == "1"]
    
    # Remove trips with an unknown or zero distance
    df = df[(df["V2_MDISTTOT"] > 0.0) | (~df["V2_MDISTTOT"].isnull())]
        
    # Merge the trips dataframe with the data about individuals and household cars
    df = pd.merge(df, indiv, on="IDENT_IND")
    df = pd.merge(df, hh[["city_category", "IDENT_MEN", "cs1_ref_pers"]], on="IDENT_MEN")
    df = pd.merge(df, cars, on="IDENT_MEN")
        
    # days data base : group the trips by days
    days_trip = df[["IDENT_JOUR", "weekday", "city_category", "cs1", "n_cars", "PONDKI"]]
    days_trip.columns = ["day_id", "weekday", "city_category", "cs1", "n_cars", "pondki"]
    # keep only the first trip of each day to have one row per day
    days_trip = days_trip.groupby("day_id").first()
    days_trip.set_index(["weekday", "city_category", "cs1", "n_cars"], inplace=True)
    
    # Filter and format the columns
    df = df[["IDENT_IND", "IDENT_JOUR", "weekday", "city_category", "cs1", "n_cars", "V2_MMOTIFORI", "V2_MMOTIFDES", "V2_MTP", "V2_MDISTTOT", "n_trip_companions", "PONDKI"]]
    df.columns = ["indiv_id", "chain_id", "weekday", "city_category", "cs1", "n_cars", "ori_loc_mot_id", "dest_loc_mot_id", "mode_id", "dist", "n_trip_companions", "pondki"]
    df.set_index(["chain_id"], inplace=True)
        
    indiv_pondki = df.groupby("indiv_id")["pondki"].first()
    
    # ------------------------------------------
    # Long distance trips dataset
    df_long = pd.read_csv(
        data_folder_path / "input/sdes/entd_2008/K_voydepdet.csv",
        encoding="latin-1",
        sep=";",
        dtype=str,
        usecols=["IDENT_IND", "IDENT_VOY", "V2_OLDMOT", "V2_DVO_ODV", "V2_OLDMTP",
                 "V2_OLDPAX",
                 "V2_OLDACPA01", "V2_OLDACPA02", "V2_OLDACPA03", "V2_OLDACPA04", "V2_OLDACPA05", "V2_OLDACPA06", "V2_OLDACPA07", "V2_OLDACPA08", "V2_OLDACPA09"]
    )
    
    df_long["V2_DVO_ODV"] = df_long["V2_DVO_ODV"].astype(float)
    
    df_long["n_trip_companions"] = df_long[["V2_OLDACPA01", "V2_OLDACPA02", "V2_OLDACPA03", "V2_OLDACPA04", "V2_OLDACPA05", "V2_OLDACPA06", "V2_OLDACPA07", "V2_OLDACPA08", "V2_OLDACPA09"]].count(axis=1)
    df_long["n_trip_companions"] += df_long["V2_OLDPAX"].astype(float)
    df_long.loc[df_long["n_trip_companions"].isnull(), "n_trip_companions"] = 0.0
    df_long["n_trip_companions"] = df_long["n_trip_companions"].astype(int)
    
    # Merge with the data about individuals and household cars
    df_long = pd.merge(df_long, indiv, on="IDENT_IND")
    df_long = pd.merge(df_long, hh[["city_category", "IDENT_MEN", "cs1_ref_pers"]], on="IDENT_MEN")
    df_long = pd.merge(df_long, cars, on="IDENT_MEN")
    
    # Filter and format the columns
    df_long = df_long[["IDENT_IND", "IDENT_VOY", "city_category", "cs1", "n_cars", "V2_OLDMOT", "V2_OLDMTP", "V2_DVO_ODV", "n_trip_companions"]]
    df_long.columns = ["indiv_id", "travel_id", "city_category", "cs1", "n_cars", "dest_loc_mot_id", "mode_id", "dist", "n_trip_companions"]
    df_long.set_index("indiv_id", inplace=True)
    
    df_long = pd.merge(df_long, indiv_pondki, left_index=True, right_index=True)
    
    df_long["ori_loc_mot_id"] = np.nan

    # travel data base
    travels = df_long.reset_index()[["travel_id", "city_category", "cs1", "n_cars", "pondki"]]
    # keep only the first trip of each travel to have one row per travel
    travels = travels.groupby("travel_id").first()
    travels.set_index(["city_category", "cs1", "n_cars"])
    
    # Population by csp in 2008 from the weigths in the data base k_mobilite
    # These weights have been computed to be representative of the french population (6 years old and older)
    # 56.173e6
    indiv_mob = pd.read_csv(
        data_folder_path / "input/sdes/entd_2008/K_mobilite.csv",
        encoding="latin-1",
        sep=";",
        dtype=str,
        usecols=["IDENT_IND", "PONDKI"]
    )
    indiv_mob['PONDKI'] = indiv_mob['PONDKI'].astype(float)
    
    indiv_mob = pd.merge(indiv_mob, indiv, on="IDENT_IND")
    indiv_mob = indiv_mob.groupby('cs1')['PONDKI'].sum()
    indiv_mob.name = 'n_pop'
    csp_pop_2008 = pd.DataFrame(indiv_mob)
    
    # ------------------------------------------
    # Number of long distance trips in a 4 week period, given the CSP
    travel_csp_pop = travels.groupby(["cs1"])["pondki"].sum()
    travel_csp_pop = pd.merge(travel_csp_pop, csp_pop_2008,  left_index=True, right_index=True)
    travel_csp_pop["n_travel_cs1"] = travel_csp_pop["pondki"]/travel_csp_pop["n_pop"]
    n_travel_cs1 = travel_csp_pop["n_travel_cs1"]
    
    # ------------------------------------------
    # Probability of owning a car given the city category, the CSP of the ref person
    # and the number of persons in the household
    p_car = pd.merge(hh, cars, on="IDENT_MEN")
    
    p_car["n_pers"] = np.where(p_car["n_pers"] < 3, p_car["n_pers"].astype(str), "3+")
    
    p_car = p_car.groupby(["city_category", "cs1_ref_pers", "n_pers", "n_cars"])["n_cars"].count()
    p_car = p_car/p_car.groupby(["city_category", "cs1_ref_pers", "n_pers"]).sum()
    
    # ------------------------------------------
    # Probability of detailed public transport modes and two wheels vehicles (bikes, motorcycles)
    # given city category, distance travelled
    p_det_mode = df.copy()
    p_det_mode = p_det_mode[p_det_mode["mode_id"].isin(["2.20", "2.22", "2.23", "2.24", "2.25", "2.29", "5.50", "5.51", "5.52", "5.53", "5.54", "5.55", "5.56", "5.57", "5.58", "5.59"])]
    
    p_det_mode["mode_group"] = "2"
    p_det_mode.loc[p_det_mode["mode_id"].isin(["5.50", "5.51", "5.52", "5.53", "5.54", "5.55", "5.56", "5.57", "5.58", "5.59"]), "mode_group"] = "5"
    
    p_det_mode["dist_bin"] = pd.qcut(p_det_mode["dist"].values, 4)
    p_det_mode["dist_bin_left"] = p_det_mode["dist_bin"].apply(lambda x: x.left)
    p_det_mode["dist_bin_right"] = p_det_mode["dist_bin"].apply(lambda x: x.right)
    
    p_det_mode = p_det_mode.groupby(["city_category", "dist_bin_left", "dist_bin_right", "mode_group", "mode_id"])["pondki"].sum()
    p_det_mode_tot = p_det_mode.groupby(["city_category", "mode_group", "dist_bin_left", "dist_bin_right"]).sum()
    
    p_det_mode = p_det_mode/p_det_mode_tot
    p_det_mode.dropna(inplace=True)
    """
    # ------------------------------------------
    # Write datasets to parquet files
    df.to_parquet(data_folder_path / "input/sdes/entd_2008/short_dist_trips.parquet")
    days_trip.to_parquet(data_folder_path / "input/sdes/entd_2008/days_trip.parquet")
    df_long.to_parquet(data_folder_path / "input/sdes/entd_2008/long_dist_trips.parquet")
    travels.to_parquet(data_folder_path / "input/sdes/entd_2008/travels.parquet")
    n_travel_cs1.to_frame().to_parquet(data_folder_path / "input/sdes/entd_2008/long_dist_travel_number.parquet")
    p_car.to_frame().to_parquet(data_folder_path / "input/sdes/entd_2008/car_ownership_probability.parquet")
    p_det_mode.to_frame().to_parquet(data_folder_path / "input/sdes/entd_2008/insee_modes_to_entd_modes.parquet")
    """
    return

prepare_entd_2008()

def prepare_emd_2018_2019():

    # Même démarche que pour l'ENTD 2008
    return

def get_survey_data(source):
    """
    This function transforms raw survey data into dataframes needed for the sampling procedures.
    
    Args:
        source (str) : The source of the travels and trips data ("ENTD-2008" or "EMD-2018-2019", the default).

    Returns:
        survey_data (dict) : a dict of dataframes.
            "short_trips" (pd.DataFrame)
            "travels" (pd.DataFrame)
            "n_travels" (pd.DataFrame)
    """
    
    # Tester si les fichiers parquet existent déjà pour la source demandée
    # Si oui, charger les parquet dans un dict
    # Si non, utiliser les fonctions de préparation pour les créer avant de les charger dans un dict
    return