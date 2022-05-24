import numpy as np
import pandas as pd

import os
from pathlib import Path

def prepare_entd_2008():
    """
    This function loads the raw survey data from the survey ENTD 2008 stored in ../data/input/sdes/entd_2008
    filter the data we need and writes these data bases into parquet files
    """

    # Préparer la dataframe des déplacements locaux
    # On va essayer de changer de logique d'échantillonage :
    # Au lieu d'échantilloner des déplacements, on va échantilloner des journées entières de déplacements
    # On aura de cette manière plus d'informations sur les chaines de déplacements, qui sont déterminantes dans le choix de modes notamment
    # On évite aussi d'avoir à calculer un nombre de déplacements par jour

    # Préparer la dataframe des voyages

    # Préparer la dataframe du nombre de voyages par année
    
    data_folder_path = Path(os.path.dirname(__file__)).parent / "data"
    
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
        
    # Data base of days trip : group the trips by days
    days_trip = df[["IDENT_JOUR", "weekday", "city_category", "cs1", "n_cars", "PONDKI"]].copy()
    days_trip.columns = ["day_id", "weekday", "city_category", "cs1", "n_cars", "pondki"]
    # Keep only the first trip of each day to have one row per day
    days_trip = days_trip.groupby("day_id").first()
    days_trip.set_index(["city_category", "cs1", "n_cars", "weekday"], inplace=True)
    
    # Filter and format the columns
    df = df[["IDENT_IND", "IDENT_JOUR", "weekday", "city_category", "cs1", "n_cars", "V2_MMOTIFORI", "V2_MMOTIFDES", "V2_MTP", "V2_MDISTTOT", "n_trip_companions", "PONDKI"]]
    df.columns = ["indiv_id", "day_id", "weekday", "city_category", "cs1", "n_cars", "ori_loc_mot_id", "dest_loc_mot_id", "mode_id", "dist", "n_trip_companions", "pondki"]
    df.set_index(["day_id"], inplace=True)
        
    indiv_pondki = df.groupby("indiv_id")["pondki"].first()
    
    # ------------------------------------------
    # Long distance trips dataset
    df_long = pd.read_csv(
        data_folder_path / "input/sdes/entd_2008/K_voydepdet.csv",
        encoding="latin-1",
        sep=";",
        dtype=str,
        usecols=["IDENT_IND", "IDENT_VOY", "V2_OLDVMH", "V2_OLDMOT", "V2_DVO_ODV", 
                 "V2_OLDMTP", "V2_OLDPAX",
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
    df_long = df_long[["IDENT_IND", "IDENT_VOY", "city_category", "cs1", "n_cars", "V2_OLDVMH", "V2_OLDMOT", "V2_OLDMTP", "V2_DVO_ODV", "n_trip_companions"]]
    df_long.columns = ["indiv_id", "travel_id", "city_category", "cs1", "n_cars", "nb_nights", "dest_loc_mot_id", "mode_id", "dist", "n_trip_companions"]
    df_long.set_index("indiv_id", inplace=True)
    
    # Merge to get the weights of the individuals pondki
    df_long = pd.merge(df_long, indiv_pondki, left_index=True, right_index=True)
    df_long.reset_index()

    # Travel data base : group the long distance trips by travel
    travels = df_long[["indiv_id", "travel_id", "city_category", "cs1", "n_cars", "nb_nights", "dest_loc_mot_id", "pondki"]]
    travels.columns = ["indiv_id", "travel_id", "city_category", "cs1", "n_cars", "nb_nights", "travel_mot_id", "pondki"]
    # Keep only the first trip of each travel to have one row per travel
    travels = travels.groupby("travel_id").first()
    travels.set_index(["city_category", "cs1", "n_cars"])
    
    df_long["ori_loc_mot_id"] = np.nan
    df_long.drop(["nb_nights", "indiv_id"], axis=1, inplace=True)
    df_long.set_index("travel_id")
    
    # ------------------------------------------
    # Population by csp in 2008 from the weigths in the data base k_mobilite
    # These weights have been computed to be representative of the french population (6 years old and older) = 56.173e6 individuals
    indiv_mob = pd.read_csv(
        data_folder_path / "input/sdes/entd_2008/K_mobilite.csv",
        encoding="latin-1",
        sep=";",
        dtype={"IDENT_IND": str, 'V2_IMMODEP_A': bool, 'V2_IMMODEP_B': bool, 'V2_IMMODEP_B': bool, 'V2_IMMODEP_C': bool,
               'V2_IMMODEP_D': bool, 'V2_IMMODEP_E': bool, 'V2_IMMODEP_F': bool, 'V2_IMMODEP_G': bool},
        usecols=["IDENT_IND", "PONDKI",
                 "V2_IMMODEP_A", "V2_IMMODEP_B", "V2_IMMODEP_C", "V2_IMMODEP_D", "V2_IMMODEP_E", "V2_IMMODEP_F", "V2_IMMODEP_G",
                 "MDATENQ2V"]
    )
    indiv_mob = pd.merge(indiv_mob, indiv, on="IDENT_IND")
    csp_pop_2008 = indiv_mob.groupby('cs1')['PONDKI'].sum()
    csp_pop_2008.name = 'n_pop'
    csp_pop_2008 = pd.DataFrame(csp_pop_2008)
    
    # ------------------------------------------
    # Number of travels in a 4 week period, given the CSP
    travel_csp_pop = travels.groupby(["cs1"])["pondki"].sum()
    travel_csp_pop = pd.merge(travel_csp_pop, csp_pop_2008,  left_index=True, right_index=True)
    travel_csp_pop["n_travel_cs1"] = travel_csp_pop["pondki"]/travel_csp_pop["n_pop"]
    n_travel_cs1 = travel_csp_pop["n_travel_cs1"]
    
    # ------------------------------------------
    # Probability of being immobile during a weekday or a week-end day given the CSP
    
    indiv_mob['V2_IMMODEP_A'] = indiv_mob['V2_IMMODEP_A'] * indiv_mob['PONDKI']
    indiv_mob['V2_IMMODEP_B'] = indiv_mob['V2_IMMODEP_B'] * indiv_mob['PONDKI']
    indiv_mob['V2_IMMODEP_C'] = indiv_mob['V2_IMMODEP_C'] * indiv_mob['PONDKI']
    indiv_mob['V2_IMMODEP_D'] = indiv_mob['V2_IMMODEP_D'] * indiv_mob['PONDKI']
    indiv_mob['V2_IMMODEP_E'] = indiv_mob['V2_IMMODEP_E'] * indiv_mob['PONDKI']
    indiv_mob['V2_IMMODEP_F'] = indiv_mob['V2_IMMODEP_F'] * indiv_mob['PONDKI']
    indiv_mob['V2_IMMODEP_G'] = indiv_mob['V2_IMMODEP_G'] * indiv_mob['PONDKI']
    
    # Determine the day of the week (from 0 to 6 corresponding to monday to sunday)
    # for each surveyed day : day A (one day before the visit), day B (2 days before the visit),
    # ... day G (7 days before the visit)
    indiv_mob["MDATENQ2V"] =  pd.to_datetime(indiv_mob['MDATENQ2V'], format="%d/%m/%Y")
    indiv_mob["V2_weekday"] = indiv_mob["MDATENQ2V"].apply(lambda x: x.weekday())
    indiv_mob["weekday_A"] = np.where(indiv_mob["V2_weekday"]==0, 6, indiv_mob["V2_weekday"]-1)
    indiv_mob["weekday_B"] = np.where(indiv_mob["weekday_A"]==0, 6, indiv_mob["weekday_A"]-1)
    indiv_mob["weekday_C"] = np.where(indiv_mob["weekday_B"]==0, 6, indiv_mob["weekday_B"]-1)
    indiv_mob["weekday_D"] = np.where(indiv_mob["weekday_C"]==0, 6, indiv_mob["weekday_C"]-1)
    indiv_mob["weekday_E"] = np.where(indiv_mob["weekday_D"]==0, 6, indiv_mob["weekday_D"]-1)
    indiv_mob["weekday_F"] = np.where(indiv_mob["weekday_E"]==0, 6, indiv_mob["weekday_E"]-1)
    indiv_mob["weekday_G"] = np.where(indiv_mob["weekday_F"]==0, 6, indiv_mob["weekday_F"]-1)
    
    # Determine if the day A, B ... G is a weekday (weekday_X=True) or a week-end day (weekday_X=False)
    indiv_mob[["weekday_A", "weekday_B", "weekday_C", "weekday_D", "weekday_E", "weekday_F", "weekday_G"]] = indiv_mob[["weekday_A", "weekday_B", "weekday_C", "weekday_D", "weekday_E", "weekday_F", "weekday_G"]] < 5
    
    # Compute the number of immobility days during the week (sum only on the weekdays)
    indiv_mob["immobility_weekday"] = np.where(indiv_mob["weekday_A"], indiv_mob["V2_IMMODEP_A"], 0)
    indiv_mob["immobility_weekday"] += np.where(indiv_mob["weekday_B"], indiv_mob["V2_IMMODEP_B"], 0)
    indiv_mob["immobility_weekday"] += np.where(indiv_mob["weekday_C"], indiv_mob["V2_IMMODEP_C"], 0)
    indiv_mob["immobility_weekday"] += np.where(indiv_mob["weekday_D"], indiv_mob["V2_IMMODEP_D"], 0)
    indiv_mob["immobility_weekday"] += np.where(indiv_mob["weekday_E"], indiv_mob["V2_IMMODEP_E"], 0)
    indiv_mob["immobility_weekday"] += np.where(indiv_mob["weekday_F"], indiv_mob["V2_IMMODEP_F"], 0)
    indiv_mob["immobility_weekday"] += np.where(indiv_mob["weekday_G"], indiv_mob["V2_IMMODEP_G"], 0)
    
    # Compute the number of immobility days during the week-end
    indiv_mob["immobility_weekend"] = np.where(indiv_mob["weekday_A"], 0, indiv_mob["V2_IMMODEP_A"])
    indiv_mob["immobility_weekend"] += np.where(indiv_mob["weekday_B"], 0, indiv_mob["V2_IMMODEP_B"])
    indiv_mob["immobility_weekend"] += np.where(indiv_mob["weekday_C"], 0, indiv_mob["V2_IMMODEP_C"])
    indiv_mob["immobility_weekend"] += np.where(indiv_mob["weekday_D"], 0, indiv_mob["V2_IMMODEP_D"])
    indiv_mob["immobility_weekend"] += np.where(indiv_mob["weekday_E"], 0, indiv_mob["V2_IMMODEP_E"])
    indiv_mob["immobility_weekend"] += np.where(indiv_mob["weekday_F"], 0, indiv_mob["V2_IMMODEP_F"])
    indiv_mob["immobility_weekend"] += np.where(indiv_mob["weekday_G"], 0, indiv_mob["V2_IMMODEP_G"])
    
    # Sum on all the indivuduals grouped by csp
    indiv_mob = indiv_mob.groupby('cs1').sum()
    indiv_mob["immobility_weekday"] = indiv_mob["immobility_weekday"] / indiv_mob['PONDKI'] / 5
    indiv_mob["immobility_weekend"] = indiv_mob["immobility_weekend"] / indiv_mob['PONDKI'] / 2
    # Compute the probability of being immobile during a weekday and a week-end day given the csp
    p_immobility = indiv_mob[["immobility_weekday", "immobility_weekend"]]
    
    """ sans la distinction jour de semaine
    indiv_mob = indiv_mob.groupby('cs1').sum()
    indiv_mob['V2_IMMODEP_A'] = indiv_mob['V2_IMMODEP_A'] / indiv_mob['PONDKI']
    indiv_mob['V2_IMMODEP_B'] = indiv_mob['V2_IMMODEP_B'] / indiv_mob['PONDKI']
    indiv_mob['V2_IMMODEP_C'] = indiv_mob['V2_IMMODEP_C'] / indiv_mob['PONDKI']
    indiv_mob['V2_IMMODEP_D'] = indiv_mob['V2_IMMODEP_D'] / indiv_mob['PONDKI']
    indiv_mob['V2_IMMODEP_E'] = indiv_mob['V2_IMMODEP_E'] / indiv_mob['PONDKI']
    indiv_mob['V2_IMMODEP_F'] = indiv_mob['V2_IMMODEP_F'] / indiv_mob['PONDKI']
    indiv_mob['V2_IMMODEP_G'] = indiv_mob['V2_IMMODEP_G'] / indiv_mob['PONDKI']
    p_immobility = indiv_mob[["V2_IMMODEP_A", "V2_IMMODEP_B", "V2_IMMODEP_C", "V2_IMMODEP_D", "V2_IMMODEP_E", "V2_IMMODEP_F", "V2_IMMODEP_G"]].sum(axis=1) / 7
    """

    # ------------------------------------------
    # Probability of owning a car given the city category, the CSP of the ref person
    # and the number of persons in the household
    p_car = pd.merge(hh, cars, on="IDENT_MEN")
    
    p_car["n_pers"] = np.where(p_car["n_pers"] < 3, p_car["n_pers"].astype(str), "3+")
    
    p_car = p_car.groupby(["city_category", "cs1_ref_pers", "n_cars", "n_pers"])["n_cars"].count()
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
    
    # ------------------------------------------
    # Write datasets to parquet files
    df.to_parquet(data_folder_path / "input/sdes/entd_2008/short_dist_trips.parquet")
    days_trip.to_parquet(data_folder_path / "input/sdes/entd_2008/days_trip.parquet")
    p_immobility.to_parquet(data_folder_path / "input/sdes/entd_2008/immobility_probability.parquet")
    df_long.to_parquet(data_folder_path / "input/sdes/entd_2008/long_dist_trips.parquet")
    travels.to_parquet(data_folder_path / "input/sdes/entd_2008/travels.parquet")
    n_travel_cs1.to_frame().to_parquet(data_folder_path / "input/sdes/entd_2008/long_dist_travel_number.parquet")
    p_car.to_frame().to_parquet(data_folder_path / "input/sdes/entd_2008/car_ownership_probability.parquet")
    p_det_mode.to_frame().to_parquet(data_folder_path / "input/sdes/entd_2008/insee_modes_to_entd_modes.parquet")
    
    return

def prepare_emd_2018_2019():
    """
    This function loads the raw survey data from the survey EMP 2019 stored in ../data/input/sdes/emp_2019
    filter the data we need and writes these data bases into parquet files
    """
    
    data_folder_path = Path(os.path.dirname(__file__)).parent / "data"
    
    # Info about the individuals (CSP, city category...)
    indiv = pd.read_csv(
        data_folder_path / "input/sdes/emp_2019/tcm_ind_kish_public.csv",
        encoding="latin-1",
        sep=";",
        dtype=str,
        usecols=["ident_men", "ident_ind", "CS24"]
    )
    
    # the terminology of the entd is used to be consistent with the function prepare_entd_2008
    indiv.columns = ["IDENT_IND", "IDENT_MEN", "CS24"]
    
    indiv["cs1"] = indiv["CS24"].str.slice(0, 1)
    indiv.loc[indiv["cs1"].isnull(), "cs1"] = "no_csp"
    
    indiv["cs1"] = indiv["CS24"].str.slice(0, 1)
    indiv.loc[indiv["cs1"].isnull(), "cs1"] = "no_csp"
    indiv.loc[indiv["cs1"]=='0', "cs1"] = "no_csp"
        
    # Info about households
    hh = pd.read_csv(
        data_folder_path / "input/sdes/emp_2019/tcm_men_public.csv",
        encoding="latin-1",
        sep=";",
        dtype=str,
        usecols=["ident_men", "STATUTCOM_UU_RES", "NPERS", "CS24PR"]
    )
    hh.columns = ["IDENT_MEN", "n_pers", "cs1", "city_category"]
    
    # the R category of the ENTD correspond to the H category of the EMP 2019
    hh["city_category"].loc[hh["city_category"]=='H'] = 'R'
    
    hh["cs1"] = hh["cs1"].str.slice(0, 1)
    hh["cs1_ref_pers"] = hh["cs1"]
    hh["n_pers"] = hh["n_pers"].astype(int)
    
    # Number of cars in each household
    cars = pd.read_csv(
        data_folder_path / "input/sdes/emp_2019/q_menage_public.csv",
        encoding="latin-1",
        sep=";",
        dtype=str,
        usecols=["IDENT_MEN", "JNBVEH"]
    )
    
    cars["n_cars"] = "0"
    cars.loc[cars["JNBVEH"].astype(int) == 1, "n_cars"] = "1"
    cars.loc[cars["JNBVEH"].astype(int) > 1, "n_cars"] = "2+"
    
    cars = cars[["IDENT_MEN", "n_cars"]]
    
    # Infos about the individuals (weights, immobility)
    k_indiv = pd.read_csv(
    data_folder_path / "input/sdes/emp_2019/k_individu_public.csv",
    encoding="latin-1",
    sep=";",
    dtype={"IDENT_IND": str, "pond_indC": float},
    usecols=["IDENT_IND", "pond_indC",
             "IMMODEP_A", "IMMODEP_B", "IMMODEP_C", "IMMODEP_D", "IMMODEP_E", "IMMODEP_F", "IMMODEP_G",
             "MDATE_jour", "MDATE_delai"]
    )
    
    # ------------------------------------------
    # Trips dataset
    df = pd.read_csv(
        data_folder_path / "input/sdes/emp_2019/k_deploc_public.csv",
        encoding="latin-1",
        sep=";",
        dtype={"IDENT_DEP": str, "IDENT_IND": str, "mobloc": str, 
               "TYPEJOUR": str, "MMOTIFDES": str, "MOTPREC": str, "mtp": str},
        usecols=["IDENT_DEP", "IDENT_IND", "POND_JOUR",
                 "TYPEJOUR", "mobloc", "MMOTIFDES", "MOTPREC",
                 "MDISTTOT_fin", "mtp", "MACCOMPM", "MACCOMPHM"]
    )

    df["POND_JOUR"] = df["POND_JOUR"].astype(float)
    df["MDISTTOT_fin"] = df["MDISTTOT_fin"].astype(float)
    df.loc[df["MACCOMPM"].isnull() ,"MACCOMPM"] = 0
    df.loc[df["MACCOMPHM"].isnull() ,"MACCOMPHM"] = 0
    df["n_trip_companions"] = df["MACCOMPM"].astype(int) + df["MACCOMPHM"].astype(int)
    df["weekday"] = np.where(df["TYPEJOUR"] == "1", True, False)
        
    # the day weight is divided by 5 if it's a weekday and 2 otherwise in order to be consistent with the ENTD
    df["POND_JOUR"] = np.where(df["weekday"], df["POND_JOUR"]/5, df["POND_JOUR"]/2)
    
    # Remove long distance trips (> 80 km from home)
    df = df[df["mobloc"] == "1"]
    
    # Remove trips with an unknown or zero distance
    df = df[(df["MDISTTOT_fin"] > 0.0) | (~df["MDISTTOT_fin"].isnull())]
    
    # Convert the mode id from the EMP terminology to the ENTD one
    data = np.array([["1.1", "1.10"], ["1.2", "1.11"], ["1.3", "1.12"], ["1.4", "1.13"],
              ["2.1", "2.20"], ["2.2", "2.20"], ["2.3", "2.22"], ["2.4", "2.23"], ["2.5", "2.24"], ["2.6", "2.25"], ["2.7", "2.29"],
              ["3.1", "3.30"], ["3.2", "3.32"],
              ["3.3", "3.33"], ["3.4", "3.39"],
              ["4.1", "4.40"], ["4.2", "4.41"], ["4.3", "4.42"], ["4.4", "4.43"],
              ["5.1", "5.50"], ["5.2", "5.51"], ["5.3", "5.52"], ["5.4", "5.53"], ["5.5", "5.54"], ["5.6", "5.55"], ["5.7", "5.56"], ["5.8", "5.57"], ["5.9", "5.58"], ["5.10", "5.59"],
              ["6.1", "6.60"], ["6.2", "6.61"], ["6.3", "6.62"], ["6.4", "6.63"], ["6.5", "6.64"],
              ["7.1", "7.70"], ["7.2", "7.70"], ["7.3", "7.70"],
              ["8.1", "8.80"],
              ["9.1", "9.90"]])
    emp_modes_to_entd_modes = pd.DataFrame(data , columns=["emp_mode_id", "entd_mode_id"])
    
    emp_modes_to_entd_modes.columns = ["mtp", "entd_mode_id"]
    df = pd.merge(df, emp_modes_to_entd_modes, on="mtp")
    df.drop(columns="mtp", inplace=True)
    df.rename(columns={"entd_mode_id": "mtp"}, inplace=True)
    
    # Convert the motive id from the EMP terminology to the ENTD one
    data = np.array([["1.1", "1.1"], ["1.2", "1.2"], ["1.3", "1.3"], ["1.4", "1.11"], ["1.5", "1.12"],
                     ["2.1", "2.20"], ["2.2", "2.21"],
                     ["3.1", "3.31"], ["4.1", "4.41"], ["4.12", "4.41"],
                     ["5.1", "5.51"], ["5.2", "5.52"],
                     ["6.1", "6.61"], ["6.2", "6.62"], ["6.3", "6.63"], ["6.4", "6.64"],
                     ["7.1", "7.71"], ["7.2", "7.72"], ["7.3", "7.73"], ["7.4", "7.74"], ["7.5", "7.75"], ["7.6", "7.76"], ["7.7", "7.77"], ["7.8", "7.78"],
                     ["8.1", "8.80"], ["8.2", "8.81"], ["8.3", "8.82"], ["8.4", "8.89"],
                     ["9.1", "9.91"], ["9.2", "9.92"], ["9.3", "9.94"], ["9.4", "9.95"], ["9.5", "9.96"]])
    emp_motives_to_entd_motives = pd.DataFrame(data , columns=["emp_motive_id", "entd_motive_id"])

    emp_motives_to_entd_motives.columns = ["MOTPREC", "entd_motive_id_ori"]
    df = pd.merge(df, emp_motives_to_entd_motives, on="MOTPREC")
    
    emp_motives_to_entd_motives.columns = ["MMOTIFDES", "entd_motive_id_des"]
    df = pd.merge(df, emp_motives_to_entd_motives, on="MMOTIFDES")
    
    df.drop(columns=["MOTPREC", "MMOTIFDES"], inplace=True)
    df.rename(columns={"entd_motive_id_ori": "MOTPREC", "entd_motive_id_des": "MMOTIFDES"}, inplace=True)
    
    # Merge the trips dataframe with the data about individuals and household cars
    df = pd.merge(df, indiv, on="IDENT_IND")
    #df = pd.merge(df, k_indiv[["IDENT_IND", "pond_indC"]], on="IDENT_IND")
    df = pd.merge(df, hh[["city_category", "IDENT_MEN", "cs1_ref_pers"]], on="IDENT_MEN")
    df = pd.merge(df, cars, on="IDENT_MEN")
    
    # Transform the deplacement id into a day id
    df["IDENT_DEP"] = df["IDENT_DEP"].str.slice(0, 14)
    
    # Data base of days trip : group the trips by days
    days_trip = df[["IDENT_DEP", "weekday", "city_category", "cs1", "n_cars", "POND_JOUR"]].copy()
    days_trip.columns = ["day_id", "weekday", "city_category", "cs1", "n_cars", "pond_jour"]
    
    # Keep only the first trip of each day to have one row per day
    days_trip = days_trip.groupby("day_id").first()
    days_trip.set_index(["city_category", "cs1", "n_cars", "weekday"], inplace=True)
    
    # Filter and format the columns
    df = df[["IDENT_IND", "IDENT_DEP", "weekday", "city_category", "cs1", "n_cars", "MOTPREC", "MMOTIFDES", "mtp", "MDISTTOT_fin", "n_trip_companions", "POND_JOUR"]]
    df.columns = ["indiv_id", "day_id", "weekday", "city_category", "cs1", "n_cars", "ori_loc_mot_id", "dest_loc_mot_id", "mode_id", "dist", "n_trip_companions", "pond_jour"]
    df.set_index(["day_id"], inplace=True)
    
    # ------------------------------------------
    # Long distance trips dataset
    df_long = pd.read_csv(
        data_folder_path / "input/sdes/emp_2019/k_voy_depdet_public.csv",
        encoding="latin-1",
        sep=";",
        dtype=str,
        usecols=["IDENT_IND", "IDENT_VOY", "OLDVMH", "OLDMOT", "dvo_orides", "mtp", "nbaccomp"]
        )
    
    df_long["dvo_orides"] = df_long["dvo_orides"].astype(float)
    df_long["n_trip_companions"] = df_long["nbaccomp"].astype(int)
    
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
    df_long = pd.merge(df_long, hh[["city_category", "IDENT_MEN", "cs1_ref_pers"]], on="IDENT_MEN")
    df_long = pd.merge(df_long, cars, on="IDENT_MEN")
    
    # Filter and format the columns
    df_long = df_long[["IDENT_IND", "IDENT_VOY", "city_category", "cs1", "n_cars", "OLDVMH", "OLDMOT", "mtp", "dvo_orides", "n_trip_companions", "pond_indC"]]
    df_long.columns = ["indiv_id", "travel_id", "city_category", "cs1", "n_cars", "nb_nights", "dest_loc_mot_id", "mode_id", "dist", "n_trip_companions", "pondki"]
        
    # Travel data base : group the long distance trips by travel
    travels = df_long[["travel_id", "city_category", "cs1", "n_cars", "nb_nights", "dest_loc_mot_id", "pondki"]]
    travels.columns = ['travel_id', 'city_category', 'cs1', 'n_cars', 'nb_nights', 'travel_mot_id', 'pondki']
    # keep only the first trip of each travel to have one row per travel
    travels = travels.groupby("travel_id").first()
    travels.set_index(["city_category", "cs1", "n_cars"])
    
    df_long.set_index("travel_id", inplace=True)
    df_long["ori_loc_mot_id"] = np.nan
    df_long.drop(["nb_nights", "indiv_id"], axis=1, inplace=True)
    
    # ------------------------------------------
    # Population by csp in 2019 from the weigths in the data base k_individu
    # These weights have been computed to be representative of the french population (6 years old and older) = 59.482e6 individuals
    k_indiv = pd.merge(k_indiv, indiv, on="IDENT_IND")
    csp_pop_2019 = k_indiv.groupby('cs1')['pond_indC'].sum()
    csp_pop_2019.name = 'n_pop'
    csp_pop_2019 = pd.DataFrame(csp_pop_2019)
    
    # ------------------------------------------
    # Number of travels in a 4 week period, given the CSP
    travel_csp_pop = travels.groupby(["cs1"])["pondki"].sum()
    travel_csp_pop = pd.merge(travel_csp_pop, csp_pop_2019,  left_index=True, right_index=True)
    travel_csp_pop["n_travel_cs1"] = travel_csp_pop["pondki"]/travel_csp_pop["n_pop"]
    n_travel_cs1 = travel_csp_pop["n_travel_cs1"]

    # ------------------------------------------
    # Probability of being immobile during a weekday or a week-end day given the CSP
    
    indiv_mob = k_indiv.loc[:, ["IMMODEP_A", "IMMODEP_B", "IMMODEP_C", "IMMODEP_D",
                         "IMMODEP_E", "IMMODEP_F", "IMMODEP_G", "pond_indC", "cs1", "MDATE_jour", "MDATE_delai"]]
    
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
        {"day": ["lundi", "mardi", "mercredi", "jeudi", "vendredi", "samedi", "dimanche"],
         "num_day": [0, 1, 2, 3, 4, 5, 6]})
    indiv_mob = pd.merge(indiv_mob, num_weekday, left_on="MDATE_jour", right_on="day")
    # The delay between the ref day and the day of the visit is added to get the day of the visit
    indiv_mob["num_day_visit"] = indiv_mob["num_day"] + indiv_mob["MDATE_delai"]
    indiv_mob["num_day_visit"] = np.where( indiv_mob["num_day_visit"]== -1, 6, indiv_mob["num_day_visit"])
    indiv_mob["num_day_visit"] = np.where( indiv_mob["num_day_visit"]== -2, 5, indiv_mob["num_day_visit"])
    indiv_mob["num_day_visit"] = np.where( indiv_mob["num_day_visit"]== -3, 4, indiv_mob["num_day_visit"])
    indiv_mob["num_day_visit"] = np.where( indiv_mob["num_day_visit"]== -4, 3, indiv_mob["num_day_visit"])
    indiv_mob["num_day_visit"] = np.where( indiv_mob["num_day_visit"]== -5, 2, indiv_mob["num_day_visit"])
    indiv_mob["num_day_visit"] = np.where( indiv_mob["num_day_visit"]== -6, 1, indiv_mob["num_day_visit"])
    indiv_mob["num_day_visit"] = np.where( indiv_mob["num_day_visit"]== -7, 0, indiv_mob["num_day_visit"])
    
    # Determine the day of the week (from 0 to 6 corresponding to monday to sunday)
    # for each surveyed day : day A (one day before the visit), day B (2 days before the visit),
    # ... day G (7 days before the visit)
    indiv_mob["weekday_A"] = np.where(indiv_mob["num_day_visit"]==0, 6, indiv_mob["num_day_visit"]-1)
    indiv_mob["weekday_B"] = np.where(indiv_mob["weekday_A"]==0, 6, indiv_mob["weekday_A"]-1)
    indiv_mob["weekday_C"] = np.where(indiv_mob["weekday_B"]==0, 6, indiv_mob["weekday_B"]-1)
    indiv_mob["weekday_D"] = np.where(indiv_mob["weekday_C"]==0, 6, indiv_mob["weekday_C"]-1)
    indiv_mob["weekday_E"] = np.where(indiv_mob["weekday_D"]==0, 6, indiv_mob["weekday_D"]-1)
    indiv_mob["weekday_F"] = np.where(indiv_mob["weekday_E"]==0, 6, indiv_mob["weekday_E"]-1)
    indiv_mob["weekday_G"] = np.where(indiv_mob["weekday_F"]==0, 6, indiv_mob["weekday_F"]-1)
    
    # Determine if the day A, B ... G is a weekday (weekday_X=True) or a week-end day (weekday_X=False)
    indiv_mob[["weekday_A", "weekday_B", "weekday_C", "weekday_D", "weekday_E", "weekday_F", "weekday_G"]] = indiv_mob[["weekday_A", "weekday_B", "weekday_C", "weekday_D", "weekday_E", "weekday_F", "weekday_G"]] < 5
    
    # Compute the number of immobility days during the week (sum only on the weekdays)
    indiv_mob["immobility_weekday"] = np.where(indiv_mob["weekday_A"], indiv_mob["IMMODEP_A"], 0)
    indiv_mob["immobility_weekday"] += np.where(indiv_mob["weekday_B"], indiv_mob["IMMODEP_B"], 0)
    indiv_mob["immobility_weekday"] += np.where(indiv_mob["weekday_C"], indiv_mob["IMMODEP_C"], 0)
    indiv_mob["immobility_weekday"] += np.where(indiv_mob["weekday_D"], indiv_mob["IMMODEP_D"], 0)
    indiv_mob["immobility_weekday"] += np.where(indiv_mob["weekday_E"], indiv_mob["IMMODEP_E"], 0)
    indiv_mob["immobility_weekday"] += np.where(indiv_mob["weekday_F"], indiv_mob["IMMODEP_F"], 0)
    indiv_mob["immobility_weekday"] += np.where(indiv_mob["weekday_G"], indiv_mob["IMMODEP_G"], 0)
    
    # Compute the number of immobility days during the week-end
    indiv_mob["immobility_weekend"] = np.where(indiv_mob["weekday_A"], 0, indiv_mob["IMMODEP_A"])
    indiv_mob["immobility_weekend"] += np.where(indiv_mob["weekday_B"], 0, indiv_mob["IMMODEP_B"])
    indiv_mob["immobility_weekend"] += np.where(indiv_mob["weekday_C"], 0, indiv_mob["IMMODEP_C"])
    indiv_mob["immobility_weekend"] += np.where(indiv_mob["weekday_D"], 0, indiv_mob["IMMODEP_D"])
    indiv_mob["immobility_weekend"] += np.where(indiv_mob["weekday_E"], 0, indiv_mob["IMMODEP_E"])
    indiv_mob["immobility_weekend"] += np.where(indiv_mob["weekday_F"], 0, indiv_mob["IMMODEP_F"])
    indiv_mob["immobility_weekend"] += np.where(indiv_mob["weekday_G"], 0, indiv_mob["IMMODEP_G"])
    
    # Sum on all the indivuduals grouped by csp
    indiv_mob = indiv_mob.groupby('cs1').sum()
    
    # Compute the probability of being immobile during a weekday and a week-end day given the csp
    indiv_mob["immobility_weekday"] = indiv_mob["immobility_weekday"] / indiv_mob['pond_indC'] / 5
    indiv_mob["immobility_weekend"] = indiv_mob["immobility_weekend"] / indiv_mob['pond_indC'] / 2
    p_immobility = indiv_mob[["immobility_weekday", "immobility_weekend"]]
    """
    #sans la distinction jour de semaine / jour de weekend
    indiv_mob = indiv_mob.groupby('cs1').sum()
    indiv_mob['IMMODEP_A'] = indiv_mob['IMMODEP_A'] / indiv_mob['pond_indC']
    indiv_mob['IMMODEP_B'] = indiv_mob['IMMODEP_B'] / indiv_mob['pond_indC']
    indiv_mob['IMMODEP_C'] = indiv_mob['IMMODEP_C'] / indiv_mob['pond_indC']
    indiv_mob['IMMODEP_D'] = indiv_mob['IMMODEP_D'] / indiv_mob['pond_indC']
    indiv_mob['IMMODEP_E'] = indiv_mob['IMMODEP_E'] / indiv_mob['pond_indC']
    indiv_mob['IMMODEP_F'] = indiv_mob['IMMODEP_F'] / indiv_mob['pond_indC']
    indiv_mob['IMMODEP_G'] = indiv_mob['IMMODEP_G'] / indiv_mob['pond_indC']
    p_immobility = indiv_mob[["IMMODEP_A", "IMMODEP_B", "IMMODEP_C", "IMMODEP_D", "IMMODEP_E", "IMMODEP_F", "IMMODEP_G"]].sum(axis=1) / 7
    """
        
    # ------------------------------------------
    # Probability of owning a car given the city category, the CSP of the ref person
    # and the number of persons in the household
    p_car = pd.merge(hh, cars, on="IDENT_MEN")
    
    p_car["n_pers"] = np.where(p_car["n_pers"] < 3, p_car["n_pers"].astype(str), "3+")
    
    p_car = p_car.groupby(["city_category", "cs1_ref_pers", "n_cars", "n_pers"])["n_cars"].count()
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
    
    p_det_mode = p_det_mode.groupby(["city_category", "dist_bin_left", "dist_bin_right", "mode_group", "mode_id"])["pond_jour"].sum()
    p_det_mode_tot = p_det_mode.groupby(["city_category", "mode_group", "dist_bin_left", "dist_bin_right"]).sum()
    
    p_det_mode = p_det_mode/p_det_mode_tot
    p_det_mode.dropna(inplace=True)
    
    # ------------------------------------------
    # Write datasets to parquet files
    df.to_parquet(data_folder_path / "input/sdes/emp_2019/short_dist_trips.parquet")
    days_trip.to_parquet(data_folder_path / "input/sdes/emp_2019/days_trip.parquet")
    p_immobility.to_parquet(data_folder_path / "input/sdes/emp_2019/immobility_probability.parquet")
    df_long.to_parquet(data_folder_path / "input/sdes/emp_2019/long_dist_trips.parquet")
    travels.to_parquet(data_folder_path / "input/sdes/emp_2019/travels.parquet")
    n_travel_cs1.to_frame().to_parquet(data_folder_path / "input/sdes/emp_2019/long_dist_travel_number.parquet")
    p_car.to_frame().to_parquet(data_folder_path / "input/sdes/emp_2019/car_ownership_probability.parquet")
    p_det_mode.to_frame().to_parquet(data_folder_path / "input/sdes/emp_2019/insee_modes_to_entd_modes.parquet")
    
    return 

#prepare_entd_2008()
#prepare_emd_2018_2019()

def get_survey_data(source="EMD-2018-2019"):
    """
    This function transforms raw survey data into dataframes needed for the sampling procedures.
    
    Args:
        source (str) : The source of the travels and trips data ("ENTD-2008" or "EMD-2018-2019", the default).

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
    
    if source == "ENTD-2008" : path = data_folder_path / "input/sdes/entd_2008"
    elif source == "EMD-2018-2019" : path = data_folder_path / "input/sdes/emp_2019"
    else :
        print("The source specified doesn't exist. The EMP 2019 is used by default")
        source = "EMD-2018-2019"
        path = data_folder_path / "input/sdes/emp_2019"
    
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
        if source == "ENTD-2008" : prepare_entd_2008()
        else :
            prepare_emd_2018_2019()
    
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

survey = get_survey_data(source="EMD-2018-2019")