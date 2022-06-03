import numpy as np
import pandas as pd
import os
from pathlib import Path
import requests
import zipfile

def prepare_entd_2008(proxies={}):
    """
    This function loads the raw survey data from the survey ENTD 2008 stored in ../data/input/sdes/entd_2008
    filter the data we need and writes these data bases into parquet files
    """
    
    data_folder_path = Path(os.path.dirname(__file__)).parents[1] / "data/surveys/entd-2008"
    
    # Download the raw survey data from data.gouv.fr if needed
    path = data_folder_path / "entd-2008.zip"
    if path.exists() is False:
        
        # Download the zip file
        r = requests.get(
            url="https://www.data.gouv.fr/fr/datasets/r/896647f1-35b3-4dbe-8967-5a956cb99b95",
            proxies=proxies
        )
        with open(path, "wb") as file:
            file.write(r.content)
        
        # Unzip the content
        with zipfile.ZipFile(path, "r") as zip_ref:
            zip_ref.extractall(data_folder_path)
            
    
    # Info about the individuals (CSP, city category...)
    indiv = pd.read_csv(
        data_folder_path / "Q_tcm_individu.csv",
        encoding="latin-1",
        sep=";",
        dtype=str,
        usecols=["IDENT_MEN", "IDENT_IND", "CS24"]
    )
    indiv["csp"] = indiv["CS24"].str.slice(0, 1)
    indiv.loc[indiv["csp"].isnull(), "csp"] = "no_csp"
    
    # Info about households
    hh = pd.read_csv(
        data_folder_path / "Q_tcm_menage_0.csv",
        encoding="latin-1",
        sep=";",
        dtype=str,
        usecols=["idENT_MEN", "numcom_UU2010", "NPERS", "CS24PR"]
    )
    hh.columns = ["IDENT_MEN", "csp", "n_pers", "city_category"]
    hh["csp"] = hh["csp"].str.slice(0, 1)
    hh["csp_household"] = hh["csp"]
    hh["n_pers"] = hh["n_pers"].astype(int)
    
    # Number of cars in each household
    cars = pd.read_csv(
        data_folder_path / "Q_menage.csv",
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
        data_folder_path / "K_deploc.csv",
        encoding="latin-1",
        sep=";",
        dtype=str,
        usecols=["IDENT_IND", "IDENT_JOUR", "PONDKI",
                 "V2_TYPJOUR", "V2_DLOCAL", "V2_MMOTIFDES", "V2_MMOTIFORI",
                 "V2_MDISTTOT", "V2_MTP", "V2_MACCOMPM", "V2_MACCOMPHM"]
    )
    df["V2_MDISTTOT"] = df["V2_MDISTTOT"].astype(float)
    df["PONDKI"] = df["PONDKI"].astype(float)
    df["n_other_passengers"] = df["V2_MACCOMPM"].astype(int) + df["V2_MACCOMPHM"].astype(int)    
    df["weekday"] = np.where(df["V2_TYPJOUR"] == "1", True, False)
    
    # Remove long distance trips (> 80 km from home)
    df = df[df["V2_DLOCAL"] == "1"]
    
    # Remove trips with an unknown or zero distance
    df = df[(df["V2_MDISTTOT"] > 0.0) | (~df["V2_MDISTTOT"].isnull())]
        
    # Merge the trips dataframe with the data about individuals and household cars
    df = pd.merge(df, indiv, on="IDENT_IND")
    df = pd.merge(df, hh[["city_category", "IDENT_MEN", "csp_household"]], on="IDENT_MEN")
    df = pd.merge(df, cars, on="IDENT_MEN")
        
    # Data base of days trip : group the trips by days
    days_trip = df[["IDENT_JOUR", "weekday", "city_category", "csp", "n_cars", "PONDKI"]].copy()
    days_trip.columns = ["day_id", "weekday", "city_category", "csp", "n_cars", "pondki"]
    # Keep only the first trip of each day to have one row per day
    days_trip = days_trip.groupby("day_id").first() 
    days_trip.reset_index(inplace=True)
    days_trip.set_index(['csp', 'n_cars', 'weekday', 'city_category'], inplace=True)
    
    # Filter and format the columns
    df = df[["IDENT_IND", "IDENT_JOUR", "weekday", "city_category", "csp", "n_cars", "V2_MMOTIFORI", "V2_MMOTIFDES", "V2_MTP", "V2_MDISTTOT", "n_other_passengers", "PONDKI"]]
    df.columns = ["individual_id", "day_id", "weekday", "city_category", "csp", "n_cars", "previous_motive", "motive", "mode_id", "distance", "n_other_passengers", "pondki"]
    df.set_index(["day_id"], inplace=True)
        
    indiv_pondki = df.groupby("individual_id")["pondki"].first()
    
    # ------------------------------------------
    # Long distance trips dataset
    df_long = pd.read_csv(
        data_folder_path / "K_voydepdet.csv",
        encoding="latin-1",
        sep=";",
        dtype=str,
        usecols=["IDENT_IND", "IDENT_VOY", "V2_OLDVMH", "V2_OLDMOT", "V2_DVO_ODV", 
                 "V2_OLDMTP", "V2_OLDPAX",
                 "V2_OLDACPA01", "V2_OLDACPA02", "V2_OLDACPA03", "V2_OLDACPA04", "V2_OLDACPA05", "V2_OLDACPA06", "V2_OLDACPA07", "V2_OLDACPA08", "V2_OLDACPA09",
                 "V2_OLDARCOM_UUCat"]
    )
    df_long["V2_DVO_ODV"] = df_long["V2_DVO_ODV"].astype(float)
    df_long["n_other_passengers"] = df_long[["V2_OLDACPA01", "V2_OLDACPA02", "V2_OLDACPA03", "V2_OLDACPA04", "V2_OLDACPA05", "V2_OLDACPA06", "V2_OLDACPA07", "V2_OLDACPA08", "V2_OLDACPA09"]].count(axis=1)
    df_long["n_other_passengers"] += df_long["V2_OLDPAX"].astype(float)
    df_long.loc[df_long["n_other_passengers"].isnull(), "n_other_passengers"] = 0.0
    df_long["n_other_passengers"] = df_long["n_other_passengers"].astype(int)
    df_long["V2_OLDVMH"] = df_long["V2_OLDVMH"].astype(float)
    
    # Convert the urban category of the destination to the {'C', 'B', 'I', 'R'} terminology
    dict_urban_category = pd.DataFrame([['ville centre', 'C'],
                                        ['banlieue', 'B'],
                                        ['ville isolée', 'I'],
                                        ['commune rurale', 'R'],
                                        [np.nan, np.nan]],
                                       columns=['labels', 'UU_id'])
    dict_urban_category.columns = ['V2_OLDARCOM_UUCat', 'UU_id']
    df_long = pd.merge(df_long, dict_urban_category, on="V2_OLDARCOM_UUCat")                                
    
    # Merge with the data about individuals and household cars
    df_long = pd.merge(df_long, indiv, on="IDENT_IND")
    df_long = pd.merge(df_long, hh[["city_category", "IDENT_MEN", "csp_household"]], on="IDENT_MEN")
    df_long = pd.merge(df_long, cars, on="IDENT_MEN")
    
    # If the city category of the destination is not available
    # the home's city category is used
    df_long.loc[df_long['UU_id'].isna(), 'UU_id'] = df_long.loc[df_long['UU_id'].isna(), 'city_category']
    
    # Filter and format the columns
    df_long = df_long[["IDENT_IND", "IDENT_VOY", "city_category", "UU_id", "csp", "n_cars", "V2_OLDVMH", "V2_OLDMOT", "V2_OLDMTP", "V2_DVO_ODV", "n_other_passengers"]]
    df_long.columns = ["individual_id", "travel_id", "city_category", "destination_city_category", "csp", "n_cars", "n_nights", "motive", "mode_id", "distance", "n_other_passengers"]
    df_long.set_index("individual_id", inplace=True)
    
    # Merge to get the weights of the individuals pondki
    df_long = pd.merge(df_long, indiv_pondki, left_index=True, right_index=True)
    df_long.reset_index(inplace=True)

    # Travel data base : group the long distance trips by travel
    travels = df_long.loc[:, ["individual_id", "travel_id", "city_category", "destination_city_category", "csp", "n_cars", "n_nights", "motive", "pondki"]].copy()
    travels.columns = ["individual_id", "travel_id", "city_category", "destination_city_category", "csp", "n_cars", "n_nights", "motive", "pondki"]
    # Keep only the first trip of each travel to have one row per travel
    travels = travels.groupby("travel_id").first()
    travels.reset_index(inplace=True)
    travels.set_index(['csp', 'n_cars', 'city_category'], inplace=True)
    df_long["previous_motive"] = np.nan
    df_long.drop(["n_nights", "individual_id", "destination_city_category"], axis=1, inplace=True)
    df_long.set_index("travel_id", inplace=True)
    
    # ------------------------------------------
    # Population by csp in 2008 from the weigths in the data base k_mobilite
    # These weights have been computed to be representative of the french population (6 years old and older) = 56.173e6 individuals
    indiv_mob = pd.read_csv(
        data_folder_path / "K_mobilite.csv",
        encoding="latin-1",
        sep=";",
        dtype={"IDENT_IND": str, 'V2_IMMODEP_A': bool, 'V2_IMMODEP_B': bool, 'V2_IMMODEP_C': bool,
               'V2_IMMODEP_D': bool, 'V2_IMMODEP_E': bool, 'V2_IMMODEP_F': bool, 'V2_IMMODEP_G': bool}, 
        usecols=["IDENT_IND", "PONDKI",
                 "V2_IMMODEP_A", "V2_IMMODEP_B", "V2_IMMODEP_C", "V2_IMMODEP_D", "V2_IMMODEP_E", "V2_IMMODEP_F", "V2_IMMODEP_G",
                 "MDATENQ2V"]
    )
    indiv_mob = pd.merge(indiv_mob, indiv, on="IDENT_IND")
    csp_pop_2008 = indiv_mob.groupby('csp')['PONDKI'].sum()
    csp_pop_2008.name = 'n_pop'
    csp_pop_2008 = pd.DataFrame(csp_pop_2008)
  
    # ------------------------------------------
    # Number of travels in a 4 week period, given the CSP
    travel_csp_pop = travels.groupby(["csp"])["pondki"].sum()
    travel_csp_pop = pd.merge(travel_csp_pop, csp_pop_2008,  left_index=True, right_index=True)
    travel_csp_pop["n_travel_by_csp"] = travel_csp_pop["pondki"]/travel_csp_pop["n_pop"]
    n_travel_by_csp = travel_csp_pop["n_travel_by_csp"]
    
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
    indiv_mob = indiv_mob.groupby('csp').sum()
    indiv_mob["immobility_weekday"] = indiv_mob["immobility_weekday"] / indiv_mob['PONDKI'] / 5
    indiv_mob["immobility_weekend"] = indiv_mob["immobility_weekend"] / indiv_mob['PONDKI'] / 2
    # Compute the probability of being immobile during a weekday and a week-end day given the csp
    p_immobility = indiv_mob[["immobility_weekday", "immobility_weekend"]]

    # ------------------------------------------
    # Probability of owning a car given the city category, the CSP of the ref person
    # and the number of persons in the household
    p_car = pd.merge(hh, cars, on="IDENT_MEN")
    
    p_car["n_pers"] = np.where(p_car["n_pers"] < 3, p_car["n_pers"].astype(str), "3+")
    
    p_car = p_car.groupby(["city_category", "csp_household", "n_cars", "n_pers"])["n_cars"].count()
    p_car = p_car/p_car.groupby(["city_category", "csp_household", "n_pers"]).sum()
    
    # ------------------------------------------
    # Probability of detailed public transport modes and two wheels vehicles (bikes, motorcycles)
    # given city category, distance travelled
    p_det_mode = df.copy()
    p_det_mode = p_det_mode[p_det_mode["mode_id"].isin(["2.20", "2.22", "2.23", "2.24", "2.25", "2.29", "5.50", "5.51", "5.52", "5.53", "5.54", "5.55", "5.56", "5.57", "5.58", "5.59"])]
    
    p_det_mode["mode_group"] = "2"
    p_det_mode.loc[p_det_mode["mode_id"].isin(["5.50", "5.51", "5.52", "5.53", "5.54", "5.55", "5.56", "5.57", "5.58", "5.59"]), "mode_group"] = "5"
    
    p_det_mode["dist_bin"] = pd.qcut(p_det_mode["distance"].values, 4)
    p_det_mode["dist_bin_left"] = p_det_mode["dist_bin"].apply(lambda x: x.left)
    p_det_mode["dist_bin_right"] = p_det_mode["dist_bin"].apply(lambda x: x.right)
    
    p_det_mode = p_det_mode.groupby(["city_category", "dist_bin_left", "dist_bin_right", "mode_group", "mode_id"])["pondki"].sum()
    p_det_mode_tot = p_det_mode.groupby(["city_category", "mode_group", "dist_bin_left", "dist_bin_right"]).sum()
    
    p_det_mode = p_det_mode/p_det_mode_tot
    p_det_mode.dropna(inplace=True)

    # ------------------------------------------
    # Write datasets to parquet files
    df.to_parquet(data_folder_path / "short_dist_trips.parquet")
    days_trip.to_parquet(data_folder_path / "days_trip.parquet")
    p_immobility.to_parquet(data_folder_path / "immobility_probability.parquet")
    df_long.to_parquet(data_folder_path / "long_dist_trips.parquet")
    travels.to_parquet(data_folder_path / "travels.parquet")
    n_travel_by_csp.to_frame().to_parquet(data_folder_path / "long_dist_travel_number.parquet")
    p_car.to_frame().to_parquet(data_folder_path / "car_ownership_probability.parquet")
    p_det_mode.to_frame().to_parquet(data_folder_path / "insee_modes_to_entd_modes.parquet")
    
    return