from pathlib import Path
import os
import pandas as pd
from mobility.parsers.prepare_dwellings import prepare_dwellings
from mobility.parsers.prepare_cities import prepare_cities
import numpy as np


class Population:
    
    def __init__(self, codgeo):
        self.codgeo = codgeo
        self.households = None
        self.individuals = None
        self.home_work_mobility = None

        data_folder_path = Path(os.path.dirname(__file__)) / "data/insee"
        self.data_folder_path = data_folder_path
        
        
        # Find the canton ville given the CODGEO of the city
        cities_folder_path = self.data_folder_path / "cities"
        cities_file_path = cities_folder_path/ "cities.csv.gz"
        
        if not cities_file_path.exists():
            print("Writing the INSEE cities file.")
            prepare_cities()
    
        cities = pd.read_csv(cities_file_path, compression="gzip", dtype=str)
        self.cv = cities.loc[cities["CODGEO"] == codgeo, "CV"].values[0]
        self.reg = cities.loc[cities["CODGEO"] == codgeo, "REG"].values[0].zfill(2)

        print("CODGEO : "+str(self.codgeo))
        print("CV : "+ str(self.cv))
        print("reg : "+ str(self.reg))
        
    def sample(self, N_households=None, programme=None, order=None, year="2016"):
        self.households, self.individuals = self.sample_households(N_households, programme, order)
    
    def sample_households(self, N_households=None, programme=None, order=None):
        """
        Samples a population of N_samples households, given location and (optional) housing program.
    
        Args:
            N_households (int): Number of households to generate.
            programme (pandas.DataFrame): DataFrame containing the number of apartments with 1/2/3/... rooms in each building.
        """
    
        # Load INSEE dwellings file depending on location
        if self.reg == "11":
            zone = "A"
        elif self.reg in ["24", "27", "28", "32"]:
            zone = "B"
        elif self.reg in ["44", "52", "53"]:
            zone = "C"
        elif self.reg in ["75", "76"]:
            zone = "D"            
        else:
            zone = "E"
            
        dwellings_folder_path = Path(os.path.dirname(__file__)) / "data/insee/dwellings"
        dwellings_file_path = dwellings_folder_path/ f"FD_INDCVIZ{zone}_2019.parquet"
        
        if not dwellings_file_path.exists():
            print("Writing the INSEE parquet files.")
            prepare_dwellings(self.reg)
        
    
        with open(dwellings_file_path, 'rb') as f:
            df = pd.read_parquet(f)
            
            
            
        if self.cv == "75ZZ":
            # Keep only individuals in the "arrondissement" and those that live in households in Paris.
            df = df[(df["ARM"] == self.codgeo) & (df["NPERR"] != "Z")].copy()
            print(df["CANTVILLE"])
        elif self.cv == "69ZZ":
            # Keep individuals in the "arrondissement" or the specified "canton ville" in Lyon.
            arrondissement = [str(k) for k in range(69381, 69390)]
            if self.codgeo in arrondissement:
                df = df[(df["ARM"] == self.codgeo) & (df["NPERR"] != "Z")].copy()
                print(df["CANTVILLE"])
            else:
                df = df[(df["CANTVILLE"] == self.cv) & (df["NPERR"] != "Z")].copy()
                print(df["CANTVILLE"])
        elif self.cv == "1398":
            # Keep individuals in the "arrondissement" or the specified "canton ville" in Marseille.
            arrondissement = [str(k) for k in range(13201, 13217)]
            if self.codgeo in arrondissement:
                df = df[(df["ARM"] == self.codgeo) & (df["NPERR"] != "Z")].copy()
                print(df["CANTVILLE"])
            else:
                df = df[(df["CANTVILLE"] == self.cv) & (df["NPERR"] != "Z")].copy()
                print(df["CANTVILLE"])
        else:
            # Keep individuals in the specified "canton ville" who live in households.
            df = df[(df["CANTVILLE"] == self.cv) & (df["NPERR"] != "Z")].copy()
            print(df["CANTVILLE"])
    
        # Format the data
        df["IPONDI"] = df["IPONDI"].astype(float)
        df["NPERR"] = df["NPERR"].astype(int)
        self.seed = 1810
    
        if programme is not None:
            flats = programme[["building_id", "n_rooms", "n_flats", "area_per_flat"]].copy()
            flats = flats[flats["n_flats"] > 0]
            flats["SURF"] = pd.cut(flats["area_per_flat"], bins=[0, 30, 40, 60, 80, 100, 120, 10000], labels=["1", "2", "3", "4", "5", "6", "7"])
            
            # print(flats)
            sampled_ids = []
            # print(flats.groupby(["building_id", "n_rooms", "n_flats"]))

            def sample_household(row):
                # print(sampled_ids)
                n_rooms = row["n_rooms"].values[0]
                n_flats = row["n_flats"].values[0]
                surf = row["SURF"].values[0]
                n_rooms = '{:02d}'.format(n_rooms)
                # print("  n rooms : "+ str(n_rooms)+ " | n flats : "+str(n_flats))
                
                # BUG : STOCD 22 = public housing
                # the line below only works for private only housing programmes
                mask = (df["STOCD"] != "22") & (df["NBPI"] == n_rooms) & (df["LPRM"] == "1") & (df["SURF"] == surf) & (~df["NUMMI"].isin(sampled_ids))
                # mask = (df["STOCD"] != "22") & (df["NBPI"] == n_rooms) & (df["LPRM"] == "1") & (df["SURF"] == surf)  #LG
                if len(df[mask]["IPONDI"])==0:
                    print("No new household belongs to this type of flat - trying with all matching households...")  #LG
                    mask = (df["STOCD"] != "22") & (df["NBPI"] == n_rooms) & (df["LPRM"] == "1") & (df["SURF"] == surf) #LG
                    if len(df[mask]["IPONDI"])==0:
                        print("PROBLEM : No household belongs to this type of flat!!!")
                        print("  n rooms : "+ str(n_rooms)+ " | n flats : "+str(n_flats))
                        mask = (df["STOCD"] != "22") & (df["NBPI"] == n_rooms) & (df["LPRM"] == "1") #LG
                    
                    # print(df[mask]["IPONDI"])
                    
                if len(df[mask]["IPONDI"])<n_flats:
                    # print ("replace mode")
                    if order is None:
                        hh_ids = df[mask].sample(n_flats, weights=df[mask]["IPONDI"], replace=True)["NUMMI"]
                    else:
                        hh_ids = df[mask].sample(n_flats, weights=df[mask]["IPONDI"], replace=True, random_state=self.seed)["NUMMI"]

                else:
                    if order is None:
                        hh_ids = df[mask].sample(n_flats, weights=df[mask]["IPONDI"])["NUMMI"]
                    else:
                        hh_ids = df[mask].sample(n_flats, weights=df[mask]["IPONDI"], random_state=self.seed)["NUMMI"]
                
                samples = pd.DataFrame({"NUMMI":hh_ids}) #LG
                samples["hh_id"]=np.arange(0, samples.shape[0]) #LG
                samples["hh_id"] = samples["hh_id"].astype(str) +"_"+str(n_rooms)  #LG
                samples = pd.merge(samples, df, on="NUMMI", how="left") #LG
                
                
                # samples = df[df["NUMMI"].isin(hh_ids)]
                print(samples)
                sampled_ids.extend(hh_ids.values.tolist())
                
                self.seed +=10
                return samples

            hh = flats.groupby(["n_rooms", "n_flats", "building_id"])[["n_rooms", "n_flats", "SURF"]].apply(sample_household)

        elif N_households is not None:

            mask = (df["LPRM"] == "1")
            if order is None:
                hh_ids = df[mask].sample(N_households, weights=df[mask]["IPONDI"])["NUMMI"]
            else:
                hh_ids = df[mask].sample(N_households, weights=df[mask]["IPONDI"], random_state=0)["NUMMI"]
                
            samples = pd.DataFrame({"NUMMI":hh_ids}) #LG
            samples["hh_id"]=np.arange(0, samples.shape[0]) #LG                
            hh = pd.merge(samples, df, on="NUMMI", how="left") #LG
            # hh = df[df["NUMMI"].isin(hh_ids)].copy()

        else:

            raise ValueError("The number of households (N_households) or the programme dataframe must be specified.")




        def compute_uc_count(ages):
            ages = ages.astype(int).values
            n_sup_14 = len(ages[ages >= 14])
            n_inf_14 = len(ages[ages < 14])
            n_uc = 1 + 0.5*(n_sup_14-1) + 0.3*n_inf_14
            return n_uc
            
        # hh["hh_id"] = np.arange(0, hh.shape[0])
        # hh["n_uc"] = hh.groupby(["NUMMI"])["AGEREV"].transform(compute_uc_count)
        # hh["n_uc"] = hh.groupby(["NUMMI", "hh_id"])["AGEREV"].transform(compute_uc_count) #LG 10mai
        # print(hh)
        
        hhuc = hh.drop(columns="hh_id")
        hhuc = hhuc.drop_duplicates()
        # print(hhuc)
        hhuc["n_uc"] = hhuc.groupby(["NUMMI"])["AGEREV"].transform(compute_uc_count)
        # print(hh.reset_index())
        # print(hhuc)
        hh = pd.merge(hh.reset_index(), hhuc[["NUMMI", "n_uc"]].drop_duplicates(), on="NUMMI", how="left")
        # print("AFTER MERGE")
        # print(hh)
        
        hh = hh[hh["LPRM"] == "1"]
        hh.reset_index(inplace=True)
        hh["AGEREV"] = hh["AGEREV"].str.zfill(3)
        # print("AFTER RESET")
        # print(hh)
        # print("after reset")
        # print(hh)

        if programme is not None:
            hh = pd.merge(hh, flats[["building_id", "n_rooms", "area_per_flat"]], on=["building_id", "n_rooms"])


        sfm_househod_type = pd.read_excel(self.data_folder_path / "dwellings/sfm_household_type.xlsx", dtype=str)
        sfm_househod_type_filosofi = pd.read_excel(self.data_folder_path / "dwellings/sfm_household_type_filosofi.xlsx", dtype=str)
        agerev_age = pd.read_excel(self.data_folder_path / "dwellings/agerev_age.xlsx", dtype=str)
        agerev_age_filosofi = pd.read_excel(self.data_folder_path / "dwellings/agerev_age_filosofi.xlsx", dtype=str)
        stocd = pd.read_excel(self.data_folder_path / "dwellings/stocd_filosofi.xlsx", dtype=str)

        hh = pd.merge(hh, sfm_househod_type, on="SFM")
        hh = pd.merge(hh, sfm_househod_type_filosofi, on="SFM")
        hh = pd.merge(hh, agerev_age, on="AGEREV")
        hh = pd.merge(hh, agerev_age_filosofi, on="AGEREV")
        hh = pd.merge(hh, stocd, on="STOCD")
        
        

        if programme is not None:
            hh = hh[["NUMMI",  "building_id", "n_rooms", "area_per_flat", "NPERR",  "n_uc", "stocd_bdf", "stocd", "household_type", "household_type_filosofi", "age", "age_filosofi", "GARL", "VOIT"]]
            hh.columns = ["NUMMI", "building_id", "n_rooms", "area_per_flat", "n_persons", "n_uc", "occupation_status", "occupation_status_filosofi", "household_type", "household_type_filosofi", "ref_pers_age", "ref_pers_age_filosofi", "GARL", "VOIT"]
            
        elif N_households is not None:

            # Sample areas
            flat_area_table = pd.DataFrame({
                "SURF": ["1", "2", "3", "4", "5", "6", "7"],
                "area_lower":  [20, 30, 40, 60, 80, 100, 120],
                "area_upper": [30, 40, 60, 80, 100, 120, 200]
            })
            hh = pd.merge(hh, flat_area_table, on="SURF")
            hh["area_per_flat"] = np.round(np.random.uniform(hh["area_lower"], hh["area_upper"], hh.shape[0]))
            hh.drop(["area_lower", "area_upper", "SURF"], axis=1, inplace=True)
            
            # Convert formats
            hh["NBPI"] = hh["NBPI"].astype(int)
            
            hh = hh[["NUMMI", "NBPI", "area_per_flat", "NPERR", "n_uc",  "stocd_bdf", "stocd", "household_type", "household_type_filosofi", "age", "age_filosofi", "GARL", "VOIT"]]
            hh.columns = ["NUMMI", "n_rooms", "area_per_flat", "n_persons", "n_uc", "occupation_status", "occupation_status_filosofi", "household_type", "household_type_filosofi", "ref_pers_age", "ref_pers_age_filosofi", "GARL", "VOIT"]

        # hh = hh.reset_index(drop=True).reset_index().rename(columns={"index":"household_id"}) #LG
        hh["household_id"] = np.arange(0, hh.shape[0])
        
        # hh["n_uc"] = hh.groupby(["household_id", "hh_id"])["AGEREV"].transform(compute_uc_count) #LG 10mai
        
        # indiv = df[df["NUMMI"].isin(hh["household_id"])]
        indiv = pd.merge(hh[["household_id", "NUMMI"]], df, on="NUMMI", how="left") #LG
        indiv = indiv[["household_id", "NUMMI", "LPRM", "AGEREVQ", "TACT", "CS1", "NA5"]]
        indiv["individual_id"] = np.arange(0, indiv.shape[0])

        indiv = indiv[["household_id", "individual_id", "LPRM", "TACT", "CS1", "NA5", "AGEREVQ"]]
        indiv.columns = ["household_id", "individual_id", "ref_person_link", "activity_type", "socio_pro_category", "economic_sector", "age_q"]
        
        
        return hh.drop(columns=["NUMMI"]), indiv
        

    def sample_home_work_mobility(self, individuals):

        # Read the data (subset the columns)
        with open(self.data_folder_path / "input/insee/recensement/RP2016_MOBPRO.parquet", 'rb') as f:
            mob = pd.read_parquet(f, columns=["COMMUNE", "ARM", "DCLT", "CS1", "TRANS", "AGEREVQ", "IPONDI"])

        # Replace Paris, Lyon and Marseille city CODGEO by their districts CODGEO
        mob.loc[mob["COMMUNE"].isin(["75056", "13201", "69123"]), "COMMUNE"] = mob.loc[mob["COMMUNE"].isin(["75056", "13201", "69123"]), "ARM"]

        mob = mob[mob["COMMUNE"] == self.codgeo].copy()
        mob["IPONDI"] = mob["IPONDI"].astype(float)

        def sample_work_trip(csp, agerevq):
            # Find matching individuals
            mask = (mob["CS1"] == csp.values[0]) & (mob["AGEREVQ"] == agerevq.values[0])
            # If no match is found, relax the constraints one by one
            if np.sum(mask) == 0:
                mask = (mob["CS1"] == csp.values[0])
            if np.sum(mask) == 0:
                mask = np.ones(mob.shape[0]).astype(bool)
            # Sample a workplace
            trip = mob[mask].sample(1, weights=mob[mask]["IPONDI"])
            return trip[["DCLT", "TRANS"]]
            
        indiv = individuals.copy()
        workers = indiv[(indiv["activity_type"] == "11") & (indiv["age_q"].astype(int) > 15) & (indiv["age_q"].astype(int) < 65)]

        workers_trips = workers.groupby(["individual_id"]).apply(lambda x: sample_work_trip(x["socio_pro_category"], x["age_q"]))
        workers_trips.reset_index(inplace=True)
        workers_trips.drop("level_1", axis=1, inplace=True)

        workers_trips = pd.merge(workers, workers_trips, on=["individual_id"])

        workers_trips = workers_trips[["individual_id", "DCLT", "TRANS"]]
        workers_trips.columns = ["individual_id", "place_of_work", "transport_mode"]

        return workers_trips




