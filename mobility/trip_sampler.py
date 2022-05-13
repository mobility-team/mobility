#Mobility is free software developed by Elioth (https://elioth.com/) ; you can redistribute it and/or modify it under the terms of the GNU General Public License

import os
import time
from pathlib import Path

import pandas as pd
import numpy as np

from .utilities import read_parquet
from .mobility_model import WorkMobilityModel, ShopsMobilityModel

class TripSampler:
    
    def __init__(self, codgeo):
        
        data_folder_path = Path(os.path.dirname(__file__)).parent / "data"
        
        # ---------------------------------------
        # Store geographic info
        self.codgeo = codgeo
        
        # Find the city category
        cities_category = pd.read_csv(data_folder_path / "input/insee/cities_category.csv", dtype=str)
        cities_category.set_index("codgeo", inplace=True)
        
        if codgeo in [str(75100 + i) for i in range(1, 21)]:
            self.city_category = cities_category.loc["75056"].values[0]
        elif codgeo in [str(69380 + i) for i in range(1, 10)]:
            self.city_category = cities_category.loc["69123"].values[0]
        elif codgeo in [str(13200 + i) for i in range(1, 16)]:
            self.city_category = cities_category.loc["13055"].values[0]
            self.codgeo = "13055"
        else:
            self.city_category = cities_category.loc[codgeo].values[0]
        

        # ---------------------------------------
        # Import datasets
        self.short_trips_db = read_parquet(data_folder_path / "input/sdes/entd_2008/short_dist_trips.parquet")
        self.n_short_trips = read_parquet(data_folder_path / "input/sdes/entd_2008/short_dist_trip_number.parquet")
        
        self.long_trips_db = read_parquet(data_folder_path / "input/sdes/entd_2008/long_dist_trips.parquet")
        self.n_long_trips = read_parquet(data_folder_path / "input/sdes/entd_2008/long_dist_trip_number.parquet")
        
        self.p_car = read_parquet(data_folder_path / "input/sdes/entd_2008/car_ownership_probability.parquet")

        self.n_short_trips = self.n_short_trips.squeeze()
        self.n_long_trips = self.n_long_trips.squeeze()
        self.p_car = self.p_car.squeeze()

        self.short_trips_db = self.short_trips_db.xs(self.city_category, level=1)
        self.long_trips_db = self.long_trips_db.xs(self.city_category)
        self.p_car = self.p_car.xs(self.city_category)

        # ---------------------------------------
        # Initialize the work-home, shops-home mobility models
        self.work_model = WorkMobilityModel(self.codgeo)
        self.shops_model = ShopsMobilityModel(self.codgeo)
        
        # ---------------------------------------
        # Modes probabilities given the origin and destination of a trip
        self.p_mode = read_parquet(data_folder_path / "input/mobility/modes/modes_probability.parquet")
        self.p_mode.reset_index("mode_id", inplace=True)
        self.p_mode.index.rename(["ori_codgeo", "dest_codgeo", "has_cars"], inplace=True)

        
        # ---------------------------------------
        # Modes emission factors
        self.mode_ef = pd.read_csv(
            data_folder_path / "input/mobility/emission_factors/mode_ef.csv",
            dtype={"mode_id": str, "value": float}
        )
        # Subset the mode probability data to handle a smaller dataframe
        # (keeping only cities within a 80 km radius around the origin)
        nearby_locations = self.shops_model.compute_distance_to_sinks(self.codgeo,  self.shops_model.sources, self.shops_model.sinks, 80.0e3).index
        self.p_mode = self.p_mode[self.p_mode.index.get_level_values("ori_codgeo").isin(nearby_locations)]
        self.p_mode = self.p_mode[self.p_mode.index.get_level_values("dest_codgeo").isin(nearby_locations)]
        self.seed=1810
        
    def get_trips(self, person_id, csp_ref_pers, csp, n_pers, na5=None, n_cars=None, replicable=None):
        
        if replicable:
            np.random.seed(self.seed)
            
        # Number of years to simulate
        # (> 1 to create an average mobility behavior over several years)
        n_years = 1
    
        # Compute the number of cars based on the city category,
        # the CSP of the reference person and the number of persons in the household
        if n_cars is None:
            p = self.p_car.xs(csp_ref_pers).xs(n_pers)
            n_cars = np.random.choice(p.index.to_numpy(), 1, p=p)[0]
            
        result = []
        
        
        # Sample trips from the ENTD database
        # For weekdays and weekends
        for weekday in [True, False]:
            
            # Compute the number of trips
            n_days = n_years*52*5 if weekday else n_years*52*2
            n_trips = np.round(n_days*self.n_short_trips.xs(weekday).xs(csp)).astype(int)
            
            # Sample n_trips based on the city category,
            # the csp of the person and the number of cars in the household
            # (using the ENTD sampling weight PONDKI)
            if replicable:
                trips_bis = self.short_trips_db.xs(weekday).xs(csp)
                if not trips_bis.reset_index('n_cars')['n_cars'].eq(n_cars).any():
                    print("empty")
                    if n_cars =="0":
                        n_cars ="1"
                    else:
                        n_cars ="0"
                trips = self.short_trips_db.xs(weekday).xs(csp).xs(n_cars).sample(n_trips, weights="pondki", replace=True, random_state=self.seed)
                
            else:
                trips = self.short_trips_db.xs(weekday).xs(csp).xs(n_cars).sample(n_trips, weights="pondki", replace=True)
            
            trips.drop(["pondki"], axis=1, inplace=True)
            trips.reset_index(inplace=True, drop=True)
            
            result.append(trips)
 
        # For long trips
        n_trips = n_years*13*np.round(self.n_long_trips.xs(csp)).astype(int)
        
        if n_cars in self.long_trips_db.xs(csp).index.get_level_values("n_cars"):
            if replicable:
                trips = self.long_trips_db.xs(csp).xs(n_cars).sample(n_trips, weights="pondki", replace=True)
            else:
                trips = self.long_trips_db.xs(csp).xs(n_cars).sample(n_trips, weights="pondki", replace=True, random_state=self.seed)
        else:
            if replicable:
                trips = self.long_trips_db.xs(csp).sample(n_trips, weights="pondki", replace=True)
            else:
                trips = self.long_trips_db.xs(csp).sample(n_trips, weights="pondki", replace=True, random_state=self.seed)
        
        trips.drop(["pondki"], axis=1, inplace=True)
        trips.reset_index(inplace=True, drop=True)
        
        result.append(trips)
        
        trips = pd.concat(result)
        trips["trip_id"] = np.arange(0, trips.shape[0])
        
        # ---------------------------------------
        # Replace ENTD distances with the available mobility models

        def location_map(motive, locations, p):
            
            if not isinstance(p, list):
                locations = [locations]
                p = [p]
            
            origin_to_codgeo = pd.DataFrame({
                "ori_loc_mot_id": motive,
                "ori_codgeo": locations,
                "p_ori": p
            })
            
            return origin_to_codgeo
            
        ori_map = []
        
        # -------------------------------
        # Set or compute the probabilities of the locations
        # of the origins and destinations, if available
        # Home
        o = location_map("1.1", self.codgeo, 1.0)
        ori_map.append(o)
        
        # Work
        if csp in ["1", "2", "3", "4", "5", "6"]: #csp 7 and 8 don't work
            #correction of db errors
            if na5=="ZZ":
                if csp in ["1"]:
                    na5="AZ"
                else:
                    na5="GU"
            # print("p_work")
            p_work = self.work_model.compute_sink_probabilities(self.codgeo, na5=na5, cs1=csp)
            codgeo_work = np.random.choice(p_work.index.to_numpy(), 1, p=p_work)[0]
            o = location_map("9.91", codgeo_work, 1.0)
            ori_map.append(o)
        
        # Large shops
        p_shops = self.shops_model.compute_sink_probabilities(self.codgeo)
        o = location_map("2.20", p_shops.index.to_numpy().tolist(), p_shops.values.tolist())
        ori_map.append(o)
        
        # Small shops (no difference for now)
        o = location_map("2.21", p_shops.index.to_numpy().tolist(), p_shops.values.tolist())
        ori_map.append(o)
        
        
        # Merge with the trips df
        ori_map = pd.concat(ori_map)
        dest_map = ori_map.copy()
        dest_map.rename({"ori_loc_mot_id": "dest_loc_mot_id", "ori_codgeo": "dest_codgeo", "p_ori": "p_dest"}, axis=1, inplace=True)

        # ---------------------------------------
        # Add the probabilities to the trips df
        trips = pd.merge(trips, ori_map, on="ori_loc_mot_id", how="left")
        trips = pd.merge(trips, dest_map, on="dest_loc_mot_id", how="left")
        
        trips["p"] = trips["p_ori"]*trips["p_dest"]
        trips["p"].fillna(1.0, inplace=True)
        
        trips.set_index("trip_id", inplace=True)
        trips["p"] /= trips.groupby(["trip_id"])["p"].sum()

        # For each trip, sample one possibility among all origin - destinations
        if replicable:
            trips = trips.sample(frac = 1.0, weights="p", random_state=self.seed).groupby("trip_id").head(1)
        else:
            trips = trips.sample(frac = 1.0, weights="p").groupby("trip_id").head(1)
        
        # ---------------------------------------
        # Compute the distances if possible
        trips = pd.merge(trips, self.work_model.locations, left_on="ori_codgeo", right_on="location_id", right_index=True, how="left")
        trips = pd.merge(trips, self.work_model.locations, left_on="dest_codgeo", right_on="location_id", right_index=True, how="left")
        
        d = np.sqrt(np.power(trips["x_x"] - trips["x_y"], 2) + np.power(trips["y_x"] - trips["y_y"], 2))
        d = trips["r_x"]/3 + d + trips["r_y"]/3
        
        d = np.where(trips["ori_codgeo"] == trips["dest_codgeo"], trips["d_internal_x"], d)
        
        d = d*(1.1+0.3*np.exp(-d/20))
        d /= 1000.0
        
        # Replace the distance from the ENTD database with the precise distance computed
        trips["dist"] = np.where(np.isnan(d), trips["dist"], d)
        # trips["dist_source"] = np.where(np.isnan(d), "entd", "geo")
        
        # Drop useless columns
        trips.drop(["r_x", "r_y", "d_internal_x", "d_internal_y", "x_x", "y_x", "x_y", "y_y", "p_ori", "p_dest", "p"], axis=1, inplace=True)
        
        # ----------------------------------------
        # When origin and destination are known, replace the ENTD mode by probable modes
        # given the origin/destination, the distance class and the car ownership True/False flag
        # (based on INSEE work-home mobility data + ENTD detailed mode data given city category, car ownership and distance class)
        if n_cars != "0":
            has_cars = True
        else:
            has_cars = False

        # Subset the mode probability dataframe
        p_mode = self.p_mode.xs(has_cars, level=2)
        
        trips = pd.merge(
            trips.reset_index(),
            p_mode.reset_index(),
            on=["ori_codgeo", "dest_codgeo"],
            how="left"
        )


        trips["p_mode"] = np.where(trips["p_mode"].isna(), 1.0, trips["p_mode"])
        if replicable:
            trips = trips.sample(frac=1.0, weights="p_mode", random_state=self.seed).groupby("trip_id").head(1).copy()
        else:
            trips = trips.sample(frac=1.0, weights="p_mode").groupby("trip_id").head(1).copy()
        trips["mode_id"] = np.where(trips["mode_id_y"].isnull(), trips["mode_id_x"], trips["mode_id_y"])
        # trips["mode_source"] = np.where(trips["mode_id_y"].isnull(), "insee", "entd")

        # Drop useless columns
        trips.reset_index(inplace=True)
        #trips.drop(["ori_codgeo", "dest_codgeo", "p_mode", "mode_id_x", "mode_id_y"], axis=1, inplace=True)
        trips.drop(["p_mode", "mode_id_x", "mode_id_y"], axis=1, inplace=True)
        
        # ---------------------------------------------
        # Compute GHG emissions        
        trips = pd.merge(trips, self.mode_ef, on="mode_id")
        
        # Car emission factors are corrected to account for other passengers
        # (no need to do this for the other modes, their emission factors are already in kgCO2e/passenger.km)
        k_ef_car = 1/(1 + trips["n_trip_companions"])
        trips["k_ef"] = np.where(trips["mode_id"].str.slice(0, 1) == "3", k_ef_car, 1.0)
        
        trips["co2"] = trips["ef"]*trips["dist"]*trips["k_ef"]
        trips.drop(["k_ef", "ef"], axis=1, inplace=True)
        
        trips["person_id"] = person_id
        trips["n_cars"] = n_cars

        self.seed +=10

        return trips
    
