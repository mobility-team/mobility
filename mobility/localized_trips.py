import os
import pathlib
import logging
import pandas as pd
import numpy as np

from mobility.asset import Asset

from mobility.travel_costs import TravelCosts
from mobility.public_transport_travel_costs import PublicTransportTravelCosts
from mobility import radiation_model
from mobility.get_insee_data import get_insee_data

class LocalizedTrips(Asset):
    
    def __init__(self, trips: Asset):
        
        transport_zones = trips.inputs["population"].inputs["transport_zones"]
        
        car_travel_costs = TravelCosts(transport_zones, "car")
        walk_travel_costs = TravelCosts(transport_zones, "walk")
        bicycle_travel_costs = TravelCosts(transport_zones, "bicycle")
        pub_trans_travel_costs = PublicTransportTravelCosts(transport_zones)
        
        inputs = {
            "car_travel_costs": car_travel_costs,
            "walk_travel_costs": walk_travel_costs,
            "bicycle_travel_costs": bicycle_travel_costs,
            "pub_trans_travel_costs": pub_trans_travel_costs,
            "trips": trips
        }

        file_name = "trips_localized.parquet"
        cache_path = pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"]) / file_name

        super().__init__(inputs, cache_path)
        
        
    def get_cached_asset(self) -> pd.DataFrame:

        logging.info("Trips already localized. Reusing the file : " + str(self.cache_path))
        trips = pd.read_parquet(self.cache_path)

        return trips
    
    def create_and_get_asset(self) -> pd.DataFrame:
        
        logging.info("Localizing each trip...")
        
        trips_asset = self.inputs["trips"]
        population_asset = trips_asset.inputs["population"]
        
        transport_zones = population_asset.inputs["transport_zones"].get()
        population = population_asset.get()
        trips = trips_asset.get()
        
        car_travel_costs = self.inputs["car_travel_costs"].get()
        walk_travel_costs = self.inputs["car_travel_costs"].get()
        bicycle_travel_costs = self.inputs["bicycle_travel_costs"].get()
        pub_trans_travel_costs = self.inputs["pub_trans_travel_costs"].get()
        
        trips = self.localize_trips(
            transport_zones, population, car_travel_costs, walk_travel_costs,
            bicycle_travel_costs, pub_trans_travel_costs, trips
        )
        
        trips.to_parquet(self.cache_path)

        return trips
    
    
    def localize_trips(
            self, transport_zones: pd.DataFrame, population: pd.DataFrame,
            car: pd.DataFrame, walk: pd.DataFrame, 
            bicycle: pd.DataFrame, pub_trans: pd.DataFrame,
            trips: pd.DataFrame
        ):
        
        costs = self.prepare_costs(car, walk, bicycle, pub_trans)
        trips = self.sample_origins_destinations(trips, transport_zones, population, costs)
        trips = self.sample_modes(trips, costs)
        trips = self.replace_distances(trips, costs)

        return trips
    
    
    def prepare_costs(
            self, car: pd.DataFrame, walk: pd.DataFrame, 
            bicycle: pd.DataFrame, pub_trans: pd.DataFrame
        ):
        
        logging.info("Aggregating travel costs between transport zones...")
        
        car["mode"] = "car"
        walk["mode"] = "walk"
        bicycle["mode"] = "bicycle"
        
        # Fix public transport times to only have one row per OD pair
        # (should be fixed in PublicTransportTravelCosts !)
        pub_trans = pub_trans.sort_values(["from", "to", "time"])
        pub_trans = pub_trans.groupby(["from", "to"], as_index=False).first()
        
        costs = pd.concat([
            car,
            walk,
            bicycle,
            pub_trans
        ])
        
        # Remove null costs that might occur
        # (bug that should be fixed in TravelCosts !)
        costs = costs[(~costs["time"].isnull()) & (~costs["distance"].isnull())]
        
        costs["from"] = costs["from"].astype(int)
        costs["to"] = costs["to"].astype(int)
        
        costs.set_index(["from", "to", "mode"], inplace=True)
        
        # Basic utility function : U = ct*time
        # Cost of time (ct) : 10 €/h
        costs["utility"] = -20*costs["time"]
        
        costs["prob"] = np.exp(costs["utility"])
        costs["prob"] = costs["prob"]/costs.groupby(["from", "to"])["prob"].sum()
        
        costs["average_utility"] = costs["prob"]*costs["utility"]
        
        return costs
    

    
    
    def sample_origins_destinations(self, trips, transport_zones, population, costs):
        
        work_prob = self.prepare_work_destination_choice_model(transport_zones, costs)
        
        logging.info("Sampling origins and destinations by applying the choice models...")
        
        # Individual -> home mapping
        home = population[["individual_id", "transport_zone_id"]].copy()
        home["motive"] = "1.1"
        home["p"] = 1.0
        
        # Individual -> work mapping
        work = work_prob.reset_index()
        work.columns = ["transport_zone_id", "to_transport_zone_id", "p"]
        work = pd.merge(population[["individual_id", "transport_zone_id"]], work, on=["transport_zone_id"])
        work = work[["individual_id", "to_transport_zone_id", "p"]]
        work.columns = ["individual_id", "transport_zone_id", "p"]
        work["motive"] = "9.91"
        work = work.sample(frac=1.0, weights="p").groupby("individual_id", as_index=False).head(1)
        work["p"] = 1.0
        
        # Concat all mappings
        motive_mappings = pd.concat([home, work])
        
        # Localize trips origins and destinations
        trips = pd.merge(
            trips,
            motive_mappings.rename({
                "motive": "previous_motive",
                "transport_zone_id": "from_transport_zone_id",
                "p": "p_from"
            }, axis=1),
            on=["individual_id", "previous_motive"],
            how="left"
        )
        
        trips = pd.merge(
            trips,
            motive_mappings.rename({
                "transport_zone_id": "to_transport_zone_id",
                "p": "p_to"
            }, axis=1),
            on=["individual_id", "motive"],
            how="left"
        )
        
        trips["p"] = trips["p_from"]*trips["p_to"]
        trips["p"].fillna(1.0, inplace=True)
        
        trips = trips.sample(frac=1.0, weights="p").groupby("trip_id", as_index=False).head(1)
        
        trips = trips.drop(["p_from", "p_to", "p"], axis=1)
        
        return trips
    
    
    def prepare_work_destination_choice_model(self, transport_zones, costs):
        
        logging.info("Preparing the work destination choice model...")
    
        insee_data = get_insee_data()
        active_population = insee_data["active_population"]
        jobs = insee_data["jobs"]
        
        active_population = active_population.loc[transport_zones["admin_id"]].sum(axis=1).reset_index()
        jobs = jobs.loc[transport_zones["admin_id"]].sum(axis=1).reset_index()
        
        active_population = pd.merge(active_population, transport_zones[["admin_id", "transport_zone_id"]], left_on="CODGEO", right_on="admin_id")
        jobs = pd.merge(jobs, transport_zones[["admin_id", "transport_zone_id"]], left_on="CODGEO", right_on="admin_id")
        
        active_population = active_population[["transport_zone_id", 0]]
        active_population.columns = ["transport_zone_id", "source_volume"]
        active_population.set_index("transport_zone_id", inplace=True)
        
        jobs = jobs[["transport_zone_id", 0]]
        jobs.columns = ["transport_zone_id", "sink_volume"]
        jobs.set_index("transport_zone_id", inplace=True)
        
        average_utilities = costs.groupby(["from", "to"])["average_utility"].sum()
        average_utilities = average_utilities.reset_index()
        average_utilities.columns = ["from", "to", "cost"]
        
        flows, _, _ = radiation_model.iter_radiation_model(
            sources=active_population,
            sinks=jobs,
            costs=average_utilities,
            alpha=0.0,
            beta=1.0
        )
        
        work_prob = flows/flows.groupby("from").sum()
        
        return work_prob
    
    
    def sample_modes(self, trips, costs):
        
        # Individual -> origin, destination, mode mapping
        logging.info("Replacing modes for localized trips...")
        
        modes = costs.reset_index()[["from", "to", "mode", "prob"]]
        modes.columns = ["from_transport_zone_id", "to_transport_zone_id", "mode", "p"]
        
        
        localized_trips = trips[(~trips["from_transport_zone_id"].isnull()) & (~trips["to_transport_zone_id"].isnull())].copy()
        localized_trips = localized_trips[["individual_id", "trip_id", "from_transport_zone_id", "to_transport_zone_id"]]
        
        localized_trips = localized_trips.melt(["individual_id", "trip_id"])
        localized_trips["value"] = localized_trips["value"].astype(int).astype(str)
        localized_trips = localized_trips.sort_values("value")
        
        # localized_trips = localized_trips.set_index(["individual_id", "trip_id"])
        
        trip_routes = localized_trips.groupby(["individual_id", "trip_id"], as_index=False)["value"].apply("-".join)
        
        unique_routes = trip_routes.groupby(["individual_id", "value"], as_index=False).first()
        unique_routes[["from_transport_zone_id", "to_transport_zone_id"]] = unique_routes["value"].str.split("-", expand=True)
        unique_routes["from_transport_zone_id"] = unique_routes["from_transport_zone_id"].astype(int)
        unique_routes["to_transport_zone_id"] = unique_routes["to_transport_zone_id"].astype(int)

        unique_routes = pd.merge(unique_routes, modes, on=["from_transport_zone_id", "to_transport_zone_id"])
        
        unique_routes = unique_routes.sample(frac=1.0, weights="p").groupby(["individual_id", "value"], as_index=False).head(1)
        
        trip_routes = pd.merge(trip_routes, unique_routes[["individual_id", "value", "mode"]], on=["individual_id", "value"])
        trip_routes = trip_routes[["trip_id", "mode"]]
        trip_routes = trip_routes.rename({"mode": "mode_id"}, axis=1)
        
        trips = pd.merge(trips, trip_routes, on="trip_id", how="left")
        trips["mode_id"] = np.where(trips["mode_id_y"].isnull(), trips["mode_id_x"], trips["mode_id_y"])
        
        return trips
    
    
    def replace_distances(self, trips, costs):
        
        logging.info("Replacing distances for localized trips...")
        
        distances = costs.reset_index()[["from", "to", "mode", "distance"]]
        distances.columns = ["from_transport_zone_id", "to_transport_zone_id", "mode_id", "distance"]
        
        trips = pd.merge(trips, distances, on=["from_transport_zone_id", "to_transport_zone_id", "mode_id"], how="left")
        trips["distance"] = np.where(trips["distance_y"].isnull(), trips["distance_x"], trips["distance_y"])

        trips = trips.drop(["distance_x", "distance_y", "mode_id_x", "mode_id_y"], axis=1)
        
        return trips
            