import os
import pathlib
import logging
import pandas as pd
import numpy as np

from mobility.asset import Asset

from mobility.choice_models.transport_mode_choice_model import TransportModeChoiceModel
from mobility.choice_models.work_destination_choice_model import WorkDestinationChoiceModel
from mobility.transport_modes import MultiModalMode


class LocalizedTrips(Asset):
    
    def __init__(
            self, trips: Asset, cost_of_time: float = 20.0,
            work_alpha: float = 0.2, work_beta: float = 0.8
        ):
        
        transport_zones = trips.inputs["population"].inputs["transport_zones"]
        
        travel_costs = MultiModalMode(transport_zones).travel_costs
        trans_mode_cm = TransportModeChoiceModel(travel_costs, cost_of_time)
        work_dest_cm = WorkDestinationChoiceModel(transport_zones, travel_costs, cost_of_time, work_alpha, work_beta)
        
        inputs = {
            "travel_costs": travel_costs,
            "trans_mode_cm": trans_mode_cm,
            "work_dest_cm": work_dest_cm,
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
        
        trips = self.inputs["trips"].get()
        population = self.inputs["trips"].inputs["population"].get()
        travel_costs = self.inputs["travel_costs"].get()
        trans_mode_cm = self.inputs["trans_mode_cm"].get()
        work_dest_cm = self.inputs["work_dest_cm"].get()
        
        trips = self.localize_trips(trips, population, travel_costs, trans_mode_cm, work_dest_cm)
        trips.to_parquet(self.cache_path)

        return trips
    
    
    def localize_trips(
            self, trips: pd.DataFrame, population: pd.DataFrame,
            travel_costs: pd.DataFrame,
            trans_mode_cm: pd.DataFrame, work_dest_cm: pd.DataFrame
        ):
        
        trips = self.sample_origins_destinations(trips, population, work_dest_cm)
        trips = self.sample_modes(trips, trans_mode_cm)
        trips = self.replace_distances(trips, travel_costs)

        return trips
    

    
    def sample_origins_destinations(self, trips, population, work_dest_cm):
        
        logging.info("Sampling origins and destinations by applying the choice models...")
        
        # Individual -> home mapping
        home = population[["individual_id", "transport_zone_id"]].copy()
        home["motive"] = "1.1"
        home["p"] = 1.0
        
        # Individual -> work mapping
        work = work_dest_cm.copy()
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
    
    
    def sample_modes(self, trips, trans_mode_cm):
        
        # Individual -> origin, destination, mode mapping
        logging.info("Replacing modes for localized trips...")
        
        modes = trans_mode_cm.reset_index()[["from", "to", "mode", "prob"]]
        modes.columns = ["from_transport_zone_id", "to_transport_zone_id", "mode", "p"]
        
        localized_trips = trips[(~trips["from_transport_zone_id"].isnull()) & (~trips["to_transport_zone_id"].isnull())].copy()
        localized_trips = localized_trips[["individual_id", "trip_id", "from_transport_zone_id", "to_transport_zone_id"]]
        
        localized_trips = localized_trips.melt(["individual_id", "trip_id"])
        localized_trips["value"] = localized_trips["value"].astype(int).astype(str)
        localized_trips = localized_trips.sort_values("value")
        
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
            