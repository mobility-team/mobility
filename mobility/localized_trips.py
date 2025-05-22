import os
import pathlib
import logging
import pandas as pd
import numpy as np
import random

from typing import List

from mobility.file_asset import FileAsset
from mobility.choice_models.destination_choice_model import DestinationChoiceModel
from mobility.choice_models.transport_mode_choice_model import TransportModeChoiceModel


class LocalizedTrips(FileAsset):
    """
    Localizes trips by assigning origin, destination, and transport mode.

    This class uses destination and mode choice models to assign plausible
    from/to transport zones and a mode of transport to each trip, based on
    individual-level attributes and OD probabilities.

    Parameters
    ----------
    dest_cm_list : list of DestinationChoiceModel
        List of destination choice models, each tied to specific motives.
    mode_cm_list : list of TransportModeChoiceModel
        List of transport mode choice models, aligned with the destination models.
    trips : FileAsset
        File containing the base trips to localize (must include individual_id, 
        trip_id, motive, previous_motive).

    Returns
    -------
    pd.DataFrame
        Localized trips with updated from/to zones, mode_id, and travel distance.
    """


    def __init__(
            self, 
            dest_cm_list: List[TransportModeChoiceModel], 
            mode_cm_list: List[DestinationChoiceModel], 
            trips: FileAsset):
        
        inputs = {
            "dest_cm_list": dest_cm_list,
            "mode_cm_list": mode_cm_list,
            "trips": trips
        }
        
        file_name = "trips_localized.parquet"
        cache_path = pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"]) / file_name
        
        super().__init__(inputs, cache_path)
        

    def get_cached_asset(self) -> pd.DataFrame:
        """
        Load cached localized trips.

        Returns
        -------
        pd.DataFrame
            Cached localized trips.
        """
        logging.info(f"Trips already localized. Reusing the file: {self.cache_path}")
        return pd.read_parquet(self.cache_path)

    def create_and_get_asset(self) -> pd.DataFrame:
        """
        Run the localization process (origin, destination, and mode),
        cache the result, and return the final trips.

        Returns
        -------
        pd.DataFrame
            Localized and cached trips.
        """
        logging.info("Localizing each trip...")
        trips = self.inputs["trips"].get()
        population = self.inputs["trips"].inputs["population"].get()
        dest_cm_list = self.inputs["dest_cm_list"]
        mode_cm_list = self.inputs["mode_cm_list"]

        trips = self.localize_trips(trips, population, dest_cm_list, mode_cm_list)
        trips.to_parquet(self.cache_path)
        return trips

    def localize_trips(self, 
                       trips: pd.DataFrame, 
                       population: pd.DataFrame,
                       dest_cm_list: List[DestinationChoiceModel], 
                       mode_cm_list: List[TransportModeChoiceModel]
                       ) -> pd.DataFrame:
        """
        Apply all localization steps:
        - Assign origin/destination zones
        - Sample a transport mode
        - Replace distances

        Parameters
        ----------
        trips : pd.DataFrame
            Input trips to localize.
        population : pd.DataFrame
            Population table linked to individuals.
        dest_cm_list : list of DestinationChoiceModel
            List of destination choice models.
        mode_cm_list : list of TransportModeChoiceModel
            List of transport mode choice models.

        Returns
        -------
        pd.DataFrame
            Fully localized trips.
        """
        trips = self.sample_origins_destinations(trips, population, dest_cm_list)
        trips = self.sample_modes(trips, mode_cm_list, dest_cm_list)
        trips = self.compute_new_distances(trips, dest_cm_list)
        
        return trips

    def sample_origins_destinations(
            self, 
            trips: pd.DataFrame, 
            population: pd.DataFrame,
            dest_cm_list: List[TransportModeChoiceModel]
            ) -> pd.DataFrame:
        """
        Assign origin and destination zones to each trip.

        Uses probabilistic destination choice based on:
        - home zone for the first trip
        - previous destination for chained trips

        Returns
        -------
        pd.DataFrame
            Trips with added 'from_transport_zone_id' and 'to_transport_zone_id'.
        """
        home = population[["individual_id", "transport_zone_id"]].copy()
        home["motive"] = "1.1"
        home["p"] = 1.0

        all_dest_from_home = []
        for dest_cm in dest_cm_list:
            motive_ids = dest_cm.inputs["parameters"].motive_ids
            dest_cm_df = dest_cm.get()
            dest_cm_df.columns = ["transport_zone_id", "to_transport_zone_id", "p"]

            dest_cm_df = pd.merge(population[["individual_id", "transport_zone_id"]], dest_cm_df, on="transport_zone_id")
            dest_cm_df = dest_cm_df[["individual_id", "to_transport_zone_id", "p"]]
            dest_cm_df.columns = ["individual_id", "transport_zone_id", "p"]

            for motive in motive_ids:
                df_copy = dest_cm_df.copy()
                df_copy["motive"] = motive
                all_dest_from_home.append(df_copy)

        all_dest_from_home = pd.concat(all_dest_from_home, ignore_index=True)
        all_dest_from_home = (
            all_dest_from_home.sample(frac=1.0, weights="p")
            .groupby(["individual_id", "motive"], as_index=False)
            .head(1)
        )
        all_dest_from_home["p"] = 1.0

        motive_mappings = pd.concat([home, all_dest_from_home])

        # Merge origin and destination zones
        trips = pd.merge(
            trips,
            motive_mappings.rename(columns={
                "motive": "previous_motive",
                "transport_zone_id": "from_transport_zone_id",
                "p": "p_from"
            }),
            on=["individual_id", "previous_motive"],
            how="left"
        )
        trips = pd.merge(
            trips,
            motive_mappings.rename(columns={
                "transport_zone_id": "to_transport_zone_id",
                "p": "p_to"
            }),
            on=["individual_id", "motive"], how="left"
        )

        trips["p"] = trips["p_from"] * trips["p_to"]
        trips["p"] = trips["p"].fillna(1.0)
        trips = trips.sample(frac=1.0, weights="p").groupby("trip_id", as_index=False).head(1)
        trips = trips.drop(["p_from", "p_to", "p"], axis=1)

        # Compute joint destination probabilities for chained trips
        home_zones = trips[trips["motive"] == "1.1"][["individual_id", "to_transport_zone_id"]].drop_duplicates()
        home_zones.columns = ["individual_id", "home_zone_id"]

        first_motive_ids = dest_cm_list[0].inputs["parameters"].motive_ids
        first_motive_zone = trips[trips["motive"].isin(first_motive_ids)][["individual_id", "to_transport_zone_id"]].drop_duplicates()
        first_motive_zone.columns = ["individual_id", "first_motive_zone_id"]

        next_motive_ids = [m for dcm in dest_cm_list[1:] for m in dcm.inputs["parameters"].motive_ids]
        trips_to_loc = trips[
            trips["motive"].isin(first_motive_ids + next_motive_ids) |
            trips["previous_motive"].isin(first_motive_ids + next_motive_ids)
        ].drop_duplicates()

        all_joint_dests = []
        for dc in dest_cm_list[1:]:
            motive_ids = dc.inputs["parameters"].motive_ids
            base_df = dc.get().rename(columns={"from": "from_transport_zone_id", "to": "to_transport_zone_id", "prob": "p"})

            df_home = pd.merge(home_zones, base_df, left_on="home_zone_id", right_on="from_transport_zone_id")
            df_home = df_home[["individual_id", "to_transport_zone_id", "p"]].rename(columns={"p": "p_home"})

            df_first = pd.merge(first_motive_zone, base_df, left_on="first_motive_zone_id", right_on="from_transport_zone_id")
            df_first = df_first[["individual_id", "to_transport_zone_id", "p"]].rename(columns={"p": "p_first"})

            df_joint = pd.merge(df_home, df_first, on=["individual_id", "to_transport_zone_id"])
            df_joint["p"] = df_joint["p_home"] * df_joint["p_first"]

            for motive in motive_ids:
                dest = df_joint[["individual_id", "to_transport_zone_id", "p"]].copy()
                dest = dest.rename(columns={"to_transport_zone_id": "transport_zone_id"})
                dest["motive"] = motive
                all_joint_dests.append(dest)

        joint_dest_df = pd.concat(all_joint_dests, ignore_index=True)
        joint_dest_df = (
            joint_dest_df.sample(frac=1.0, weights="p")
            .groupby(["individual_id", "motive"], as_index=False)
            .head(3)
            .drop(columns="p")
            .drop_duplicates()
        )

        choices_dict = joint_dest_df.groupby(["individual_id", "motive"])["transport_zone_id"].apply(list).to_dict()

        def draw_random_zone(row, choices_dict, fallback_column):
            key = (row["individual_id"], row["motive"])
            options = choices_dict.get(key)
            return random.choice(options) if options else row.get(fallback_column)

        trips_to_loc["to_transport_zone_id"] = trips_to_loc.apply(
            lambda row: draw_random_zone(row, choices_dict, fallback_column="to_transport_zone_id"), axis=1
        )
        trips_to_loc["from_transport_zone_id"] = trips_to_loc.apply(
            lambda row: draw_random_zone(row, choices_dict, fallback_column="from_transport_zone_id"), axis=1
        )

        updated_ids = trips_to_loc["trip_id"]
        trips = pd.concat([trips[~trips["trip_id"].isin(updated_ids)], trips_to_loc], ignore_index=True)

        return trips
    
    
    
    def sample_modes(
            self,
            trips: pd.DataFrame,
            mode_cm_list: List[TransportModeChoiceModel],
            dest_cm_list: List[TransportModeChoiceModel]
        ) -> pd.DataFrame:
        """
        Assign transport modes to trips based on origin-destination pairs and motive.
    
        Each transport mode model is applied only to the subset of trips associated
        with the motives it is calibrated for.
    
        Parameters
        ----------
        trips : pd.DataFrame
            DataFrame containing the trips to be updated with transport modes.
        mode_cm_list : list of TransportModeChoiceModel
            List of transport mode choice models.
        dest_cm_list : list of DestinationChoiceModel
            List of destination choice models (used to retrieve relevant motives).
    
        Returns
        -------
        pd.DataFrame
            Trips with an additional column 'mode_id' representing the selected mode.
        """
        logging.info("Assigning transport modes for localized trips...")
    
        all_mode_choices = []
    
        for mode_cm, dest_cm in zip(mode_cm_list, dest_cm_list):
            # Get motives associated with this model
            motive_ids = dest_cm.inputs["parameters"].motive_ids
    
            # Filter relevant trips
            target_trips = trips[trips["motive"].isin(motive_ids)].copy()
            if target_trips.empty:
                continue
    
            # Load OD probabilities per mode
            mode_probs = mode_cm.get()
            mode_probs = mode_probs.rename(columns={
                "from": "from_transport_zone_id",
                "to": "to_transport_zone_id",
                "prob": "p"
            })
            mode_probs = mode_probs[["from_transport_zone_id", "to_transport_zone_id", "mode", "p"]]
    
            # Join trips with mode probabilities
            merged = pd.merge(
                target_trips,
                mode_probs,
                on=["from_transport_zone_id", "to_transport_zone_id"],
                how="left"
            )
    
            # Weighted sampling based on OD-mode probabilities
            sampled = (
                merged
                .dropna(subset=["p"])  # Skip OD pairs with no available mode
                .sample(frac=1.0, weights="p", random_state=42)
                .groupby("trip_id", as_index=False)
                .head(1)
            )
            sampled = sampled[["trip_id", "mode"]].rename(columns={"mode": "mode_id"})
            all_mode_choices.append(sampled)
    
        if not all_mode_choices:
            logging.warning("No transport modes assigned: no valid matches found.")
            trips["mode_id"] = None
            return trips
    
        mode_assignment = pd.concat(all_mode_choices, ignore_index=True)
    
        # Merge assigned modes into the trip DataFrame
        trips = pd.merge(trips, mode_assignment, on="trip_id", how="left", suffixes=("", "_new"))
        # Note: logic to keep original mode_id if fallback desired can be added here
        return trips

            
        
    def compute_new_distances(
            self,
            trips: pd.DataFrame,
            dest_cm_list: List[DestinationChoiceModel]
        ) -> pd.DataFrame:
        """
        Replace or add travel distances to trips using OD-specific distance matrices
        by transport mode and motive.
    
        Parameters
        ----------
        trips : pd.DataFrame
            Trips DataFrame with at least from/to zones, mode, and motive.
        dest_cm_list : list of DestinationChoiceModel
            Destination models containing access to OD travel costs (distance).
    
        Returns
        -------
        pd.DataFrame
            Trips with the column 'distance' updated where new values are available.
        """
        logging.info("Replacing distances for localized trips...")
    
        all_distance_data = []
    
        for dest_cm in dest_cm_list:
            motive_ids = dest_cm.inputs["parameters"].motive_ids
    
            # Extract OD travel distances
            travel_costs = dest_cm.inputs["costs"]
            distance_df = travel_costs.get(metrics=["distance"], aggregate_by_od=False).to_pandas()
            distance_df = distance_df.reset_index()[["from", "to", "mode", "distance"]]
            distance_df.columns = ["from_transport_zone_id", "to_transport_zone_id", "mode_id_new", "distance"]
    
            # Assign motives for later filtering
            for motive in motive_ids:
                temp = distance_df.copy()
                temp["motive"] = motive
                all_distance_data.append(temp)
    
        if not all_distance_data:
            logging.warning("No distance data found — skipping distance replacement.")
            return trips
    
        distances_all = pd.concat(all_distance_data, ignore_index=True)
    
        # Merge with trips and attach new distances
        trips = pd.merge(
            trips,
            distances_all,
            on=["from_transport_zone_id", "to_transport_zone_id", "mode_id_new", "motive"],
            how="left",
            suffixes=("", "_new")
        )
    
        # Uncomment if you'd like to overwrite with new values where available
        # trips["distance"] = trips["distance_new"].combine_first(trips.get("distance"))
        # trips = trips.drop(columns=["distance_new"], errors="ignore")
    
        return trips
