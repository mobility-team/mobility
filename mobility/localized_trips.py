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
            trips: FileAsset,
            keep_survey_cols: bool = False
        ):
        
        inputs = {
            "dest_cm_list": dest_cm_list,
            "mode_cm_list": mode_cm_list,
            "trips": trips,
            "keep_survey_cols": keep_survey_cols
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
        keep_survey_cols = self.inputs["keep_survey_cols"]

        trips = self.localize_trips(trips, population, dest_cm_list, mode_cm_list, keep_survey_cols)
        trips.to_parquet(self.cache_path)
        return trips

    def localize_trips(
            self, 
            trips: pd.DataFrame, 
            population: pd.DataFrame,
            dest_cm_list: List[DestinationChoiceModel], 
            mode_cm_list: List[TransportModeChoiceModel],
            keep_survey_cols: bool
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
        trips = self.sample_modes(trips, mode_cm_list, dest_cm_list, keep_survey_cols)
        trips = self.compute_new_distances(trips, dest_cm_list, keep_survey_cols)
        
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
        logging.info("Assigning origins and destinations to transport zones...")
        
        home = population[["individual_id", "transport_zone_id"]].copy()
        home["model_id"] = "home"
        home["prob"] = 1.0
        
        # Assign motive ids to models and assemble a dataframe with destination 
        # probabilities for all models
        motive_id_to_model_id = {"1.1": "home"}
        
        for dest_cm in dest_cm_list:
            for motive_id in dest_cm.inputs["parameters"].motive_ids:
                motive_id_to_model_id[motive_id] = dest_cm.inputs_hash
        
        dest_probs = [
            ( 
                dest_cm
                .get()
                .assign(model_id=dest_cm.inputs_hash)
                .assign(n_possible_destinations=dest_cm.n_possible_destinations)
                .rename({"from": "from_transport_zone_id", "to": "to_transport_zone_id"}, axis=1)
            )
            for dest_cm in dest_cm_list
        ]
        
        dest_probs = pd.concat(dest_probs)
        
        # Choose n destinations for each model as if every origin was the home
        potential_dests = pd.merge(
            population[["individual_id", "transport_zone_id"]].rename({"transport_zone_id": "from_transport_zone_id"}, axis=1),
            dest_probs,
            on="from_transport_zone_id"
        )
        
        potential_dests = potential_dests.sample(frac=1.0, weights="prob")
        mask = potential_dests.groupby(["individual_id", "model_id"]).cumcount() < potential_dests["n_possible_destinations"]
        potential_dests = potential_dests[mask]
        
        potential_dests = pd.concat([
            potential_dests[["individual_id", "model_id", "to_transport_zone_id", "prob"]].rename({"to_transport_zone_id": "transport_zone_id"}, axis=1),
            home
        ])
        
        # Map motives to models in the trips dataframe
        loc_trips = trips.copy()
        loc_trips["from_model_id"] = loc_trips["previous_motive"].map(motive_id_to_model_id)
        loc_trips["to_model_id"] = loc_trips["motive"].map(motive_id_to_model_id)
        loc_trips = loc_trips[(~loc_trips["from_model_id"].isnull()) & (~loc_trips["to_model_id"].isnull())]
        
        # Sample origins
        loc_trips = pd.merge(
            loc_trips,
            potential_dests.rename({"model_id": "from_model_id", "transport_zone_id": "from_transport_zone_id"}, axis=1),
            on=["individual_id", "from_model_id"]
        )
        
        loc_trips = loc_trips.sample(frac=1.0, weights="prob", ignore_index=True).drop_duplicates("trip_id", keep="first")
        loc_trips.drop("prob", axis=1, inplace=True)
        
        # Sample destinations
        loc_trips = pd.merge(
            loc_trips,
            potential_dests.rename({"model_id": "to_model_id", "transport_zone_id": "to_transport_zone_id"}, axis=1),
            on=["individual_id", "to_model_id"]
        )
        
        loc_trips = loc_trips.sample(frac=1.0, weights="prob", ignore_index=True).drop_duplicates("trip_id", keep="first")

        # Assign the trip origins and destinations in the trips dataframe
        trips = pd.merge(
            trips,
            loc_trips[["trip_id", "from_transport_zone_id", "to_transport_zone_id"]],
            on="trip_id",
            how="left"
        )

        return trips
    
    
    
    def sample_modes(
            self,
            trips: pd.DataFrame,
            mode_cm_list: List[TransportModeChoiceModel],
            dest_cm_list: List[TransportModeChoiceModel],
            keep_survey_cols: bool
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
        
        # Assign motive ids to models and assemble a dataframe with mode 
        # probabilities for all models
        motive_id_to_model_id = {"1.1": mode_cm_list[0].inputs_hash}
        
        for mode_cm, dest_cm in zip(mode_cm_list, dest_cm_list):
            for motive_id in dest_cm.inputs["parameters"].motive_ids:
                motive_id_to_model_id[motive_id] = mode_cm.inputs_hash
        
        mode_probs = [
            ( 
                mode_cm
                .get()
                .assign(model_id=mode_cm.inputs_hash)
                .rename({"from": "from_transport_zone_id", "to": "to_transport_zone_id"}, axis=1)
            )
            for mode_cm in mode_cm_list
        ]
        
        mode_probs = pd.concat(mode_probs)
        
        # Find the unique OD pairs that each individual travels
        ods = ( 
            trips
            .groupby(["individual_id", "motive", "from_transport_zone_id", "to_transport_zone_id"], as_index=False)
            .head(1)
            [["individual_id", "motive", "from_transport_zone_id", "to_transport_zone_id"]]
            .dropna()
        )
        
        # Map them to the available models and mode probabilities
        ods["model_id"] = ods["motive"].map(motive_id_to_model_id)
        
        # !!! BUG :
        # A small number of ODs (like 5 out of 100 000) have no mode_probs,
        # which makes the next sampling step crash.
        # This should not happen because ODs are sampled based on destination 
        # probabilities, which are based on mode costs (so there should be at
        # least one mode available).
        ods = pd.merge(
            ods,
            mode_probs,
            on=["model_id", "from_transport_zone_id", "to_transport_zone_id"]
        )
        
        # Sample one mode for each OD pair
        ods = (
            ods
            .sample(frac=1.0, weights="prob", random_state=42)
            .groupby(
                [
                    "individual_id", "motive",
                    "from_transport_zone_id", "to_transport_zone_id"
                ],
                as_index=False
            )
            .first()
        )
        
        # Assign the modes to each of the OD pairs that could be sampled,
        # and use the survey modes otherwise
        trips = pd.merge(
            trips,
            ods,
            on=[
                "individual_id", "motive",
                "from_transport_zone_id", "to_transport_zone_id"
            ],
            how="left"
        )
        
        trips["mode"] = trips["mode_id"].where(
            trips["mode"].isnull(),
            trips["mode"]
        )
        
        trips.drop(["model_id", "prob"], axis=1, inplace=True)
        
        # If the user wants to keep the original mode (the one from the survey),
        # create a new survey_mode_id column, otherwise replace the survey mode
        # with the modelled/survey modes
        if keep_survey_cols is True:
            trips["survey_mode_id"] = trips["mode_id"]
            trips.drop("mode_id", axis=1, inplace=True)  
            trips.rename({"mode": "mode_id"}, axis=1, inplace=True)
        else:
            trips.drop("mode_id", axis=1, inplace=True)   
            trips.rename({"mode": "mode_id"}, axis=1, inplace=True)

        return trips

            
        
    def compute_new_distances(
            self,
            trips: pd.DataFrame,
            dest_cm_list: List[DestinationChoiceModel],
            keep_survey_cols: bool
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

        
        mode_dists = ( 
            dest_cm_list[0].inputs["costs"]
            .get(
                metrics=["distance"],
                congestion=True,
                aggregate_by_od=False,
                detail_distances=True
            )
            .to_pandas()
            .rename(
                {
                    "from": "from_transport_zone_id",
                    "to": "to_transport_zone_id",
                    "mode": "mode_id"
                },
                axis=1
            )
        )
        
        
        trips = pd.merge(
            trips,
            mode_dists,
            on=["from_transport_zone_id", "to_transport_zone_id", "mode_id"],
            how="left",
            suffixes=["_survey", ""]
        )
        
        trips["distance"] = trips["distance_survey"].where(
            trips["distance"].isnull(),
            trips["distance"]
        )
        
        if keep_survey_cols is False:
            trips.drop("distance_survey", axis=1, inplace=True)    
    
        return trips
