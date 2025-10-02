import os
import pathlib
import logging
import shutil
import random

import geopandas as gpd
import matplotlib.pyplot as plt
import polars as pl

from typing import List

from mobility.file_asset import FileAsset
from mobility.population import Population
from mobility.choice_models.travel_costs_aggregator import TravelCostsAggregator
from mobility.choice_models.population_trips_parameters import PopulationTripsParameters
from mobility.choice_models.destination_sequence_sampler import DestinationSequenceSampler
from mobility.choice_models.top_k_mode_sequence_search import TopKModeSequenceSearch
from mobility.choice_models.state_initializer import StateInitializer
from mobility.choice_models.state_updater import StateUpdater
from mobility.motives import Motive
from mobility.transport_modes.transport_mode import TransportMode
from mobility.parsers.mobility_survey import MobilitySurvey

class PopulationTrips(FileAsset):
    """
        Distributes the population between possible daily schedules.
        
        A daily schedule is a sequence of activities (being home, working, 
        studying...) occuring in distinct transport zones, with a trip to 
        go from one activity to the other, using available modes (walk, car,
        bicycle...). A special "stay home all day" is also modelled.

        The model generates alternative schedules for each population group, 
        based on schedules extracted from mobility surveys. The process 
        spatializes these schedules (attributes a transport zone to each 
        activity), then finds valid mode sequences (accounting for available
        modes and vehicle availability constraints). 

        Each activity has a utility that depends on its duration and 
        opportunities occupancy, and each trip has a negative utility (a 
        cost) that depends on its duration and distance. The utility of a 
        given schedule is the sum of these activities and trips utilities.

        People choose schedules based on their utilities through a discrete
        choice model.
        
    """
    
    def __init__(
            self,
            population: Population,
            modes: List[TransportMode] = None,
            motives: List[Motive] = None,
            surveys: List[MobilitySurvey] = None,
            parameters: PopulationTripsParameters = None
        ):
        
        if modes is None:
            raise ValueError("PopulationTrips needs at least one mode in the modes list argument.")
            
        if motives is None:
            raise ValueError("PopulationTrips needs at least one motive in the motives list argument.")
            
        if surveys is None:
            raise ValueError("PopulationTrips needs at least one survey in the surveys list argument.")
            
        if parameters is None:
            parameters = PopulationTripsParameters()
        parameters.validate()
            
        if parameters.seed is None:
            self.rng = random.Random()
        else:
            self.rng = random.Random(parameters.seed)
        
        self.destination_sequence_sampler = DestinationSequenceSampler()
        self.top_k_mode_sequence_search = TopKModeSequenceSearch()
        self.state_initializer = StateInitializer()
        self.state_updater = StateUpdater()

        costs_aggregator = TravelCostsAggregator(modes)
        
        inputs = {
            "population": population,
            "costs_aggregator": costs_aggregator,
            "motives": motives,
            "surveys": surveys,
            "parameters": parameters
        }
        
        project_folder = pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"])
        cache_path = {
            "weekday_flows": project_folder / "population_trips" / "weekday" / "weekday_flows.parquet",
            "weekend_flows": project_folder / "population_trips" / "weekend" / "weekend_flows.parquet"
        }
        
        super().__init__(inputs, cache_path)
        
        
    def get_cached_asset(self):
        return {k: pl.scan_parquet(v) for k, v in self.cache_path.items()}
        
        
    def create_and_get_asset(self):
        
        weekday_flows = self.compute_flows(is_weekday=True)
        weekend_flows = self.compute_flows(is_weekday=False)
        
        weekday_flows.write_parquet(self.cache_path["weekday_flows"])
        weekend_flows.write_parquet(self.cache_path["weekend_flows"])
            
        return {k: pl.scan_parquet(v) for k, v in self.cache_path.items()}

    def compute_flows(self, is_weekday):
        """Run the iterative assignment for weekday/weekend and return flows.
        
        Args:
          is_weekday (bool): Whether to compute weekday flows.
        
        Returns:
          pl.DataFrame: Final step-level flows with utilities, durations, and flags.
        """

        population = self.inputs["population"]
        costs_aggregator = self.inputs["costs_aggregator"]
        motives = self.inputs["motives"]
        surveys = self.inputs["surveys"]
        parameters = self.inputs["parameters"]
        
        cache_path = self.cache_path["weekday_flows"] if is_weekday is True else self.cache_path["weekend_flows"]
        tmp_folders = self.prepare_tmp_folders(cache_path)

        chains, demand_groups = self.state_initializer.get_chains(
            population,
            surveys,
            motives,
            is_weekday
        )
        
        motive_dur, home_night_dur = self.state_initializer.get_mean_activity_durations(
            chains,
            demand_groups
        )
        
        stay_home_state, current_states = self.state_initializer.get_stay_home_state(
            demand_groups,
            home_night_dur,
            parameters
        )
        
        sinks = self.state_initializer.get_sinks(
            chains,
            motives,
            population.transport_zones
        )
        
        costs = self.state_initializer.get_current_costs(
            costs_aggregator,
            congestion=False
        )
        
        remaining_sinks = sinks.clone()
        
        for iteration in range(1, parameters.n_iterations+1):
            
            logging.info(f"Iteration n°{iteration}")
            
            seed = self.rng.getrandbits(64)
              
            ( 
                self.destination_sequence_sampler.run(
                    motives,
                    population.transport_zones,
                    remaining_sinks,
                    iteration,
                    chains,
                    demand_groups,
                    costs,
                    tmp_folders,
                    parameters,
                    seed
                )
                .write_parquet(tmp_folders["spatialized-chains"] / f"spatialized_chains_{iteration}.parquet")
            )
            
            
            (
                self.top_k_mode_sequence_search.run(
                    iteration,
                    costs_aggregator,
                    tmp_folders,
                    parameters
                )
                .write_parquet(tmp_folders["modes"] / f"mode_sequences_{iteration}.parquet")
            )
            
            current_states, current_states_steps = self.state_updater.get_new_states(
                current_states,
                demand_groups,
                chains,
                costs_aggregator,
                remaining_sinks,
                motive_dur,
                iteration,
                tmp_folders,
                home_night_dur,
                stay_home_state,
                parameters
            )
            
            costs = self.state_updater.get_new_costs(
                costs,
                iteration,
                parameters.n_iter_per_cost_update,
                current_states_steps,
                costs_aggregator
            )
            
            remaining_sinks = self.state_updater.get_new_sinks(
                current_states_steps,
                sinks
            )
            
    
        current_states_steps = (
            current_states_steps
            .join(
                demand_groups.select(["demand_group_id", "home_zone_id", "csp", "n_cars"]),
                on=["demand_group_id"]
            )
            .drop("demand_group_id")
        )

        current_states_steps = current_states_steps.with_columns(
            is_weekday=pl.lit(is_weekday)
        )

        return current_states_steps
    

    def prepare_tmp_folders(self, cache_path):
        """Create per-run temp folders next to the cache path.
        
        Args:
          cache_path (pathlib.Path): Target cache file used to derive temp roots.
        
        Returns:
          dict[str, pathlib.Path]: Mapping of temp folder names to paths.
        """
        
        inputs_hash = str(cache_path.stem).split("-")[0]
        
        def rm_then_mkdirs(folder_name):
            path = cache_path.parent / (inputs_hash + "-" + folder_name)
            shutil.rmtree(path, ignore_errors=True)
            os.makedirs(path)
            return path
        
        folders = ["spatialized-chains", "modes", "flows", "sequences-index"]
        folders = {f: rm_then_mkdirs(f) for f in folders}
        
        return folders

        

    def plot_modal_share(self, zone="origin", mode="car", period="weekdays",
                         labels=None, labels_size=[10, 6, 4], labels_color="black"):
        """
        Plot modal share for the given mode in the origin or destination zones during weekdays or weekends.

        Parameters
        ----------
        zone : string, optional
            "origin" or "destination" zones. The default is "origin".
        mode : string, optional
            Mode for which you want to see the share. Could be one of the modes previously defined (such as "bicycle")
            or "public_transport" (will show all the journeys at least partly made with public transport. The default is "car".
        period : string, optional
            "weekdays" or ""weekends". The default is "weekdays".

        Returns
        -------
        mode_share : pd.DataFrame
            Mode share for the given mode in each transport zone.

        """
        logging.info(f"🗺️ Plotting {mode} modal share for {zone} zones during {period}")

        if period == "weekdays":
            population_df = self.get()["weekday_flows"].collect().to_pandas()
        elif period == "weekends":
            population_df = self.get()["weekend_flows"].collect().to_pandas()
        else:
            logging.info(f"{period} not implemented yet!")
            return NotImplemented

        if zone == "origin":
            left_column = "from"
        elif zone == "destination":
            left_column = "to"

        mode_share = population_df.groupby([left_column, "mode"]).sum("n_persons")
        mode_share = mode_share.reset_index().set_index([left_column])
        mode_share["total"] = mode_share.groupby([left_column])["n_persons"].sum()
        mode_share["modal_share"] = mode_share["n_persons"] / mode_share["total"]

        if mode == "public_transport":
            mode_name = "Public transport"
            mode_share["mode"] = mode_share["mode"].replace("\S+\/public_transport\/\S+", "public_transport", regex=True)
        else:
            mode_name = mode.capitalize()
        mode_share = mode_share[mode_share["mode"] == mode]

        transport_zones_df = self.population.transport_zones.get()
        gc = gpd.GeoDataFrame(
            mode_share.merge(transport_zones_df, left_on=left_column, right_on="transport_zone_id", suffixes=('', '_z')))
        gcp = gc.plot("modal_share", legend=True)
        gcp.set_axis_off()
        plt.title(f"{mode_name} share per {zone} transport zone ({period})")

        if isinstance(labels, gpd.GeoDataFrame):
            self.__show_labels__(labels, labels_size, labels_color)        
        
        plt.show()

        return mode_share

    def plot_od_flows(self, mode="all", motive="all", period="weekdays", level_of_detail=0,
                      n_largest=2000, color="blue", transparency=0.2, zones_color="xkcd:light grey",
                      labels=None, labels_size=[10, 6, 4], labels_color="black"):
        """
        Plot flows between the different zones for the given mode, motive, period and level of detail.

        Number of OD shows, colors and transparency are configurable.

        Parameters
        ----------
            mode : TYPE, optional
                DESCRIPTION. The default is "all".
            motive : TYPE, optional
                DESCRIPTION. The default is "all".
            period : TYPE, optional
                DESCRIPTION. The default is "weekdays".
            level_of_detail : TYPE, optional
                DESCRIPTION. The default is 0.
            n_largest : TYPE, optional
                DESCRIPTION. The default is 2000.
            color : TYPE, optional
                DESCRIPTION. The default is "blue".
            transparency : TYPE, optional
                DESCRIPTION. The default is 0.2.
            zones_color : TYPE, optional
            DESCRIPTION. The default is "gray".

        Returns
        -------
        biggest_flows : pd.DataFrame
            Biggest flows between different transport zones.

        """
        if level_of_detail == 0:
            logging.info("OD between communes not implemented yet")
            return NotImplemented
        elif level_of_detail != 1:
            logging.info("Level of detail should be 0 or 1")
            return NotImplemented
        else:
            logging.info(f"🗺️ Plotting {mode} origin-destination flows during {period}")

        if motive != "all":
            logging.info("Speficic motives not implemented yet")
            return NotImplemented

        if period == "weekdays":
            population_df = self.get()["weekday_flows"].collect().to_pandas()
        elif period == "weekends":
            population_df = self.get()["weekend_flows"].collect().to_pandas()

        mode_name = mode.capitalize()

        if mode != "all":
            if mode == "public_transport":
                mode_name = "Public transport"
                population_df = population_df[population_df["mode"].str.contains("public_transport")]
            else:
                population_df = population_df[population_df["mode"] == mode]

        # Find all biggest origin-destination between different transport zones
        biggest_flows = population_df.groupby(["from", "to"]).sum("n_persons").reset_index()
        biggest_flows = biggest_flows.where(biggest_flows["from"] != biggest_flows["to"]).nlargest(n_largest, "n_persons")
        transport_zones_df = self.population.transport_zones.get()
        biggest_flows = biggest_flows.merge(
            transport_zones_df, left_on="from", right_on="transport_zone_id", suffixes=('', '_from'))
        biggest_flows = biggest_flows.merge(
            transport_zones_df, left_on="to", right_on="transport_zone_id", suffixes=('', '_to'))

        # Add all the transport zones in gray, as background
        gc = gpd.GeoDataFrame(transport_zones_df)
        gcp = gc.plot(color=zones_color)
        gcp.set_axis_off()

        # Put a legend for width on bottom right, title on the top
        x_min = float(biggest_flows[["x"]].min().iloc[0])
        y_min = float(biggest_flows[["y"]].min().iloc[0])
        plt.plot([x_min, x_min+4000], [y_min, y_min], linewidth=2, color=color)
        plt.text(x_min+6000, y_min-1000, "1 000", color=color)
        plt.title(f"{mode_name} flows between transport zones on {period}")

        # Draw all origin-destinations
        for index, row in biggest_flows.iterrows():
            plt.plot([row["x"], row["x_to"]], [row["y"], row["y_to"]],
                     linewidth=row["n_persons"]/500, color=color, alpha=transparency)

        if isinstance(labels, gpd.GeoDataFrame):
            self.__show_labels__(labels, labels_size, labels_color)

        plt.show()

        return biggest_flows

    def __show_labels__(self, labels, size, color):
        for index, row in labels.iterrows():
            if row["prominence"] == 1:
                plt.annotate(row["local_admin_unit_name"], (row["x"], row["y"]),
                             size=size[0], ha="center", va="center", color=color)
            elif row["prominence"] <3:
                plt.annotate(row["local_admin_unit_name"], (row["x"], row["y"]),
                             size=size[1], ha="center", va="center", color=color)
            else:
                plt.annotate(row["local_admin_unit_name"], (row["x"], row["y"]),
                             size=size[2], ha="center", va="center", color=color)

    def get_prominent_cities(self, n_cities=20, n_levels=3, distance_km=2):
        """
        Get the most prominent cities, ie the biggest cities that are not close to a bigger city.

        Useful to label a map and reducing the number of overlaps without mising an important city.

        Parameters
        ----------
        n_cities : int, optional
            Number of cities to include in the list. The default is 20.
        n_levels : int, optional
            Levels of prominence to consider.
        distance_km : int, optional
            If a city is closer than this distance to a bigger one, it will be considered less prominent.
            The default is 2.

        Returns
        -------
        None.

        """
        # Get the flows, the study area and the transport zones dataframes
        population_df = self.get()["weekday_flows"].collect().to_pandas()
        study_area_df = self.population.transport_zones.study_area.get()
        tzdf = self.population.transport_zones.get()

        # Group flows per local admin unit
        flows_per_commune = population_df.merge(tzdf, left_on="from", right_on="transport_zone_id")
        flows_per_commune = flows_per_commune.groupby("local_admin_unit_id")["n_persons"].sum().reset_index()
        flows_per_commune = flows_per_commune.merge(study_area_df)

        # Keep the most important cities and five them an initial prominence depending on total flows
        # Use n_levels here in the future
        flows_per_commune = flows_per_commune.sort_values(by="n_persons", ascending=False).head(n_cities*2).reset_index()
        flows_per_commune.loc[0, "prominence"] = 1
        flows_per_commune.loc[1:n_cities//2, "prominence"] = 2
        flows_per_commune.loc[n_cities//2+1:n_cities, "prominence"] = 3
        flows_per_commune.loc[n_cities+1:n_cities*2, "prominence"] = 3

        # Transform them into a GeoDataFrame
        geoflows = gpd.GeoDataFrame(flows_per_commune)

        # If an admin unit is too close to a bigger admin unit, make it less prominent
        for i in range(n_cities//2):
            coords = flows_per_commune.loc[i, "geometry"]
            geoflows["dists"] = geoflows["geometry"].distance(coords)
            # Use distance_km here
            geoflows.loc[
                ((geoflows["dists"] < distance_km*1000) & (geoflows.index > i)), "prominence"
                ] = geoflows["prominence"] + 2
            geoflows = geoflows.sort_values(by="prominence").reset_index(drop=True)

        # Keep only the most prominent admin units and add their centroids
        geoflows = geoflows[geoflows["prominence"] <= n_levels]
        xy_coords = geoflows["geometry"].centroid.get_coordinates()
        geoflows = geoflows.merge(xy_coords, left_index=True, right_index=True)

        return geoflows
