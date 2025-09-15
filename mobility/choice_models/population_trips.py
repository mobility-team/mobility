import os
import pathlib
import logging
import shutil
import subprocess
import pickle
import json
import random

import geopandas as gpd
import matplotlib.pyplot as plt
import polars as pl
import numpy as np

from scipy.stats import norm
from typing import List
from importlib import resources
from collections import defaultdict

from rich.spinner import Spinner
from rich.live import Live

from mobility.file_asset import FileAsset
from mobility.population import Population
from mobility.choice_models.travel_costs_aggregator import TravelCostsAggregator
from mobility.motives import Motive
from mobility.transport_modes.transport_mode import TransportMode
from mobility.parsers.mobility_survey import MobilitySurvey
from mobility.transport_modes.compute_subtour_mode_probabilities import modes_list_to_dict

class PopulationTrips(FileAsset):
    
    def __init__(
            self,
            population: Population,
            modes: List[TransportMode] = None,
            motives: List[Motive] = None,
            surveys: List[MobilitySurvey] = None,
            n_iterations: int = 10,
            alpha: float = 0.01,
            k_mode_sequences: int = 6,
            dest_prob_cutoff: float = 0.99,
            activity_utility_coeff: float = 2.0,
            stay_home_utility_coeff: float = 1.0,
            n_iter_per_cost_update: int = 3,
            cost_uncertainty_sd: float = 1.0
        ):
        
        modes = [] if modes is None else modes
        motives = [] if motives is None else motives
        surveys = [] if surveys is None else surveys

        costs_aggregator = TravelCostsAggregator(modes)
        
        inputs = {
            "population": population,
            "costs_aggregator": costs_aggregator,
            "motives": motives,
            "surveys": surveys,
            "n_iterations": n_iterations,
            "alpha": alpha,
            "k_mode_sequences": k_mode_sequences,
            "dest_prob_cutoff": dest_prob_cutoff,
            "activity_utility_coeff": activity_utility_coeff,
            "stay_home_utility_coeff": stay_home_utility_coeff,
            "n_iter_per_cost_update": n_iter_per_cost_update,
            "cost_uncertainty_sd": cost_uncertainty_sd
        }
        
        project_folder = pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"])
        cache_path = {
            "weekday_flows": project_folder / "population_trips" / "weekday" / "weekday_flows.parquet",
            "weekend_flows": project_folder / "population_trips" / "weekend" / "weekend_flows.parquet"
        }
        
        super().__init__(inputs, cache_path)
        
        
    def get_cached_asset(self):
        return {
            "weekday_flows": pl.scan_parquet(self.cache_path["weekday_flows"]),
            "weekend_flows": pl.scan_parquet(self.cache_path["weekend_flows"])
        }
        
        
    def create_and_get_asset(self):
        
        weekday_flows = self.compute_flows(is_weekday=True)
        weekend_flows = self.compute_flows(is_weekday=False)
        
        weekday_flows.write_parquet(self.cache_path["weekday_flows"])
        weekend_flows.write_parquet(self.cache_path["weekend_flows"])
            
        return {
            "weekday_flows": weekday_flows,
            "weekend_flows": weekend_flows
        }

    def compute_flows(self, is_weekday):

        population = self.inputs["population"]
        costs_aggregator = self.inputs["costs_aggregator"]
        motives = self.inputs["motives"]
        surveys = self.inputs["surveys"]
        n_iterations = self.inputs["n_iterations"]
        alpha = self.inputs["alpha"]
        dest_prob_cutoff = self.inputs["dest_prob_cutoff"]
        k_mode_sequences = self.inputs["k_mode_sequences"]
        activity_utility_coeff = self.inputs["activity_utility_coeff"]
        stay_home_utility_coeff = self.inputs["stay_home_utility_coeff"]
        n_iter_per_cost_update = self.inputs["n_iter_per_cost_update"]
        cost_uncertainty_sd = self.inputs["cost_uncertainty_sd"]
        
        cache_path = self.cache_path["weekday_flows"] if is_weekday is True else self.cache_path["weekend_flows"]
        tmp_folders = self.prepare_tmp_folders(cache_path)

        chains, demand_groups, motive_seqs = self.get_chains(population, surveys, motives, is_weekday)
        motive_dur, home_night_dur = self.get_mean_activity_durations(chains, demand_groups)
        current_states = self.get_initial_states(chains, demand_groups)
        sinks = self.get_sinks(chains, motives, population.transport_zones)
        costs = self.get_current_costs(costs_aggregator, congestion=False)
        
        remaining_sinks = sinks.clone()
        
        for iteration in range(0, n_iterations):
            
            logging.info(f"Sampling step n°{iteration}")
            
            iteration = 0
            
            utilities = self.get_utilities(
                motives,
                population.transport_zones,
                remaining_sinks,
                costs,
                cost_uncertainty_sd
            )
            
            dest_prob = self.get_destination_probability(
                utilities,
                motives,
                dest_prob_cutoff
            )
            
            self.spatialize_trip_chains(iteration, chains, demand_groups, dest_prob, motives, costs, alpha, tmp_folders)
            self.search_top_k_mode_sequences(iteration, costs_aggregator, k_mode_sequences, tmp_folders)
            
            possible_states_steps = self.get_possible_states_steps(demand_groups, chains, costs_aggregator, motive_dur, iteration, activity_utility_coeff, tmp_folders)
            possible_states_utility = self.get_possible_states_utility(possible_states_steps, home_night_dur, stay_home_utility_coeff)
            
            transition_prob = self.get_transition_probabilities(current_states, possible_states_utility)
            current_states = self.apply_transitions(current_states, transition_prob)
            current_states_steps = self.get_current_states_steps(current_states, possible_states_steps)
            
            costs = self.update_costs(costs, iteration, n_iter_per_cost_update, current_states_steps, costs_aggregator)
            
            current_states_steps = self.fix_overflow(current_states_steps, sinks)
            remaining_sinks = self.get_remaining_sinks(current_states_steps, sinks)
        
        
        current_states_steps = (
            current_states_steps
            .join(demand_groups, on=["demand_group_id"])
            .drop("demand_group_id")
        )

        current_states_steps = current_states_steps.with_columns(
            is_weekday=pl.lit(is_weekday)
        )

        return current_states_steps
    

    def prepare_tmp_folders(self, cache_path):
        
        inputs_hash = str(cache_path.stem).split("-")[0]
        
        def rm_then_mkdirs(folder_name):
            path = cache_path.parent / (inputs_hash + "-" + folder_name)
            shutil.rmtree(path, ignore_errors=True)
            os.makedirs(path)
            return path
        
        folders = ["spatialized-chains", "modes", "flows"]
        folders = {f: rm_then_mkdirs(f) for f in folders}
        
        return folders
        

    def get_chains(self, population, surveys, motives, is_weekday):
        """
            Get the count of trip chains per person and per day for all transport 
            zones and socio professional categories (0.8 "home -> work -> home" 
            chains per day for employees in the transport zone "123", for example)
        """

        # Map local admin units to urban unit categories (C, B, I, R) to be able
        # to get population counts by urban unit category
        lau_to_city_cat = ( 
            pl.from_pandas(
                population.transport_zones.study_area.get()
                .drop("geometry", axis=1)
                [["local_admin_unit_id", "urban_unit_category"]]
                .rename({"urban_unit_category": "city_category"}, axis=1)
            )
            .with_columns(
                country=pl.col("local_admin_unit_id").str.slice(0, 2)
            )
        )
        
        countries = lau_to_city_cat["country"].unique().to_list()

        # Aggregate population groups by transport zone, city category, socio pro 
        # category and number of cars in the household
        demand_groups = (
            
            pl.scan_parquet(population.get()["population_groups"])
            .rename({
                "socio_pro_category": "csp",
                "transport_zone_id": "home_zone_id",
                "weight": "n_persons"
            })
            .join(lau_to_city_cat.lazy(), on=["local_admin_unit_id"])
            .group_by(["country", "home_zone_id", "city_category", "csp", "n_cars"])
            .agg(pl.col("n_persons").sum())

            # Cast strings to enums to speed things up
            .with_columns(
                country=pl.col("country").cast(pl.Enum(countries)),
                city_category=pl.col("city_category").cast(pl.Enum(["C", "B", "R", "I"])),
                csp=pl.col("csp").cast(pl.Enum(["1", "2", "3", "4", "5", "6", "7", "8", "no_csp"])),
                n_cars=pl.col("n_cars").cast(pl.Enum(["0", "1", "2+"]))
            )
            .with_row_count("demand_group_id")

            .collect(engine="streaming")
            
        )

        # Get the chain probabilities from the mobility surveys
        surveys = [s for s in surveys if s.country in countries]
        
        p_chain = (
            pl.concat(
                [
                    (
                        survey
                        .get_chains_probability(motives)
                        .with_columns(
                            country=pl.lit(survey.inputs["country"])
                        )
                    )
                    for survey in surveys
                ]
            )
            .with_columns(
                country=pl.col("country").cast(pl.Enum(countries))
            )
        )
        
        # Create an index for motive sequences to avoid moving giant strings around
        motive_seqs = ( 
            p_chain
            .select(["motive_seq", "seq_step_index", "motive"])
            .unique()
            .with_columns(
                motive_seq_id=pl.col("motive_seq").hash()
            )
        )
        
        p_chain = (
            p_chain
            .join(motive_seqs.select(["motive_seq", "motive_seq_id"]), on="motive_seq")
            .drop("motive_seq")
        )
        
        motive_seqs = motive_seqs.select(["motive_seq_id", "seq_step_index", "motive"])
        
        # Compute the amount of demand (= duration) per demand group and motive sequence
        anchors = {m.name: m.is_anchor for m in motives}
        
        chains = (

            demand_groups
            .join(p_chain, on=["country", "city_category", "csp", "n_cars"])
            .filter(pl.col("is_weekday") == is_weekday)
            .drop("is_weekday")
            .with_columns(
                n_persons=pl.col("n_persons")*pl.col("p_seq")
            )
            .with_columns(
                duration=(
                    (
                        pl.col("duration_morning")
                        + pl.col("duration_midday")
                        + pl.col("duration_evening")
                    )
                )
            )
            
            .group_by(["demand_group_id", "motive_seq_id", "seq_step_index", "motive"])
            .agg(
                n_persons=pl.col("n_persons").sum(),
                duration=(
                    (
                        pl.col("duration_morning")
                        + pl.col("duration_midday")
                        + pl.col("duration_evening")
                    )
                    * pl.col("n_persons")
                ).sum()
            )
            
            .sort(["demand_group_id", "motive_seq_id",  "seq_step_index"])
            .with_columns(
                is_anchor=pl.col("motive").replace_strict(anchors)
            )
        
        )
        
        # Add an empty schedule for each demand group
        # This schedule is identfied later on because its id is the max id,
        # which might not be very robust... The schedule has a zero duration 
        # dummy home activity, which does nothing but is necessary because
        # we can't have zero activity schedules for now.
        
        motive_seqs = pl.concat([
            motive_seqs,
            pl.DataFrame(
                data=[{
                    "motive_seq_id": 0,
                    "seq_step_index": 1,
                    "motive": "home"
                }],
                schema={
                    "motive_seq_id": chains["motive_seq_id"].dtype,
                    "seq_step_index": chains["seq_step_index"].dtype,
                    "motive":chains["motive"].dtype
                }
            )
        ])
        
        empty_schedule = (
            demand_groups.select(["demand_group_id"])
            .with_columns(
                motive_seq_id=pl.lit(0, chains["motive_seq_id"].dtype),
                seq_step_index=pl.lit(1, chains["seq_step_index"].dtype),
                motive=pl.lit("home", chains["motive"].dtype),
                n_persons=1e-9,
                duration=0.0,
                is_anchor=True
            )
        )
        
        chains = pl.concat([chains, empty_schedule])
        
        # Drop unecessary columns from demand groups
        demand_groups = (
            demand_groups
            .drop(["country", "city_category"])
        )

        return chains, demand_groups, motive_seqs
    
    
    def get_mean_activity_durations(self, chains, demand_groups):
        
        two_minutes = 120.0/3600.0
        
        chains = ( 
            chains
            .join(demand_groups.select(["demand_group_id", "csp"]), on="demand_group_id")
            .with_columns(duration_per_pers=pl.col("duration")/pl.col("n_persons"))
        )
        
        mean_motive_durations = (
            chains
            .filter(pl.col("seq_step_index") != pl.col("seq_step_index").max().over(["demand_group_id", "motive_seq_id"]))
            .group_by(["csp", "motive"])
            .agg(
                mean_duration_per_pers=pl.max_horizontal([
                    (pl.col("duration_per_pers")*pl.col("n_persons")).sum()/pl.col("n_persons").sum(),
                    pl.lit(two_minutes)
                ])
            )
        )
        
        mean_home_night_durations = (
            chains
            .group_by(["demand_group_id", "csp", "motive_seq_id"])
            .agg(
                n_persons=pl.col("n_persons").first(),
                home_night_per_pers=24.0 - pl.col("duration_per_pers").sum()
            )
            .group_by("csp")
            .agg(
                 mean_home_night_per_pers=pl.max_horizontal([
                     (pl.col("home_night_per_pers")*pl.col("n_persons")).sum()/pl.col("n_persons").sum(),
                     pl.lit(two_minutes)
                  ])
            )
        )
        
        return mean_motive_durations, mean_home_night_durations
    
    
    def get_initial_states(self, chains, demand_groups):
        
        initial_states = (
            chains 
            .select(["demand_group_id", "motive_seq_id"])
            .filter(pl.col("motive_seq_id") == 0)
            .join(
                demand_groups.select(["demand_group_id", "n_persons"]),
                on="demand_group_id"
            )
            .with_columns(
                dest_seq_id=pl.lit(0, dtype=pl.UInt64()),
                mode_seq_id=pl.lit(0, dtype=pl.UInt64()),
                utility=pl.lit(-1e6)
            )
        )
        
        return initial_states
    

    def get_sinks(self, chains, motives, transport_zones):

        demand = ( 
            chains
            .group_by(["motive"])
            .agg(pl.col("duration").sum())
        )
        
        motive_names = [m.name for m in motives]

        # Load and adjust sinks
        sinks = (
            
            pl.concat(
                [
                    (
                        motive
                        .get_opportunities(transport_zones)
                        .with_columns(
                            motive=pl.lit(motive.name),
                            sink_saturation_coeff=pl.lit(motive.sink_saturation_coeff)
                        )
                    )
                    for motive in motives if motive.has_opportunities is True
                ]
            )

            .with_columns(
                motive=pl.col("motive").cast(pl.Enum(motive_names)),
                to=pl.col("to").cast(pl.Int32)
            )
            .join(demand, on="motive")
            .with_columns(
                sink_duration=pl.col("n_opp")/pl.col("n_opp").sum().over("motive")*pl.col("duration")*pl.col("sink_saturation_coeff")
            )
            .select(["to", "motive", "sink_duration"])
        )

        return sinks

    def get_current_costs(self, costs, congestion):

        current_costs = (
            costs.get(congestion=congestion)
            .with_columns([
                pl.col("from").cast(pl.Int32()),
                pl.col("to").cast(pl.Int32())
            ])
        )

        return current_costs
    

    def get_destination_probability(self, utilities, motives, dest_prob_cutoff):
        
        # Compute the probability of choosing a destination, given a trip motive, an 
        # origin and the costs to get to destinations
        logging.info("Computing the probability of choosing a destination based on current location, potential destinations, and motive (with radiation models)...")
        
        costs_bin = utilities[0]
        cost_bin_to_dest = utilities[1]

        motives_lambda = {motive.name: motive.radiation_lambda for motive in motives}
        
        prob = (
                
            # Apply the radiation model for each motive and origin
            costs_bin
            .with_columns(
                s_ij=pl.col("sink_duration").cum_sum().over(["from", "motive"]),
                selection_lambda=pl.col("motive").replace_strict(motives_lambda)
            )
            .with_columns(
                p_a = (1 - pl.col("selection_lambda")**(1+pl.col('s_ij'))) / (1+pl.col('s_ij')) / (1-pl.col("selection_lambda"))
            )
            .with_columns(
                p_a_lag=( 
                    pl.col('p_a')
                    .shift(fill_value=1.0)
                    .over(["from", "motive"])
                    .alias('p_a_lag')
                )
            )
            .with_columns(
                p_ij=pl.col('p_a_lag') - pl.col('p_a')
            )
            .with_columns(
                p_ij=pl.col('p_ij') / pl.col('p_ij').sum().over(["from", "motive"])
            )
            .filter(pl.col("p_ij") > 0.0)
            
            # Keep only the first 99 % of the distribution
            .sort("p_ij", descending=True)
            .with_columns(
                p_ij_cum=pl.col("p_ij").cum_sum().over(["from", "motive"]),
                p_count=pl.col("p_ij").cum_count().over(["from", "motive"])
            )
            .filter((pl.col("p_ij_cum") < dest_prob_cutoff) | (pl.col("p_count") == 1))
            .with_columns(p_ij=pl.col("p_ij")/pl.col("p_ij").sum().over(["from", "motive"]))
            
            # Disaggregate bins -> destinations
            .join(cost_bin_to_dest, on=["motive", "from", "cost_bin"])
            .with_columns(p_ij=pl.col("p_ij")*pl.col("p_to"))
            .group_by(["motive", "from", "to"])
            .agg(pl.col("p_ij").sum())
            
            # Keep only the first 99 % of the distribution
            # (or the destination that has a 100% probability, which can happen)
            .sort("p_ij", descending=True)
            .with_columns(
                p_ij_cum=pl.col("p_ij").cum_sum().over(["from", "motive"]),
                p_count=pl.col("p_ij").cum_count().over(["from", "motive"])
            )
            .filter((pl.col("p_ij_cum") < dest_prob_cutoff) | (pl.col("p_count") == 1))
            .with_columns(p_ij=pl.col("p_ij")/pl.col("p_ij").sum().over(["from", "motive"]))
            
            .select(["motive", "from", "to", "p_ij"])
            
            .collect(engine="streaming")
        )
        
        return prob
    
    
        
    def spatialize_trip_chains(self, iteration, chains, demand_groups, dest_prob, motives, costs, alpha, tmp_folders):

        
        if iteration > 0:
            chains = chains.filter(pl.col("motive_seq_id") != 0)
        
        chains = (
            chains
            .join(demand_groups.select(["demand_group_id", "home_zone_id"]), on="demand_group_id")
            .select(["demand_group_id", "home_zone_id", "motive_seq_id", "motive", "is_anchor", "seq_step_index"])
        )
        
        
        chains = self.spatialize_anchor_motives(chains, dest_prob)
        chains = self.spatialize_other_motives(chains, dest_prob, costs, alpha)
        
        dest_seq_index = ( 
            chains
            .sort("seq_step_index")
            .group_by(["demand_group_id", "motive_seq_id"])
            .agg(
                to=pl.col("to")
            )
            .with_columns(
                dest_seq_id=pl.col("to").hash()
            )
        )
        
        chains = (
            chains
            .join(
                dest_seq_index.select(["demand_group_id", "motive_seq_id", "dest_seq_id"]),
                on=["demand_group_id", "motive_seq_id"]
            )
        )

        ( 
            chains
            .drop(["home_zone_id", "motive"])
            .with_columns(iteration=pl.lit(iteration).cast(pl.UInt32))
            .write_parquet(tmp_folders["spatialized-chains"] / f"spatialized_chains_{iteration}.parquet")
        )
        
        
    def spatialize_anchor_motives(self, chains, dest_prob):
        
        logging.info("Spatializing anchor motives...")
        
        seed = random.getrandbits(64)
        
        spatialized_anchors = ( 
            
            chains
            .filter((pl.col("is_anchor")) & (pl.col("motive") != "home"))
            .select(["demand_group_id", "home_zone_id", "motive_seq_id", "motive"])
            .unique()
            
            .join(
                dest_prob,
                left_on=["home_zone_id", "motive"],
                right_on=["from", "motive"]
            )
            
            .with_columns(
                noise=( 
                    pl.struct(["demand_group_id", "motive_seq_id", "to"])
                    .hash(seed=seed)
                    .cast(pl.Float64) 
                    .truediv(pl.lit(18446744073709551616.0))
                    .log()
                    .neg()
                )
            )
            
            .with_columns(
                sample_score=pl.col("noise")/pl.col("p_ij")
            )
            
            .with_columns(
                min_score=pl.col("sample_score").min().over(["demand_group_id", "motive_seq_id"])
            )
            .filter(pl.col("sample_score") == pl.col("min_score"))
            .select(["demand_group_id", "motive_seq_id", "motive", "to"])
            
        )
        
        chains = (
            
            chains
            .join(
                spatialized_anchors.rename({"to": "anchor_to"}),
                on=["demand_group_id", "motive_seq_id", "motive"],
                how="left"
            )
            .with_columns(
                anchor_to=pl.when(
                    pl.col("motive") == "home"
                ).then(
                    pl.col("home_zone_id")
                ).otherwise(
                    pl.col("anchor_to")
                )
            )
            .sort(["demand_group_id", "motive_seq_id", "seq_step_index"])
            .with_columns(
                anchor_to=pl.col("anchor_to").backward_fill()
            )
            
        ) 
                    
        return chains
    
    
    def spatialize_other_motives(self, chains, dest_prob, costs, alpha):
        
        chains_step = ( 
            chains
            .filter(pl.col("seq_step_index") == 1)
            .with_columns(pl.col("home_zone_id").alias("from"))
        )
        
        seq_step_index = 1
        spatialized_chains = []
        
        while chains_step.height > 0:
            
            logging.info(f"Spatializing motives for motive sequence step n°{seq_step_index}...")
            
            spatialized_step = ( 
                self.spatialize_trip_chains_step(seq_step_index, chains_step, dest_prob, costs, alpha)
                .with_columns(
                    seq_step_index=pl.lit(seq_step_index).cast(pl.UInt32)
                )
            )
            
            spatialized_chains.append(spatialized_step)
            
            # Create the next steps in the chains, using the latest locations as 
            # origins for the next trip
            seq_step_index += 1
            
            chains_step = ( 
                chains
                .filter(pl.col("seq_step_index") == seq_step_index)
                .join(
                    (
                        spatialized_step
                        .select(["demand_group_id", "home_zone_id", "motive_seq_id", "to"])
                        .rename({"to": "from"})
                    ),
                    on=["demand_group_id", "home_zone_id", "motive_seq_id"]
                )
            )
            
            
        return pl.concat(spatialized_chains)
        
        
    def spatialize_trip_chains_step(self, seq_step_index, chains_step, dest_prob, costs, alpha):
        
        # Tweak the destination probabilities so that the sampling takes into
        # account the cost of travel to the next anchor (so we avoid drifting
        # away too far).
        
        # Use the exponential sort trick to sample destinations based on their probabilities
        # (because polars cannot do weighted sampling like pandas)
        # https://timvieira.github.io/blog/post/2019/09/16/algorithms-for-sampling-without-replacement/
        
        seed = random.getrandbits(64)
        
        steps = (
        
            chains_step
            .filter(pl.col("is_anchor").not_())
            
            .join(dest_prob, on=["from", "motive"])
            
            .join(
                costs,
                left_on=["to", "anchor_to"],
                right_on=["from", "to"]
            )
            
            .with_columns(
                p_ij_corr=(pl.col("p_ij").log() - alpha*pl.col("cost")).exp(),
                noise=( 
                    pl.struct(["demand_group_id", "motive_seq_id", "to"])
                    .hash(seed=seed)
                    .cast(pl.Float64) 
                    .truediv(pl.lit(18446744073709551616.0))
                    .log()
                    .neg()
                )
            )
            
            .with_columns(
                sample_score=pl.col("noise")/pl.col("p_ij")
            )
            
            .with_columns(
                min_score=pl.col("sample_score").min().over(["demand_group_id", "motive_seq_id"])
            )
            .filter(pl.col("sample_score") == pl.col("min_score"))
            
            .select(["demand_group_id", "home_zone_id", "motive_seq_id", "motive", "anchor_to", "from", "to"])
            
        )
        
        
        # Add the steps that end end up at anchor destinations
        steps_anchor = (
            chains_step
            .filter(pl.col("is_anchor"))
            .with_columns(
                to=pl.col("anchor_to")
            )
            .select(["demand_group_id", "home_zone_id", "motive_seq_id", "motive", "anchor_to", "from", "to"])
        )
        
        steps = pl.concat([steps, steps_anchor])
            
        
        return steps
    
    
    def search_top_k_mode_sequences(self, iteration, costs_aggregator, k_mode_sequences, tmp_folders):
        
        parent_folder_path = tmp_folders["spatialized-chains"].parent
        
        chains_path = tmp_folders["spatialized-chains"] / f"spatialized_chains_{iteration}.parquet"
        costs_path = parent_folder_path / "tmp-costs.pkl"
        leg_modes_path = parent_folder_path / "tmp-leg-modes.pkl"
        modes_path = parent_folder_path / "modes-props.json"
        location_chains_path = parent_folder_path / "tmp-location-chains.parquet"
        tmp_path = parent_folder_path / "tmp_results"
        output_path = tmp_folders["modes"] / f"mode_sequences_{iteration}.parquet"
        
        shutil.rmtree(tmp_path, ignore_errors=True)
        os.makedirs(tmp_path)
        
        # Format the modes info as a dict and save the result in a temp file
        modes = modes_list_to_dict(costs_aggregator.modes)
        
        with open(modes_path, "w") as f:
            f.write(json.dumps(modes))
        
        # Format the costs as dict and save it as pickle to be ready for the parallel workers
        mode_id = {n: i for i, n in enumerate(modes)}
        id_to_mode = {i: n for i, n in enumerate(modes)}
    
        costs = ( 
            costs_aggregator.get_costs_by_od_and_mode(
                ["cost"],
                congestion=True,
                detail_distances=False
            )
            .with_columns(
                mode_id=pl.col("mode").replace_strict(mode_id, return_dtype=pl.UInt8())
            )
        )

        costs = {(row["from"], row["to"], row["mode_id"]): row["cost"] for row in costs.to_dicts()}
        
        with open(costs_path, "wb") as f:
            pickle.dump(costs, f, protocol=pickle.HIGHEST_PROTOCOL)  
        
        # Format the available modes list for each OD as dict and save it as pickle to be ready for the parallel workers
        is_return_mode = {mode_id[k]: v["is_return_mode"] for  k, v in modes.items()}
        
        leg_modes = defaultdict(list)
        for (from_, to_, mode) in costs.keys():
            if not is_return_mode[mode]:
                leg_modes[(from_, to_)].append(mode)
        
        with open(leg_modes_path, "wb") as f:
            pickle.dump(leg_modes, f, protocol=pickle.HIGHEST_PROTOCOL)
            
        
        # Prepare a list of location chains
        spat_chains = ( 
            pl.scan_parquet(chains_path)
            .group_by(["demand_group_id", "motive_seq_id", "dest_seq_id"])
            .agg(
                locations=pl.col("from").sort_by("seq_step_index"),
                locations_index=pl.col("from").str.join("-").hash()
            )
            .collect()
        )
        
        unique_location_chains = ( 
            spat_chains
            .group_by(["locations_index"])
            .agg(
                pl.col("locations").first()
            )
        )
        
        unique_location_chains.write_parquet(location_chains_path)
        
        # Launch the mode sequence probability calculation
        with Live(Spinner("dots", text="Finding probable mode sequences for the spatialized trip chains..."), refresh_per_second=10):
        
            process = subprocess.Popen(
                [
                    "python",
                    "-u",
                    str(resources.files('mobility') / "transport_modes" / "compute_subtour_mode_probabilities.py"),
                    "--k_sequences", str(k_mode_sequences),
                    "--location_chains_path", str(location_chains_path),
                    "--costs_path", str(costs_path),
                    "--leg_modes_path", str(leg_modes_path),
                    "--modes_path", str(modes_path),
                    "--output_path", str(output_path),
                    "--tmp_path", str(tmp_path)
                ]
            )
            
            process.wait()

        
        # Agregate all mode sequences chunks
        all_results = (
            spat_chains.select(["demand_group_id", "motive_seq_id", "dest_seq_id", "locations_index"])
            .join(pl.read_parquet(tmp_path), left_on="locations_index", right_on="index")
            .with_columns(
                mode=pl.col("mode_index").replace_strict(id_to_mode)
            )
        )
        
        mode_seq_index = (
            all_results
            .sort(["seq_step_index"])
            .group_by(["demand_group_id", "motive_seq_id", "dest_seq_id", "mode_seq_id"])
            .agg(
                pl.col("mode_index")
            )
            .with_columns(
                mode_seq_id_hash=pl.col("mode_index").hash()
            )
            .drop("mode_index")
        )
        
        all_results = (
            all_results
            .join(mode_seq_index, on=["demand_group_id", "motive_seq_id", "dest_seq_id", "mode_seq_id"])
            .drop("mode_seq_id")
            .rename({"mode_seq_id_hash": "mode_seq_id"})
            .select(["demand_group_id", "motive_seq_id", "dest_seq_id", "mode_seq_id", "seq_step_index", "mode"])
            .with_columns(iteration=pl.lit(iteration).cast(pl.UInt32))
        )
        
        all_results.write_parquet(output_path)
            
        
    def get_possible_states_steps(
            self,
            demand_groups,
            chains,
            costs_aggregator,
            motive_dur,
            iteration,
            activity_utility_coeff,
            tmp_folders
        ):
        
        spat_chains = pl.read_parquet(tmp_folders['spatialized-chains'])
        modes = pl.read_parquet(tmp_folders["modes"])
        
        cost_by_od_and_modes = ( 
            costs_aggregator.get_costs_by_od_and_mode(
                ["cost"],
                congestion=True,
                detail_distances=False
            )
        )
        
        chains = ( 
            chains
            .join(demand_groups.select(["demand_group_id", "csp"]), on="demand_group_id")
            .with_columns(duration_per_pers=pl.col("duration")/pl.col("n_persons"))
        )
        
        # Keep only one mode / row per empty activity schedule.
        # The mode search returns multiple rows because we use a dummy home motive
        # for this empty schedule. We should find a better way to handle this.
        modes = pl.concat([
            modes.filter(pl.col("motive_seq_id") != 0),
            modes.filter(pl.col("motive_seq_id") == 0).group_by(["demand_group_id", "motive_seq_id"]).head(1)
        ])
        
        states = (
            modes
            .join(spat_chains, on=["iteration", "demand_group_id", "motive_seq_id", "seq_step_index"])
            .join(chains, on=["demand_group_id", "motive_seq_id", "seq_step_index"])
            .join(cost_by_od_and_modes, on=["from", "to", "mode"])
            .join(motive_dur, on=["csp", "motive"])
            .with_columns(
                duration_per_pers=pl.max_horizontal([
                    pl.col("duration_per_pers"),
                    pl.col("mean_duration_per_pers")*0.1
                 ])
            )
            .with_columns(
                utility=activity_utility_coeff*pl.col("mean_duration_per_pers")*(pl.col("duration_per_pers")/0.1/pl.col("mean_duration_per_pers")).log() - pl.col("cost")
            )
            
            # Set the utility of the home activity of the stay at home activity to zero
            # (utility is computed directly at the day level in the compute_states_probability function)
            .with_columns(
                utility=(
                    pl.when(pl.col("motive_seq_id") != 0)
                    .then(pl.col("utility"))
                    .otherwise(0.0)
                )
            )
            
        )
        
        return states
        
    
    def get_possible_states_utility(self, possible_states_steps, home_night_dur, stay_home_utility_coeff):
                    
        possible_states_utility = (
            
            possible_states_steps
            .group_by(["demand_group_id", "csp", "motive_seq_id", "dest_seq_id", "mode_seq_id"])
            .agg(
                utility=pl.col("utility").sum(),
                home_night_per_pers=24.0 - pl.col("duration_per_pers").sum()
            )
            
            .join(home_night_dur, on="csp")
            .with_columns(
                home_night_per_pers=pl.max_horizontal([
                    pl.col("home_night_per_pers"),
                    pl.col("mean_home_night_per_pers")*0.1
                ])
            )
            .with_columns(
                utility_stay_home=stay_home_utility_coeff*pl.col("mean_home_night_per_pers")*(pl.col("home_night_per_pers")/0.1/pl.col("mean_home_night_per_pers")).log()
            )
            
            .with_columns(
                utility=pl.col("utility") + pl.col("utility_stay_home")
            )
            
            .select(["demand_group_id", "motive_seq_id", "mode_seq_id", "dest_seq_id", "utility"])
        )
        
        
        return possible_states_utility
    
    
    
    def get_transition_probabilities(self, current_states, possible_states_utility): 
        
        transition_probabilities = (
            
            current_states
            .select(["demand_group_id", "motive_seq_id", "dest_seq_id", "mode_seq_id", "utility"])
            
            .join_where(
                possible_states_utility,
                pl.col("demand_group_id") == pl.col("demand_group_id_trans"),
                pl.col("utility") < pl.col("utility_trans"),
                suffix="_trans"
            )
            
            .with_columns(
                delta_utility=pl.col("utility_trans") - pl.col("utility")
            )
            
            .with_columns(
                delta_utility=pl.col("delta_utility") - pl.col("delta_utility").max().over(["demand_group_id", "motive_seq_id", "dest_seq_id", "mode_seq_id"])
            )
            .filter(pl.col("delta_utility") > -5.0)
            
            .with_columns(
                p_transition=pl.col("delta_utility").exp()/pl.col("delta_utility").exp().sum().over(["demand_group_id", "motive_seq_id", "dest_seq_id", "mode_seq_id"])
            )
            
            .select([
                "demand_group_id",
                "motive_seq_id", "dest_seq_id", "mode_seq_id",
                "motive_seq_id_trans", "dest_seq_id_trans", "mode_seq_id_trans",
                "utility_trans", "p_transition"]
            )
        
        )
        
        return transition_probabilities
    
    
    def apply_transitions(self, current_states, transition_probabilities):
        
        new_states = (
            
            current_states
            .join(
                transition_probabilities,
                on=["demand_group_id", "motive_seq_id", "dest_seq_id", "mode_seq_id"],
                how="left"
            )
            .with_columns(
                p_transition=pl.col("p_transition").fill_null(1.0),
                utility=pl.coalesce([pl.col("utility_trans", "utility")]),
                motive_seq_id=pl.coalesce([pl.col("motive_seq_id_trans", "motive_seq_id")]),
                dest_seq_id=pl.coalesce([pl.col("dest_seq_id_trans", "dest_seq_id")]),
                mode_seq_id=pl.coalesce([pl.col("mode_seq_id_trans", "mode_seq_id")])
            )
            .with_columns(
                n_persons=pl.col("n_persons")*pl.col("p_transition")
            )
            .select(["demand_group_id", "motive_seq_id", "dest_seq_id", "mode_seq_id", "n_persons", "utility"])
        )
        
        return new_states
    
    
    def get_current_states_steps(self, current_states, possible_states_steps):
        
        current_states_steps = (
            current_states
            .join(
                possible_states_steps.select([
                    "demand_group_id", "motive_seq_id", "dest_seq_id",
                    "mode_seq_id", "motive", "from", "to", "mode", "duration_per_pers"
                ]),
                on=["demand_group_id", "motive_seq_id", "dest_seq_id", "mode_seq_id"]
            )
            .with_columns(
                duration=pl.col("duration_per_pers")*pl.col("n_persons")
            )
            .drop("duration_per_pers")
        )
        
        return current_states_steps
    
    
    def update_costs(self, costs, iteration, n_iter_per_cost_update, current_states_steps, costs_aggregator):
        """
            If a cost update is needed, aggregate the flows by origin, destination
            and mode, compute the user equilibrium on the road network and 
            recompute the travel times and distances of the shortest paths 
            in after congestion.
        """
        
        if n_iter_per_cost_update > 0 and iteration > 0 and iteration % n_iter_per_cost_update == 0:
            
            od_flows_by_mode = (
                current_states_steps
                .filter(pl.col("motive_seq_id") != 0)
                .group_by(["from", "to", "mode"])
                .agg(
                    flow_volume=pl.col("n_persons").sum()
                )
            )
            
            costs_aggregator.update(od_flows_by_mode)
            costs = self.get_current_costs(costs_aggregator, congestion=True)

        return costs
        

    

        
    def fix_overflow(self, states_steps, sinks):
        
        # Compute the share of persons in each OD flow that could not find an
        # opportunity because too many people chose the same destiation
        # p_overflow_motive = 1.0 - duration/available duration
        
        # A given chain is "overflowing" opportunities at destination if 
        # any one of the destinations is "overflowing", so :
        # p_overflow = max(p_overflow_motive)
        logging.info("Correcting flows for sink saturation...")
        
        overflow = (
            
            states_steps
            
            .join(sinks, on=["motive", "to"], how="left")
            .with_columns(
                p_overflow=(
                    pl.when(pl.col("motive") == "home")
                    .then(0.0)
                    .otherwise((1.0 - 1.0/(pl.col("duration").sum().over(["to", "motive"])/pl.col("sink_duration"))).clip(0.0, 1.0))
                )
            )
            .with_columns(
                p_overflow_max=pl.col("p_overflow").max().over(["demand_group_id", "motive_seq_id", "dest_seq_id", "mode_seq_id"])
            )
            .with_columns(
                n_persons_overflow=pl.col("n_persons")*pl.col("p_overflow_max")
            )
            
            .group_by(["demand_group_id", "motive_seq_id", "dest_seq_id", "mode_seq_id"])
            .agg(pl.col("n_persons_overflow").first())
            
            .group_by(["demand_group_id"])
            .agg(pl.col("n_persons_overflow").sum())
            
        )
        
        states_stay_home = (
            
            states_steps
            .filter(pl.col("motive_seq_id") == 0)
            .join(overflow, on="demand_group_id")
            .with_columns(
                n_persons=pl.col("n_persons") + pl.col("n_persons_overflow")
            )
            .drop("n_persons_overflow")
            
        )
        
        states_steps_fixed = pl.concat([
            states_steps.filter(pl.col("motive_seq_id") != 0),
            states_stay_home
        ])
        
        return states_steps_fixed
    
    


    def get_remaining_sinks(self, current_states_steps, sinks):
        
        logging.info("Computing remaining opportunities at destinations...")

        # Compute the remaining number of opportunities by motive and destination
        # once assigned flows are accounted for
        remaining_sinks = (
        
            current_states_steps
            .group_by(["to", "motive"])
            .agg(pl.col("duration").sum())
            .join(sinks, on=["to", "motive"], how="full", coalesce=True)
            .with_columns(
                sink_duration=( 
                    pl.col("sink_duration").fill_null(0.0).fill_nan(0.0)
                    -
                    pl.col("duration").fill_null(0.0).fill_nan(0.0)
                )
            )
            .select(["motive", "to", "sink_duration"])
            .filter(pl.col("sink_duration") > 0.0)
            
        )
        
        return remaining_sinks
        
        
        
    def get_utilities(self, motives, transport_zones, sinks, costs, cost_uncertainty_sd):
        
        motive_names = [m.name for m in motives]
        
        utilities = [(m.name, m.get_utilities(transport_zones)) for m in motives]
        utilities = [u for u in utilities if u[1] is not None]
        utilities = [u[1].with_columns(motive=pl.lit(u[0])) for u in utilities]
        
        utilities = (
            
            pl.concat(utilities)
            .with_columns(
                motive=pl.col("motive").cast(pl.Enum(motive_names)),
                to=pl.col("to").cast(pl.Int32)
            )

        )
        
        def offset_costs(costs, delta, prob):
            return (
                costs
                .with_columns([
                    (pl.col("cost") + delta).alias("cost"),
                    pl.lit(prob).alias("prob")
                ])
            )
        
        x = [-2.0, -1.0, 0.0, 1.0, 2.0]
        p = norm.pdf(x, loc=0.0, scale=cost_uncertainty_sd)
        p /= p.sum()
        
        costs = pl.concat([offset_costs(costs, x[i], p[i]) for i in range(len(p))])

        costs = (
            costs.lazy()
            .join(sinks.lazy(), on="to")
            .join(utilities.lazy(), on=["motive", "to"], how="left")
            .with_columns(
                utility=pl.col("utility").fill_null(0.0),
                n_opp=pl.col("sink_duration")*pl.col("prob")
            )
            .drop("prob")
            .with_columns(cost_bin=(pl.col("cost") - pl.col("utility")).round().cast(pl.Int32()))
        )

        cost_bin_to_dest = (
            costs
            .with_columns(p_to=pl.col("sink_duration")/pl.col("sink_duration").sum().over(["from", "motive", "cost_bin"]))
            .select(["motive", "from", "cost_bin", "to", "p_to"])
        )

        costs_bin = (
            costs
            .group_by(["from", "motive", "cost_bin"])
            .agg(pl.col("sink_duration").sum())
            .sort(["from", "motive", "cost_bin"])
        )
        
        return costs_bin, cost_bin_to_dest
    
    

    
    


    def plot_modal_share(self, zone="origin", mode="car", period="weekdays"):
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

        mode_share = population_df.groupby([left_column, "mode"]).sum("flow_volume")
        mode_share = mode_share.reset_index().set_index([left_column])
        mode_share["total"] = mode_share.groupby([left_column])["flow_volume"].sum()
        mode_share["modal_share"] = mode_share["flow_volume"] / mode_share["total"]

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
        plt.show()

        return mode_share

    def plot_od_flows(self, mode="all", motive="all", period="weekdays", level_of_detail=0,
                      n_largest=2000, color="blue", transparency=0.2, zones_color="gray"):
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
        biggest_flows = population_df.groupby(["from", "to"]).sum("flow_volume").reset_index()
        biggest_flows = biggest_flows.where(biggest_flows["from"] != biggest_flows["to"]).nlargest(n_largest, "flow_volume")
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
                     linewidth=row["flow_volume"]/500, color=color, alpha=transparency)

        plt.show()

        return biggest_flows
