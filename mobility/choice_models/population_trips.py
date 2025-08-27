import os
import pathlib
import logging
import shutil
import subprocess

import geopandas as gpd
import matplotlib.pyplot as plt
import polars as pl
import numpy as np

from scipy.stats import norm
from typing import List
from importlib import resources

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
            alpha: float = 0.5,
            n_iter_per_cost_update: int = 3,
            cost_uncertainty_sd: float = 1.0,
            delta_cost_change: float = 0.0,
            random_switch: float = 4.0
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
            "n_iter_per_cost_update": n_iter_per_cost_update,
            "cost_uncertainty_sd": cost_uncertainty_sd,
            "delta_cost_change": delta_cost_change,
            "random_switch": random_switch
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
        
        weekday_flows.sink_parquet(self.cache_path["weekday_flows"])
        weekend_flows.sink_parquet(self.cache_path["weekend_flows"])
            
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
        n_iter_per_cost_update = self.inputs["n_iter_per_cost_update"]
        cost_uncertainty_sd = self.inputs["cost_uncertainty_sd"]
        delta_cost_change = self.inputs["delta_cost_change"]
        random_switch = self.inputs["random_switch"]
        
        cache_path = self.cache_path["weekday_flows"] if is_weekday is True else self.cache_path["weekend_flows"]
        tmp_folders = self.prepare_tmp_folders(cache_path)

        chains = self.get_chains(population, surveys, motives, is_weekday)
        sinks = self.get_sinks(chains, motives, population.transport_zones)
        costs = self.get_current_costs(costs_aggregator, congestion=False)

        random_switch_rate = 1.0/(1.0+random_switch*np.arange(1, n_iterations+1))
        previous_flows = None        
        remaining_sinks = sinks.clone()
        
        for iteration in range(0, n_iterations):
            
            logging.info(f"Sampling step n°{iteration}")
            
            utilities = self.get_utilities(
                motives,
                population.transport_zones,
                remaining_sinks,
                costs,
                cost_uncertainty_sd
            )
            
            dest_prob = self.get_destination_probability(
                utilities,
                motives
            )
            
            self.spatialize_trip_chains(iteration, chains, dest_prob, motives, alpha, tmp_folders)
            self.search_top_k_mode_sequences(iteration, costs_aggregator, tmp_folders)
            
            flows = self.assign_flow_volumes(iteration, chains, previous_flows, tmp_folders)
            flows = self.assign_modes(iteration, flows, costs_aggregator, tmp_folders)
            
            if previous_flows is not None:
                flows = pl.concat([flows, previous_flows])
            
            costs = self.update_costs(costs, iteration, n_iter_per_cost_update, flows, costs_aggregator)
            flows = self.unassign_overflow(flows, remaining_sinks)
            flows = self.unassign_optim(flows, costs, delta_cost_change)
            flows = self.unassign_random(flows, random_switch_rate[iteration])
            
            previous_flows, remaining_sinks, chains = self.prepare_next_iteration_vars(flows, sinks)
            
        flows = self.disaggregate_by_mode(flows_path, n_iterations, costs_aggregator)

        flows = flows.with_columns(
            is_weekday=pl.lit(is_weekday)
        )

        return flows
    

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
        pop_groups = (
            
            pl.scan_parquet(population.get()["population_groups"])
            .rename({"socio_pro_category": "csp"})
            .join(lau_to_city_cat.lazy(), on=["local_admin_unit_id"])
            .group_by(["country", "transport_zone_id", "city_category", "csp", "n_cars"])
            .agg(pl.col("weight").sum())

            # Cast strings to enums to speed things up
            .with_columns(
                country=pl.col("country").cast(pl.Enum(countries)),
                city_category=pl.col("city_category").cast(pl.Enum(["C", "B", "R", "I"])),
                csp=pl.col("csp").cast(pl.Enum(["1", "2", "3", "4", "5", "6", "7", "8", "no_csp"])),
                n_cars=pl.col("n_cars").cast(pl.Enum(["0", "1", "2+"]))
            )

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
        
        chains = (

            pop_groups
            .join(p_chain, on=["country", "city_category", "csp", "n_cars"])
            .filter(pl.col("is_weekday") == is_weekday)
            .drop("is_weekday")
            .with_columns(n_subseq=pl.col("weight")*pl.col("p_subseq")) 
            
            .group_by(["transport_zone_id", "csp", "motive_subseq", "subseq_step_index", "motive"])
            .agg(
                n_subseq=pl.col("n_subseq").sum(),
                duration=(
                    (
                        pl.col("duration_morning")
                        + pl.col("duration_midday")
                        + pl.col("duration_evening")
                    )
                    * pl.col("n_subseq")
                ).sum()
            )
            .with_columns(
                duration_per_subseq=pl.col("duration")/pl.col("n_subseq")
            )
        
        )


        return chains
    

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

        
    def spatialize_trip_chains(self, iteration, chains, dest_prob, motives, alpha, tmp_folders):
        
        
        chains_step = ( 
            chains
            .filter(pl.col("subseq_step_index") == 1)
            .with_columns(pl.col("transport_zone_id").alias("from"))
            .rename({"transport_zone_id": "home_zone_id"})
        )
        
        subseq_step_index = 1
        spatialized_chains = []
        
        while chains_step.height > 0:
            
            logging.info(f"Estimating flows sequence step n°{subseq_step_index}...")
            
            spatialized_step = ( 
                self.spatialize_trip_chains_step(chains_step, dest_prob, alpha)
                .with_columns(
                    subseq_step_index=pl.lit(subseq_step_index).cast(pl.UInt32)
                )
            )
            
            spatialized_chains.append(spatialized_step)
            
            # Create the next steps in the chains, using the latest locations as 
            # origins for the next trip
            subseq_step_index += 1
            
            chains_step = ( 
                chains
                .filter(pl.col("subseq_step_index") == subseq_step_index)
                .rename({"transport_zone_id": "home_zone_id"})
                .join(
                    (
                        spatialized_step
                        .select(["home_zone_id", "csp", "motive_subseq", "to"])
                        .rename({"to": "from"})
                    ),
                    on=["home_zone_id", "csp", "motive_subseq"]
                )
            )
            
        ( 
            pl.concat(spatialized_chains)
            .with_columns(iteration=pl.lit(iteration).cast(pl.UInt32))
            .write_parquet(tmp_folders["spatialized-chains"] / f"spatialized_chains_{iteration}.parquet")
        )
        
        
        
    def spatialize_trip_chains_step(self, chains_step, dest_prob, alpha):
        
        # Blend the probability to choose a destination based on current location
        # with the probability to choose this destination from home
        # When alpha = 0, people ignore where they live when they choose a destination (except for the first trip)
        # When alpha = 1, people ignore where they currently and only consider where they live when they choose a destination
        # When 0 < alpha < 1, people take the two into account

        steps = (
            
            chains_step.lazy()
            .filter(pl.col("motive") != "home")
            .select(["home_zone_id", "csp", "motive_subseq", "motive", "from"])
            
            .join(
                dest_prob,
                on=["motive", "from"]
            )
            
            .join(
                dest_prob
                .select(["motive", "from", "to", "p_ij"])
                .rename({"from": "home_zone_id", "p_ij": "p_ij_home"}),
                on=["home_zone_id", "motive", "to"],
                how="left"
            )
            
            # Some low probability destinations have no probability because
            # of the 99 % cutoff applied when computing p_ij, so we set them to zero
            .with_columns(
                p_ij_home=pl.col("p_ij_home").fill_null(0.0)
            )
            
            .with_columns(
                p_ij=( 
                    pl.when(pl.col("home_zone_id") == pl.col("from"))
                    .then(pl.col("p_ij"))
                    .otherwise(pl.col("p_ij").pow(1-alpha)*pl.col("p_ij_home").pow(alpha))
                )
            )
            .with_columns(p_ij=pl.col("p_ij")/pl.col("p_ij").sum().over(["motive_subseq", "motive", "home_zone_id", "csp", "from"]))
            
            # Keep only the first 99 % of the distribution
            .sort("p_ij", descending=True)
            .with_columns(
                p_ij_cum=pl.col("p_ij").cum_sum().over(["motive_subseq", "home_zone_id", "csp", "from", "motive"]),
                p_ij_count=pl.col("p_ij").cum_count().over(["motive_subseq", "home_zone_id", "csp", "from", "motive"])
            )
            .filter((pl.col("p_ij_cum") < 0.99) | (pl.col("p_ij_count") == 1))
            .with_columns(
                p_ij=pl.col("p_ij")/pl.col("p_ij").sum().over(["motive_subseq", "home_zone_id", "csp", "from", "motive"])
            )
            
            .collect(engine="streaming")
            
        )
        
        
        # Use the exponential sort trick to sample destinations based on their probabilities
        # (because polars cannot do weighted sampling like pandas)
        # https://timvieira.github.io/blog/post/2019/09/16/algorithms-for-sampling-without-replacement/
        
        noise = -np.log(np.random.rand(steps.height))
        
        steps = (
            
            steps.lazy()
            .with_columns(pl.Series("noise", noise))
            .with_columns(
                sample_score=pl.col("noise")/pl.col("p_ij")
            )
            
            .sort(["sample_score"])
            .group_by(["home_zone_id", "csp", "motive_subseq", "motive", "from"])
            .head(1)
            
            .select(["home_zone_id", "csp", "motive_subseq", "motive", "from", "to", "p_ij"])
            .collect(engine="streaming")
            
        )
        
        # Add the back to home step
        steps_home = (
            chains_step
            .filter(pl.col("motive") == "home")
            .with_columns(
                p_ij=1.0,
                to=pl.col("home_zone_id")
            )
            .select(["home_zone_id", "csp", "motive_subseq", "motive", "from", "to", "p_ij"])
        )
        
        steps = pl.concat([steps, steps_home])
        
        return steps
    
    
    def search_top_k_mode_sequences(self, iteration, costs_aggregator, tmp_folders):
        
        parent_folder_path = tmp_folders["spatialized-chains"].parent
        
        chains_path = tmp_folders["spatialized-chains"] / f"spatialized_chains_{iteration}.parquet"
        costs_path = parent_folder_path / "tmp-costs.parquet"
        modes_props_path = parent_folder_path / "modes-props.json"
        tmp_path = parent_folder_path / "tmp_results"
        output_path = tmp_folders["modes"] / f"mode_sequences_{iteration}.parquet"
        
        shutil.rmtree(tmp_path, ignore_errors=True)
        os.makedirs(tmp_path)
        
        # Write the costs as parquet so the parallel processes can use them
        ( 
            costs_aggregator.get_costs_by_od_and_mode(
                ["cost"],
                congestion=True,
                detail_distances=False
            )
            .write_parquet(costs_path)
        )
        
        # Format the modes info as a dict and save the result in a temp file
        with open(modes_props_path, "w") as f:
            f.write(modes_list_to_dict(costs_aggregator.modes))
        
        # Launch the mode sequence probability calculation
        with Live(Spinner("dots", text="Finding probable mode sequences for the spatialized trip chains..."), refresh_per_second=10):
        
            process = subprocess.Popen(
                [
                    "python",
                    "-u",
                    str(resources.files('mobility') / "transport_modes" / "compute_subtour_mode_probabilities.py"),
                    "--chains_path", str(chains_path),
                    "--costs_path", str(costs_path),
                    "--modes_path", str(modes_props_path),
                    "--output_path", str(output_path),
                    "--tmp_path", str(tmp_path)
                ]
            )
            
            process.wait()

        
        
        
    def assign_flow_volumes(self, iteration, chains, previous_flows, tmp_folders):
        
        logging.info("Computing the number of persons on each possible chain...")
        
        
        flows = (
            
            pl.scan_parquet(tmp_folders["spatialized-chains"] / f"spatialized_chains_{iteration}.parquet")
            .join(
                chains.rename({"transport_zone_id": "home_zone_id"}).lazy(),
                on=["home_zone_id", "csp", "motive_subseq", "motive", "subseq_step_index"]
            )
            .select([
                'home_zone_id', "csp", 'motive_subseq', 'motive', 'from', 'to',
                'subseq_step_index', 'iteration', 'n_subseq', 'duration', "duration_per_subseq"
            ])
            
            .collect(engine="streaming")
            
        )
        
        return flows
    
    
        
    
    def assign_modes(self, iteration, flows, costs_aggregator, tmp_folders):
        
        logging.info("Assigning modes...")
        
        mode_sequences = pl.read_parquet(tmp_folders["modes"] / f"mode_sequences_{iteration}.parquet")
        
        costs = costs_aggregator.get_costs_by_od_and_mode(["cost"], congestion=True, detail_distances=False)
        
        flows = (
            flows
            .join(mode_sequences, on=["home_zone_id", "csp", "motive_subseq", "subseq_step_index"])
        )
        
        p_mode_seq = ( 
            flows
            .join(costs, on=["from", "to", "mode"])
            .group_by(["home_zone_id", "csp", "motive_subseq", "mode_seq_index"])
            .agg(pl.col("cost").sum())
            .with_columns(
                p_mode_seq=pl.col("cost").neg().exp()/pl.col("cost").neg().exp().sum().over(["home_zone_id", "csp", "motive_subseq"])
            )
            
            # Keep only the first 99 % of the distribution
            .sort("p_mode_seq", descending=True)
            .with_columns(
                p_mode_seq_cum=pl.col("p_mode_seq").cum_sum().over(["home_zone_id", "csp", "motive_subseq"]),
                p_mode_seq_count=pl.col("p_mode_seq").cum_count().over(["home_zone_id", "csp", "motive_subseq"])
            )
            .filter((pl.col("p_mode_seq_cum") < 0.99) | (pl.col("p_mode_seq_count") == 1))
            .with_columns(
                p_ij=pl.col("p_mode_seq")/pl.col("p_mode_seq").sum().over(["home_zone_id", "csp", "motive_subseq"])
            )
            
            .select(["home_zone_id", "csp", "motive_subseq", "mode_seq_index", "p_mode_seq"])
        )
        
        flows = (
            flows 
            .join(p_mode_seq, on=["home_zone_id", "csp", "motive_subseq", "subseq_step_index", "mode_seq_index"])
            .with_columns(
                n_subseq=pl.col("n_subseq")*pl.col("p_mode_seq")
            )
            .select([
                'home_zone_id', 'csp', 'motive_subseq', "mode_seq_index",
                'motive', 'from', 'to', 'subseq_step_index', 'iteration',
                'n_subseq', 'duration', 'duration_per_subseq'
            ])
        )

        return flows
    
        
    
    
    def update_costs(self, iteration, n_iter_per_cost_update, flows, costs_aggregator):
        """
            If a cost update is needed, aggregate the flows by origin, destination
            and mode, compute the user equilibrium on the road network and 
            recompute the travel times and distances of the shortest paths 
            in after congestion.
        """
        
        if n_iter_per_cost_update > 0 and iteration > 0 and iteration % n_iter_per_cost_update == 0:
            
            od_flows_by_mode = (
                flows
                .group_by(["from", "to", "mode"])
                .agg(
                    flow_volume=pl.col("n_subseq").sum()
                )
            )
            
            costs_aggregator.update(od_flows_by_mode)
            costs = self.get_current_costs(costs_aggregator, congestion=True)

        return costs
        

    

        
    def unassign_overflow(self, flows, sinks):
        
        # Compute the share of persons in each OD flow that could not find an
        # opportunity because too many people chose the same destiation
        # p_overflow_motive = 1.0 - duration/available duration
        
        # A given chain is "overflowing" opportunities at destination if 
        # any one of the destinations is "overflowing", so :
        # p_overflow = max(p_overflow_motive)
        logging.info("Correcting flows for sink saturation...")
        
        flows_overflow = (
            
            flows
            
            .join(sinks, on=["motive", "to"], how="left")
            .with_columns(
                p_overflow=(
                    pl.when(pl.col("motive") == "home")
                    .then(0.0)
                    .otherwise((1.0 - 1.0/(pl.col("duration").sum().over(["to", "motive"])/pl.col("sink_duration"))).clip(0.0, 1.0))
                )
            )
            .with_columns(
                p_overflow_max=pl.col("p_overflow").max().over(["home_zone_id", "csp", "motive_subseq", "mode_seq_index", "iteration"])
            )
            .with_columns(
                overflow=pl.col("n_subseq")*pl.col("p_overflow_max")
            )
            .with_columns(
                n_subseq=pl.col("n_subseq") - pl.col("overflow")
            )
            
        )
        
        return flows_overflow
    
    
    
    def unassign_optim(self, flows, costs, delta_cost_change):
        
        # Compute the number of persons switching destinations after comparing their 
        # utility to the average utility of persons living in the same place
        # (adding X € to the no switch decision to account for transition costs)
        logging.info("Correcting flows for persons optimizing their cost...")
        
        p_seq_change = (
            
            flows
        
            .join(costs, on=["from", "to"])
            .group_by(["home_zone_id", "csp", "motive_subseq", "i"])
            .agg(
                cost=pl.col("cost").sum(),
                n_subseq=pl.col("n_subseq").first()
            )
            .with_columns(
                average_cost=(
                    (pl.col("cost")*pl.col("n_subseq"))
                    .sum().over(["home_zone_id", "csp", "motive_subseq"])
                    /
                    pl.col("n_subseq")
                    .sum().over(["home_zone_id", "csp", "motive_subseq"])
                )
            )
            .with_columns(
                delta_cost=pl.col("average_cost") - (pl.col("cost") + delta_cost_change)
            )
            .with_columns(
                p_seq_change=(
                    pl.when(pl.col("delta_cost").abs() > 10.0)
                    .then(pl.when(pl.col("delta_cost") > 0.0).then(0.0).otherwise(1.0))
                    .otherwise(1.0/(1.0+pl.col("delta_cost").exp()))
                )
            )
        
            .select(["home_zone_id", "csp", "motive_subseq", "i", "p_seq_change"])
            
        )
        
        flows_change = (
        
            flows
            .join(p_seq_change, on=["home_zone_id", "csp", "motive_subseq", "i"])
            .with_columns(
                change=pl.col("n_subseq")*pl.col("p_seq_change")
            )
            .with_columns(
                n_subseq=pl.col("n_subseq") - pl.col("change")
            )
            
        )
        
        return flows_change
    
    
    def unassign_random(self, flows, random_switch_rate):
        
        # Compute the share of persons switching destinations for random reasons
        flows_rand_switch = (
            
            flows
            .with_columns(
                random_switch=pl.col("n_subseq")*random_switch_rate
            )
            .with_columns(
                n_subseq=pl.col("n_subseq") - pl.col("random_switch"),
                delta_n_subseq=pl.col("overflow") + pl.col("change") + pl.col("random_switch")
            )
            .with_columns(
                duration=pl.col("n_subseq")*pl.col("duration_per_subseq"),
                delta_duration=pl.col("delta_n_subseq")*pl.col("duration_per_subseq")
            )
            
        )        
        
        return flows_rand_switch


    def prepare_next_iteration_vars(self, flows, sinks):

        # Compute the remaining number of opportunities by motive and destination
        # once assigned flows are accounted for
        remaining_sinks = (
        
            flows
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
        
        # Compute the number of unassigned persons by motive sequence and transport zone
        chains = (
            flows
            .group_by(["home_zone_id", "csp", "motive_subseq", "motive", "subseq_step_index"])
            .agg(
                n_subseq=pl.col("delta_n_subseq").sum(),
                duration=pl.col("delta_duration").sum()
            )
            .with_columns(
                duration_per_subseq=pl.col("duration")/pl.col("n_subseq")
            )
            .rename({"home_zone_id": "transport_zone_id"})
        )
        
        
        previous_flows = (
            flows
            .select(
                [
                    'home_zone_id', "csp", 'motive_subseq', 'motive', 'from', 'to',
                    'subseq_step_index', 'i', 'n_subseq', "duration", "duration_per_subseq"
                ]
            )
        )
        
        return previous_flows, remaining_sinks, chains
        
        
        
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
    
    
    def get_destination_probability(self, utilities, motives):
        
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
            .filter((pl.col("p_ij_cum") < 0.99) | (pl.col("p_count") == 1))
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
            .filter((pl.col("p_ij_cum") < 0.99) | (pl.col("p_count") == 1))
            .with_columns(p_ij=pl.col("p_ij")/pl.col("p_ij").sum().over(["from", "motive"]))
            
            .select(["motive", "from", "to", "p_ij"])
            
            # .collect(engine="streaming")
        )
        
        return prob
    
    
    def disaggregate_by_mode(self, flows_path, n_samples, costs):
        
        
        p_od_to_mode = costs.get_prob_by_od_and_mode(["cost"], congestion=True)
        
        # c = costs.get_costs_by_od_and_mode(["cost"], congestion=True, detail_distances=True)
        # c = c.filter(pl.col("from") == 71).filter(pl.col("to") == 492).to_pandas()
        # c = c.filter(pl.col("from") == 571).filter(pl.col("to") == 348).to_pandas()
        
        # costs.get_costs_by_od_and_mode(["cost"], congestion=True, detail_distances=False).write_parquet("d:/data/mobility/costs.parquet")
        # costs.get_prob_by_od_and_mode(["cost"], congestion=True).write_parquet("d:/data/mobility/probs.parquet")
        
        # Add symetrical mode names for multimodal modes
        mode_names =  [m.name for m in costs.modes]
        sym_mode_names = []
        for name in mode_names:
            if "public_transport" in name:
                legs = name.split("/")
                if legs[0] != legs[2]:
                    sym_mode_names.append(legs[2] + "/public_transport/" + legs[0])
        mode_names.extend(sym_mode_names)
        
        f = ( 
            pl.read_parquet(flows_path / f"flows_{n_samples-1}.parquet")
            .with_row_index()
            .select(["index", "from", "to"])
            .join(p_od_to_mode, ["from", "to"])
            .pivot(on="mode", index=["index", "from", "to"])
        )
        
        p_mat = f.select(mode_names).fill_null(1e-6).to_numpy()
        p_mat = np.repeat(p_mat[:, :, np.newaxis], 10, axis=2)
        E = -np.log(np.random.uniform(0, 1, size=p_mat.shape))/p_mat
        samples = np.argmin(E, axis=1)
        samples = pl.DataFrame(samples)
        samples.columns = [f"sample_{i}" for i in range(len(samples.columns))]
        

            
        flows = ( 
            pl.scan_parquet(flows_path / f"flows_{n_samples-1}.parquet")
            .join(p_od_to_mode.lazy(), on=["from", "to"])
            .with_columns(
                flow_volume=pl.col("n_subseq")*pl.col("prob")
            )
            .drop(["prob", "n_subseq"])
        )
        
        return flows


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
