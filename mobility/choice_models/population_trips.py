import os
import pathlib
import logging
import shutil

import polars as pl
import pandas as pd
import numpy as np

from scipy.stats import norm
from typing import List

from mobility.file_asset import FileAsset
from mobility.population import Population
from mobility.choice_models.travel_costs_aggregator import TravelCostsAggregator
from mobility.motives import Motive
from mobility.transport_modes.transport_mode import TransportMode
from mobility.parsers.mobility_survey import MobilitySurvey

class PopulationTrips(FileAsset):
    
    def __init__(
            self,
            population: Population,
            modes: List[TransportMode] = [],
            motives: List[Motive] = [],
            surveys: List[MobilitySurvey] = [],
            n_samples: int = 10,
            alpha: float = 0.5,
            n_iter_per_cost_update: int = 3,
            cost_uncertainty_sd: float = 1.0,
            delta_cost_change: float = 0.0,
            random_switch: float = 4.0
        ):

        costs_aggregator = TravelCostsAggregator(modes)
        
        inputs = {
            "population": population,
            "costs_aggregator": costs_aggregator,
            "motives": motives,
            "surveys": surveys,
            "n_samples": n_samples,
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
        n_samples = self.inputs["n_samples"]
        alpha = self.inputs["alpha"]
        n_iter_per_cost_update = self.inputs["n_iter_per_cost_update"]
        cost_uncertainty_sd = self.inputs["cost_uncertainty_sd"]
        delta_cost_change = self.inputs["delta_cost_change"]
        random_switch = self.inputs["random_switch"]
        
        cache_path = self.cache_path["weekday_flows"] if is_weekday is True else self.cache_path["weekend_flows"]
        inputs_hash = str(cache_path.stem).split("-")[0]
        chains_path = cache_path.parent / (inputs_hash + "-chains")
        flows_path = cache_path.parent / (inputs_hash + "-flows")
        
        self.prepare_folders(chains_path, flows_path)

        chains = self.get_chains(population, surveys, motives, is_weekday)
        sinks = self.get_sinks(chains, motives, population.transport_zones)
        costs = self.get_current_costs(costs_aggregator, congestion=False)

        random_switch_rate = 1.0/(1.0+random_switch*np.arange(1, n_samples+1))
        previous_flows = None        
        remaining_sinks = sinks.clone()
        
        for i in range(0, n_samples):
            
            logging.info(f"Sampling step n°{i}")
            
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
            
            self.spatialize_trip_chains(i, chains, dest_prob, motives, alpha, chains_path)
            
            flows, od_flows = self.assign_flow_volumes(i, chains, chains_path, previous_flows, flows_path)
            costs = self.update_costs(costs, i, n_iter_per_cost_update, od_flows, costs_aggregator)
            flows = self.unassign_overflow(flows, remaining_sinks)
            flows = self.unassign_optim(flows, costs, delta_cost_change)
            flows = self.unassign_random(flows, random_switch_rate[i])
            
            previous_flows, remaining_sinks, chains = self.prepare_next_iteration_vars(flows, sinks)
            
        flows = self.disaggregate_by_mode(flows_path, n_samples, costs_aggregator)

        flows = flows.with_columns(
            is_weekday=pl.lit(is_weekday)
        )

        return flows
    

    def prepare_folders(self, chains_path, flows_path):
        
        shutil.rmtree(chains_path, ignore_errors=True)
        shutil.rmtree(flows_path, ignore_errors=True)
        os.makedirs(chains_path)
        os.makedirs(flows_path)


    def get_chains(self, population, surveys, motives, is_weekday):

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
            
            .group_by(["transport_zone_id", "motive_subseq", "subseq_step_index", "motive"])
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
                        motive.get_opportunities(transport_zones)
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

        
    def spatialize_trip_chains(self, i, chains, dest_prob, motives, alpha, chains_path):
        
        
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
                        .select(["home_zone_id", "motive_subseq", "to"])
                        .rename({"to": "from"})
                    ),
                    on=["home_zone_id", "motive_subseq"]
                )
            )
            
        chains = ( 
            pl.concat(spatialized_chains)
            .with_columns(i=pl.lit(i).cast(pl.UInt32))
            .write_parquet(chains_path / f"chains_{i}.parquet")
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
            .select(["home_zone_id", "motive_subseq", "motive", "from"])
            
            .join(
                dest_prob.lazy(),
                on=["motive", "from"]
            )
            
            .join(
                dest_prob.lazy()
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
            .with_columns(p_ij=pl.col("p_ij")/pl.col("p_ij").sum().over(["motive_subseq", "motive", "home_zone_id", "from"]))
            
            # Keep only the first 99 % of the distribution
            .sort("p_ij", descending=True)
            .with_columns(
                p_ij_cum=pl.col("p_ij").cum_sum().over(["motive_subseq", "home_zone_id", "from", "motive"]),
                p_ij_count=pl.col("p_ij").cum_count().over(["motive_subseq", "home_zone_id", "from", "motive"])
            )
            .filter((pl.col("p_ij_cum") < 0.99) | (pl.col("p_ij_count") == 1))
            .with_columns(
                p_ij=pl.col("p_ij")/pl.col("p_ij").sum().over(["motive_subseq", "home_zone_id", "from", "motive"])
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
            .group_by(["home_zone_id", "motive_subseq", "motive", "from"])
            .head(1)
            
            .select(["home_zone_id", "motive_subseq", "motive", "from", "to", "p_ij"])
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
            .select(["home_zone_id", "motive_subseq", "motive", "from", "to", "p_ij"])
        )
        
        steps = pl.concat([steps, steps_home])
        
        return steps

        
    def assign_flow_volumes(self, i, chains, chains_path, previous_flows, flows_path):
        
        logging.info("Computing the number of persons on each possible chain...")
        
        # Compute the probability of each sequence
        p_seq = (
            
            pl.scan_parquet(str(chains_path) + "/*.parquet")
            .filter(pl.col("subseq_step_index") == 1)
            .with_columns(
                p_seq=pl.col("p_ij").log().sum().over(["home_zone_id", "motive_subseq"]).exp()
            )
            .with_columns(
                p_seq=pl.col('p_seq') / pl.col('p_seq').sum().over(["home_zone_id", "motive_subseq"])
            )
            .select(["home_zone_id", "motive_subseq", "i", "p_seq"])
            
        )
        
        flows = (
            
            pl.scan_parquet(str(chains_path) + "/*.parquet")
            .join(p_seq, on=["home_zone_id", "motive_subseq", "i"])
            
            # Compute the number of persons at each destination, for each motive
            .join(
                chains.rename({"transport_zone_id": "home_zone_id"}).lazy(),
                on=["home_zone_id", "motive_subseq", "motive", "subseq_step_index"]
            )
            .with_columns(
                n_subseq=pl.col("n_subseq")*pl.col("p_seq"),
                duration=pl.col("duration")*pl.col("p_seq")
            )
            .select([
                'home_zone_id', 'motive_subseq', 'motive', 'from', 'to',
                'subseq_step_index', 'i', 'n_subseq', 'duration', "duration_per_subseq"
            ])
            
            .collect(engine="streaming")
            
        )
        
        
        if previous_flows is not None:
            
            logging.info("Combining with the flows from the previous steps...")
            
            flows = pl.concat([flows, previous_flows])
            flows = (
                flows
                .group_by(["home_zone_id", "motive_subseq", "motive", "from", "to", "subseq_step_index", "i"])
                .agg(
                    n_subseq=pl.col("n_subseq").sum(),
                    duration=pl.col("duration").sum()
                )
                .with_columns(
                    duration_per_subseq=pl.col("duration")/pl.col("n_subseq")
                )
            )
            
        
        ( 
            flows
            .with_columns(i=pl.lit(i).cast(pl.UInt32))
            .write_parquet(flows_path / f"flows_{i}.parquet")
        )
        
        # Compute the origin - destination flows
        od_flows = (
            
            flows
            .group_by(["from", "to"])
            .agg(
                flow_volume=pl.col("n_subseq").sum()
            )
            
        )
        
        return flows, od_flows
    

    def update_costs(self, costs, i, n_iter_per_cost_update, od_flows, costs_aggregator):

        if n_iter_per_cost_update > 0 and i > 0 and i % n_iter_per_cost_update == 0: 
            costs_aggregator.update(od_flows)
            costs = self.get_current_costs(costs_aggregator, congestion=False)

        return costs
        
        
    def unassign_overflow(self, flows, sinks):
        
        # Compute the share of persons in each OD flow that could not find an
        # opportunity because too many people chose the same destiation
        # p_overflow_motive = 1.0 - duration/available duration
        
        # A given chain is "overflowing" opportunities at destination if 
        # at soon as one of the destinations is "overflowing", so :
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
                p_overflow_max=pl.col("p_overflow").max().over(["home_zone_id", "motive_subseq", "i"])
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
            .group_by(["home_zone_id", "motive_subseq", "i"])
            .agg(
                cost=pl.col("cost").sum(),
                n_subseq=pl.col("n_subseq").first()
            )
            .with_columns(
                average_cost=(
                    (pl.col("cost")*pl.col("n_subseq"))
                    .sum().over(["home_zone_id", "motive_subseq"])
                    /
                    pl.col("n_subseq")
                    .sum().over(["home_zone_id", "motive_subseq"])
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
        
            .select(["home_zone_id", "motive_subseq", "i", "p_seq_change"])
            
        )
        
        flows_change = (
        
            flows
            .join(p_seq_change, on=["home_zone_id", "motive_subseq", "i"])
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
            .group_by(["home_zone_id", "motive_subseq", "motive", "subseq_step_index"])
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
                    'home_zone_id', 'motive_subseq', 'motive', 'from', 'to',
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
            .collect()
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
            .join(cost_bin_to_dest.lazy(), on=["motive", "from", "cost_bin"])
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
            
            .collect(engine="streaming")
        )
        
        return prob
    
    
    def disaggregate_by_mode(self, flows_path, n_samples, costs):
        
        p_od_to_mode = costs.get_prob_by_od_and_mode(["cost"], congestion=True)
            
        flows = ( 
            pl.scan_parquet(flows_path / f"flows_{n_samples-1}.parquet")
            .join(p_od_to_mode.lazy(), on=["from", "to"])
            .with_columns(
                flow_volume=pl.col("n_subseq")*pl.col("prob")
            )
            .drop(["prob", "n_subseq"])
        )
        
        return flows