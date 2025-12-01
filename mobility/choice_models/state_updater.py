import logging
import math

import polars as pl


class StateUpdater:
    """Updates population state distributions over motive/destination/mode sequences.
    
    Builds candidate states, scores utilities (including home-night term),
    computes transition probabilities, applies transitions, and returns the
    updated aggregate states plus their per-step expansion.
    """
    
    def get_new_states(
            self,
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
            parameters,
            motives
        ):
        """Advance one iteration of state updates.

        Orchestrates: candidate step generation → state utilities →
        transition probabilities → transitioned states → per-step expansion.
        
        Args:
            current_states (pl.DataFrame): Current aggregate states with columns
                ["demand_group_id","motive_seq_id","dest_seq_id","mode_seq_id",
                 "utility","n_persons"].
            demand_groups (pl.DataFrame): Demand groups (e.g., csp, counts).
            chains (pl.DataFrame): Chain templates with durations per step.
            costs_aggregator (TravelCostsAggregator): Provides mode/OD costs.
            remaining_sinks (pl.DataFrame): Available opportunities per (motive,to).
            motive_dur (pl.DataFrame): Mean activity durations by (csp,motive).
            iteration (int): Current iteration (1-based).
            tmp_folders (dict[str, pathlib.Path]): Paths to “spatialized-chains” and “modes”.
            home_night_dur (pl.DataFrame): Mean remaining home-night duration by csp.
            stay_home_state (pl.DataFrame): Baseline “stay-home” state rows.
            parameters (PopulationTripsParameters): Coefficients and tunables.
        
        Returns:
            tuple[pl.DataFrame, pl.DataFrame]:
                - updated `current_states`
                - `current_states_steps` expanded to per-step rows.
        """
        
        possible_states_steps = self.get_possible_states_steps(
            current_states,
            demand_groups,
            chains,
            costs_aggregator,
            remaining_sinks,
            motive_dur,
            iteration,
            motives,
            parameters.min_activity_time_constant,
            tmp_folders
        )
        
        home_motive = [m for m in motives if m.name == "home"][0]
        
        possible_states_utility = self.get_possible_states_utility(
            possible_states_steps,
            home_night_dur,
            home_motive.value_of_time_stay_home,
            stay_home_state,
            parameters.min_activity_time_constant
        )
        
        transition_prob = self.get_transition_probabilities(current_states, possible_states_utility)
        current_states = self.apply_transitions(current_states, transition_prob)
        current_states_steps = self.get_current_states_steps(current_states, possible_states_steps)
        
        if current_states["n_persons"].is_null().any() or current_states["n_persons"].is_nan().any():
            raise ValueError("Null or NaN values in the n_persons column, something went wrong.")
        
        return current_states, current_states_steps
    
    def get_possible_states_steps(
            self,
            current_states,
            demand_groups,
            chains,
            costs_aggregator,
            sinks,
            motive_dur,
            iteration,
            motives,
            min_activity_time_constant,
            tmp_folders
        ):
        """Enumerate candidate state steps and compute per-step utilities.

        Joins latest spatialized chains and mode sequences, merges costs and
        mean activity durations, filters out saturated destinations, and
        computes per-step utility = activity utility − travel cost.
        
        Args:
            current_states (pl.DataFrame): Current aggregate states (used for scoping).
            demand_groups (pl.DataFrame): Demand groups with csp and sizes.
            chains (pl.DataFrame): Chain steps with durations per person.
            costs_aggregator (TravelCostsAggregator): Per-mode OD costs.
            sinks (pl.DataFrame): Remaining sinks per (motive,to).
            motive_dur (pl.DataFrame): Mean durations per (csp,motive).
            iteration (int): Current iteration to pick latest artifacts.
            activity_utility_coeff (float): Coefficient for activity utility.
            tmp_folders (dict[str, pathlib.Path]): Must contain "spatialized-chains" and "modes".
        
        Returns:
            pl.DataFrame: Candidate per-step rows with columns including
                ["demand_group_id","csp","motive_seq_id","dest_seq_id","mode_seq_id",
                 "seq_step_index","motive","from","to","mode",
                 "duration_per_pers","utility"].
        """
        
        
        cost_by_od_and_modes = ( 
            costs_aggregator.get_costs_by_od_and_mode(
                ["cost"],
                congestion=True,
                detail_distances=False
            )
        )
        
        chains_w_home = ( 
            chains
            .join(demand_groups.select(["demand_group_id", "csp"]), on="demand_group_id")
            .with_columns(duration_per_pers=pl.col("duration")/pl.col("n_persons"))
        )
        
        # Keep only the last occurrence of any motive - destination sequence
        # (the sampler might generate the same sequence twice)
        spat_chains = (
            pl.scan_parquet(tmp_folders['spatialized-chains'])
            .sort("iteration", descending=True)
            .unique(
                subset=["demand_group_id", "motive_seq_id", "dest_seq_id", "seq_step_index"],
                keep="first"
            )

        )
        
        # Keep only the last occurrence of any motive - destination - mode sequence
        # (the sampler might generate the same sequence twice)
        modes = (
            pl.scan_parquet(tmp_folders['modes'])
            .sort("iteration", descending=True)
            .unique(
                subset=["demand_group_id", "motive_seq_id", "dest_seq_id", "mode_seq_id", "seq_step_index"],
                keep="first"
            )

        )
        
        # Get the activities values of time
        value_of_time = ( 
            pl.from_dicts(
                [{"motive": m.name, "value_of_time": m.value_of_time} for m in motives]
            )
            .with_columns(
                motive=pl.col("motive").cast(pl.Enum(motive_dur["motive"].dtype.categories))
            )
        )
        
        possible_states_steps = (
            
            modes
            .join(spat_chains, on=["demand_group_id", "motive_seq_id", "dest_seq_id", "seq_step_index"])
            .join(chains_w_home.lazy(), on=["demand_group_id", "motive_seq_id", "seq_step_index"])
            .join(cost_by_od_and_modes.lazy(), on=["from", "to", "mode"])
            .join(motive_dur.lazy(), on=["csp", "motive"])
            .join(value_of_time.lazy(), on="motive")
            .join(sinks.select(["to", "motive", "k_saturation_utility"]).lazy(), on=["to", "motive"], how="left")
            
            .with_columns(
                k_saturation_utility=pl.col("k_saturation_utility").fill_null(1.0),
                min_activity_time=pl.col("mean_duration_per_pers")*math.exp(-min_activity_time_constant)
            )
            
            .with_columns(
                utility=( 
                    pl.col("k_saturation_utility")*pl.col("value_of_time")*pl.col("mean_duration_per_pers")*(pl.col("duration_per_pers")/pl.col("min_activity_time")).log().clip(0.0)
                    - pl.col("cost")
                )
            )
            
        )
        
        return possible_states_steps
        
    
    def get_possible_states_utility(
            self,
            possible_states_steps,
            home_night_dur,
            value_of_time_stay_home,
            stay_home_state,
            min_activity_time_constant
        ):
        """Aggregate per-step utilities to state-level utilities (incl. home-night).

        Sums step utilities per state, adds home-night utility, prunes dominated
        states, and appends the explicit stay-home baseline.
        
        Args:
            possible_states_steps (pl.DataFrame): Candidate step rows with per-step utility.
            home_night_dur (pl.DataFrame): Mean home-night duration by csp.
            stay_home_utility_coeff (float): Coefficient for home-night utility.
            stay_home_state (pl.DataFrame): Baseline state rows to append.
        
        Returns:
            pl.DataFrame: State-level utilities with
                ["demand_group_id","motive_seq_id","mode_seq_id","dest_seq_id","utility"].
        """
                    
        possible_states_utility = (
            
            possible_states_steps
            .group_by(["demand_group_id", "csp", "motive_seq_id", "dest_seq_id", "mode_seq_id"])
            .agg(
                utility=pl.col("utility").sum(),
                home_night_per_pers=24.0 - pl.col("duration_per_pers").sum()
            )
            
            .join(home_night_dur.lazy(), on="csp")
            .with_columns(
                min_activity_time=pl.col("mean_home_night_per_pers")*math.exp(-min_activity_time_constant)
            )
            .with_columns(
                utility_stay_home=( 
                    value_of_time_stay_home*pl.col("mean_home_night_per_pers")
                    * (pl.col("home_night_per_pers")/pl.col("min_activity_time")).log().clip(0.0)
                )
            )
            
            .with_columns(
                utility=pl.col("utility") + pl.col("utility_stay_home")
            )
            
            # Prune states that are below a certain distance from the best state
            # (because they will have very low probabilities to be selected)
            .filter(
                (pl.col("utility") > pl.col("utility").max().over(["demand_group_id"]) - 5.0)
            )
            
            .select(["demand_group_id", "motive_seq_id", "mode_seq_id", "dest_seq_id", "utility"])
        )
        
        possible_states_utility = ( 
            
            pl.concat([
                possible_states_utility,
                ( 
                    stay_home_state.lazy()
                    .select(["demand_group_id", "motive_seq_id", "mode_seq_id", "dest_seq_id", "utility"])
                )
            ])
            
        )
        
        return possible_states_utility
    
    
    
    def get_transition_probabilities(
            self,
            current_states,
            possible_states_utility,
            transition_cost: float = 0.0
        ): 
        """Compute transition probabilities from current to candidate states.

        Uses softmax over Δutility (with stabilization and pruning) within each
        demand group and current state key.
        
        Args:
            current_states (pl.DataFrame): Current states with utilities.
            possible_states_utility (pl.DataFrame): Candidate states with utilities.
        
        Returns:
            pl.DataFrame: Transitions with
                ["demand_group_id","motive_seq_id","dest_seq_id","mode_seq_id",
                 "motive_seq_id_trans","dest_seq_id_trans","mode_seq_id_trans",
                 "utility_trans","p_transition"].
        """
        
        state_cols = ["demand_group_id", "motive_seq_id", "dest_seq_id", "mode_seq_id"]
        
        transition_probabilities = (
            
            current_states.lazy()
            .select(state_cols)
            
            # Join the updated utility of the current states
            .join(possible_states_utility, on=state_cols)
            
            # Join the possible states when they can improve the utility compared to the current states
            # (join also the current state so it is included in the probability calculation)
            .join_where(
                possible_states_utility,
                (
                    (pl.col("demand_group_id") == pl.col("demand_group_id_trans")) &
                    (
                        (pl.col("utility_trans") > pl.col("utility") - 5.0) | 
                        (
                            (pl.col("motive_seq_id") == pl.col("motive_seq_id_trans")) & 
                            (pl.col("dest_seq_id") == pl.col("dest_seq_id_trans")) & 
                            (pl.col("mode_seq_id") == pl.col("mode_seq_id_trans"))
                        )
                    )
                ),
                suffix="_trans"
            )
            
            .drop("demand_group_id_trans")
            
            # Add a transition cost
            .with_columns(
                utility_trans=( 
                    pl.when(
                        (
                            (pl.col("motive_seq_id") == pl.col("motive_seq_id_trans")) & 
                            (pl.col("dest_seq_id") == pl.col("dest_seq_id_trans")) & 
                            (pl.col("mode_seq_id") == pl.col("mode_seq_id_trans"))
                        )
                    ).then(
                        pl.col("utility_trans") + transition_cost
                    ).otherwise(
                        pl.col("utility_trans")
                    )
                )
            )
            
            .with_columns(
                delta_utility=pl.col("utility_trans") - pl.col("utility_trans").max().over(state_cols)
            )
            
            .filter(
                (pl.col("delta_utility") > -5.0)
            )
            
            .with_columns(
                p_transition=pl.col("delta_utility").exp()/pl.col("delta_utility").exp().sum().over(state_cols)
            )
            
            # Keep only the first 99% of the distribution
            .sort("p_transition", descending=True)
            .with_columns(
                p_transition_cum=pl.col("p_transition").cum_sum().over(state_cols),
                p_count=pl.col("p_transition").cum_count().over(state_cols)
            )
            .filter((pl.col("p_transition_cum") < 0.99) | (pl.col("p_count") == 1))
            .with_columns(
                p_transition=( 
                    pl.col("p_transition")
                    /
                    pl.col("p_transition").sum()
                    .over(state_cols))
                )
            
            .select([
                "demand_group_id",
                "motive_seq_id", "dest_seq_id", "mode_seq_id",
                "motive_seq_id_trans", "dest_seq_id_trans", "mode_seq_id_trans",
                "utility_trans", "p_transition"
            ])
            
            .collect(engine="streaming")
        
        )
        
        return transition_probabilities
    
    
    def apply_transitions(self, current_states, transition_probabilities):
        """Apply transition probabilities to reweight populations and update states.
        
        Left-joins transitions onto current states, defaults to self-transition
        when absent, redistributes `n_persons` by `p_transition`, and aggregates
        by the new state keys.
        
        Args:
            current_states (pl.DataFrame): Current states with ["n_persons","utility"].
            transition_probabilities (pl.DataFrame): Probabilities produced by
                `get_transition_probabilities`.
        
        Returns:
            pl.DataFrame: Updated `current_states` aggregated by
                ["demand_group_id","motive_seq_id","dest_seq_id","mode_seq_id"].
        """
        
        state_cols = ["demand_group_id", "motive_seq_id", "dest_seq_id", "mode_seq_id"]
        
        new_states = (
            
            current_states
            .join(transition_probabilities, on=state_cols, how="left")
            .with_columns(
                p_transition=pl.col("p_transition").fill_null(1.0),
                utility=pl.coalesce([pl.col("utility_trans"), pl.col("utility")]),
                motive_seq_id=pl.coalesce([pl.col("motive_seq_id_trans"), pl.col("motive_seq_id")]),
                dest_seq_id=pl.coalesce([pl.col("dest_seq_id_trans"), pl.col("dest_seq_id")]),
                mode_seq_id=pl.coalesce([pl.col("mode_seq_id_trans"), pl.col("mode_seq_id")]),
            )
            .with_columns(
                n_persons=pl.col("n_persons")*pl.col("p_transition")
            )
            .group_by(state_cols)
            .agg(
                n_persons=pl.col("n_persons").sum(),
                utility=pl.col("utility").first()
            )  
           
            
        )
        
        return new_states
    
    
    def get_current_states_steps(self, current_states, possible_states_steps):
        """Expand aggregate states to per-step rows (flows).
        
        Joins selected states back to their step sequences and converts
        per-person durations to aggregate durations.
        
        Args:
            current_states (pl.DataFrame): Updated aggregate states.
            possible_states_steps (pl.DataFrame): Candidate steps universe.
        
        Returns:
            pl.DataFrame: Per-step flows with columns including
                ["demand_group_id","motive_seq_id","dest_seq_id","mode_seq_id",
                 "seq_step_index","motive","from","to","mode","n_persons","duration"].
        """
        
        current_states_steps = (
            current_states.lazy()
            .join(
                possible_states_steps.select([
                    "demand_group_id", "motive_seq_id", "dest_seq_id",
                    "mode_seq_id", "seq_step_index", "motive",
                    "from", "to", "mode", "duration_per_pers"
                ]),
                on=["demand_group_id", "motive_seq_id", "dest_seq_id", "mode_seq_id"],
                how="left"
            )
            .with_columns(
                duration=pl.col("duration_per_pers").fill_null(24.0)*pl.col("n_persons")
            )
            .drop("duration_per_pers")
            
            .collect(engine="streaming")
            
        )
        
        return current_states_steps
    
    
        
    
    def get_new_costs(self, costs, iteration, n_iter_per_cost_update, current_states_steps, costs_aggregator):
        """Optionally recompute congested costs from current flows.

        Aggregates OD flows by mode, updates network/user-equilibrium in the
        `costs_aggregator`, and returns refreshed costs when the cadence matches.
        
        Args:
            costs (pl.DataFrame): Current OD costs.
            iteration (int): Current iteration (1-based).
            n_iter_per_cost_update (int): Update cadence; 0 disables updates.
            current_states_steps (pl.DataFrame): Step-level flows (by mode).
            costs_aggregator (TravelCostsAggregator): Cost updater.
        
        Returns:
            pl.DataFrame: Updated OD costs (or original if no update ran).
        """
        
        if n_iter_per_cost_update > 0 and (iteration-1) % n_iter_per_cost_update == 0:
            
            logging.info("Updating costs...")
            
            od_flows_by_mode = (
                current_states_steps
                .filter(pl.col("motive_seq_id") != 0)
                .group_by(["from", "to", "mode"])
                .agg(
                    flow_volume=pl.col("n_persons").sum()
                )
            )
            
            costs_aggregator.update(od_flows_by_mode)
            costs = costs_aggregator.get(congestion=True)

        return costs
    
    
    def get_new_sinks(
            self,
            current_states_steps,
            sinks,
            motives
        ):
        """Recompute remaining opportunities per (motive, destination).
    
        Subtracts assigned durations from capacities, computes availability and a
        saturation utility factor.
    
        Args:
            current_states_steps (pl.DataFrame): Step-level assigned durations.
            sinks (pl.DataFrame): Initial capacities per (motive,to).
    
        Returns:
            pl.DataFrame: Updated sinks with
                ["motive","to","sink_capacity","sink_available","k_saturation_utility"].
        """
        
        logging.info("Computing remaining opportunities at destinations...")
        
        saturation_fun_parameters = ( 
            pl.from_dicts(
                [
                    {
                        "motive": m.name,
                        "beta": m.saturation_fun_beta,
                        "ref_level": m.saturation_fun_ref_level
                    }
                    for m in motives
                ]
            )
            .with_columns(
                motive=pl.col("motive").cast(pl.Enum(sinks["motive"].dtype.categories))
            )
        )

        # Compute the remaining number of opportunities by motive and destination
        # once assigned flows are accounted for
        remaining_sinks = (
        
            current_states_steps
            .filter(
                (pl.col("motive_seq_id") != 0) & 
                (pl.col("motive") != "home")
            )
            .group_by(["to", "motive"])
            .agg(
                sink_occupation=pl.col("duration").sum()
            )
            
            .join(sinks, on=["to", "motive"], how="full", coalesce=True)
            .join(saturation_fun_parameters, on="motive")
            
            .with_columns(
                sink_occupation=pl.col("sink_occupation").fill_null(0.0)
            )
            .with_columns(
                k=pl.col("sink_occupation")/pl.col("sink_capacity"),
                sink_available=(pl.col("sink_capacity") - pl.col("sink_occupation")).clip(0.0)
            )
            .with_columns(
                k_saturation_utility=(1.0 - pl.col("k").pow(pl.col("beta"))/(pl.col("ref_level").pow(pl.col("beta")))).clip(0.0)
            )
            .select(["motive", "to", "sink_capacity", "sink_available", "k_saturation_utility"])
            
        )
        
        return remaining_sinks