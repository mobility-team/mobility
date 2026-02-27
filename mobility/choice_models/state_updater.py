import logging
import math
from typing import Any

import polars as pl

from mobility.choice_models.transition_schema import TRANSITION_EVENT_COLUMNS

class StateUpdater:
    """Updates population state distributions over motive/destination/mode sequences.
    
    Builds candidate states, scores utilities (including home-night term),
    computes transition probabilities, applies transitions, and returns the
    updated aggregate states plus their per-step expansion.
    """
    
    def get_new_states(
            self,
            current_states: pl.DataFrame,
            demand_groups: pl.DataFrame,
            chains: pl.DataFrame,
            costs_aggregator: Any,
            remaining_sinks: pl.DataFrame,
            motive_dur: pl.DataFrame,
            iteration: int,
            tmp_folders: dict[str, Any],
            home_night_dur: pl.DataFrame,
            stay_home_state: pl.DataFrame,
            parameters: Any,
            motives: list[Any]
        ) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
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
            remaining_sinks (pl.DataFrame): Sink state per (motive,to) with
                capacity and saturation utility penalty.
            motive_dur (pl.DataFrame): Mean activity durations by (csp,motive).
            iteration (int): Current iteration (1-based).
            tmp_folders (dict[str, pathlib.Path]): Paths to “spatialized-chains” and “modes”.
            home_night_dur (pl.DataFrame): Mean remaining home-night duration by csp.
            stay_home_state (pl.DataFrame): Baseline “stay-home” state rows.
            parameters (PopulationTripsParameters): Coefficients and tunables.
        
        Returns:
            tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
                Updated current states, expanded per-step states, and transition events.
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
        self._assert_current_states_covered_by_possible_steps(
            current_states,
            possible_states_steps,
            iteration
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
        current_states, transition_events = self.apply_transitions(current_states, transition_prob, iteration)
        transition_events = self.add_transition_state_details(transition_events, possible_states_steps)
        current_states_steps = self.get_current_states_steps(current_states, possible_states_steps)
        
        if current_states["n_persons"].is_null().any() or current_states["n_persons"].is_nan().any():
            raise ValueError("Null or NaN values in the n_persons column, something went wrong.")
        
        return current_states, current_states_steps, transition_events

    def _assert_current_states_covered_by_possible_steps(
            self,
            current_states: pl.DataFrame,
            possible_states_steps: pl.LazyFrame,
            iteration: int
        ) -> None:
        """Fail when non-stay-home current states have no step details.

        Args:
            current_states (pl.DataFrame): Current aggregate states.
            possible_states_steps (pl.LazyFrame): Candidate state-step rows.
            iteration (int): Current model iteration.

        Raises:
            ValueError: If any non-stay-home current-state key is absent from
                `possible_states_steps`.
        """
        state_keys = ["demand_group_id", "motive_seq_id", "dest_seq_id", "mode_seq_id"]

        missing_current = (
            current_states.lazy()
            .filter(pl.col("mode_seq_id") != 0)
            .select(state_keys)
            .join(
                possible_states_steps.select(state_keys).unique(),
                on=state_keys,
                how="anti",
            )
            .collect(engine="streaming")
        )
        if missing_current.height == 0:
            return

        sample = missing_current.head(5).to_dicts()
        raise ValueError(
            "Current non-stay-home states are missing from possible_states_steps "
            f"at iteration={iteration}. Missing={missing_current.height}. "
            f"Sample keys={sample}"
        )
    
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
            sinks (pl.DataFrame): Sink state per (motive,to).
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
                ["cost", "distance", "time"],
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
            current_states: pl.DataFrame,
            possible_states_utility: pl.LazyFrame,
            transition_cost: float = 0.0
        ) -> pl.DataFrame:
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
            .select(state_cols + ["utility"])
            .rename({"utility": "utility_prev_from"})
            
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
                "utility_prev_from",
                pl.col("utility").alias("utility_from_updated"),
                "utility_trans",
                "p_transition"
            ])
            
            .collect(engine="streaming")
        
        )
        
        return transition_probabilities
    
    
    def apply_transitions(
            self,
            current_states: pl.DataFrame,
            transition_probabilities: pl.DataFrame,
            iteration: int
        ) -> tuple[pl.DataFrame, pl.DataFrame]:
        """Apply transition probabilities and emit transition events.
        
        Left-joins transitions onto current states, defaults to self-transition
        when absent, redistributes `n_persons` by `p_transition`, and aggregates
        by the new state keys.
        
        Args:
            current_states (pl.DataFrame): Current states with ["n_persons","utility"].
            transition_probabilities (pl.DataFrame): Probabilities produced by
                `get_transition_probabilities`.
        
        Returns:
            tuple[pl.DataFrame, pl.DataFrame]:
                - Updated `current_states`, aggregated by destination state keys.
                - `transition_events` with one row per realized transition split.
        """
        
        state_cols = ["demand_group_id", "motive_seq_id", "dest_seq_id", "mode_seq_id"]
        
        transitions = (
            current_states
            .join(transition_probabilities, on=state_cols, how="left")
            .with_columns(
                p_transition=pl.col("p_transition").fill_null(1.0),
                utility_from_updated=pl.col("utility_from_updated").fill_null(pl.col("utility")),
                utility_trans=pl.coalesce([pl.col("utility_trans"), pl.col("utility")]),
                utility_prev_from=pl.coalesce([pl.col("utility_prev_from"), pl.col("utility")]),
                motive_seq_id_trans=pl.coalesce([pl.col("motive_seq_id_trans"), pl.col("motive_seq_id")]),
                dest_seq_id_trans=pl.coalesce([pl.col("dest_seq_id_trans"), pl.col("dest_seq_id")]),
                mode_seq_id_trans=pl.coalesce([pl.col("mode_seq_id_trans"), pl.col("mode_seq_id")]),
            )
            .with_columns(
                n_persons_moved=pl.col("n_persons") * pl.col("p_transition")
            )
        )

        # Previous-iteration utility for destination state if it existed already.
        prev_to_lookup = (
            current_states
            .select(["demand_group_id", "motive_seq_id", "dest_seq_id", "mode_seq_id", "utility"])
            .rename(
                {
                    "motive_seq_id": "motive_seq_id_trans",
                    "dest_seq_id": "dest_seq_id_trans",
                    "mode_seq_id": "mode_seq_id_trans",
                    "utility": "utility_prev_to",
                }
            )
        )

        transitions = transitions.join(
            prev_to_lookup,
            on=["demand_group_id", "motive_seq_id_trans", "dest_seq_id_trans", "mode_seq_id_trans"],
            how="left",
        )

        transition_events = (
            transitions
            .with_columns(
                iteration=pl.lit(iteration).cast(pl.UInt32),
                utility_from=pl.col("utility_from_updated"),  # updated utility used by MNL for from-state
                utility_to=pl.col("utility_trans"),
                utility_prev_from=pl.col("utility_prev_from"),
                utility_prev_to=pl.col("utility_prev_to"),
                is_self_transition=(
                    (pl.col("motive_seq_id") == pl.col("motive_seq_id_trans"))
                    & (pl.col("dest_seq_id") == pl.col("dest_seq_id_trans"))
                    & (pl.col("mode_seq_id") == pl.col("mode_seq_id_trans"))
                ),
            )
            .select(
                [
                    "iteration",
                    "demand_group_id",
                    "motive_seq_id",
                    "dest_seq_id",
                    "mode_seq_id",
                    "motive_seq_id_trans",
                    "dest_seq_id_trans",
                    "mode_seq_id_trans",
                    "n_persons_moved",
                    "utility_prev_from",
                    "utility_prev_to",
                    "utility_from",
                    "utility_to",
                    "is_self_transition",
                ]
            )
        )

        new_states = (
            transitions
            .group_by(["demand_group_id", "motive_seq_id_trans", "dest_seq_id_trans", "mode_seq_id_trans"])
            .agg(
                n_persons=pl.col("n_persons_moved").sum(),
                utility=pl.col("utility_trans").first()
            )  
            .rename(
                {
                    "motive_seq_id_trans": "motive_seq_id",
                    "dest_seq_id_trans": "dest_seq_id",
                    "mode_seq_id_trans": "mode_seq_id",
                }
            )
        )
        
        return new_states, transition_events

    def add_transition_state_details(
            self,
            transition_events: pl.DataFrame,
            possible_states_steps: pl.LazyFrame
        ) -> pl.DataFrame:
        """Attach full from/to state details to transition events.

        This makes transition logs self-contained for diagnostics, so plotting
        code does not need to recover sequence details from final-state tables.

        Args:
            transition_events (pl.DataFrame): Transition rows produced by
                `apply_transitions`.
            possible_states_steps (pl.LazyFrame): Candidate state-step rows used
                to compute state-level details.

        Returns:
            pl.DataFrame: Transition events enriched with from/to state details.

        Raises:
            ValueError: If non-stay-home transition from/to keys are missing from
                the state-details lookup.
        """
        state_keys = ["demand_group_id", "motive_seq_id", "dest_seq_id", "mode_seq_id"]

        state_details = (
            possible_states_steps
            .with_columns(
                step_desc=pl.format(
                    "#{} | to: {} | motive: {} | mode: {} | dist_km: {} | time_h: {}",
                    pl.col("seq_step_index"),
                    pl.col("to").cast(pl.String),
                    pl.col("motive").cast(pl.String),
                    pl.col("mode").cast(pl.String),
                    pl.col("distance").fill_null(0.0).round(3),
                    pl.col("time").fill_null(0.0).round(3),
                )
            )
            .group_by(state_keys)
            .agg(
                trip_count=pl.len().cast(pl.Float64),
                activity_time=pl.col("duration_per_pers").fill_null(0.0).sum(),
                travel_time=pl.col("time").fill_null(0.0).sum(),
                distance=pl.col("distance").fill_null(0.0).sum(),
                steps=pl.col("step_desc").sort_by("seq_step_index").str.concat("<br>"),
            )
            .collect(engine="streaming")
        )

        from_details = state_details.rename(
            {
                "trip_count": "trip_count_from",
                "activity_time": "activity_time_from",
                "travel_time": "travel_time_from",
                "distance": "distance_from",
                "steps": "steps_from",
            }
        )
        to_details = state_details.rename(
            {
                "motive_seq_id": "motive_seq_id_trans",
                "dest_seq_id": "dest_seq_id_trans",
                "mode_seq_id": "mode_seq_id_trans",
                "trip_count": "trip_count_to",
                "activity_time": "activity_time_to",
                "travel_time": "travel_time_to",
                "distance": "distance_to",
                "steps": "steps_to",
            }
        )

        missing_from_keys = (
            transition_events
            .filter(pl.col("mode_seq_id") != 0)
            .select(state_keys)
            .join(from_details.select(state_keys), on=state_keys, how="anti")
        )
        missing_to_keys = (
            transition_events
            .filter(pl.col("mode_seq_id_trans") != 0)
            .select(["demand_group_id", "motive_seq_id_trans", "dest_seq_id_trans", "mode_seq_id_trans"])
            .join(
                to_details.select(["demand_group_id", "motive_seq_id_trans", "dest_seq_id_trans", "mode_seq_id_trans"]),
                on=["demand_group_id", "motive_seq_id_trans", "dest_seq_id_trans", "mode_seq_id_trans"],
                how="anti",
            )
        )
        if missing_from_keys.height > 0 or missing_to_keys.height > 0:
            sample_from = missing_from_keys.head(5).to_dicts()
            sample_to = missing_to_keys.head(5).to_dicts()
            raise ValueError(
                "Transition keys are missing from state-details lookup for non-stay-home states. "
                f"Missing from-keys={missing_from_keys.height}, to-keys={missing_to_keys.height}. "
                f"Sample from={sample_from}. Sample to={sample_to}."
            )

        events_with_details = (
            transition_events
            .join(from_details, on=state_keys, how="left")
            .join(
                to_details,
                on=["demand_group_id", "motive_seq_id_trans", "dest_seq_id_trans", "mode_seq_id_trans"],
                how="left",
            )
        )

        missing_from = (
            events_with_details
            .filter(
                (pl.col("mode_seq_id") != 0)
                & (
                    pl.col("trip_count_from").is_null()
                    | pl.col("activity_time_from").is_null()
                    | pl.col("travel_time_from").is_null()
                    | pl.col("distance_from").is_null()
                    | pl.col("steps_from").is_null()
                )
            )
            .height
        )
        missing_to = (
            events_with_details
            .filter(
                (pl.col("mode_seq_id_trans") != 0)
                & (
                    pl.col("trip_count_to").is_null()
                    | pl.col("activity_time_to").is_null()
                    | pl.col("travel_time_to").is_null()
                    | pl.col("distance_to").is_null()
                    | pl.col("steps_to").is_null()
                )
            )
            .height
        )
        if missing_from > 0 or missing_to > 0:
            raise ValueError(
                "Transition details are missing for non-stay-home states "
                f"(from={missing_from}, to={missing_to}). "
                "This indicates inconsistent state keys between transitions and possible states."
            )

        return (
            events_with_details
            .with_columns(
                trip_count_from=pl.when(pl.col("mode_seq_id") == 0).then(0.0).otherwise(pl.col("trip_count_from")).fill_null(0.0),
                activity_time_from=pl.when(pl.col("mode_seq_id") == 0).then(24.0).otherwise(pl.col("activity_time_from")).fill_null(0.0),
                travel_time_from=pl.when(pl.col("mode_seq_id") == 0).then(0.0).otherwise(pl.col("travel_time_from")).fill_null(0.0),
                distance_from=pl.when(pl.col("mode_seq_id") == 0).then(0.0).otherwise(pl.col("distance_from")).fill_null(0.0),
                steps_from=pl.when(pl.col("mode_seq_id") == 0).then(pl.lit("none")).otherwise(pl.col("steps_from")),
                trip_count_to=pl.when(pl.col("mode_seq_id_trans") == 0).then(0.0).otherwise(pl.col("trip_count_to")).fill_null(0.0),
                activity_time_to=pl.when(pl.col("mode_seq_id_trans") == 0).then(24.0).otherwise(pl.col("activity_time_to")).fill_null(0.0),
                travel_time_to=pl.when(pl.col("mode_seq_id_trans") == 0).then(0.0).otherwise(pl.col("travel_time_to")).fill_null(0.0),
                distance_to=pl.when(pl.col("mode_seq_id_trans") == 0).then(0.0).otherwise(pl.col("distance_to")).fill_null(0.0),
                steps_to=pl.when(pl.col("mode_seq_id_trans") == 0).then(pl.lit("none")).otherwise(pl.col("steps_to")),
            )
            .select(TRANSITION_EVENT_COLUMNS)
        )
    
    
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
    
    
        
    
    def get_new_costs(self, costs, iteration, n_iter_per_cost_update, current_states_steps, costs_aggregator, run_key=None):
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

            has_congestion = any(getattr(m, "congestion", False) for m in costs_aggregator.modes)

            # Only build/update congestion snapshots when at least one mode handles congestion.
            if has_congestion:
                costs_aggregator.update(od_flows_by_mode, run_key=run_key, iteration=iteration)
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
                ["motive","to","sink_capacity","k_saturation_utility"].
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
                k=pl.col("sink_occupation")/pl.col("sink_capacity")
            )
            .with_columns(
                k_saturation_utility=(1.0 - pl.col("k").pow(pl.col("beta"))/(pl.col("ref_level").pow(pl.col("beta")))).clip(0.0)
            )
            .select(["motive", "to", "sink_capacity", "k_saturation_utility"])
            
        )
        
        return remaining_sinks
