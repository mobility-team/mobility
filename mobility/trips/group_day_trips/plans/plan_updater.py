import logging
import math
from typing import Any

import polars as pl

from mobility.trips.group_day_trips.core.parameters import BehaviorChangeScope
from .destination_sequences import DestinationSequences
from .mode_sequences import ModeSequences
from ..transitions.transition_schema import TRANSITION_EVENT_COLUMNS


class PlanUpdater:
    """Updates population plan distributions over activity/destination/mode sequences."""

    def get_new_plans(
        self,
        current_plans: pl.DataFrame,
        current_plan_steps: pl.DataFrame | None,
        demand_groups: pl.DataFrame,
        chains: pl.DataFrame,
        costs_aggregator: Any,
        congestion_state: Any,
        remaining_opportunities: pl.DataFrame,
        activity_dur: pl.DataFrame,
        iteration: int,
        resolved_activity_parameters: dict[str, Any],
        destination_sequences: DestinationSequences,
        mode_sequences: ModeSequences,
        home_night_dur: pl.DataFrame,
        stay_home_plan: pl.DataFrame,
        parameters: Any,
    ) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
        """Advance one iteration of plan updates."""

        possible_plan_steps = self.get_possible_plan_steps(
            current_plans,
            current_plan_steps,
            demand_groups,
            chains,
            costs_aggregator,
            congestion_state,
            remaining_opportunities,
            activity_dur,
            iteration,
            resolved_activity_parameters,
            parameters.min_activity_time_constant,
            destination_sequences,
            mode_sequences,
        )
        self._assert_current_plans_covered_by_possible_plan_steps(
            current_plans,
            possible_plan_steps,
            iteration,
        )

        possible_plan_utility = self.get_possible_plan_utility(
            possible_plan_steps,
            home_night_dur,
            resolved_activity_parameters["home"].value_of_time_stay_home,
            stay_home_plan,
            parameters.min_activity_time_constant,
        )

        transition_prob = self.get_transition_probabilities(
            current_plans,
            possible_plan_utility,
            parameters.get_behavior_change_scope(iteration),
        )
        current_plans, transition_events = self.apply_transitions(
            current_plans,
            transition_prob,
            iteration,
        )
        transition_events = self.add_transition_plan_details(transition_events, possible_plan_steps)
        current_plan_steps = self.get_current_plan_steps(current_plans, possible_plan_steps)

        if current_plans["n_persons"].is_null().any() or current_plans["n_persons"].is_nan().any():
            raise ValueError("Null or NaN values in the n_persons column, something went wrong.")

        return current_plans, current_plan_steps, transition_events

    def _assert_current_plans_covered_by_possible_plan_steps(
        self,
        current_plans: pl.DataFrame,
        possible_plan_steps: pl.LazyFrame,
        iteration: int,
    ) -> None:
        """Fail when non-stay-home current plans have no step details."""
        plan_keys = ["demand_group_id", "activity_seq_id", "dest_seq_id", "mode_seq_id"]

        missing_current = (
            current_plans.lazy()
            .filter(pl.col("mode_seq_id") != 0)
            .select(plan_keys)
            .join(
                possible_plan_steps.select(plan_keys).unique(),
                on=plan_keys,
                how="anti",
            )
            .collect(engine="streaming")
        )
        if missing_current.height == 0:
            return

        sample = missing_current.head(5).to_dicts()
        raise ValueError(
            "Current non-stay-home plans are missing from possible_plan_steps "
            f"at iteration={iteration}. Missing={missing_current.height}. "
            f"Sample keys={sample}"
        )

    def get_possible_plan_steps(
        self,
        current_plans,
        current_plan_steps,
        demand_groups,
        chains,
        costs_aggregator,
        congestion_state,
        opportunities,
        activity_dur,
        iteration: int,
        resolved_activity_parameters: dict[str, Any],
        min_activity_time_constant,
        destination_sequences: DestinationSequences,
        mode_sequences: ModeSequences,
    ):
        """Enumerate candidate plan steps and compute per-step utilities."""

        cost_by_od_and_modes = costs_aggregator.get_costs_by_od_and_mode(
            ["cost", "distance", "time"],
            congestion=(congestion_state is not None),
            detail_distances=False,
            congestion_state=congestion_state,
        )

        chains_w_home = (
            chains.join(demand_groups.select(["demand_group_id", "csp"]), on="demand_group_id")
            .with_columns(duration_per_pers=pl.col("duration") / pl.col("n_persons"))
        )

        spat_chains = destination_sequences.get_cached_asset().lazy()
        modes = mode_sequences.get_cached_asset().lazy()
        value_of_time = (
            pl.from_dicts(
                [
                    {"activity": activity_name, "value_of_time": activity_parameters.value_of_time}
                    for activity_name, activity_parameters in resolved_activity_parameters.items()
                ]
            ).with_columns(
                activity=pl.col("activity").cast(pl.Enum(activity_dur["activity"].dtype.categories))
            )
        )

        possible_plan_step_columns = [
            "demand_group_id",
            "activity_seq_id",
            "dest_seq_id",
            "mode_seq_id",
            "seq_step_index",
            "activity",
            "from",
            "to",
            "mode",
            "duration_per_pers",
            "iteration",
            "csp",
            "cost",
            "distance",
            "time",
            "mean_duration_per_pers",
            "value_of_time",
            "k_saturation_utility",
            "min_activity_time",
            "utility",
        ]

        possible_plan_steps = (
            modes.join(spat_chains, on=["demand_group_id", "activity_seq_id", "dest_seq_id", "seq_step_index"])
            .join(chains_w_home.lazy(), on=["demand_group_id", "activity_seq_id", "seq_step_index"])
            .join(cost_by_od_and_modes.lazy(), on=["from", "to", "mode"])
            .join(activity_dur.lazy(), on=["csp", "activity"])
            .join(value_of_time.lazy(), on="activity")
            .join(
                opportunities.select(["to", "activity", "k_saturation_utility"]).lazy(),
                on=["to", "activity"],
                how="left",
            )
            .with_columns(
                k_saturation_utility=pl.col("k_saturation_utility").fill_null(1.0),
                min_activity_time=pl.col("mean_duration_per_pers") * math.exp(-min_activity_time_constant),
            )
            .with_columns(
                utility=(
                    pl.col("k_saturation_utility")
                    * pl.col("value_of_time")
                    * pl.col("mean_duration_per_pers")
                    * (pl.col("duration_per_pers") / pl.col("min_activity_time")).log().clip(0.0)
                    - pl.col("cost")
                )
            )
            .with_columns(iteration=pl.col("iteration"))
            .select(possible_plan_step_columns)
        )

        if current_plan_steps is not None:
            zero_mass_steps = (
                current_plan_steps.lazy()
                .filter(pl.col("n_persons") <= 0.0)
                .select(
                    [
                        "demand_group_id",
                        "activity_seq_id",
                        "dest_seq_id",
                        "mode_seq_id",
                        "seq_step_index",
                        "n_persons",
                    ]
                )
                .collect(engine="streaming")
            )
            if zero_mass_steps.height > 0:
                sample = zero_mass_steps.head(5).to_dicts()
                raise ValueError(
                    "Found carried-forward current_plan_steps with non-positive n_persons while "
                    f"building possible_plan_steps at iteration={iteration}. "
                    f"Invalid rows={zero_mass_steps.height}. Sample={sample}"
                )

            current_possible_plan_steps = (
                current_plan_steps.lazy()
                .with_columns(
                    duration_per_pers=pl.col("duration") / pl.col("n_persons"),
                    iteration=pl.lit(iteration).cast(pl.UInt32),
                )
                .join(demand_groups.select(["demand_group_id", "csp"]).lazy(), on="demand_group_id")
                # Keep current plans even when refreshed routing costs no longer
                # provide a path for one carried-forward OD/mode combination.
                # Those plans should remain valid "from" states for the
                # transition model, but with a very poor refreshed utility so
                # agents can move away from them.
                .join(
                    cost_by_od_and_modes.lazy(),
                    on=["from", "to", "mode"],
                    how="left",
                )
                .join(activity_dur.lazy(), on=["csp", "activity"])
                .join(value_of_time.lazy(), on="activity")
                .join(
                    opportunities.select(["to", "activity", "k_saturation_utility"]).lazy(),
                    on=["to", "activity"],
                    how="left",
                )
                .with_columns(
                    cost=pl.col("cost").fill_null(1e9),
                    distance=pl.col("distance").fill_null(0.0),
                    time=pl.col("time").fill_null(0.0),
                    k_saturation_utility=pl.col("k_saturation_utility").fill_null(1.0),
                    min_activity_time=pl.col("mean_duration_per_pers") * math.exp(-min_activity_time_constant),
                )
                .with_columns(
                    utility=(
                        pl.col("k_saturation_utility")
                        * pl.col("value_of_time")
                        * pl.col("mean_duration_per_pers")
                        * (pl.col("duration_per_pers") / pl.col("min_activity_time")).log().clip(0.0)
                        - pl.col("cost")
                    )
                )
                .select(possible_plan_step_columns)
            )

            possible_plan_steps = (
                pl.concat([possible_plan_steps, current_possible_plan_steps], how="vertical_relaxed")
                .unique(
                    subset=[
                        "demand_group_id",
                        "activity_seq_id",
                        "dest_seq_id",
                        "mode_seq_id",
                        "seq_step_index",
                    ],
                    keep="first",
                )
            )

        return possible_plan_steps

    def get_possible_plan_utility(
        self,
        possible_plan_steps,
        home_night_dur,
        value_of_time_stay_home,
        stay_home_plan,
        min_activity_time_constant,
    ):
        """Aggregate per-step utilities to plan-level utilities."""

        possible_plan_utility = (
            possible_plan_steps.group_by(
                ["demand_group_id", "csp", "activity_seq_id", "dest_seq_id", "mode_seq_id"]
            )
            .agg(
                utility=pl.col("utility").sum(),
                home_night_per_pers=24.0 - pl.col("duration_per_pers").sum(),
            )
            .join(home_night_dur.lazy(), on="csp")
            .with_columns(
                min_activity_time=pl.col("mean_home_night_per_pers") * math.exp(-min_activity_time_constant)
            )
            .with_columns(
                utility_stay_home=(
                    value_of_time_stay_home
                    * pl.col("mean_home_night_per_pers")
                    * (pl.col("home_night_per_pers") / pl.col("min_activity_time")).log().clip(0.0)
                )
            )
            .with_columns(utility=pl.col("utility") + pl.col("utility_stay_home"))
            .filter(pl.col("utility") > pl.col("utility").max().over(["demand_group_id"]) - 5.0)
            .select(["demand_group_id", "activity_seq_id", "mode_seq_id", "dest_seq_id", "utility"])
        )

        possible_plan_utility = pl.concat(
            [
                possible_plan_utility,
                stay_home_plan.lazy()
                .with_columns(
                    min_activity_time=pl.col("mean_home_night_per_pers") * math.exp(-min_activity_time_constant)
                )
                .with_columns(
                    utility=(
                        value_of_time_stay_home
                        * pl.col("mean_home_night_per_pers")
                        * (pl.col("mean_home_night_per_pers") / pl.col("min_activity_time")).log().clip(0.0)
                    )
                )
                .select(["demand_group_id", "activity_seq_id", "mode_seq_id", "dest_seq_id", "utility"]),
            ]
        )

        return possible_plan_utility

    def get_transition_probabilities(
        self,
        current_plans: pl.DataFrame,
        possible_plan_utility: pl.LazyFrame,
        behavior_change_scope: BehaviorChangeScope,
        transition_cost: float = 0.0,
    ) -> pl.DataFrame:
        """Compute transition probabilities from current to candidate plans."""

        plan_cols = ["demand_group_id", "activity_seq_id", "dest_seq_id", "mode_seq_id"]

        current_plans_for_transitions = current_plans.lazy()
        possible_plan_utility_for_transitions = possible_plan_utility

        if behavior_change_scope != BehaviorChangeScope.FULL_REPLANNING:
            current_plans_for_transitions = current_plans_for_transitions.filter(pl.col("mode_seq_id") != 0)
            possible_plan_utility_for_transitions = possible_plan_utility_for_transitions.filter(
                pl.col("mode_seq_id") != 0
            )

        scope_pair_constraint = pl.lit(True)
        if behavior_change_scope == BehaviorChangeScope.DESTINATION_REPLANNING:
            scope_pair_constraint = pl.col("activity_seq_id") == pl.col("activity_seq_id_trans")
        elif behavior_change_scope == BehaviorChangeScope.MODE_REPLANNING:
            scope_pair_constraint = (
                (pl.col("activity_seq_id") == pl.col("activity_seq_id_trans"))
                & (pl.col("dest_seq_id") == pl.col("dest_seq_id_trans"))
            )
        transition_probabilities = (
            current_plans_for_transitions
            .select(plan_cols + ["utility"])
            .rename({"utility": "utility_prev_from"})
            .join(possible_plan_utility_for_transitions, on=plan_cols)
            .join_where(
                possible_plan_utility_for_transitions,
                (
                    (pl.col("demand_group_id") == pl.col("demand_group_id_trans"))
                    & scope_pair_constraint
                    & (
                        (pl.col("utility_trans") > pl.col("utility") - 5.0)
                        | (
                            (pl.col("activity_seq_id") == pl.col("activity_seq_id_trans"))
                            & (pl.col("dest_seq_id") == pl.col("dest_seq_id_trans"))
                            & (pl.col("mode_seq_id") == pl.col("mode_seq_id_trans"))
                        )
                    )
                ),
                suffix="_trans",
            )
            .drop("demand_group_id_trans")
            .with_columns(
                utility_trans=pl.when(
                    (pl.col("activity_seq_id") == pl.col("activity_seq_id_trans"))
                    & (pl.col("dest_seq_id") == pl.col("dest_seq_id_trans"))
                    & (pl.col("mode_seq_id") == pl.col("mode_seq_id_trans"))
                )
                .then(pl.col("utility_trans") + transition_cost)
                .otherwise(pl.col("utility_trans"))
            )
            .with_columns(delta_utility=pl.col("utility_trans") - pl.col("utility_trans").max().over(plan_cols))
            .filter(pl.col("delta_utility") > -5.0)
            .with_columns(
                p_transition=pl.col("delta_utility").exp() / pl.col("delta_utility").exp().sum().over(plan_cols)
            )
            .sort("p_transition", descending=True)
            .with_columns(
                p_transition_cum=pl.col("p_transition").cum_sum().over(plan_cols),
                p_count=pl.col("p_transition").cum_count().over(plan_cols),
            )
            .filter((pl.col("p_transition_cum") < 0.99) | (pl.col("p_count") == 1))
            .with_columns(p_transition=pl.col("p_transition") / pl.col("p_transition").sum().over(plan_cols))
            .select(
                [
                    "demand_group_id",
                    "activity_seq_id",
                    "dest_seq_id",
                    "mode_seq_id",
                    "activity_seq_id_trans",
                    "dest_seq_id_trans",
                    "mode_seq_id_trans",
                    "utility_prev_from",
                    pl.col("utility").alias("utility_from_updated"),
                    "utility_trans",
                    "p_transition",
                ]
            )
            .collect(engine="streaming")
        )

        return transition_probabilities

    def apply_transitions(
        self,
        current_plans: pl.DataFrame,
        transition_probabilities: pl.DataFrame,
        iteration: int,
    ) -> tuple[pl.DataFrame, pl.DataFrame]:
        """Apply transition probabilities and emit transition events."""

        plan_cols = ["demand_group_id", "activity_seq_id", "dest_seq_id", "mode_seq_id"]

        transitions = (
            current_plans.join(transition_probabilities, on=plan_cols, how="left")
            .with_columns(
                p_transition=pl.col("p_transition").fill_null(1.0),
                utility_from_updated=pl.col("utility_from_updated").fill_null(pl.col("utility")),
                utility_trans=pl.coalesce([pl.col("utility_trans"), pl.col("utility")]),
                utility_prev_from=pl.coalesce([pl.col("utility_prev_from"), pl.col("utility")]),
                activity_seq_id_trans=pl.coalesce([pl.col("activity_seq_id_trans"), pl.col("activity_seq_id")]),
                dest_seq_id_trans=pl.coalesce([pl.col("dest_seq_id_trans"), pl.col("dest_seq_id")]),
                mode_seq_id_trans=pl.coalesce([pl.col("mode_seq_id_trans"), pl.col("mode_seq_id")]),
            )
            .with_columns(n_persons_moved=pl.col("n_persons") * pl.col("p_transition"))
        )

        prev_to_lookup = (
            current_plans.select(["demand_group_id", "activity_seq_id", "dest_seq_id", "mode_seq_id", "utility"])
            .rename(
                {
                    "activity_seq_id": "activity_seq_id_trans",
                    "dest_seq_id": "dest_seq_id_trans",
                    "mode_seq_id": "mode_seq_id_trans",
                    "utility": "utility_prev_to",
                }
            )
        )

        transitions = transitions.join(
            prev_to_lookup,
            on=["demand_group_id", "activity_seq_id_trans", "dest_seq_id_trans", "mode_seq_id_trans"],
            how="left",
        )

        transition_events = (
            transitions.with_columns(
                iteration=pl.lit(iteration).cast(pl.UInt32),
                utility_from=pl.col("utility_from_updated"),
                utility_to=pl.col("utility_trans"),
                utility_prev_from=pl.col("utility_prev_from"),
                utility_prev_to=pl.col("utility_prev_to"),
                is_self_transition=(
                    (pl.col("activity_seq_id") == pl.col("activity_seq_id_trans"))
                    & (pl.col("dest_seq_id") == pl.col("dest_seq_id_trans"))
                    & (pl.col("mode_seq_id") == pl.col("mode_seq_id_trans"))
                ),
            ).select(
                [
                    "iteration",
                    "demand_group_id",
                    "activity_seq_id",
                    "dest_seq_id",
                    "mode_seq_id",
                    "activity_seq_id_trans",
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
            transitions.group_by(
                ["demand_group_id", "activity_seq_id_trans", "dest_seq_id_trans", "mode_seq_id_trans"]
            )
            .agg(
                n_persons=pl.col("n_persons_moved").sum(),
                utility=pl.col("utility_trans").first(),
            )
            .rename(
                {
                    "activity_seq_id_trans": "activity_seq_id",
                    "dest_seq_id_trans": "dest_seq_id",
                    "mode_seq_id_trans": "mode_seq_id",
                }
            )
        )

        return new_states, transition_events

    def add_transition_plan_details(
        self,
        transition_events: pl.DataFrame,
        possible_plan_steps: pl.LazyFrame,
    ) -> pl.DataFrame:
        """Attach full from/to plan details to transition events."""
        plan_keys = ["demand_group_id", "activity_seq_id", "dest_seq_id", "mode_seq_id"]

        plan_details = (
            possible_plan_steps.with_columns(
                step_desc=pl.format(
                    "#{} | to: {} | activity: {} | mode: {} | dist_km: {} | time_h: {}",
                    pl.col("seq_step_index"),
                    pl.col("to").cast(pl.String),
                    pl.col("activity").cast(pl.String),
                    pl.col("mode").cast(pl.String),
                    pl.col("distance").fill_null(0.0).round(3),
                    pl.col("time").fill_null(0.0).round(3),
                )
            )
            .group_by(plan_keys)
            .agg(
                trip_count=pl.len().cast(pl.Float64),
                activity_time=pl.col("duration_per_pers").fill_null(0.0).sum(),
                travel_time=pl.col("time").fill_null(0.0).sum(),
                distance=pl.col("distance").fill_null(0.0).sum(),
                steps=pl.col("step_desc").sort_by("seq_step_index").str.join("<br>"),
            )
            .collect(engine="streaming")
        )

        from_details = plan_details.rename(
            {
                "trip_count": "trip_count_from",
                "activity_time": "activity_time_from",
                "travel_time": "travel_time_from",
                "distance": "distance_from",
                "steps": "steps_from",
            }
        )
        to_details = plan_details.rename(
            {
                "activity_seq_id": "activity_seq_id_trans",
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
            transition_events.filter(pl.col("mode_seq_id") != 0)
            .select(plan_keys)
            .join(from_details.select(plan_keys), on=plan_keys, how="anti")
        )
        missing_to_keys = (
            transition_events.filter(pl.col("mode_seq_id_trans") != 0)
            .select(["demand_group_id", "activity_seq_id_trans", "dest_seq_id_trans", "mode_seq_id_trans"])
            .join(
                to_details.select(
                    ["demand_group_id", "activity_seq_id_trans", "dest_seq_id_trans", "mode_seq_id_trans"]
                ),
                on=["demand_group_id", "activity_seq_id_trans", "dest_seq_id_trans", "mode_seq_id_trans"],
                how="anti",
            )
        )
        if missing_from_keys.height > 0 or missing_to_keys.height > 0:
            sample_from = missing_from_keys.head(5).to_dicts()
            sample_to = missing_to_keys.head(5).to_dicts()
            raise ValueError(
                "Transition keys are missing from plan-details lookup for non-stay-home plans. "
                f"Missing from-keys={missing_from_keys.height}, to-keys={missing_to_keys.height}. "
                f"Sample from={sample_from}. Sample to={sample_to}."
            )

        events_with_details = (
            transition_events.join(from_details, on=plan_keys, how="left").join(
                to_details,
                on=["demand_group_id", "activity_seq_id_trans", "dest_seq_id_trans", "mode_seq_id_trans"],
                how="left",
            )
        )

        missing_from = (
            events_with_details.filter(
                (pl.col("mode_seq_id") != 0)
                & (
                    pl.col("trip_count_from").is_null()
                    | pl.col("activity_time_from").is_null()
                    | pl.col("travel_time_from").is_null()
                    | pl.col("distance_from").is_null()
                    | pl.col("steps_from").is_null()
                )
            ).height
        )
        missing_to = (
            events_with_details.filter(
                (pl.col("mode_seq_id_trans") != 0)
                & (
                    pl.col("trip_count_to").is_null()
                    | pl.col("activity_time_to").is_null()
                    | pl.col("travel_time_to").is_null()
                    | pl.col("distance_to").is_null()
                    | pl.col("steps_to").is_null()
                )
            ).height
        )
        if missing_from > 0 or missing_to > 0:
            raise ValueError(
                "Transition details are missing for non-stay-home states "
                f"(from={missing_from}, to={missing_to}). "
                "This indicates inconsistent plan keys between transitions and possible plans."
            )

        return (
            events_with_details.with_columns(
                trip_count_from=pl.when(pl.col("mode_seq_id") == 0)
                .then(0.0)
                .otherwise(pl.col("trip_count_from"))
                .fill_null(0.0),
                activity_time_from=pl.when(pl.col("mode_seq_id") == 0)
                .then(24.0)
                .otherwise(pl.col("activity_time_from"))
                .fill_null(0.0),
                travel_time_from=pl.when(pl.col("mode_seq_id") == 0)
                .then(0.0)
                .otherwise(pl.col("travel_time_from"))
                .fill_null(0.0),
                distance_from=pl.when(pl.col("mode_seq_id") == 0)
                .then(0.0)
                .otherwise(pl.col("distance_from"))
                .fill_null(0.0),
                steps_from=pl.when(pl.col("mode_seq_id") == 0)
                .then(pl.lit("none"))
                .otherwise(pl.col("steps_from")),
                trip_count_to=pl.when(pl.col("mode_seq_id_trans") == 0)
                .then(0.0)
                .otherwise(pl.col("trip_count_to"))
                .fill_null(0.0),
                activity_time_to=pl.when(pl.col("mode_seq_id_trans") == 0)
                .then(24.0)
                .otherwise(pl.col("activity_time_to"))
                .fill_null(0.0),
                travel_time_to=pl.when(pl.col("mode_seq_id_trans") == 0)
                .then(0.0)
                .otherwise(pl.col("travel_time_to"))
                .fill_null(0.0),
                distance_to=pl.when(pl.col("mode_seq_id_trans") == 0)
                .then(0.0)
                .otherwise(pl.col("distance_to"))
                .fill_null(0.0),
                steps_to=pl.when(pl.col("mode_seq_id_trans") == 0)
                .then(pl.lit("none"))
                .otherwise(pl.col("steps_to")),
            ).select(TRANSITION_EVENT_COLUMNS)
        )

    def get_current_plan_steps(self, current_plans, possible_plan_steps):
        """Expand aggregate plans to per-step rows."""

        current_plan_steps = (
            current_plans.lazy()
            .join(
                possible_plan_steps.select(
                    [
                        "demand_group_id",
                        "activity_seq_id",
                        "dest_seq_id",
                        "mode_seq_id",
                        "seq_step_index",
                        "activity",
                        "from",
                        "to",
                        "mode",
                        "duration_per_pers",
                    ]
                ),
                on=["demand_group_id", "activity_seq_id", "dest_seq_id", "mode_seq_id"],
                how="left",
            )
            .with_columns(duration=pl.col("duration_per_pers").fill_null(24.0) * pl.col("n_persons"))
            .drop("duration_per_pers")
            .collect(engine="streaming")
        )

        return current_plan_steps

    def get_new_costs(
        self,
        costs,
        iteration,
        n_iter_per_cost_update,
        current_plan_steps,
        costs_aggregator,
        congestion_state=None,
        run_key=None,
        is_weekday=None,
    ):
        """Return the OD costs to use after the current iteration."""

        if n_iter_per_cost_update <= 0:
            return costs, congestion_state

        od_flows_by_mode = (
            current_plan_steps.filter(pl.col("activity_seq_id") != 0)
            .group_by(["from", "to", "mode"])
            .agg(flow_volume=pl.col("n_persons").sum())
        )

        return costs_aggregator.get_costs_for_next_iteration(
            iteration=iteration,
            cost_update_interval=n_iter_per_cost_update,
            od_flows_by_mode=od_flows_by_mode,
            congestion_state=congestion_state,
            run_key=run_key,
            is_weekday=is_weekday,
        )

    def get_new_opportunities(
        self,
        current_plan_steps,
        opportunities,
        resolved_activity_parameters: dict[str, Any],
    ):
        """Recompute remaining opportunities per (activity, destination)."""

        logging.info("Computing remaining opportunities at destinations...")
        saturation_fun_parameters = (
            pl.from_dicts(
                [
                    {
                        "activity": activity_name,
                        "beta": activity_parameters.saturation_fun_beta,
                        "ref_level": activity_parameters.saturation_fun_ref_level,
                    }
                    for activity_name, activity_parameters in resolved_activity_parameters.items()
                ]
            ).with_columns(activity=pl.col("activity").cast(pl.Enum(opportunities["activity"].dtype.categories)))
        )

        remaining_opportunities = (
            current_plan_steps.filter((pl.col("activity_seq_id") != 0) & (pl.col("activity") != "home"))
            .group_by(["to", "activity"])
            .agg(opportunity_occupation=pl.col("duration").sum())
            .join(opportunities, on=["to", "activity"], how="full", coalesce=True)
            .join(saturation_fun_parameters, on="activity")
            .with_columns(opportunity_occupation=pl.col("opportunity_occupation").fill_null(0.0))
            .with_columns(k=pl.col("opportunity_occupation") / pl.col("opportunity_capacity"))
            .with_columns(
                k_saturation_utility=(
                    1.0 - pl.col("k").pow(pl.col("beta")) / (pl.col("ref_level").pow(pl.col("beta")))
                ).clip(0.0)
            )
            .select(["activity", "to", "opportunity_capacity", "k_saturation_utility"])
        )

        return remaining_opportunities
