import logging
import math
from typing import Any

import polars as pl

from mobility.trips.group_day_trips.core.parameters import BehaviorChangeScope
from mobility.trips.group_day_trips.core.memory_logging import log_memory_checkpoint
from ..transitions.transition_events import (
    add_transition_plan_details,
    build_transition_events_lazy,
)
from .candidate_plan_steps import CandidatePlanStepsAsset
from .plan_distance import PlanDistance
from .plan_ids import PLAN_KEY_COLS, add_plan_id
from .destination_sequences import DestinationSequences
from .mode_sequences import ModeSequences


class PlanUpdater:
    """Updates population plan distributions over activity/destination/mode sequences."""

    @staticmethod
    def _compress_step_state(
        frame: pl.DataFrame,
        *,
        drop_plan_id: bool,
    ) -> pl.DataFrame:
        """Reduce in-memory footprint of persisted step-state tables."""
        mode_values = []
        if "mode" in frame.columns:
            mode_values = frame.select(pl.col("mode").cast(pl.String).drop_nulls().unique()).to_series().to_list()
            mode_values = sorted(mode_values)

        columns: list[pl.Expr] = []
        for name in frame.columns:
            if drop_plan_id and name == "plan_id":
                continue

            expr = pl.col(name)
            if name in {"from", "to"}:
                expr = expr.cast(pl.UInt16)
            elif name == "seq_step_index":
                expr = expr.cast(pl.UInt8)
            elif name in {"iteration", "first_seen_iteration", "last_active_iteration"}:
                expr = expr.cast(pl.UInt16)
            elif name in {"departure_time", "arrival_time", "next_departure_time", "duration_per_pers", "duration"}:
                expr = expr.cast(pl.Float32)
            elif name == "mode" and mode_values:
                expr = expr.cast(pl.String).cast(pl.Enum(mode_values))
            columns.append(expr.alias(name))

        return frame.select(columns)

    @staticmethod
    def _ensure_plan_id(frame: pl.DataFrame | pl.LazyFrame) -> pl.DataFrame | pl.LazyFrame:
        """Add a temporary plan id from the plan keys when tests pass raw frames."""
        schema_names = frame.collect_schema().names() if isinstance(frame, pl.LazyFrame) else frame.columns
        if "plan_id" in schema_names:
            return frame

        return frame.with_columns(
            plan_id=pl.concat_str(
                [
                    pl.col("demand_group_id").cast(pl.String),
                    pl.col("activity_seq_id").cast(pl.String),
                    pl.col("time_seq_id").cast(pl.String),
                    pl.col("dest_seq_id").cast(pl.String),
                    pl.col("mode_seq_id").cast(pl.String),
                ],
                separator="|",
            )
        )

    def get_new_plans(
        self,
        current_plans: pl.DataFrame,
        current_plan_steps: pl.DataFrame | None,
        candidate_plan_steps: pl.DataFrame | None,
        demand_groups: pl.DataFrame,
        survey_plan_steps: pl.DataFrame,
        transport_costs: Any,
        destination_saturation: pl.DataFrame,
        activity_dur: pl.DataFrame,
        iteration: int,
        resolved_activity_parameters: dict[str, Any],
        destination_sequences: DestinationSequences,
        mode_sequences: ModeSequences,
        home_night_dur: pl.DataFrame,
        stay_home_plan: pl.DataFrame,
        transport_zones: Any,
        sequence_index_folder,
        parameters: Any,
    ) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.LazyFrame | None]:
        """Advance one iteration of plan updates."""

        possible_plan_steps = self.get_possible_plan_steps(
            current_plans,
            current_plan_steps,
            candidate_plan_steps,
            demand_groups,
            survey_plan_steps,
            transport_costs,
            destination_saturation,
            activity_dur,
            iteration,
            resolved_activity_parameters,
            parameters.min_activity_time_constant,
            destination_sequences,
            mode_sequences,
            parameters,
        )
        log_memory_checkpoint(
            f"plan_updater:iteration:{iteration}:possible_plan_steps_lazy",
            possible_plan_steps=possible_plan_steps,
        )
        possible_plan_steps = add_plan_id(possible_plan_steps, index_folder=sequence_index_folder)
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
            sequence_index_folder=sequence_index_folder,
        )
        candidate_plan_steps = possible_plan_steps.select(
            CandidatePlanStepsAsset.STRUCTURAL_COLUMNS
            + CandidatePlanStepsAsset.RETENTION_COLUMNS
        ).collect(engine="streaming")
        candidate_plan_steps = self._compress_step_state(
            candidate_plan_steps,
            drop_plan_id=False,
        )
        log_memory_checkpoint(
            f"plan_updater:iteration:{iteration}:candidate_plan_steps",
            candidate_plan_steps=candidate_plan_steps,
        )
        possible_plan_steps = self.add_stay_home_plan_steps(
            possible_plan_steps,
            stay_home_plan,
            sequence_index_folder=sequence_index_folder,
        ).collect(
            engine="streaming"
        )
        log_memory_checkpoint(
            f"plan_updater:iteration:{iteration}:possible_plan_steps",
            possible_plan_steps=possible_plan_steps,
        )

        transition_prob = self.get_transition_probabilities(
            current_plans,
            possible_plan_utility,
            possible_plan_steps,
            parameters.get_behavior_change_scope(iteration),
            transport_zones=transport_zones,
            transition_distance_threshold=parameters.transition_distance_threshold,
            enable_transition_distance_model=parameters.enable_transition_distance_model,
            transition_revision_probability=parameters.transition_revision_probability,
            transition_logit_scale=parameters.transition_logit_scale,
            transition_utility_pruning_delta=parameters.transition_utility_pruning_delta,
            transition_distance_friction=parameters.transition_distance_friction,
            plan_embedding_dimension_weights=parameters.plan_embedding_dimension_weights,
        )
        log_memory_checkpoint(
            f"plan_updater:iteration:{iteration}:transition_probabilities",
            transition_prob=transition_prob,
        )
        current_plans, transition_events = self.apply_transitions(
            current_plans,
            transition_prob,
            iteration,
        )
        log_memory_checkpoint(
            f"plan_updater:iteration:{iteration}:after_apply_transitions",
            current_plans=current_plans,
            transition_events=transition_events,
        )
        current_plan_steps = self.get_current_plan_steps(current_plans, possible_plan_steps.lazy())
        log_memory_checkpoint(
            f"plan_updater:iteration:{iteration}:current_plan_steps",
            current_plan_steps=current_plan_steps,
        )
        if parameters.save_transition_events:
            transition_events = add_transition_plan_details(
                transition_events,
                possible_plan_steps.lazy(),
            )
        else:
            transition_events = None

        if current_plans["n_persons"].is_null().any() or current_plans["n_persons"].is_nan().any():
            raise ValueError("Null or NaN values in the n_persons column, something went wrong.")

        return current_plans, current_plan_steps, candidate_plan_steps, transition_events

    def _assert_current_plans_covered_by_possible_plan_steps(
        self,
        current_plans: pl.DataFrame,
        possible_plan_steps: pl.LazyFrame,
        iteration: int,
    ) -> None:
        """Fail when non-stay-home current plans have no step details."""
        plan_keys = ["demand_group_id", "activity_seq_id", "time_seq_id", "dest_seq_id", "mode_seq_id"]

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
        candidate_plan_steps,
        demand_groups,
        survey_plan_steps,
        transport_costs,
        destination_saturation,
        activity_dur,
        iteration: int,
        resolved_activity_parameters: dict[str, Any],
        min_activity_time_constant,
        destination_sequences: DestinationSequences,
        mode_sequences: ModeSequences,
        parameters: Any,
    ):
        """Enumerate candidate plan steps and compute per-step utilities."""
        aggregated_candidates = CandidatePlanStepsAsset.build_candidate_memory(
            destination_sequences=destination_sequences,
            mode_sequences=mode_sequences,
            survey_plan_steps=survey_plan_steps,
            demand_groups=demand_groups,
            current_plans=current_plans,
            previous_candidate_plan_steps=candidate_plan_steps,
            current_iteration=iteration,
            n_warmup_iterations=parameters.n_warmup_iterations,
            max_inactive_age=parameters.max_inactive_age,
        )
        log_memory_checkpoint(
            f"plan_updater:iteration:{iteration}:candidate_memory",
            aggregated_candidates=aggregated_candidates,
        )

        return self.compute_plan_steps_candidates_utility(
            candidates=aggregated_candidates,
            transport_costs=transport_costs,
            destination_saturation=destination_saturation,
            activity_dur=activity_dur,
            resolved_activity_parameters=resolved_activity_parameters,
            min_activity_time_constant=min_activity_time_constant,
            allow_missing_costs_for_current_plans=(candidate_plan_steps is not None),
        )

    def compute_plan_steps_candidates_utility(
        self,
        *,
        candidates: pl.LazyFrame,
        transport_costs,
        destination_saturation: pl.DataFrame,
        activity_dur: pl.DataFrame,
        resolved_activity_parameters: dict[str, Any],
        min_activity_time_constant,
        allow_missing_costs_for_current_plans: bool,
    ) -> pl.LazyFrame:
        """Score plan-step candidates under current costs and destination saturation."""

        cost_by_od_and_modes = transport_costs.get_costs_by_od_and_mode(
            ["cost", "distance", "time"],
            detail_distances=False,
        )
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
            "country",
            "activity_seq_id",
            "time_seq_id",
            "dest_seq_id",
            "mode_seq_id",
            "seq_step_index",
            "activity",
            "from",
            "to",
            "mode",
            "duration_per_pers",
            "departure_time",
            "arrival_time",
            "next_departure_time",
            "iteration",
            "csp",
            "first_seen_iteration",
            "last_active_iteration",
            "cost",
            "distance",
            "time",
            "mean_duration_per_pers",
            "value_of_time",
            "k_saturation_utility",
            "min_activity_time",
            "utility",
        ]

        scored_candidates = (
            candidates
            .with_columns(
                activity=pl.col("activity").cast(pl.Enum(activity_dur["activity"].dtype.categories))
            )
            .join(
                cost_by_od_and_modes.lazy(),
                on=["from", "to", "mode"],
                how="left" if allow_missing_costs_for_current_plans else "inner",
            )
            .join(activity_dur.lazy(), on=["country", "csp", "activity"])
            .join(value_of_time.lazy(), on="activity")
            .join(
                destination_saturation.select(["to", "activity", "k_saturation_utility"]).lazy(),
                on=["to", "activity"],
                how="left",
            )
            .with_columns(
                cost=pl.col("cost").fill_null(1e9) if allow_missing_costs_for_current_plans else pl.col("cost"),
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

        return scored_candidates

    def get_possible_plan_utility(
        self,
        possible_plan_steps,
        home_night_dur,
        value_of_time_stay_home,
        stay_home_plan,
        min_activity_time_constant,
        *,
        sequence_index_folder,
    ):
        """Aggregate per-step utilities to plan-level utilities."""

        possible_plan_utility = (
            possible_plan_steps.filter(pl.col("mode_seq_id") != 0).group_by(
                ["plan_id", "demand_group_id", "country", "csp", "activity_seq_id", "time_seq_id", "dest_seq_id", "mode_seq_id"]
            )
            .agg(
                utility=pl.col("utility").sum(),
                home_night_per_pers=24.0 - pl.col("duration_per_pers").sum(),
            )
            .join(home_night_dur.lazy(), on=["country", "csp"])
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
            .select(["demand_group_id", "activity_seq_id", "time_seq_id", "mode_seq_id", "dest_seq_id", "utility"])
        )

        possible_plan_utility = pl.concat(
            [
                possible_plan_utility,
                stay_home_plan.lazy()
                .with_columns(
                    min_activity_time=pl.col("mean_home_night_per_pers") * math.exp(-min_activity_time_constant),
                )
                .with_columns(
                    utility=(
                        value_of_time_stay_home
                        * pl.col("mean_home_night_per_pers")
                        * (pl.col("mean_home_night_per_pers") / pl.col("min_activity_time")).log().clip(0.0)
                    )
                )
                .select(["demand_group_id", "activity_seq_id", "time_seq_id", "mode_seq_id", "dest_seq_id", "utility"]),
            ]
        )

        return add_plan_id(possible_plan_utility, index_folder=sequence_index_folder)

    def add_stay_home_plan_steps(
        self,
        possible_plan_steps: pl.LazyFrame,
        stay_home_plan: pl.DataFrame,
        *,
        sequence_index_folder,
    ) -> pl.LazyFrame:
        """Append one synthetic timed step for the stay-home state so it can be embedded like other plans."""
        step_columns = [col for col in possible_plan_steps.collect_schema().names() if col != "plan_id"]
        possible_plan_steps_no_id = possible_plan_steps.filter(pl.col("mode_seq_id") != 0).select(step_columns)

        stay_home_steps = (
            stay_home_plan.lazy()
            .with_columns(
                cost=pl.lit(0.0),
                distance=pl.lit(0.0),
                time=pl.lit(0.0),
                mean_duration_per_pers=pl.col("duration_per_pers"),
                value_of_time=pl.lit(0.0),
                k_saturation_utility=pl.lit(1.0),
                min_activity_time=pl.lit(0.0),
                first_seen_iteration=pl.lit(None, dtype=pl.UInt32),
                last_active_iteration=pl.lit(None, dtype=pl.UInt32),
            )
            .select(step_columns)
        )

        return add_plan_id(
            pl.concat([possible_plan_steps_no_id, stay_home_steps], how="vertical_relaxed"),
            index_folder=sequence_index_folder,
        )

    def get_transition_probabilities(
        self,
        current_plans: pl.DataFrame,
        possible_plan_utility: pl.LazyFrame,
        possible_plan_steps: pl.DataFrame,
        behavior_change_scope: BehaviorChangeScope,
        *,
        transport_zones: Any,
        transition_distance_threshold: float = math.inf,
        enable_transition_distance_model: bool = False,
        transition_revision_probability: float = 1.0,
        transition_logit_scale: float = 1.0,
        transition_utility_pruning_delta: float = 3.0,
        transition_distance_friction: float = 0.0,
        plan_embedding_dimension_weights: list[float] | None = None,
    ) -> pl.DataFrame:
        """Compute transition probabilities from current to candidate plans."""

        possible_plan_utility = self._ensure_plan_id(possible_plan_utility)
        possible_plan_steps = self._ensure_plan_id(possible_plan_steps)

        allowed_transitions = self.build_allowed_plan_transitions(
            current_plans,
            possible_plan_utility,
            behavior_change_scope,
            transition_utility_pruning_delta=transition_utility_pruning_delta,
            transition_logit_scale=transition_logit_scale,
        )
        log_memory_checkpoint(
            "plan_updater:allowed_transitions",
            allowed_transitions=allowed_transitions,
        )

        if enable_transition_distance_model and math.isfinite(transition_distance_threshold):
            allowed_transitions = self.attach_transition_distances(
                allowed_transitions,
                possible_plan_steps=possible_plan_steps,
                transport_zones=transport_zones,
                plan_embedding_dimension_weights=plan_embedding_dimension_weights,
            )

        if not enable_transition_distance_model:
            transition_distance_threshold = math.inf
            transition_distance_friction = 0.0

        transition_probabilities = self.compute_transition_probabilities_from_utilities(
            allowed_transitions,
            transition_distance_threshold=transition_distance_threshold,
            transition_revision_probability=transition_revision_probability,
            transition_logit_scale=transition_logit_scale,
            transition_distance_friction=transition_distance_friction,
        )

        return transition_probabilities

    @staticmethod
    def _scale_utility_pruning_delta(
        base_delta: float,
        transition_logit_scale: float,
    ) -> float:
        """Keep the pruning window consistent with the scaled logit utility."""
        if transition_logit_scale <= 0.0:
            return math.inf
        return base_delta / transition_logit_scale

    def build_allowed_plan_transitions(
        self,
        current_plans: pl.DataFrame,
        possible_plan_utility: pl.LazyFrame,
        behavior_change_scope: BehaviorChangeScope,
        *,
        transition_utility_pruning_delta: float = 3.0,
        transition_logit_scale: float = 1.0,
    ) -> pl.LazyFrame:
        """Build allowed from-to plan pairs under the active behavior scope."""

        logging.info(
            "Building PopulationGroupDayTrips allowed plan transitions: scope=%s",
            str(behavior_change_scope),
        )

        utility_pruning_delta = self._scale_utility_pruning_delta(
            base_delta=transition_utility_pruning_delta,
            transition_logit_scale=transition_logit_scale,
        )
        plan_cols = PLAN_KEY_COLS
        current_plans_for_transitions = current_plans.lazy()
        possible_plan_utility_for_transitions = possible_plan_utility

        if behavior_change_scope != BehaviorChangeScope.FULL_REPLANNING:
            current_plans_for_transitions = current_plans_for_transitions.filter(pl.col("mode_seq_id") != 0)
            possible_plan_utility_for_transitions = possible_plan_utility_for_transitions.filter(
                pl.col("mode_seq_id") != 0
            )

        scope_pair_constraint = pl.lit(True)
        if behavior_change_scope == BehaviorChangeScope.DESTINATION_REPLANNING:
            scope_pair_constraint = pl.col("time_seq_id") == pl.col("time_seq_id_trans")
        elif behavior_change_scope == BehaviorChangeScope.MODE_REPLANNING:
            scope_pair_constraint = (
                (pl.col("time_seq_id") == pl.col("time_seq_id_trans"))
                & (pl.col("dest_seq_id") == pl.col("dest_seq_id_trans"))
            )

        is_self_transition = (
            (pl.col("time_seq_id") == pl.col("time_seq_id_trans"))
            & (pl.col("dest_seq_id") == pl.col("dest_seq_id_trans"))
            & (pl.col("mode_seq_id") == pl.col("mode_seq_id_trans"))
        )

        utility_filter = pl.lit(True)
        if math.isfinite(utility_pruning_delta):
            utility_filter = (
                is_self_transition
                | (pl.col("utility_trans") >= pl.col("max_utility_trans") - utility_pruning_delta)
            )

        return (
            current_plans_for_transitions
            .select(plan_cols + ["utility"])
            .rename({"utility": "utility_prev_from"})
            .join(possible_plan_utility_for_transitions, on=plan_cols)
            .rename({"plan_id": "plan_id_from"})
            .join_where(
                possible_plan_utility_for_transitions,
                (
                    (pl.col("demand_group_id") == pl.col("demand_group_id_trans"))
                    & scope_pair_constraint
                ),
                suffix="_trans",
            )
            .drop("demand_group_id_trans")
            .rename({"plan_id": "plan_id_trans"})
            .with_columns(
                max_utility_trans=pl.col("utility_trans").max().over(plan_cols),
            )
            .filter(utility_filter)
            .drop(["max_utility_trans"])
        )

    def attach_transition_distances(
        self,
        allowed_transitions: pl.LazyFrame,
        *,
        possible_plan_steps: pl.DataFrame,
        transport_zones: Any,
        plan_embedding_dimension_weights: list[float] | None = None,
    ) -> pl.LazyFrame:
        """Attach embedding distances to allowed plan transitions."""

        logging.info("Computing PopulationGroupDayTrips transition distances")

        is_self_transition = pl.col("plan_id_from") == pl.col("plan_id_trans")
        self_distances = (
            allowed_transitions
            .filter(is_self_transition)
            .select(["plan_id_from", "plan_id_trans"])
            .with_columns(distance=pl.lit(0.0, dtype=pl.Float64))
            .collect()
        )
        pair_index = (
            allowed_transitions
            .filter(~is_self_transition)
            .select(["plan_id_from", "plan_id_trans"])
            .collect()
        )
        non_self_distances = PlanDistance().get_plan_pair_distances(
            pair_index,
            possible_plan_steps,
            plan_id_col="plan_id",
            transport_zones=transport_zones,
            dimension_weights=plan_embedding_dimension_weights,
        )
        pair_distances = pl.concat(
            [self_distances, non_self_distances],
            how="vertical_relaxed"
        )

        return allowed_transitions.join(pair_distances.lazy(), on=["plan_id_from", "plan_id_trans"])

    def compute_transition_probabilities_from_utilities(
        self,
        allowed_transitions: pl.LazyFrame,
        *,
        transition_distance_threshold: float = math.inf,
        transition_revision_probability: float = 1.0,
        transition_logit_scale: float = 1.0,
        transition_distance_friction: float = 0.0,
    ) -> pl.DataFrame:
        """Compute one-step transition probabilities with distance-threshold filtering and revision."""

        logging.info("Collecting PopulationGroupDayTrips transition probabilities")

        plan_cols = PLAN_KEY_COLS
        if "distance" not in allowed_transitions.collect_schema().names():
            allowed_transitions = allowed_transitions.with_columns(
                distance=pl.lit(0.0, dtype=pl.Float64)
            )
        is_self_transition = pl.col("plan_id_from") == pl.col("plan_id_trans")
        filtered_transitions = allowed_transitions
        if math.isfinite(transition_distance_threshold):
            filtered_transitions = allowed_transitions.filter(
                is_self_transition | (pl.col("distance").fill_null(0.0) <= transition_distance_threshold)
            )

        friction = pl.lit(transition_distance_friction, dtype=pl.Float64)
        scaled_transition_utility = (
            pl.col("utility_trans") * pl.lit(transition_logit_scale, dtype=pl.Float64)
            - friction * pl.col("distance").fill_null(0.0)
        )
        utility_weighted_transitions = (
            filtered_transitions
            .with_columns(scaled_transition_utility=scaled_transition_utility)
            .with_columns(
                utility_weight=(
                    pl.col("scaled_transition_utility") - pl.col("scaled_transition_utility").max().over(plan_cols)
                ).exp()
            )
        )

        q_transition = pl.col("utility_weight") / pl.col("utility_weight").sum().over(plan_cols)
        revision_probability = pl.lit(transition_revision_probability, dtype=pl.Float64)

        transition_probabilities = (
            utility_weighted_transitions
            .with_columns(
                is_self_transition=is_self_transition,
                q_transition=q_transition,
                tau_transition=friction * pl.col("distance").fill_null(0.0),
                adjustment_factor=revision_probability,
            )
            .with_columns(
                p_transition=pl.when(pl.col("is_self_transition"))
                .then(1.0 - revision_probability + revision_probability * pl.col("q_transition"))
                .otherwise(revision_probability * pl.col("q_transition"))
            )
            .select(
                [
                    "plan_id_trans",
                    "demand_group_id",
                    "activity_seq_id",
                    "time_seq_id",
                    "dest_seq_id",
                    "mode_seq_id",
                    "activity_seq_id_trans",
                    "time_seq_id_trans",
                    "dest_seq_id_trans",
                    "mode_seq_id_trans",
                    "utility_prev_from",
                    pl.col("utility").alias("utility_from_updated"),
                    "utility_trans",
                    "tau_transition",
                    "q_transition",
                    "adjustment_factor",
                    "p_transition",
                ]
            )
            .collect()
        )
        log_memory_checkpoint(
            "plan_updater:transition_probabilities_collected",
            transition_probabilities=transition_probabilities,
        )
    
        logging.info(
            "Finished collecting PopulationGroupDayTrips transition probabilities."
        )

        return transition_probabilities

    def apply_transitions(
        self,
        current_plans: pl.DataFrame,
        transition_probabilities: pl.DataFrame,
        iteration: int,
    ) -> tuple[pl.DataFrame, pl.LazyFrame]:
        """Apply transition probabilities and emit transition events."""

        plan_cols = PLAN_KEY_COLS

        transitions = (
            current_plans.lazy()
            .join(transition_probabilities.lazy(), on=plan_cols, how="left")
            .with_columns(
                plan_id_trans=pl.coalesce([pl.col("plan_id_trans"), pl.col("plan_id")]),
                p_transition=pl.col("p_transition").fill_null(1.0),
                utility_from_updated=pl.col("utility_from_updated").fill_null(pl.col("utility")),
                utility_trans=pl.coalesce([pl.col("utility_trans"), pl.col("utility")]),
                utility_prev_from=pl.coalesce([pl.col("utility_prev_from"), pl.col("utility")]),
                activity_seq_id_trans=pl.coalesce([pl.col("activity_seq_id_trans"), pl.col("activity_seq_id")]),
                time_seq_id_trans=pl.coalesce([pl.col("time_seq_id_trans"), pl.col("time_seq_id")]),
                dest_seq_id_trans=pl.coalesce([pl.col("dest_seq_id_trans"), pl.col("dest_seq_id")]),
                mode_seq_id_trans=pl.coalesce([pl.col("mode_seq_id_trans"), pl.col("mode_seq_id")]),
            )
            .with_columns(n_persons_moved=pl.col("n_persons") * pl.col("p_transition"))
        )

        prev_to_lookup = (
            current_plans.lazy()
            .select(["demand_group_id", "activity_seq_id", "time_seq_id", "dest_seq_id", "mode_seq_id", "utility"])
            .rename(
                {
                    "activity_seq_id": "activity_seq_id_trans",
                    "time_seq_id": "time_seq_id_trans",
                    "dest_seq_id": "dest_seq_id_trans",
                    "mode_seq_id": "mode_seq_id_trans",
                    "utility": "utility_prev_to",
                }
            )
        )

        transitions = transitions.join(
            prev_to_lookup,
            on=["demand_group_id", "activity_seq_id_trans", "time_seq_id_trans", "dest_seq_id_trans", "mode_seq_id_trans"],
            how="left",
        )

        transition_events = build_transition_events_lazy(
            transitions,
            iteration=iteration,
        )

        new_states = (
            transitions.group_by(
                ["plan_id_trans", "demand_group_id", "activity_seq_id_trans", "time_seq_id_trans", "dest_seq_id_trans", "mode_seq_id_trans"]
            )
            .agg(
                n_persons=pl.col("n_persons_moved").sum(),
                utility=pl.col("utility_trans").first(),
            )
            .rename(
                {
                    "plan_id_trans": "plan_id",
                    "activity_seq_id_trans": "activity_seq_id",
                    "time_seq_id_trans": "time_seq_id",
                    "dest_seq_id_trans": "dest_seq_id",
                    "mode_seq_id_trans": "mode_seq_id",
                }
            )
            .collect(engine="streaming")
        )

        return new_states, transition_events
    def get_current_plan_steps(self, current_plans, possible_plan_steps):
        """Expand aggregate plans to per-step rows."""

        current_plan_steps = (
            current_plans.select(
                [
                    "demand_group_id",
                    "activity_seq_id",
                    "time_seq_id",
                    "dest_seq_id",
                    "mode_seq_id",
                    "n_persons",
                ]
            ).lazy()
            .join(
                possible_plan_steps.select(
                    [
                        "demand_group_id",
                        "activity_seq_id",
                        "time_seq_id",
                        "dest_seq_id",
                        "mode_seq_id",
                        "seq_step_index",
                        "activity",
                        "from",
                        "to",
                        "mode",
                        "duration_per_pers",
                        "departure_time",
                        "arrival_time",
                        "next_departure_time",
                    ]
                ),
                on=["demand_group_id", "activity_seq_id", "time_seq_id", "dest_seq_id", "mode_seq_id"],
                how="left",
            )
            .with_columns(duration=pl.col("duration_per_pers").fill_null(24.0) * pl.col("n_persons"))
            .drop("duration_per_pers")
            .collect(engine="streaming")
        )

        return self._compress_step_state(
            current_plan_steps,
            drop_plan_id=True,
        )

    def get_new_costs(
        self,
        costs,
        iteration,
        n_iter_per_cost_update,
        current_plan_steps,
        transport_costs,
        run,
    ):
        """Return the OD costs to use after the current iteration."""

        if (
            n_iter_per_cost_update <= 0
            or transport_costs.should_recompute_congested_costs(iteration, n_iter_per_cost_update) is False
        ):
            return costs

        od_flows_by_mode = (
            current_plan_steps.filter(pl.col("activity_seq_id") != 0)
            .with_columns(mode=pl.col("mode").cast(pl.String))
            .group_by(["from", "to", "mode"])
            .agg(flow_volume=pl.col("n_persons").sum())
        )

        return transport_costs.get_costs_for_next_iteration(
            run=run,
            iteration=iteration,
            od_flows_by_mode=od_flows_by_mode,
        )

    def get_destination_saturation(
        self,
        current_plan_steps,
        opportunities,
        resolved_activity_parameters: dict[str, Any],
    ):
        """Recompute destination saturation per (activity, destination).

        This keeps the original ``opportunity_capacity`` and updates
        ``k_saturation_utility`` from the currently occupied activity duration
        at each destination. It is not a literal remaining-capacity stock.
        """

        logging.info("Computing destination saturation at destinations...")

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

        destination_saturation = (
            current_plan_steps.filter((pl.col("activity_seq_id") != 0) & (pl.col("activity") != "home"))
            .with_columns(activity=pl.col("activity").cast(pl.Enum(opportunities["activity"].dtype.categories)))
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

        return destination_saturation
