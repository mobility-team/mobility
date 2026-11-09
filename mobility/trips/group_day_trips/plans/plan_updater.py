import logging
import math
from dataclasses import dataclass
from typing import Any

import polars as pl

from mobility.trips.group_day_trips.core.performance_trace import simulation_step, trace_simulation_step

from mobility.trips.group_day_trips.core.parameters import BehaviorChangeScope
from mobility.transport.modes.core.mode_values import get_mode_values
from mobility.trips.group_day_trips.core.memory_logging import log_memory_checkpoint
from ..transitions.transition_events import (
    add_transition_plan_details,
    build_transition_events_lazy,
)
from .candidate_plan_steps import CandidatePlanStepsAsset
from .compact_candidate_steps import CompactCandidateSteps
from .scoring_contexts import ScoringContexts
from .demand_subgroups import DEMAND_UNIT_COLS
from .plan_distance import PlanDistance
from .plan_ids import PLAN_KEY_COLS, add_plan_id
from .plan_schedule_updater import PlanScheduleUpdater
from .time_sequences import assign_time_sequence_ids
from .destination_sequences import DestinationSequences
from .mode_sequence_search import ModeSequences


@dataclass(frozen=True)
class CandidatePlanEvaluation:
    """Scenario-specific utilities and step details for one shared pool."""

    candidate_plan_steps: pl.DataFrame
    possible_plan_steps: pl.DataFrame | pl.LazyFrame
    possible_plan_utility: pl.DataFrame
    plan_id_index: pl.DataFrame
    current_plans: pl.DataFrame | None = None
    time_sequence_steps: pl.DataFrame | None = None
    scoring_contexts: ScoringContexts | None = None
    time_mapping: pl.DataFrame | None = None


class PlanUpdater:
    """Updates population plan distributions over activity/destination/mode sequences."""

    def __init__(self) -> None:
        self.schedule_updater = PlanScheduleUpdater()

    def expand_context_evaluation(
        self, evaluation: CandidatePlanEvaluation, current_plans: pl.DataFrame,
        previous_plan_id_index: pl.DataFrame | None,
    ) -> CandidatePlanEvaluation:
        """Map compact scores to demand units and their persistent plan IDs."""
        contexts = evaluation.scoring_contexts
        if contexts is None or evaluation.time_mapping is None:
            raise ValueError("A compact evaluation and its timing mapping are required.")
        utilities = contexts.expand_step_rows(evaluation.possible_plan_utility).drop("plan_id")
        utilities, plan_id_index = add_plan_id(utilities, previous_index=previous_plan_id_index)
        utilities = utilities.collect(engine="streaming")
        steps = (
            contexts.expand_step_rows(evaluation.possible_plan_steps).drop("plan_id")
            .join(plan_id_index.lazy(), on=PLAN_KEY_COLS, how="inner")
        )
        candidates = contexts.expand_step_rows(evaluation.candidate_plan_steps).collect(engine="streaming")
        mapping = contexts.expand_step_rows(evaluation.time_mapping).collect(engine="streaming")
        origin_keys = ["previous_time_seq_id" if col == "time_seq_id" else col for col in PLAN_KEY_COLS]
        origins = (
            current_plans.drop("plan_id", strict=False).rename({"time_seq_id": "previous_time_seq_id"})
            .join(mapping, on=origin_keys, how="left")
            .with_columns(time_seq_id=pl.col("time_seq_id").fill_null(pl.col("previous_time_seq_id")))
            .group_by(PLAN_KEY_COLS)
            .agg(n_persons=pl.col("n_persons").sum(), utility=pl.col("utility").first())
            .join(plan_id_index, on=PLAN_KEY_COLS, how="left")
        )
        if origins["plan_id"].null_count():
            raise ValueError("A current plan is missing from the expanded evaluation.")
        return CandidatePlanEvaluation(
            candidate_plan_steps=candidates, possible_plan_steps=steps,
            possible_plan_utility=utilities, plan_id_index=plan_id_index,
            current_plans=origins, time_sequence_steps=evaluation.time_sequence_steps,
        )

    @trace_simulation_step("behavior update")
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
        arrival_time_rigidity_by_activity: dict[str, float],
        destination_sequences: DestinationSequences,
        mode_sequences: ModeSequences,
        home_night_dur: pl.DataFrame,
        stay_home_plan: pl.DataFrame,
        transport_zones: Any,
        previous_plan_id_index: pl.DataFrame,
        parameters: Any,
        *,
        shared_candidate_plan_steps: pl.DataFrame | None = None,
        precomputed_evaluation: CandidatePlanEvaluation | None = None,
    ) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.LazyFrame | None, pl.DataFrame, pl.DataFrame]:
        """Advance one iteration of plan updates."""
        evaluation = precomputed_evaluation or self.evaluate_candidate_pool(
            current_plans=current_plans,
            current_plan_steps=current_plan_steps,
            candidate_plan_steps=candidate_plan_steps,
            shared_candidate_plan_steps=shared_candidate_plan_steps,
            demand_groups=demand_groups,
            survey_plan_steps=survey_plan_steps,
            transport_costs=transport_costs,
            destination_saturation=destination_saturation,
            activity_dur=activity_dur,
            home_night_dur=home_night_dur,
            stay_home_plan=stay_home_plan,
            transport_zones=transport_zones,
            iteration=iteration,
            resolved_activity_parameters=resolved_activity_parameters,
            arrival_time_rigidity_by_activity=arrival_time_rigidity_by_activity,
            destination_sequences=destination_sequences,
            mode_sequences=mode_sequences,
            previous_plan_id_index=previous_plan_id_index,
            parameters=parameters,
        )
        candidate_plan_steps = evaluation.candidate_plan_steps
        possible_plan_steps = evaluation.possible_plan_steps
        if isinstance(possible_plan_steps, pl.LazyFrame):
            possible_plan_steps = possible_plan_steps.collect(engine="streaming")
        possible_plan_utility = (
            evaluation.possible_plan_utility
            .join(
                evaluation.time_sequence_steps.select("time_seq_id", "reference_time_seq_id").unique(),
                on="time_seq_id", how="left",
            )
            .with_columns(reference_time_seq_id=pl.col("reference_time_seq_id").fill_null(pl.col("time_seq_id")))
            .lazy()
        )
        plan_id_index = evaluation.plan_id_index
        current_plans = evaluation.current_plans
        log_memory_checkpoint(
            f"plan_updater:iteration:{iteration}:possible_plan_steps",
            possible_plan_steps=possible_plan_steps,
        )

        transition_prob = self.get_transition_probabilities(
            current_plans,
            possible_plan_utility,
            possible_plan_steps,
            parameters.behavior_change.scope_at(iteration),
            transport_zones=transport_zones,
            transition_distance_threshold=parameters.plan_update.transition_distance_threshold,
            enable_transition_distance_model=parameters.plan_update.enable_transition_distance_model,
            transition_revision_probability=parameters.plan_update.transition_revision_probability,
            transition_logit_scale=parameters.plan_update.transition_logit_scale,
            transition_utility_pruning_delta=parameters.plan_update.transition_utility_pruning_delta,
            min_transition_utility_gain=parameters.plan_update.min_transition_utility_gain,
            transition_distance_friction=parameters.plan_update.transition_distance_friction,
            plan_embedding_dimension_weights=parameters.plan_update.plan_embedding_dimension_weights,
        )
        log_memory_checkpoint(
            f"plan_updater:iteration:{iteration}:transition_probabilities",
            transition_prob=transition_prob,
        )
        current_plans, transition_events = self.apply_transitions(
            current_plans,
            transition_prob,
            iteration,
            plan_probability_pruning_retained_share=parameters.plan_update.plan_probability_pruning_retained_share,
            plan_probability_pruning_min_step=parameters.plan_update.plan_probability_pruning_min_step,
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
        if parameters.outputs.cache_step_events:
            transition_events = add_transition_plan_details(
                transition_events,
                possible_plan_steps.lazy(),
            )
        else:
            transition_events = None

        if current_plans["n_persons"].is_null().any() or current_plans["n_persons"].is_nan().any():
            raise ValueError("Null or NaN values in the n_persons column, something went wrong.")

        return current_plans, current_plan_steps, candidate_plan_steps, transition_events, plan_id_index, evaluation.time_sequence_steps

    @trace_simulation_step("candidate evaluation")
    def evaluate_candidate_pool(
        self,
        *,
        current_plans: pl.DataFrame,
        current_plan_steps: pl.DataFrame | None,
        candidate_plan_steps: pl.DataFrame | None,
        shared_candidate_plan_steps: pl.DataFrame | None,
        demand_groups: pl.DataFrame,
        survey_plan_steps: pl.DataFrame,
        transport_costs: Any,
        destination_saturation: pl.DataFrame,
        activity_dur: pl.DataFrame,
        home_night_dur: pl.DataFrame,
        stay_home_plan: pl.DataFrame,
        transport_zones: Any,
        iteration: int,
        resolved_activity_parameters: dict[str, Any],
        arrival_time_rigidity_by_activity: dict[str, float],
        destination_sequences: DestinationSequences,
        mode_sequences: ModeSequences,
        previous_plan_id_index: pl.DataFrame,
        parameters: Any,
        compact_contexts: bool = False,
    ) -> CandidatePlanEvaluation:
        """Score all current candidates without changing the behavioral state."""
        candidate_memory = (
            shared_candidate_plan_steps
            if shared_candidate_plan_steps is not None
            else candidate_plan_steps
        )
        if isinstance(candidate_memory, CompactCandidateSteps):
            candidate_memory = candidate_memory.lazy()
        if shared_candidate_plan_steps is not None and candidate_plan_steps is not None:
            # Inner timing updates may have replaced the occupied keys. Preserve
            # the real outer origins so they can be rescored and remapped here.
            active_candidates = candidate_plan_steps.lazy().join(
                current_plans.lazy().select(PLAN_KEY_COLS), on=PLAN_KEY_COLS, how="semi",
            )
            candidate_memory = pl.concat(
                [shared_candidate_plan_steps.lazy(), active_candidates], how="vertical_relaxed",
            ).unique(subset=CandidatePlanStepsAsset.DEDUPE_COLUMNS, keep="last")
        scoring_contexts = None
        if compact_contexts:
            scoring_contexts = ScoringContexts.build(
                demand_groups, transport_costs, mode_sequences.population_segments,
            )
            candidate_memory = scoring_contexts.collapse_step_rows(candidate_memory)
            log_memory_checkpoint(
                f"plan_updater:iteration:{iteration}:scoring_context_candidates",
                candidate_steps=candidate_memory,
            )
            current_plans = scoring_contexts.collapse_allocations(current_plans)
            stay_home_plan = scoring_contexts.collapse_stay_home(stay_home_plan)
            demand_groups = scoring_contexts.demand_groups
            # These are temporary context ids, separate from persistent unit plan ids.
            previous_plan_id_index = None
            current_plan_steps = None
        possible_plan_steps = self.get_possible_plan_steps(
            current_plans,
            current_plan_steps,
            candidate_memory,
            demand_groups,
            survey_plan_steps,
            transport_costs,
            destination_saturation,
            activity_dur,
            home_night_dur,
            transport_zones,
            iteration,
            resolved_activity_parameters,
            arrival_time_rigidity_by_activity,
            parameters.plan_update.min_activity_time_constant,
            destination_sequences,
            mode_sequences,
            parameters,
            shared_candidate_pool=(shared_candidate_plan_steps is not None),
        )
        # These inputs have already been combined into activity_utility_scale
        # and min_activity_time. They are not needed in the wide timing table.
        possible_plan_steps = possible_plan_steps.drop(
            "mean_duration_per_pers", "value_of_time", "k_saturation_utility",
            "destination_shadow_price", strict=False,
        )
        if logging.getLogger().isEnabledFor(logging.DEBUG):
            logging.debug("Scored step schema | iteration=%d | columns=%s", iteration, possible_plan_steps.collect_schema())
        log_memory_checkpoint(
            f"plan_updater:iteration:{iteration}:possible_plan_steps_lazy",
            possible_plan_steps=possible_plan_steps,
        )
        with simulation_step("materialize scored steps and plan ids"):
            possible_plan_steps, plan_id_index = add_plan_id(
                possible_plan_steps,
                previous_index=previous_plan_id_index,
            )
        self._assert_current_plans_covered_by_possible_plan_steps(
            current_plans,
            possible_plan_steps,
            iteration,
        )
        possible_plan_steps = self.schedule_updater.update_plan_timings(
            possible_plan_steps,
            arrival_time_rigidity_by_activity=arrival_time_rigidity_by_activity,
            enabled=parameters.plan_update.update_plan_timings_from_modeled_travel_times,
        )

        # Timing changes create new schedule identities. Remap the current
        # distribution too, so transition origins still match the scored plans.
        with simulation_step("materialize and index modeled timings"):
            scored_steps = possible_plan_steps.drop("plan_id").collect(engine="streaming")
            log_memory_checkpoint(
                f"plan_updater:iteration:{iteration}:timed_scored_steps", scored_steps=scored_steps,
            )
            timed_steps, time_sequence_steps, time_mapping = assign_time_sequence_ids(
                scored_steps, survey_plan_steps
            )
            del scored_steps
            timed_steps, plan_id_index = add_plan_id(timed_steps, previous_index=plan_id_index)
        current_plans = (
            current_plans.rename({"time_seq_id": "previous_time_seq_id"})
            .join(time_mapping, on=["previous_time_seq_id" if col == "time_seq_id" else col for col in PLAN_KEY_COLS], how="left")
            .with_columns(time_seq_id=pl.col("time_seq_id").fill_null(pl.col("previous_time_seq_id")))
            .group_by(PLAN_KEY_COLS)
            .agg(n_persons=pl.col("n_persons").sum(), utility=pl.col("utility").first())
            .join(plan_id_index, on=PLAN_KEY_COLS, how="left")
        )
        candidate_plan_steps = timed_steps.select(
            CandidatePlanStepsAsset.STRUCTURAL_COLUMNS + CandidatePlanStepsAsset.RETENTION_COLUMNS
        ).unique(subset=CandidatePlanStepsAsset.DEDUPE_COLUMNS, keep="first")
        possible_plan_steps = timed_steps.unique(subset=PLAN_KEY_COLS + ["seq_step_index"], keep="first").lazy()
        possible_plan_steps = (
            possible_plan_steps
            .with_columns(
                utility=(
                    pl.col("activity_utility_scale")
                    * (pl.col("duration_per_pers") / pl.col("min_activity_time")).log().clip(0.0)
                    - pl.col("cost")
                )
            )
            .drop("activity_utility_scale")
        )
        possible_plan_utility, plan_id_index = self.get_possible_plan_utility(
            possible_plan_steps,
            home_night_dur,
            resolved_activity_parameters["home"].value_of_time_stay_home,
            stay_home_plan,
            parameters.plan_update.min_activity_time_constant,
            plan_id_index=plan_id_index,
        )
        possible_plan_steps = possible_plan_steps.drop(
            "_missing_cost_row",
            "_invalid_cost_row",
        )
        possible_plan_steps, plan_id_index = self.add_stay_home_plan_steps(
            possible_plan_steps,
            stay_home_plan,
            plan_id_index=plan_id_index,
        )
        with simulation_step("collect plan utilities"):
            possible_plan_utility = possible_plan_utility.collect(engine="streaming")
        return CandidatePlanEvaluation(
            candidate_plan_steps=candidate_plan_steps,
            # Inner allocation only needs a narrow step projection after shares
            # are known. Leave it lazy instead of storing another wide table.
            possible_plan_steps=possible_plan_steps,
            possible_plan_utility=possible_plan_utility,
            plan_id_index=plan_id_index,
            current_plans=current_plans,
            time_sequence_steps=time_sequence_steps,
            scoring_contexts=scoring_contexts,
            time_mapping=time_mapping,
        )

    @trace_simulation_step("fresh MNL allocation")
    def estimate_mnl_allocation(
        self,
        evaluation: CandidatePlanEvaluation,
        demand_groups: pl.DataFrame,
        *,
        logit_scale: float,
    ) -> tuple[pl.DataFrame, pl.DataFrame]:
        """Estimate fresh feasible-plan MNL shares without changing outer state."""
        if evaluation.scoring_contexts is not None:
            demand_groups = evaluation.scoring_contexts.demand_groups
        with simulation_step("compute and expand shared allocation shares"):
            plan_columns = [column for column in PLAN_KEY_COLS if column not in DEMAND_UNIT_COLS]
            feasible = evaluation.possible_plan_utility.lazy().filter(pl.col("is_feasible"))
            # Shares depend on the complete available choice set, not population
            # size or current allocation. Never merge units just because their
            # transport-cost profiles match: they may have different alternatives.
            unit_choices = feasible.group_by(DEMAND_UNIT_COLS).agg(
                _choices=pl.struct(plan_columns + ["utility"]).sort_by(plan_columns)
            ).collect(engine="streaming")
            choice_contexts = unit_choices.select("_choices").unique().with_row_index("_choice_context_id")
            context_units = unit_choices.join(choice_contexts, on="_choices").drop("_choices")
            del unit_choices
            logging.debug(
                "Fresh allocation share contexts | demand_units=%d | distinct_choice_sets=%d",
                context_units.height, choice_contexts.height,
            )
            context_choices = choice_contexts.lazy().explode("_choices").unnest("_choices")
            utility_column = pl.col("utility") * pl.lit(logit_scale, dtype=pl.Float64)
            probabilities = (
                context_choices
                .with_columns(
                    _choice_utility=utility_column,
                    _max_choice_utility=utility_column.max().over("_choice_context_id"),
                )
                .with_columns(
                    _choice_weight=(pl.col("_choice_utility") - pl.col("_max_choice_utility")).exp()
                )
                .with_columns(
                    choice_probability=(
                        pl.col("_choice_weight")
                        / pl.col("_choice_weight").sum().over("_choice_context_id")
                    )
                )
                .select(["_choice_context_id"] + plan_columns + ["utility", "choice_probability"])
            )
            allocation = (
                probabilities
                .join(context_units.lazy(), on="_choice_context_id")
                .join(
                    feasible.select(["plan_id"] + PLAN_KEY_COLS),
                    on=PLAN_KEY_COLS,
                )
                .join(
                    demand_groups.select(DEMAND_UNIT_COLS + ["n_persons"]).lazy(),
                    on=DEMAND_UNIT_COLS,
                    how="inner",
                )
                .with_columns(n_persons=pl.col("n_persons") * pl.col("choice_probability"))
                .select(["plan_id"] + PLAN_KEY_COLS + ["n_persons", "utility"])
                .collect(engine="streaming")
            )
            del probabilities, context_choices, choice_contexts, context_units, feasible
        current_plan_steps = self.get_current_plan_steps(
            allocation,
            evaluation.possible_plan_steps.lazy(),
        )
        return allocation, current_plan_steps

    def _assert_current_plans_covered_by_possible_plan_steps(
        self,
        current_plans: pl.DataFrame,
        possible_plan_steps: pl.LazyFrame,
        iteration: int,
    ) -> None:
        """Fail when non-stay-home current plans have no step details."""
        plan_keys = PLAN_KEY_COLS

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
        home_night_dur,
        transport_zones,
        iteration: int,
        resolved_activity_parameters: dict[str, Any],
        arrival_time_rigidity_by_activity: dict[str, float],
        min_activity_time_constant,
        destination_sequences: DestinationSequences,
        mode_sequences: ModeSequences,
        parameters: Any,
        *,
        shared_candidate_pool: bool = False,
    ):
        """Enumerate candidate plan steps and compute per-step utilities."""
        profile_assignments, utility_profiles = transport_costs.get_utility_profiles(
            demand_groups,
            mode_sequences.population_segments,
        )
        if shared_candidate_pool:
            if candidate_plan_steps is None:
                aggregated_candidates = pl.DataFrame(
                    schema={
                        **CandidatePlanStepsAsset.REQUIRED_SCHEMA,
                        "last_active_iteration": pl.UInt16,
                    }
                ).lazy()
            else:
                # The shared pool has already merged every scenario's batch.
                # Preserve all of it while evaluating this fixed outer state.
                aggregated_candidates = candidate_plan_steps.lazy().filter(
                    pl.col("mode_seq_id") != 0
                )
        else:
            candidate_pruner = lambda context_steps: self._prune_shared_candidate_contexts(
                candidates=context_steps,
                transport_costs=transport_costs,
                utility_profiles=utility_profiles,
                destination_saturation=destination_saturation,
                activity_dur=activity_dur,
                home_night_dur=home_night_dur,
                transport_zones=transport_zones,
                resolved_activity_parameters=resolved_activity_parameters,
                min_activity_time_constant=min_activity_time_constant,
                parameters=parameters,
                iteration=iteration,
                arrival_time_rigidity_by_activity=arrival_time_rigidity_by_activity,
            )

            aggregated_candidates = CandidatePlanStepsAsset.build_candidate_memory(
                destination_sequences=destination_sequences,
                mode_sequences=mode_sequences,
                survey_plan_steps=survey_plan_steps,
                demand_groups=demand_groups,
                current_plans=current_plans,
                previous_candidate_plan_steps=candidate_plan_steps,
                current_iteration=iteration,
                n_warmup_steps=parameters.plan_update.n_warmup_steps,
                max_inactive_age=parameters.plan_update.max_inactive_age,
                profile_assignments=profile_assignments,
                candidate_pruner=candidate_pruner,
            )
        log_memory_checkpoint(
            f"plan_updater:iteration:{iteration}:candidate_memory",
            aggregated_candidates=aggregated_candidates,
        )

        return self.compute_plan_steps_candidates_utility(
            candidates=aggregated_candidates,
            transport_costs=transport_costs,
            demand_groups=demand_groups,
            population_segments=mode_sequences.population_segments,
            destination_saturation=destination_saturation,
            activity_dur=activity_dur,
            transport_zones=transport_zones,
            resolved_activity_parameters=resolved_activity_parameters,
            min_activity_time_constant=min_activity_time_constant,
            allow_missing_costs_for_current_plans=(
                shared_candidate_pool or candidate_plan_steps is not None
            ),
            use_destination_shadow_prices=parameters.plan_update.use_destination_shadow_prices,
        )

    def _prune_shared_candidate_contexts(
        self,
        *,
        candidates: pl.LazyFrame,
        transport_costs,
        utility_profiles,
        destination_saturation: pl.DataFrame,
        activity_dur: pl.DataFrame,
        home_night_dur: pl.DataFrame,
        transport_zones,
        resolved_activity_parameters: dict[str, Any],
        min_activity_time_constant: float,
        parameters: Any,
        iteration: int,
        arrival_time_rigidity_by_activity: dict[str, float],
    ) -> pl.DataFrame:
        """Apply the existing transition utility window before demand-unit expansion."""
        scope = parameters.behavior_change.scope_at(iteration)
        if scope == BehaviorChangeScope.NO_TRANSITIONS:
            return candidates.select(
                [
                    "_score_context_id",
                    "activity_seq_id",
                    "time_seq_id",
                    "dest_seq_id",
                    "mode_seq_id",
                ]
            ).unique().collect(engine="streaming")

        candidate_steps = candidates.with_columns(
            first_seen_iteration=pl.lit(0, dtype=pl.UInt16),
            last_seen_iteration=pl.lit(0, dtype=pl.UInt16),
            last_active_iteration=pl.lit(None, dtype=pl.UInt16),
        )
        if "iteration" not in candidate_steps.collect_schema().names():
            candidate_steps = candidate_steps.with_columns(
                iteration=pl.lit(0, dtype=pl.UInt16)
            )

        scored_steps = self.compute_plan_steps_candidates_utility(
            candidates=candidate_steps,
            transport_costs=transport_costs,
            utility_profiles=utility_profiles,
            destination_saturation=destination_saturation,
            activity_dur=activity_dur,
            transport_zones=transport_zones,
            resolved_activity_parameters=resolved_activity_parameters,
            min_activity_time_constant=min_activity_time_constant,
            allow_missing_costs_for_current_plans=False,
            use_destination_shadow_prices=parameters.plan_update.use_destination_shadow_prices,
        )
        scored_steps = self.schedule_updater.update_plan_timings(
            scored_steps,
            arrival_time_rigidity_by_activity=arrival_time_rigidity_by_activity,
            enabled=parameters.plan_update.update_plan_timings_from_modeled_travel_times,
        )
        candidate_utilities = (
            scored_steps
            .with_columns(
                utility=(
                    pl.col("activity_utility_scale")
                    * (pl.col("duration_per_pers") / pl.col("min_activity_time")).log().clip(0.0)
                    - pl.col("cost")
                )
            )
            .filter(pl.col("mode_seq_id") != 0)
            .group_by(
                DEMAND_UNIT_COLS
                + [
                    "country",
                    "csp",
                    "activity_seq_id",
                    "time_seq_id",
                    "dest_seq_id",
                    "mode_seq_id",
                ]
            )
            .agg(
                utility=pl.col("utility").sum(),
                home_night_per_pers=(
                    24.0
                    - pl.col("duration_per_pers").sum()
                    - pl.col("time").fill_null(0.0).sum()
                ).clip(0.0),
            )
            .join(home_night_dur.lazy(), on=["country", "csp"])
            .with_columns(
                min_activity_time=(
                    pl.col("mean_home_night_per_pers")
                    * math.exp(-min_activity_time_constant)
                ),
            )
            .with_columns(
                utility=(
                    pl.col("utility")
                    + resolved_activity_parameters["home"].value_of_time_stay_home
                    * pl.col("mean_home_night_per_pers")
                    * (
                        pl.col("home_night_per_pers")
                        / pl.col("min_activity_time")
                    ).log().clip(0.0)
                )
            )
        )

        window_columns = list(DEMAND_UNIT_COLS)
        if scope == BehaviorChangeScope.DESTINATION_REPLANNING:
            window_columns += ["activity_seq_id", "time_seq_id"]
        elif scope == BehaviorChangeScope.MODE_REPLANNING:
            window_columns += ["activity_seq_id", "time_seq_id", "dest_seq_id"]
        delta = self._scale_utility_pruning_delta(
            parameters.plan_update.transition_utility_pruning_delta,
            parameters.plan_update.transition_logit_scale,
        )
        if not math.isfinite(delta):
            return candidates.select(
                [
                    "_score_context_id",
                    "activity_seq_id",
                    "time_seq_id",
                    "dest_seq_id",
                    "mode_seq_id",
                ]
            ).unique().collect(engine="streaming")

        retained = (
            candidate_utilities
            .with_columns(max_utility=pl.col("utility").max().over(window_columns))
            .filter(pl.col("utility") >= pl.col("max_utility") - delta)
            .select(DEMAND_UNIT_COLS + ["activity_seq_id", "time_seq_id", "dest_seq_id", "mode_seq_id"])
            .unique()
        )
        context_ids = candidates.select(
            ["_score_context_id"] + DEMAND_UNIT_COLS
        ).unique()
        return (
            retained
            .join(context_ids, on=DEMAND_UNIT_COLS, how="inner")
            .select(
                [
                    "_score_context_id",
                    "activity_seq_id",
                    "time_seq_id",
                    "dest_seq_id",
                    "mode_seq_id",
                ]
            )
            .unique()
            .collect(engine="streaming")
        )

    @trace_simulation_step("compact step scoring")
    def compute_plan_steps_candidates_utility(
        self,
        *,
        candidates: pl.LazyFrame,
        transport_costs,
        demand_groups: pl.DataFrame | None = None,
        population_segments=None,
        utility_profiles=None,
        destination_saturation: pl.DataFrame,
        activity_dur: pl.DataFrame,
        transport_zones,
        resolved_activity_parameters: dict[str, Any],
        min_activity_time_constant,
        allow_missing_costs_for_current_plans: bool,
        use_destination_shadow_prices: bool = False,
    ) -> pl.LazyFrame:
        """Score plan-step candidates under current costs and destination saturation."""

        if utility_profiles is not None:
            profile_assignments = None
            cost_by_od_and_modes = transport_costs.get_profile_costs_by_od_and_mode(
                utility_profiles,
                ["cost", "distance", "time"],
            )
            cost_join_columns = ["utility_profile_id", "from", "to", "mode"]
        elif demand_groups is None:
            profile_assignments = None
            cost_by_od_and_modes = transport_costs.get_costs_by_od_and_mode(
                ["cost", "distance", "time"],
                detail_distances=False,
            )
            cost_join_columns = ["from", "to", "mode"]
        else:
            profile_assignments, utility_profiles = transport_costs.get_utility_profiles(
                demand_groups,
                population_segments or [],
            )
            cost_by_od_and_modes = transport_costs.get_profile_costs_by_od_and_mode(
                utility_profiles,
                ["cost", "distance", "time"],
            )
            cost_join_columns = ["utility_profile_id", "from", "to", "mode"]
        cost_by_od_and_modes = cost_by_od_and_modes.with_columns(
            mode=pl.col("mode").cast(
                pl.Enum(get_mode_values(transport_costs.modes, "stay_home"))
            ),
            _cost_row_found=pl.lit(True),
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
        destination_country = pl.from_pandas(
            transport_zones.get().drop("geometry", axis=1, errors="ignore")[["transport_zone_id", "country"]]
        ).with_columns(
            to=pl.col("transport_zone_id").cast(pl.Int32),
            destination_country=pl.col("country").cast(pl.Utf8),
        ).select(["to", "destination_country"])
        country_value_coefficients = pl.from_dicts(
            [
                {
                    "activity": activity_name,
                    "destination_country": destination_country,
                    "country_value_coefficient": coefficient,
                }
                for activity_name, activity_parameters in resolved_activity_parameters.items()
                for destination_country, coefficient in (activity_parameters.country_value_coefficients or {}).items()
            ],
            schema={
                "activity": pl.Utf8,
                "destination_country": pl.Utf8,
                "country_value_coefficient": pl.Float64,
            },
        ).with_columns(
            activity=pl.col("activity").cast(pl.Enum(activity_dur["activity"].dtype.categories))
        )
        destination_saturation = self._ensure_destination_saturation_columns(
            destination_saturation
        )
        possible_plan_step_columns = [
            "demand_group_id",
            "demand_subgroup_id",
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
            "last_seen_iteration",
            "last_active_iteration",
            "cost",
            "is_feasible",
            "_missing_cost_row",
            "_invalid_cost_row",
            "distance",
            "time",
            "mean_duration_per_pers",
            "value_of_time",
            "k_saturation_utility",
            "destination_shadow_price",
            "min_activity_time",
            "activity_utility_scale",
        ]

        candidate_cost_inputs = candidates.with_columns(
            activity=pl.col("activity").cast(activity_dur.schema["activity"])
        )
        if profile_assignments is not None:
            candidate_cost_inputs = candidate_cost_inputs.join(
                profile_assignments.lazy(),
                on=DEMAND_UNIT_COLS,
            )

        # Score each distinct step and utility context before expanding scores
        # back to demand units. Retention dates and unit ids do not affect utility.
        metadata_columns = DEMAND_UNIT_COLS + [
            "iteration", "first_seen_iteration", "last_seen_iteration", "last_active_iteration",
        ]
        score_columns = [
            column for column in CandidatePlanStepsAsset.STRUCTURAL_COLUMNS
            if column not in metadata_columns
        ]
        if "utility_profile_id" in candidate_cost_inputs.collect_schema().names():
            score_columns.append("utility_profile_id")
        compact_inputs = candidate_cost_inputs.select(score_columns).unique()

        scored_candidates = (
            compact_inputs
            .with_columns(
                activity=pl.col("activity").cast(pl.Enum(activity_dur["activity"].dtype.categories))
            )
            .join(
                cost_by_od_and_modes.lazy(),
                on=cost_join_columns,
                how="left" if allow_missing_costs_for_current_plans else "inner",
            )
            .join(activity_dur.lazy(), on=["country", "csp", "activity"])
            .join(value_of_time.lazy(), on="activity")
            .join(
                destination_saturation.select(
                    [
                        "to",
                        "activity",
                        "k_saturation_utility",
                        "destination_shadow_price",
                    ]
                ).lazy(),
                on=["to", "activity"],
                how="left",
            )
            .join(destination_country.lazy(), on="to", how="left")
            .join(
                country_value_coefficients.lazy(),
                on=["activity", "destination_country"],
                how="left",
            )
            .with_columns(
                # A complete plan is usable only when every leg has a finite
                # cost in this scenario. Keep this flag before filling costs
                # so missing routing data cannot make an impossible plan look
                # merely expensive.
                is_feasible=(
                    pl.col("cost").is_not_null()
                    & pl.col("cost").is_finite()
                ).fill_null(False),
                _missing_cost_row=pl.col("_cost_row_found").is_null(),
                _invalid_cost_row=(
                    pl.col("_cost_row_found").is_not_null()
                    & ~(
                        pl.col("cost").is_not_null()
                        & pl.col("cost").is_finite()
                    ).fill_null(False)
                ).fill_null(False),
                cost=(
                    pl.when(
                        pl.col("cost").is_not_null()
                        & pl.col("cost").is_finite()
                    )
                    .then(pl.col("cost"))
                    .otherwise(1e9)
                    if allow_missing_costs_for_current_plans
                    else pl.col("cost")
                ),
                distance=pl.col("distance").fill_null(0.0),
                time=pl.col("time").fill_null(0.0),
                k_saturation_utility=pl.col("k_saturation_utility").fill_null(1.0),
                destination_shadow_price=pl.col("destination_shadow_price").fill_null(0.0),
                country_value_coefficient=pl.col("country_value_coefficient").fill_null(1.0),
                min_activity_time=pl.col("mean_duration_per_pers") * math.exp(-min_activity_time_constant),
            )
            .with_columns(
                activity_utility_scale=(
                    (
                        pl.col("country_value_coefficient") * pl.col("value_of_time")
                        + pl.col("destination_shadow_price")
                        if use_destination_shadow_prices
                        else (
                            pl.col("country_value_coefficient")
                            * pl.col("k_saturation_utility")
                            * pl.col("value_of_time")
                        )
                    )
                    * pl.col("mean_duration_per_pers")
                )
            )
            .drop(["destination_country", "country_value_coefficient"])
            .select([column for column in possible_plan_step_columns if column not in metadata_columns]
                    + (["utility_profile_id"] if "utility_profile_id" in score_columns else []))
        )

        # Materialize the compact scores once. Keeping this entire cost query
        # lazy would retain all profile OD tables during demand-unit expansion.
        compact_scores = scored_candidates.collect(engine="streaming")
        log_memory_checkpoint("plan_updater:compact_scores", compact_scores=compact_scores)
        return candidate_cost_inputs.select(score_columns + metadata_columns).join(
            compact_scores.lazy(), on=score_columns, how="inner", nulls_equal=True,
        ).select(possible_plan_step_columns)

    def get_possible_plan_utility(
        self,
        possible_plan_steps,
        home_night_dur,
        value_of_time_stay_home,
        stay_home_plan,
        min_activity_time_constant,
        *,
        plan_id_index: pl.DataFrame,
    ):
        """Aggregate per-step utilities to plan-level utilities."""

        possible_plan_utility = (
            possible_plan_steps.filter(pl.col("mode_seq_id") != 0).group_by(
                ["plan_id"] + DEMAND_UNIT_COLS + ["country", "csp", "activity_seq_id", "time_seq_id", "dest_seq_id", "mode_seq_id"]
            )
            .agg(
                utility=pl.col("utility").sum(),
                is_feasible=pl.col("is_feasible").all(),
                missing_cost_row_count=pl.col("_missing_cost_row").sum(),
                invalid_cost_row_count=pl.col("_invalid_cost_row").sum(),
                home_night_per_pers=(
                    24.0 - pl.col("duration_per_pers").sum() - pl.col("time").fill_null(0.0).sum()
                ).clip(0.0),
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
            .select(
                DEMAND_UNIT_COLS
                + [
                    "activity_seq_id",
                    "time_seq_id",
                    "mode_seq_id",
                    "dest_seq_id",
                    "utility",
                    "is_feasible",
                    "missing_cost_row_count",
                    "invalid_cost_row_count",
                ]
            )
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
                    ),
                    is_feasible=pl.lit(True),
                    missing_cost_row_count=pl.lit(0, dtype=pl.UInt32),
                    invalid_cost_row_count=pl.lit(0, dtype=pl.UInt32),
                )
                .select(
                    DEMAND_UNIT_COLS
                    + [
                        "activity_seq_id",
                        "time_seq_id",
                        "mode_seq_id",
                        "dest_seq_id",
                        "utility",
                        "is_feasible",
                        "missing_cost_row_count",
                        "invalid_cost_row_count",
                    ]
                ),
            ]
        )

        return add_plan_id(possible_plan_utility, previous_index=plan_id_index)

    def add_stay_home_plan_steps(
        self,
        possible_plan_steps: pl.LazyFrame,
        stay_home_plan: pl.DataFrame,
        *,
        plan_id_index: pl.DataFrame,
    ) -> tuple[pl.LazyFrame, pl.DataFrame]:
        """Append one synthetic timed step for the stay-home state so it can be embedded like other plans."""
        step_columns = [col for col in possible_plan_steps.collect_schema().names() if col != "plan_id"]
        possible_plan_steps_no_id = possible_plan_steps.filter(pl.col("mode_seq_id") != 0).select(step_columns)

        stay_home_steps = (
            stay_home_plan.lazy()
            .with_columns(
                cost=pl.lit(0.0),
                distance=pl.lit(0.0),
                time=pl.lit(0.0),
                is_feasible=pl.lit(True),
                mean_duration_per_pers=pl.col("duration_per_pers"),
                value_of_time=pl.lit(0.0),
                k_saturation_utility=pl.lit(1.0),
                destination_shadow_price=pl.lit(0.0),
                min_activity_time=pl.lit(0.0),
                first_seen_iteration=pl.lit(None, dtype=pl.UInt16),
                last_seen_iteration=pl.lit(None, dtype=pl.UInt16),
                last_active_iteration=pl.lit(None, dtype=pl.UInt16),
            )
            .select(step_columns)
        )

        return add_plan_id(
            pl.concat([possible_plan_steps_no_id, stay_home_steps], how="vertical_relaxed"),
            previous_index=plan_id_index,
        )

    @trace_simulation_step("behavior transition probabilities")
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
        min_transition_utility_gain: float = 0.0,
        transition_distance_friction: float = 0.0,
        plan_embedding_dimension_weights: list[float] | None = None,
    ) -> pl.DataFrame:
        """Compute transition probabilities from current to candidate plans."""

        allowed_transitions = self.build_allowed_plan_transitions(
            current_plans,
            possible_plan_utility,
            behavior_change_scope,
            transition_utility_pruning_delta=transition_utility_pruning_delta,
            transition_logit_scale=transition_logit_scale,
            min_transition_utility_gain=min_transition_utility_gain,
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
        min_transition_utility_gain: float = 0.0,
    ) -> pl.LazyFrame:
        """Build allowed from-to plan pairs under the active behavior scope."""
        logging.debug(
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
        has_feasibility = "is_feasible" in possible_plan_utility.collect_schema().names()
        # Mode and destination replanning keep the survey schedule choice.
        # Travel-time adjustments can give its alternatives different time ids.
        timing_constraint_column = (
            "reference_time_seq_id"
            if "reference_time_seq_id" in possible_plan_utility.collect_schema().names()
            else "time_seq_id"
        )
        forced_reassignment = (
            ~pl.col("is_feasible").fill_null(True)
            if has_feasibility
            else pl.lit(False)
        )

        # Keep infeasible current plans as transition origins so they can be
        # reassigned, but never let them remain valid transition targets.
        if has_feasibility:
            possible_plan_utility_for_transitions = (
                possible_plan_utility_for_transitions
                .filter(pl.col("is_feasible"))
            )

        if behavior_change_scope not in (
            BehaviorChangeScope.FULL_REPLANNING,
            BehaviorChangeScope.NO_TRANSITIONS,
        ):
            current_plans_for_transitions = current_plans_for_transitions.filter(pl.col("mode_seq_id") != 0)

        scope_pair_constraint = pl.lit(True)
        if behavior_change_scope == BehaviorChangeScope.DESTINATION_REPLANNING:
            scope_pair_constraint = (
                (pl.col("activity_seq_id") == pl.col("activity_seq_id_trans"))
                & (pl.col(timing_constraint_column) == pl.col(timing_constraint_column + "_trans"))
            )
        elif behavior_change_scope == BehaviorChangeScope.MODE_REPLANNING:
            scope_pair_constraint = (
                (pl.col("activity_seq_id") == pl.col("activity_seq_id_trans"))
                & (pl.col(timing_constraint_column) == pl.col(timing_constraint_column + "_trans"))
                & (pl.col("dest_seq_id") == pl.col("dest_seq_id_trans"))
            )
        elif behavior_change_scope == BehaviorChangeScope.NO_TRANSITIONS:
            scope_pair_constraint = (
                (pl.col("activity_seq_id") == pl.col("activity_seq_id_trans"))
                & (pl.col("time_seq_id") == pl.col("time_seq_id_trans"))
                & (pl.col("dest_seq_id") == pl.col("dest_seq_id_trans"))
                & (pl.col("mode_seq_id") == pl.col("mode_seq_id_trans"))
            )
        if behavior_change_scope not in (
            BehaviorChangeScope.FULL_REPLANNING,
            BehaviorChangeScope.NO_TRANSITIONS,
        ):
            # Restricted phases normally keep stay-home frozen. A person on
            # an infeasible plan must still be able to move to stay-home when
            # no usable non-stay-home alternative exists.
            scope_pair_constraint = (
                (scope_pair_constraint & (pl.col("mode_seq_id_trans") != 0))
                | forced_reassignment
            )
        else:
            scope_pair_constraint = scope_pair_constraint | forced_reassignment

        is_self_transition = (
            (pl.col("activity_seq_id") == pl.col("activity_seq_id_trans"))
            & (pl.col("time_seq_id") == pl.col("time_seq_id_trans"))
            & (pl.col("dest_seq_id") == pl.col("dest_seq_id_trans"))
            & (pl.col("mode_seq_id") == pl.col("mode_seq_id_trans"))
        )

        utility_filter = pl.lit(True)
        if math.isfinite(utility_pruning_delta):
            utility_filter = (
                forced_reassignment
                | is_self_transition
                | (pl.col("utility_trans") >= pl.col("max_utility_trans") - utility_pruning_delta)
            )
        gain_filter = (
            forced_reassignment
            | is_self_transition
            | (pl.col("utility_trans") >= pl.col("utility") + min_transition_utility_gain)
        )

        return (
            current_plans_for_transitions
            .select(plan_cols + ["utility"])
            .rename({"utility": "utility_prev_from"})
            .join(possible_plan_utility, on=plan_cols)
            .rename({"plan_id": "plan_id_from"})
            .join_where(
                possible_plan_utility_for_transitions,
                (
                    (pl.col("demand_group_id") == pl.col("demand_group_id_trans"))
                    & (pl.col("demand_subgroup_id") == pl.col("demand_subgroup_id_trans"))
                    & scope_pair_constraint
                ),
                suffix="_trans",
            )
            .drop("demand_group_id_trans", "demand_subgroup_id_trans")
            .rename({"plan_id": "plan_id_trans"})
            .with_columns(
                max_utility_trans=pl.col("utility_trans").max().over(plan_cols),
            )
            .filter(utility_filter & gain_filter)
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

        logging.debug("Computing PopulationGroupDayTrips transition distances")

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
        logging.debug("Collecting PopulationGroupDayTrips transition probabilities")

        plan_cols = PLAN_KEY_COLS
        has_feasibility = "is_feasible" in allowed_transitions.collect_schema().names()
        forced_reassignment = (
            ~pl.col("is_feasible").fill_null(True)
            if has_feasibility
            else pl.lit(False)
        )
        if "distance" not in allowed_transitions.collect_schema().names():
            allowed_transitions = allowed_transitions.with_columns(
                distance=pl.lit(0.0, dtype=pl.Float64)
            )
        is_self_transition = pl.col("plan_id_from") == pl.col("plan_id_trans")
        filtered_transitions = allowed_transitions
        if math.isfinite(transition_distance_threshold):
            filtered_transitions = allowed_transitions.filter(
                is_self_transition
                | forced_reassignment
                | (pl.col("distance").fill_null(0.0) <= transition_distance_threshold)
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
        revision_probability = (
            pl.when(forced_reassignment)
            .then(1.0)
            .otherwise(pl.lit(transition_revision_probability, dtype=pl.Float64))
        )

        transition_probabilities = (
            utility_weighted_transitions
            .with_columns(
                is_self_transition=is_self_transition,
                q_transition=q_transition,
                tau_transition=friction * pl.col("distance").fill_null(0.0),
                adjustment_factor=revision_probability,
            )
            .with_columns(
                p_transition=pl.when(pl.col("is_self_transition") & ~forced_reassignment)
                .then(1.0 - revision_probability + revision_probability * pl.col("q_transition"))
                .otherwise(revision_probability * pl.col("q_transition"))
            )
            .select(
                [
                    "plan_id_from",
                    "plan_id_trans",
                    "demand_group_id",
                    "demand_subgroup_id",
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

        logging.debug(
            "Finished collecting PopulationGroupDayTrips transition probabilities."
        )

        return transition_probabilities

    @trace_simulation_step("apply behavior transitions")
    def apply_transitions(
        self,
        current_plans: pl.DataFrame,
        transition_probabilities: pl.DataFrame,
        iteration: int,
        *,
        plan_probability_pruning_retained_share: float = 1.0,
        plan_probability_pruning_min_step: int = 2,
    ) -> tuple[pl.DataFrame, pl.LazyFrame]:
        """Apply transition probabilities and emit transition events."""
        plan_cols = PLAN_KEY_COLS

        transitions = (
            current_plans.lazy()
            # Keep the source id even when no transition row matches this plan.
            .with_columns(plan_id_from=pl.col("plan_id"))
            .join(transition_probabilities.lazy(), on=plan_cols, how="left")
            .with_columns(
                plan_id_trans=pl.coalesce(
                    [pl.col("plan_id_trans"), pl.col("plan_id_from")]
                ),
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

        transitions = self.remap_low_probability_transition_targets(
            transitions,
            retained_share=plan_probability_pruning_retained_share,
            min_iteration=plan_probability_pruning_min_step,
            iteration=iteration,
        )
        transitions = self.attach_previous_target_utility(current_plans, transitions)

        transition_events = build_transition_events_lazy(
            transitions,
            iteration=iteration,
        )

        new_states = (
            transitions.group_by(
                ["plan_id_trans"] + DEMAND_UNIT_COLS + ["activity_seq_id_trans", "time_seq_id_trans", "dest_seq_id_trans", "mode_seq_id_trans"]
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

    def remap_low_probability_transition_targets(
        self,
        transitions: pl.LazyFrame,
        *,
        retained_share: float,
        min_iteration: int,
        iteration: int,
    ) -> pl.LazyFrame:
        """Merge low-mass target plans into the largest retained plan per demand group."""
        if retained_share >= 1.0 or iteration < min_iteration:
            return transitions

        target_map = self.build_low_probability_target_map(
            transitions,
            retained_share=retained_share,
        )
        if target_map is None:
            return transitions

        raw_plan_count = int(target_map["raw_plan_count"][0])
        retained_plan_count = int(target_map["retained_plan_count"][0])
        dropped_person_share = float(target_map["dropped_person_share"][0])
        logging.debug(
            "Low-probability current-plan pruning check: iteration=%s retained_share=%s target_plans=%s retained_plans=%s dropped_person_share=%s",
            str(iteration),
            str(retained_share),
            str(raw_plan_count),
            str(retained_plan_count),
            str(round(dropped_person_share, 6)),
        )
        if retained_plan_count == raw_plan_count:
            return transitions

        mapping = target_map.drop(["raw_plan_count", "retained_plan_count", "dropped_person_share"])
        return (
            transitions
            .join(mapping.lazy(), on="plan_id_trans", how="left")
            .with_columns(
                plan_id_trans=pl.coalesce([pl.col("plan_id_trans_final"), pl.col("plan_id_trans")]),
                activity_seq_id_trans=pl.coalesce(
                    [pl.col("activity_seq_id_trans_final"), pl.col("activity_seq_id_trans")]
                ),
                time_seq_id_trans=pl.coalesce([pl.col("time_seq_id_trans_final"), pl.col("time_seq_id_trans")]),
                dest_seq_id_trans=pl.coalesce([pl.col("dest_seq_id_trans_final"), pl.col("dest_seq_id_trans")]),
                mode_seq_id_trans=pl.coalesce([pl.col("mode_seq_id_trans_final"), pl.col("mode_seq_id_trans")]),
                utility_trans=pl.coalesce([pl.col("utility_trans_final"), pl.col("utility_trans")]),
            )
            .drop(
                [
                    "plan_id_trans_final",
                    "activity_seq_id_trans_final",
                    "time_seq_id_trans_final",
                    "dest_seq_id_trans_final",
                    "mode_seq_id_trans_final",
                    "utility_trans_final",
                ]
            )
        )

    def build_low_probability_target_map(
        self,
        transitions: pl.LazyFrame,
        *,
        retained_share: float,
    ) -> pl.DataFrame | None:
        """Build a raw-target to retained-target map from transition mass."""
        target_cols = [
            "plan_id_trans",
            "demand_group_id",
            "demand_subgroup_id",
            "activity_seq_id_trans",
            "time_seq_id_trans",
            "dest_seq_id_trans",
            "mode_seq_id_trans",
            "utility_trans",
        ]
        target_states = (
            transitions
            .group_by(target_cols)
            .agg(n_persons=pl.col("n_persons_moved").sum())
            .with_columns(is_stay_home_target=pl.col("mode_seq_id_trans") == 0)
            .collect(engine="streaming")
        )
        if target_states.height == 0:
            return None

        ranked = (
            target_states
            .sort(
                [
                    "demand_group_id",
                    "demand_subgroup_id",
                    "is_stay_home_target",
                    "n_persons",
                    "activity_seq_id_trans",
                    "time_seq_id_trans",
                    "dest_seq_id_trans",
                    "mode_seq_id_trans",
                ],
                descending=[False, False, False, True, False, False, False, False],
            )
            .with_columns(
                group_n_persons=pl.col("n_persons").sum().over(DEMAND_UNIT_COLS + ["is_stay_home_target"]),
                cumulative_n_persons=pl.col("n_persons").cum_sum().over(DEMAND_UNIT_COLS + ["is_stay_home_target"]),
                group_rank=pl.col("n_persons").cum_count().over(DEMAND_UNIT_COLS + ["is_stay_home_target"]),
            )
            .with_columns(
                previous_cumulative_share=(
                    (pl.col("cumulative_n_persons") - pl.col("n_persons"))
                    / pl.col("group_n_persons").clip(1e-12)
                )
            )
            .with_columns(
                is_retained=(pl.col("group_rank") == 1) | (pl.col("previous_cumulative_share") < retained_share)
            )
        )
        fallback_targets = (
            ranked
            .filter(pl.col("is_retained"))
            .filter(
                pl.col("group_rank")
                == pl.col("group_rank").min().over(DEMAND_UNIT_COLS + ["is_stay_home_target"])
            )
            .select(
                [
                    "demand_group_id",
                    "demand_subgroup_id",
                    "is_stay_home_target",
                    pl.col("plan_id_trans").alias("fallback_plan_id_trans"),
                    pl.col("activity_seq_id_trans").alias("fallback_activity_seq_id_trans"),
                    pl.col("time_seq_id_trans").alias("fallback_time_seq_id_trans"),
                    pl.col("dest_seq_id_trans").alias("fallback_dest_seq_id_trans"),
                    pl.col("mode_seq_id_trans").alias("fallback_mode_seq_id_trans"),
                    pl.col("utility_trans").alias("fallback_utility_trans"),
                ]
            )
        )
        mapping = (
            ranked
            .join(fallback_targets, on=DEMAND_UNIT_COLS + ["is_stay_home_target"], how="left")
            .with_columns(
                plan_id_trans_final=pl.when(pl.col("is_retained"))
                .then(pl.col("plan_id_trans"))
                .otherwise(pl.col("fallback_plan_id_trans")),
                activity_seq_id_trans_final=pl.when(pl.col("is_retained"))
                .then(pl.col("activity_seq_id_trans"))
                .otherwise(pl.col("fallback_activity_seq_id_trans")),
                time_seq_id_trans_final=pl.when(pl.col("is_retained"))
                .then(pl.col("time_seq_id_trans"))
                .otherwise(pl.col("fallback_time_seq_id_trans")),
                dest_seq_id_trans_final=pl.when(pl.col("is_retained"))
                .then(pl.col("dest_seq_id_trans"))
                .otherwise(pl.col("fallback_dest_seq_id_trans")),
                mode_seq_id_trans_final=pl.when(pl.col("is_retained"))
                .then(pl.col("mode_seq_id_trans"))
                .otherwise(pl.col("fallback_mode_seq_id_trans")),
                utility_trans_final=pl.when(pl.col("is_retained"))
                .then(pl.col("utility_trans"))
                .otherwise(pl.col("fallback_utility_trans")),
            )
        )

        raw_plan_count = target_states.height
        retained_plan_count = mapping.filter(pl.col("is_retained")).height

        dropped_persons = mapping.filter(~pl.col("is_retained")).select(pl.col("n_persons").sum()).item()
        total_persons = mapping.select(pl.col("n_persons").sum()).item()

        return mapping.select(
            [
                "plan_id_trans",
                "plan_id_trans_final",
                "activity_seq_id_trans_final",
                "time_seq_id_trans_final",
                "dest_seq_id_trans_final",
                "mode_seq_id_trans_final",
                "utility_trans_final",
                pl.lit(raw_plan_count, dtype=pl.UInt32).alias("raw_plan_count"),
                pl.lit(retained_plan_count, dtype=pl.UInt32).alias("retained_plan_count"),
                pl.lit(float(dropped_persons) / max(float(total_persons), 1e-12)).alias("dropped_person_share"),
            ]
        )

    def attach_previous_target_utility(
        self,
        current_plans: pl.DataFrame,
        transitions: pl.LazyFrame,
    ) -> pl.LazyFrame:
        """Attach previous utility for the final transition target state."""
        prev_to_lookup = (
            current_plans.lazy()
            .select(DEMAND_UNIT_COLS + ["activity_seq_id", "time_seq_id", "dest_seq_id", "mode_seq_id", "utility"])
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

        return transitions.join(
            prev_to_lookup,
            on=[
                "demand_group_id",
                "demand_subgroup_id",
                "activity_seq_id_trans",
                "time_seq_id_trans",
                "dest_seq_id_trans",
                "mode_seq_id_trans",
            ],
            how="left",
        )

    @trace_simulation_step("expand allocated plan steps")
    def get_current_plan_steps(self, current_plans, possible_plan_steps):
        """Expand aggregate plans to per-step rows."""
        current_plan_steps = (
            current_plans.select(
                [
                    "demand_group_id",
                    "demand_subgroup_id",
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
                        "demand_subgroup_id",
                        "activity_seq_id",
                        "time_seq_id",
                        "dest_seq_id",
                        "mode_seq_id",
                        "seq_step_index",
                        "activity",
                        "from",
                        "to",
                        "mode",
                        "distance",
                        "time",
                        "duration_per_pers",
                        "departure_time",
                        "arrival_time",
                        "next_departure_time",
                    ]
                ),
                on=PLAN_KEY_COLS,
                how="left",
            )
            .with_columns(
                duration=(pl.col("duration_per_pers").fill_null(24.0) * pl.col("n_persons")).cast(pl.Float32)
            )
            .drop("duration_per_pers")
            .collect(engine="streaming")
        )

        return current_plan_steps.drop("plan_id") if "plan_id" in current_plan_steps.columns else current_plan_steps

    @trace_simulation_step("destination saturation")
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

        logging.debug("Computing destination saturation at destinations...")

        saturation_fun_parameters = (
            pl.from_dicts(
                [
                    {
                        "activity": activity_name,
                        "beta": activity_parameters.saturation_fun_beta,
                        "ref_level": activity_parameters.saturation_fun_ref_level,
                        "param_destination_soft_capacity_factor": (
                            activity_parameters.destination_soft_capacity_factor
                        ),
                        "destination_shadow_price_sensitivity": (
                            activity_parameters.destination_shadow_price_sensitivity_coefficient
                            * activity_parameters.value_of_time
                        ),
                        "destination_shadow_price_min": (
                            activity_parameters.destination_shadow_price_min_coefficient
                            * activity_parameters.value_of_time
                        ),
                        "destination_sampling_overload_gamma": (
                            activity_parameters.destination_sampling_overload_gamma
                        ),
                        "destination_sampling_min_attraction_factor": (
                            activity_parameters.destination_sampling_min_attraction_factor
                        ),
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
            .with_columns(
                destination_soft_capacity_factor=pl.col("param_destination_soft_capacity_factor")
            )
            .with_columns(opportunity_occupation=pl.col("opportunity_occupation").fill_null(0.0))
            .with_columns(
                capacity_ratio=pl.col("opportunity_occupation") / pl.col("opportunity_capacity").clip(1e-12),
                soft_capacity=(
                    pl.col("destination_soft_capacity_factor")
                    * pl.col("opportunity_capacity")
                ),
            )
            .with_columns(
                k_saturation_utility=(
                    1.0 - pl.col("capacity_ratio").pow(pl.col("beta")) / (pl.col("ref_level").pow(pl.col("beta")))
                ).clip(0.0)
            )
            .with_columns(
                overload=pl.col("opportunity_occupation") / pl.col("soft_capacity").clip(1e-12),
            )
            .with_columns(
                destination_shadow_price=(
                    pl.when(pl.col("overload") <= 1.0)
                    .then(0.0)
                    .otherwise(
                        -pl.col("destination_shadow_price_sensitivity")
                        * pl.col("overload").log()
                    )
                    .clip(pl.col("destination_shadow_price_min"), 0.0)
                )
            )
            .with_columns(
                destination_sampling_attraction_factor=(
                    pl.when(pl.col("overload") <= 1.0)
                    .then(1.0)
                    .otherwise(
                        pl.col("overload").pow(
                            -pl.col("destination_sampling_overload_gamma")
                        )
                    )
                    .clip(pl.col("destination_sampling_min_attraction_factor"), 1.0)
                )
            )
            .select([
                "activity",
                "to",
                "opportunity_capacity",
                "opportunity_occupation",
                "capacity_ratio",
                "destination_soft_capacity_factor",
                "k_saturation_utility",
                "destination_shadow_price",
                "destination_sampling_overload_gamma",
                "destination_sampling_min_attraction_factor",
                "destination_sampling_attraction_factor",
            ])
        )

        return destination_saturation

    @staticmethod
    def _ensure_destination_saturation_columns(destination_saturation: pl.DataFrame) -> pl.DataFrame:
        """Add shadow-price columns when an older saturation table is provided."""
        columns = set(destination_saturation.columns)
        expressions = []
        if "k_saturation_utility" not in columns:
            expressions.append(pl.lit(1.0, dtype=pl.Float64).alias("k_saturation_utility"))
        if "destination_shadow_price" not in columns:
            expressions.append(pl.lit(0.0, dtype=pl.Float64).alias("destination_shadow_price"))
        if "destination_sampling_overload_gamma" not in columns:
            expressions.append(
                pl.lit(1.5, dtype=pl.Float64).alias("destination_sampling_overload_gamma")
            )
        if "destination_sampling_min_attraction_factor" not in columns:
            expressions.append(
                pl.lit(0.05, dtype=pl.Float64).alias(
                    "destination_sampling_min_attraction_factor"
                )
            )
        if "destination_sampling_attraction_factor" not in columns:
            expressions.append(
                pl.lit(1.0, dtype=pl.Float64).alias(
                    "destination_sampling_attraction_factor"
                )
            )
        if "opportunity_occupation" not in columns:
            expressions.append(pl.lit(0.0, dtype=pl.Float64).alias("opportunity_occupation"))
        if "capacity_ratio" not in columns:
            expressions.append(pl.lit(0.0, dtype=pl.Float64).alias("capacity_ratio"))
        if "destination_soft_capacity_factor" not in columns:
            expressions.append(pl.lit(1.25, dtype=pl.Float64).alias("destination_soft_capacity_factor"))
        if not expressions:
            return destination_saturation
        return destination_saturation.with_columns(expressions)
