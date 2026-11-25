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
from .compact_candidate_steps import (
    CompactCandidateSteps, ScoringCandidateSteps, MEMBERSHIP_COLUMNS, PLAN_COLUMNS, STEP_COLUMNS, STEP_KEY_COLUMNS,
)
from .factored_plan_timing import time_factored_candidates
from .factored_plan_scoring import SectionScoreCache, score_factored_sections
from .whole_plan_cache import WholePlanCache, make_state, prepare_reuse, run_reuse
from .destination_saturation import get_destination_saturation, ensure_destination_saturation_columns
from .scoring_contexts import ScoringContexts, ScoringLookup, ScoringMemoryBudget
from .demand_subgroups import DEMAND_UNIT_COLS
from .plan_distance import PlanDistance
from .plan_ids import PLAN_KEY_COLS, add_plan_id
from .plan_schedule_updater import PlanScheduleUpdater
from .time_sequences import TimeSequenceIndexCache, assign_time_sequence_ids
from .destination_sequences import DestinationSequences
from .mode_sequence_search import ModeSequences

_REACHABLE_ALLOCATION_BATCH_ROWS = 500_000


@dataclass(frozen=True)
class SharedSectionStepDetails:
    """Inputs for expanding only the scored plans needed by later steps."""

    class_plans: pl.DataFrame
    physical: pl.DataFrame
    lookup: pl.DataFrame
    history: pl.DataFrame
    plan_ids: pl.DataFrame
    stay_home: pl.DataFrame

    def for_storage(self, plan_ids: pl.DataFrame) -> pl.DataFrame:
        """Build the narrow step table needed after the current search."""
        columns = PLAN_KEY_COLS + [
            "plan_id", "seq_step_index", "activity", "from", "to", "mode", "distance", "time",
            "duration_per_pers", "departure_time", "arrival_time", "next_departure_time",
        ]
        keys = ["country", "csp", "activity", "from", "to", "mode", "utility_profile_id"]
        selected = self.class_plans.join(plan_ids, on="plan_id", how="semi")
        mobile = (selected.join(self.physical, on="_timing_plan")
                  .join(self.lookup.select(keys + ["distance", "time"]), on=keys)
                  .select(columns))
        stay = (self.stay_home.join(self.plan_ids, on=PLAN_KEY_COLS)
                .join(plan_ids, on="plan_id", how="semi")
                .with_columns(distance=pl.lit(0.0), time=pl.lit(0.0))
                .select(columns))
        return pl.concat([mobile, stay], how="vertical_relaxed")

    def select(self, updater, plan_ids: pl.DataFrame | None = None) -> pl.LazyFrame:
        selected = self.class_plans.lazy()
        stay = self.stay_home
        if plan_ids is not None:
            selected = selected.join(plan_ids.lazy(), on="plan_id", how="semi")
            stay = (stay.join(self.plan_ids, on=PLAN_KEY_COLS)
                    .join(plan_ids, on="plan_id", how="semi").drop("plan_id"))
        score_keys = ["country", "csp", "activity", "from", "to", "mode", "utility_profile_id"]
        score_values = ["cost", "distance", "time", "is_feasible", "min_activity_time", "activity_utility_scale"]
        history_values = ["iteration"] + CandidatePlanStepsAsset.RETENTION_COLUMNS
        steps = (selected.join(self.physical.lazy(), on="_timing_plan")
                 .join(self.lookup.select(score_keys + score_values).lazy(), on=score_keys)
                 .join(self.history.select(PLAN_KEY_COLS + history_values).lazy(), on=PLAN_KEY_COLS)
                 .with_columns(
                     is_feasible=(pl.col("is_feasible") & (
                         (pl.col("seq_step_index") == pl.col("seq_step_index").max().over(PLAN_KEY_COLS))
                         | (pl.col("next_departure_time") >= pl.col("arrival_time")))).fill_null(False),
                     utility=pl.col("activity_utility_scale")
                     * (pl.col("duration_per_pers") / pl.col("min_activity_time")).log().clip(0.0) - pl.col("cost"),
                 ).drop("activity_utility_scale", "utility_profile_id", "_timing_plan", "previous_time_seq_id"))
        return updater.add_stay_home_plan_steps(
            steps, stay, plan_id_index=self.plan_ids, plan_id_index_complete=True,
        )[0]


@dataclass(frozen=True)
class CandidatePlanEvaluation:
    """Scenario-specific utilities and step details for one shared pool."""

    candidate_plan_steps: pl.DataFrame | CompactCandidateSteps
    possible_plan_steps: pl.DataFrame | pl.LazyFrame
    possible_plan_utility: pl.DataFrame | pl.LazyFrame
    plan_id_index: pl.DataFrame
    current_plans: pl.DataFrame | None = None
    time_sequence_steps: pl.DataFrame | None = None
    scoring_contexts: ScoringContexts | None = None
    time_mapping: pl.DataFrame | None = None
    compact_transition_evaluation: "CandidatePlanEvaluation | None" = None
    context_transition_probabilities: pl.DataFrame | None = None
    shared_plan_ids: pl.DataFrame | None = None
    shared_steps: pl.DataFrame | None = None
    shared_step_details: SharedSectionStepDetails | None = None


class PlanUpdater:
    """Updates population plan distributions over activity/destination/mode sequences."""

    def __init__(self) -> None:
        self.schedule_updater = PlanScheduleUpdater()

    @staticmethod
    def _merge_modeled_plan_history(steps: pl.DataFrame | pl.LazyFrame) -> pl.DataFrame | pl.LazyFrame:
        """Combine histories when input schedules become the same modeled plan.

        Use the same earliest/latest rules as the shared candidate catalogue.
        All steps of the resulting plan must carry the same history.
        """
        return steps.with_columns(
            pl.col("iteration").min().over(PLAN_KEY_COLS),
            pl.col("first_seen_iteration").min().over(PLAN_KEY_COLS),
            pl.col("last_seen_iteration").max().over(PLAN_KEY_COLS),
            pl.col("last_active_iteration").max().over(PLAN_KEY_COLS),
        )

    def expand_context_evaluation(
        self, evaluation: CandidatePlanEvaluation, current_plans: pl.DataFrame,
        previous_plan_id_index: pl.DataFrame | None,
        behavior_change_scope: BehaviorChangeScope = BehaviorChangeScope.FULL_REPLANNING,
        *, share_transition_probabilities: bool = False,
        transition_parameters: dict[str, Any] | None = None,
    ) -> CandidatePlanEvaluation:
        """Map compact scores to demand units and their persistent plan IDs."""
        contexts = evaluation.scoring_contexts
        if contexts is None or evaluation.time_mapping is None:
            raise ValueError("A compact evaluation and its timing mapping are required.")
        # Map occupied plans first: their actual targets, rather than every
        # scored candidate, determine which new unit-level IDs are needed.
        mapping = evaluation.time_mapping.drop("demand_subgroup_id").rename(
            {"demand_group_id": "_score_context_id"}
        )
        origin_keys = ["_score_context_id", "activity_seq_id", "previous_time_seq_id", "dest_seq_id", "mode_seq_id"]
        origins = (
            current_plans.drop("plan_id", strict=False).rename({"time_seq_id": "previous_time_seq_id"})
            .join(contexts.unit_map, on=DEMAND_UNIT_COLS, how="left")
            .join(mapping, on=origin_keys, how="left")
            .with_columns(time_seq_id=pl.col("time_seq_id").fill_null(pl.col("previous_time_seq_id")))
            .group_by(PLAN_KEY_COLS)
            .agg(n_persons=pl.col("n_persons").sort().implode().list.sum(), utility=pl.col("utility").first())
        )
        utilities = contexts.expand_step_rows(evaluation.possible_plan_utility).drop("plan_id")
        context_probabilities = evaluation.context_transition_probabilities
        if share_transition_probabilities and transition_parameters is not None:
            if context_probabilities is None:
                choices = (
                    evaluation.possible_plan_utility
                    .join(evaluation.time_sequence_steps.select("time_seq_id", "reference_time_seq_id").unique(),
                          on="time_seq_id", how="left")
                    .with_columns(reference_time_seq_id=pl.col("reference_time_seq_id").fill_null(pl.col("time_seq_id")))
                )
                context_steps = evaluation.possible_plan_steps
                if (
                    transition_parameters.get("enable_transition_distance_model", False)
                    and math.isfinite(transition_parameters.get("transition_distance_threshold", math.inf))
                    and isinstance(context_steps, pl.LazyFrame)
                ):
                    context_steps = context_steps.collect(engine="streaming")
                context_probabilities = self.get_transition_probabilities(
                    evaluation.current_plans, choices.lazy(), context_steps,
                    behavior_change_scope, **transition_parameters,
                )
            context_keys = ["_score_context_id"] + PLAN_COLUMNS
            target_columns = [column + "_trans" for column in PLAN_COLUMNS]
            target_keys = (
                origins.join(contexts.unit_map, on=DEMAND_UNIT_COLS)
                .select(DEMAND_UNIT_COLS + context_keys)
                .join(
                    context_probabilities.drop("demand_subgroup_id").rename(
                        {"demand_group_id": "_score_context_id"}
                    ).select(context_keys + target_columns),
                    on=context_keys, how="inner",
                )
                .select(DEMAND_UNIT_COLS + target_columns)
                .rename({column + "_trans": column for column in PLAN_COLUMNS})
            )
            needed_keys = [origins.select(PLAN_KEY_COLS), target_keys]
            if behavior_change_scope != BehaviorChangeScope.NO_TRANSITIONS:
                needed_keys.append(origins.select(DEMAND_UNIT_COLS).unique().with_columns(
                    **{column: pl.lit(0, dtype=pl.UInt32) for column in PLAN_COLUMNS}
                ))
            needed_keys = pl.concat(needed_keys).unique()
            _, plan_id_index = add_plan_id(needed_keys, previous_index=previous_plan_id_index)
            utilities = utilities.join(plan_id_index.lazy(), on=PLAN_KEY_COLS, how="inner")
        else:
            utilities, plan_id_index = add_plan_id(utilities, previous_index=previous_plan_id_index)
        steps = (
            contexts.expand_step_rows(evaluation.possible_plan_steps).drop("plan_id")
            .join(plan_id_index.lazy(), on=PLAN_KEY_COLS, how="inner")
        )
        # Keep context-level history and its unit mapping separate. This avoids
        # copying the same membership to every equivalent demand unit.
        compact_candidates = (
            evaluation.candidate_plan_steps
            if isinstance(evaluation.candidate_plan_steps, CompactCandidateSteps)
            else CompactCandidateSteps.from_frame(evaluation.candidate_plan_steps, prefer_compact=True)
        )
        if isinstance(compact_candidates, CompactCandidateSteps):
            candidates = CompactCandidateSteps(
                steps=compact_candidates.steps,
                membership=compact_candidates.membership,
                columns=compact_candidates.columns,
                unit_map=contexts.unit_map,
            )
        else:
            candidates = contexts.expand_step_rows(evaluation.candidate_plan_steps).collect(engine="streaming")
        origins = origins.join(plan_id_index, on=PLAN_KEY_COLS, how="left")
        if origins["plan_id"].null_count():
            raise ValueError("A current plan is missing from the expanded evaluation.")
        if not share_transition_probabilities:
            if behavior_change_scope != BehaviorChangeScope.FULL_REPLANNING:
                utilities = self._restrict_transition_choices(
                    utilities, origins, evaluation, behavior_change_scope,
                )
            utilities = utilities.collect(engine="streaming")
        return CandidatePlanEvaluation(
            candidate_plan_steps=candidates, possible_plan_steps=steps,
            possible_plan_utility=utilities, plan_id_index=plan_id_index,
            current_plans=origins, time_sequence_steps=evaluation.time_sequence_steps,
            compact_transition_evaluation=CandidatePlanEvaluation(
                candidate_plan_steps=pl.DataFrame(), possible_plan_steps=pl.DataFrame(),
                possible_plan_utility=evaluation.possible_plan_utility,
                plan_id_index=evaluation.plan_id_index, time_sequence_steps=evaluation.time_sequence_steps,
                scoring_contexts=contexts,
            ) if share_transition_probabilities else None,
            context_transition_probabilities=context_probabilities,
        )

    def get_context_transition_probabilities(
        self, evaluation: CandidatePlanEvaluation, scope: BehaviorChangeScope, *,
        transport_zones: Any, **transition_parameters: Any,
    ) -> pl.DataFrame:
        """Share probabilities by scoring context and occupied origin plan.

        Restore each unit's own plan IDs and previous utility before applying
        the probabilities, so population pruning and event details stay local.
        """
        compact = evaluation.compact_transition_evaluation
        contexts = compact.scoring_contexts
        origins = evaluation.current_plans.join(contexts.unit_map, on=DEMAND_UNIT_COLS)
        context_keys = ["_score_context_id"] + PLAN_COLUMNS
        probabilities = evaluation.context_transition_probabilities
        if probabilities is None:
            compact_index = compact.plan_id_index.drop("demand_subgroup_id").rename(
                {"demand_group_id": "_score_context_id", "plan_id": "_context_plan_id"}
            )
            context_origins = (
                origins.group_by(context_keys).agg(
                    pl.col("n_persons").sum(), pl.col("utility").first(),
                )
                .join(compact_index, on=context_keys, how="left")
                .rename({"_score_context_id": "demand_group_id", "_context_plan_id": "plan_id"})
                .with_columns(demand_subgroup_id=pl.lit(0, dtype=pl.UInt32))
            )
            if context_origins["plan_id"].null_count():
                raise ValueError("A current plan is missing from the context evaluation.")
            choices = (
                compact.possible_plan_utility
                .join(compact.time_sequence_steps.select("time_seq_id", "reference_time_seq_id").unique(),
                      on="time_seq_id", how="left")
                .with_columns(reference_time_seq_id=pl.col("reference_time_seq_id").fill_null(pl.col("time_seq_id")))
            )
            probabilities = self.get_transition_probabilities(
                context_origins, choices.lazy(), compact.possible_plan_steps, scope,
                transport_zones=transport_zones, **transition_parameters,
            )
        probabilities = probabilities.drop("demand_subgroup_id", "plan_id_from", "plan_id_trans", "utility_prev_from").rename(
            {"demand_group_id": "_score_context_id"}
        )
        target_keys = [column + "_trans" for column in PLAN_COLUMNS]
        target_ids = evaluation.plan_id_index.rename(
            {**{column: column + "_trans" for column in PLAN_COLUMNS}, "plan_id": "plan_id_trans"}
        )
        expanded = (
            origins.rename({"plan_id": "plan_id_from", "utility": "utility_prev_from"})
            .select(DEMAND_UNIT_COLS + context_keys + ["plan_id_from", "utility_prev_from"])
            .join(probabilities, on=context_keys, how="inner")
            .join(target_ids, on=DEMAND_UNIT_COLS + target_keys, how="left")
            .drop("_score_context_id")
        )
        if expanded["plan_id_trans"].null_count():
            raise ValueError("A reachable plan is missing from the unit plan index.")
        return expanded

    @trace_simulation_step("estimate reachable allocation")
    def estimate_reachable_allocation(
        self, evaluation: CandidatePlanEvaluation, current_plans: pl.DataFrame,
        iteration: int, scope: BehaviorChangeScope, *, transport_zones: Any,
        min_plan_persons: float = 0.0,
        plan_pruning_start_step: int = 2, include_reachable_ids: bool = False,
        **transition_parameters: Any,
    ) -> pl.DataFrame | tuple[pl.DataFrame, pl.DataFrame | None]:
        """Estimate the next allocation without changing the outer state.

        Probabilities are shared by context and occupied plan. Temporary IDs
        are unique per unit because low-population pruning is performed per
        unit. Only origins and reachable targets need those temporary IDs.
        """
        choices = (
            evaluation.possible_plan_utility
            .join(evaluation.time_sequence_steps.select("time_seq_id", "reference_time_seq_id").unique(),
                  on="time_seq_id", how="left")
            .with_columns(reference_time_seq_id=pl.col("reference_time_seq_id").fill_null(pl.col("time_seq_id")))
        )
        steps = evaluation.possible_plan_steps
        if (
            transition_parameters.get("enable_transition_distance_model", False)
            and math.isfinite(transition_parameters.get("transition_distance_threshold", math.inf))
            and isinstance(steps, pl.LazyFrame)
        ):
            steps = steps.collect(engine="streaming")
        if (scope == BehaviorChangeScope.NO_TRANSITIONS
                or min_plan_persons <= 0.0
                or iteration < plan_pruning_start_step):
            # Without per-unit pruning, population weights can be summed by
            # context and occupied origin before applying the same transition.
            context_origins = evaluation.current_plans
            probabilities = self.get_transition_probabilities(
                context_origins, choices.lazy(), steps, scope,
                transport_zones=transport_zones, **transition_parameters,
            )
            allocation, _ = self.apply_transitions(
                context_origins, probabilities, iteration, include_events=False,
            )
            result = (
                allocation.drop("utility")
                .join(evaluation.possible_plan_utility.select(PLAN_KEY_COLS + ["utility"]), on=PLAN_KEY_COLS)
            )
            if include_reachable_ids:
                return result, probabilities.select(pl.col("plan_id_trans").alias("plan_id")).unique()
            return result
        contexts = evaluation.scoring_contexts
        mapping = evaluation.time_mapping.drop("demand_subgroup_id").rename(
            {"demand_group_id": "_score_context_id"}
        )
        context_keys = ["_score_context_id"] + PLAN_COLUMNS
        origin_keys = ["_score_context_id", "activity_seq_id", "previous_time_seq_id", "dest_seq_id", "mode_seq_id"]
        origins = (
            current_plans.drop("plan_id", strict=False).rename({"time_seq_id": "previous_time_seq_id"})
            .join(contexts.unit_map, on=DEMAND_UNIT_COLS, how="left")
            .join(mapping, on=origin_keys, how="left")
            .with_columns(time_seq_id=pl.col("time_seq_id").fill_null(pl.col("previous_time_seq_id")))
            .group_by(DEMAND_UNIT_COLS + context_keys)
            .agg(n_persons=pl.col("n_persons").sort().implode().list.sum(), utility=pl.col("utility").first())
        )
        compact_index = evaluation.plan_id_index.drop("demand_subgroup_id").rename(
            {"demand_group_id": "_score_context_id", "plan_id": "_context_plan_id"}
        )
        context_origins = (
            origins.group_by(context_keys).agg(pl.col("n_persons").sum(), pl.col("utility").first())
            .join(compact_index, on=context_keys, how="left")
            .rename({"_score_context_id": "demand_group_id", "_context_plan_id": "plan_id"})
            .with_columns(demand_subgroup_id=pl.lit(0, dtype=pl.UInt32))
        )
        if context_origins["plan_id"].null_count():
            raise ValueError("A current plan is missing from the context evaluation.")
        probabilities = self.get_transition_probabilities(
            context_origins, choices.lazy(), steps, scope,
            transport_zones=transport_zones, **transition_parameters,
        ).drop("demand_subgroup_id", "plan_id_from", "plan_id_trans", "utility_prev_from").rename(
            {"demand_group_id": "_score_context_id"}
        )
        target_keys = [column + "_trans" for column in PLAN_COLUMNS]
        # A population floor is local to each demand unit. Limit the temporary
        # transition expansion while keeping every origin of a unit together.
        pair_counts = probabilities.group_by(context_keys).len(name="_choices")
        units = (origins.join(pair_counts, on=context_keys, how="left")
                 .group_by(DEMAND_UNIT_COLS).agg(pl.col("_choices").fill_null(1).sum())
                 .sort(DEMAND_UNIT_COLS)
                 .with_columns(_batch=((pl.col("_choices").cast(pl.UInt64).cum_sum()
                                        - pl.col("_choices")) // _REACHABLE_ALLOCATION_BATCH_ROWS))
                 .select(DEMAND_UNIT_COLS + ["_batch"]))
        origin_batches = origins.join(units, on=DEMAND_UNIT_COLS).partition_by("_batch", maintain_order=True)
        home = contexts.expand_step_rows(evaluation.possible_plan_utility.filter(pl.col("mode_seq_id") == 0))
        allocations = []
        for origin_batch in origin_batches:
            origin_batch = origin_batch.drop("_batch")
            batch_contexts = origin_batch.select(context_keys).unique()
            batch_probabilities = probabilities.join(batch_contexts, on=context_keys, how="semi")
            unit_pairs = (origin_batch.select(DEMAND_UNIT_COLS + context_keys + ["utility"])
                          .rename({"utility": "utility_prev_from"})
                          .join(batch_probabilities, on=context_keys, how="inner").drop("_score_context_id"))
            temporary_keys = pl.concat([
                origin_batch.select(PLAN_KEY_COLS),
                unit_pairs.select(DEMAND_UNIT_COLS + target_keys).rename(
                    {column + "_trans": column for column in PLAN_COLUMNS}
                ),
                origin_batch.select(DEMAND_UNIT_COLS).unique().with_columns(
                    **{column: pl.lit(0, dtype=pl.UInt32) for column in PLAN_COLUMNS}
                ),
            ]).unique().sort(PLAN_KEY_COLS).with_row_index("plan_id")
            unit_origins = origin_batch.drop("_score_context_id").join(temporary_keys, on=PLAN_KEY_COLS)
            unit_pairs = (
                unit_pairs.join(temporary_keys.rename({"plan_id": "plan_id_from"}), on=PLAN_KEY_COLS)
                .join(temporary_keys.rename({
                    **{column: column + "_trans" for column in PLAN_COLUMNS}, "plan_id": "plan_id_trans"
                }), on=DEMAND_UNIT_COLS + target_keys)
            )
            allocation, _ = self.apply_transitions(
                unit_origins, unit_pairs, iteration,
                min_plan_persons=min_plan_persons,
                plan_pruning_start_step=plan_pruning_start_step,
                stay_home_plans=(home.drop("plan_id").join(temporary_keys.lazy(), on=PLAN_KEY_COLS)
                                 .select(DEMAND_UNIT_COLS + ["plan_id", "utility"])),
                include_events=False,
            )
            allocations.append(allocation)
        allocation = pl.concat(allocations) if allocations else current_plans.head(0)
        result = (
            contexts.collapse_allocations(allocation)
            .drop("utility")
            .join(evaluation.possible_plan_utility.select(PLAN_KEY_COLS + ["utility"]), on=PLAN_KEY_COLS)
            .join(evaluation.plan_id_index, on=PLAN_KEY_COLS, how="left")
        )
        # The per-unit floor uses a different origin and ID space; those IDs
        # cannot safely be used to compact the context evaluation.
        return (result, None) if include_reachable_ids else result

    @staticmethod
    def _restrict_transition_choices(
        utilities: pl.LazyFrame,
        origins: pl.DataFrame,
        evaluation: CandidatePlanEvaluation,
        scope: BehaviorChangeScope,
    ) -> pl.LazyFrame:
        """Keep only choices an occupied plan can reach in this phase.

        An infeasible origin can be reassigned outside the normal restriction,
        so all choices for its demand unit must remain available. Stable plan
        IDs are assigned before this filter, including IDs of excluded choices.
        """
        contexts = evaluation.scoring_contexts
        context_keys = ["_score_context_id"] + PLAN_KEY_COLS[len(DEMAND_UNIT_COLS):]
        origin_contexts = origins.join(contexts.unit_map, on=DEMAND_UNIT_COLS, how="left")
        context_choices = evaluation.possible_plan_utility.drop("demand_subgroup_id").rename(
            {"demand_group_id": "_score_context_id"}
        ).select(context_keys + ["is_feasible"])
        origin_contexts = origin_contexts.join(context_choices, on=context_keys, how="left")
        active_origins = origin_contexts
        if scope in (BehaviorChangeScope.MODE_REPLANNING, BehaviorChangeScope.DESTINATION_REPLANNING):
            active_origins = active_origins.filter(pl.col("mode_seq_id") != 0)
        forced_units = active_origins.filter(~pl.col("is_feasible").fill_null(True)).select(
            DEMAND_UNIT_COLS
        ).unique().with_columns(_forced=pl.lit(True))
        current_keys = origins.select(PLAN_KEY_COLS).unique().with_columns(_current=pl.lit(True))

        if scope == BehaviorChangeScope.NO_TRANSITIONS:
            eligible = utilities.join(current_keys.lazy(), on=PLAN_KEY_COLS, how="left")
        else:
            reference_times = evaluation.time_sequence_steps.select(
                "time_seq_id", "reference_time_seq_id"
            ).unique()
            group_keys = DEMAND_UNIT_COLS + ["activity_seq_id", "reference_time_seq_id"]
            if scope == BehaviorChangeScope.MODE_REPLANNING:
                group_keys += ["dest_seq_id"]
            eligible_groups = (
                active_origins.join(reference_times, on="time_seq_id", how="left")
                .with_columns(reference_time_seq_id=pl.col("reference_time_seq_id").fill_null(pl.col("time_seq_id")))
                .select(group_keys).unique().with_columns(_eligible=pl.lit(True))
            )
            eligible = (
                utilities.join(reference_times.lazy(), on="time_seq_id", how="left")
                .with_columns(reference_time_seq_id=pl.col("reference_time_seq_id").fill_null(pl.col("time_seq_id")))
                .join(eligible_groups.lazy(), on=group_keys, how="left")
                .join(current_keys.lazy(), on=PLAN_KEY_COLS, how="left")
            )
        eligible = eligible.join(forced_units.lazy(), on=DEMAND_UNIT_COLS, how="left")
        condition = pl.col("_current").fill_null(False) | pl.col("_forced").fill_null(False)
        if scope != BehaviorChangeScope.NO_TRANSITIONS:
            condition = condition | (pl.col("_eligible").fill_null(False) & (pl.col("mode_seq_id") != 0))
        return eligible.filter(condition).select(utilities.collect_schema().names())

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
        activity_time_rigidity_by_activity: dict[str, float],
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
            activity_time_rigidity_by_activity=activity_time_rigidity_by_activity,
            destination_sequences=destination_sequences,
            mode_sequences=mode_sequences,
            previous_plan_id_index=previous_plan_id_index,
            parameters=parameters,
        )
        candidate_plan_steps = evaluation.candidate_plan_steps
        possible_plan_steps = evaluation.possible_plan_steps
        # Transition distances need the complete step table. Otherwise, wait
        # until transitions are complete so that table is not held alongside
        # the potentially large transition calculation.
        needs_transition_distances = (
            parameters.plan_update.enable_transition_distance_model
            and math.isfinite(parameters.plan_update.transition_distance_threshold)
        )
        if (needs_transition_distances and evaluation.context_transition_probabilities is None
                and isinstance(possible_plan_steps, pl.LazyFrame)):
            possible_plan_steps = possible_plan_steps.collect(engine="streaming")
        plan_id_index = evaluation.plan_id_index
        current_plans = evaluation.current_plans
        transition_parameters = dict(
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
        if (evaluation.compact_transition_evaluation is not None
                and (not needs_transition_distances or evaluation.context_transition_probabilities is not None)):
            transition_prob = self.get_context_transition_probabilities(
                evaluation, parameters.behavior_change.scope_at(iteration), **transition_parameters,
            )
        else:
            possible_plan_utility = (
                evaluation.possible_plan_utility.lazy()
                .join(
                    evaluation.time_sequence_steps.lazy().select("time_seq_id", "reference_time_seq_id").unique(),
                    on="time_seq_id", how="left",
                )
                .with_columns(reference_time_seq_id=pl.col("reference_time_seq_id").fill_null(pl.col("time_seq_id")))
            )
            transition_prob = self.get_transition_probabilities(
                current_plans, possible_plan_utility, possible_plan_steps,
                parameters.behavior_change.scope_at(iteration), **transition_parameters,
            )
        log_memory_checkpoint(
            f"plan_updater:iteration:{iteration}:transition_probabilities",
            transition_prob=transition_prob,
        )
        current_plans, transition_events = self.apply_transitions(
            current_plans,
            transition_prob,
            iteration,
            min_plan_persons=parameters.plan_update.min_plan_persons,
            plan_pruning_start_step=parameters.plan_update.plan_pruning_start_step,
            stay_home_plans=evaluation.possible_plan_utility.lazy().filter(pl.col("mode_seq_id") == 0)
                .select(DEMAND_UNIT_COLS + ["plan_id", "utility"]),
            behavior_change_scope=parameters.behavior_change.scope_at(iteration),
            include_events=parameters.outputs.cache_step_events,
        )
        log_memory_checkpoint(
            f"plan_updater:iteration:{iteration}:after_apply_transitions",
            current_plans=current_plans,
            transition_events=transition_events,
        )
        # Event details need the complete table again after the current plans
        # are expanded. Otherwise keep it lazy so only columns and rows used
        # by the current plans need to be collected.
        if parameters.outputs.cache_step_events and isinstance(possible_plan_steps, pl.LazyFrame):
            possible_plan_steps = possible_plan_steps.collect(engine="streaming")
        log_memory_checkpoint(
            f"plan_updater:iteration:{iteration}:possible_plan_steps",
            possible_plan_steps=possible_plan_steps,
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

    @trace_simulation_step("evaluate shared sections")
    def _evaluate_shared_sections(
        self, *, physical: pl.DataFrame, memberships: pl.DataFrame, contexts: ScoringContexts,
        current_plans: pl.DataFrame, stay_home_plan: pl.DataFrame, schedule_steps: pl.DataFrame,
        home_night_dur: pl.DataFrame, home_value: float, min_activity_time_constant: float,
        rigidity: dict[str, float], enabled: bool,
        scoring_lookup: ScoringLookup, time_index_cache: TimeSequenceIndexCache | None,
        needed_score_keys: pl.DataFrame | None = None,
        whole_plan_cache: WholePlanCache | None = None,
        prepared_reuse: tuple | None = None,
    ) -> CandidatePlanEvaluation | None:
        """Score timed sections once before expanding them to scoring contexts.

        Return None when the compact scoring lookup does not cover every step;
        the normal evaluator then handles those incomplete candidate inputs.
        """
        physical = physical.filter(pl.col("mode_seq_id") != 0).select(STEP_COLUMNS)
        memberships = memberships.filter(pl.col("mode_seq_id") != 0).select(MEMBERSHIP_COLUMNS)
        if physical.is_empty() or memberships.is_empty() or scoring_lookup.scores is None:
            return None
        profiles = contexts.demand_groups.select(DEMAND_UNIT_COLS + ["country", "csp", "utility_profile_id"])
        score_keys = ["country", "csp", "activity", "from", "to", "mode", "utility_profile_id"]
        lookup = scoring_lookup.scores
        if needed_score_keys is None:
            classes = (memberships.select(PLAN_KEY_COLS).join(profiles, on=DEMAND_UNIT_COLS)
                       .select(PLAN_COLUMNS + ["country", "csp", "utility_profile_id"]).unique())
            needed = classes.join(physical, on=PLAN_COLUMNS).select(score_keys).unique()
        else:
            needed = needed_score_keys
        if needed.join(lookup.select(score_keys), on=score_keys, how="anti", nulls_equal=True).height:
            return None
        if scoring_lookup.section_cache is None:
            scoring_lookup.section_cache = SectionScoreCache(budget=scoring_lookup.budget)
        previous = whole_plan_cache.state if whole_plan_cache is not None else None
        reuse_result = None
        if previous is not None and prepared_reuse is not None:
            reuse_result = run_reuse(
                previous, physical, memberships, profiles, lookup, schedule_steps,
                home_night_dur, home_value, min_activity_time_constant, rigidity, enabled,
                scoring_lookup.section_cache, time_index_cache, self.schedule_updater, prepared_reuse,
            )
            if reuse_result is None:
                # The lookup may contain only the newly requested keys. Let the
                # established evaluator rebuild the full set on failure.
                whole_plan_cache.retain(None)
                return None
        if reuse_result is not None:
            timing, scored, next_state = reuse_result
            whole_plan_cache.retain(next_state)
        else:
            travel = lookup.select("utility_profile_id", "from", "to", "mode", "time").unique()
            timing = time_factored_candidates(
                physical, memberships, profiles, travel, schedule_steps,
                activity_time_rigidity_by_activity=rigidity, enabled=enabled,
                index_cache=time_index_cache, schedule_updater=self.schedule_updater,
            )
            scored = score_factored_sections(
                timing, profiles, lookup, home_night_dur,
                rigid_activities={name for name, value in rigidity.items() if value == 1.0},
                value_of_time_stay_home=home_value, min_activity_time_constant=min_activity_time_constant,
                cache=scoring_lookup.section_cache,
            )
            if whole_plan_cache is not None:
                whole_plan_cache.retain(make_state(
                    physical, lookup, timing, scored, home_night_dur=home_night_dur,
                    home_value=home_value, min_activity_time_constant=min_activity_time_constant,
                    rigidity=rigidity, enabled=enabled,
                ))
        modeled_utilities = scored.utilities
        scored_context_plans = scored.context_plans
        # Section references and profile scores are no longer needed by this
        # evaluation; the optional cache holds only what fits its budget.
        del scored
        time_mapping = scored_context_plans.select(
            DEMAND_UNIT_COLS + ["activity_seq_id", "previous_time_seq_id", "dest_seq_id", "mode_seq_id", "time_seq_id"]
        )
        utility_columns = DEMAND_UNIT_COLS + [
            "activity_seq_id", "time_seq_id", "mode_seq_id", "dest_seq_id", "utility", "is_feasible",
            "missing_cost_row_count", "invalid_cost_row_count", "plan_id",
        ]
        # IDs are temporary within this evaluation. The scored mobile plans
        # are already unique, so assign IDs directly instead of joining a
        # separately constructed index back onto millions of rows.
        mobile = modeled_utilities.sort(PLAN_KEY_COLS).with_row_index("plan_id").select(utility_columns)
        del modeled_utilities
        stay = (stay_home_plan.with_columns(
            min_activity_time=pl.col("mean_home_night_per_pers") * math.exp(-min_activity_time_constant),
        ).with_columns(
            utility=home_value * pl.col("mean_home_night_per_pers")
            * (pl.lit(24.0) / pl.col("min_activity_time")).log().clip(0.0),
            is_feasible=pl.lit(True), missing_cost_row_count=pl.lit(0, dtype=pl.UInt32),
            invalid_cost_row_count=pl.lit(0, dtype=pl.UInt32),
        ).sort(PLAN_KEY_COLS).with_row_index("plan_id", offset=mobile.height).select(utility_columns))
        plan_id_index = pl.concat([
            mobile.select(PLAN_KEY_COLS + ["plan_id"]), stay.select(PLAN_KEY_COLS + ["plan_id"]),
        ])
        utilities = pl.concat([mobile, stay], how="vertical_relaxed")
        del mobile, stay

        origin_keys = DEMAND_UNIT_COLS + [
            "activity_seq_id", "previous_time_seq_id", "dest_seq_id", "mode_seq_id",
        ]
        current = (current_plans.rename({"time_seq_id": "previous_time_seq_id"})
                   .join(time_mapping, on=origin_keys, how="left")
                   .with_columns(time_seq_id=pl.col("time_seq_id").fill_null(pl.col("previous_time_seq_id")))
                   .group_by(PLAN_KEY_COLS)
                   .agg(n_persons=pl.col("n_persons").sort().implode().list.sum(), utility=pl.col("utility").first())
                   .join(plan_id_index, on=PLAN_KEY_COLS, how="left"))
        if current["plan_id"].null_count():
            return None
        history = (memberships.rename({"time_seq_id": "previous_time_seq_id"})
                   .join(time_mapping, on=origin_keys, how="inner")
                   .group_by(PLAN_KEY_COLS).agg(
                       pl.col("country").first(), pl.col("csp").first(), pl.col("iteration").min(),
                       pl.col("first_seen_iteration").min(), pl.col("last_seen_iteration").max(),
                       pl.col("last_active_iteration").max(),
                   ).select(MEMBERSHIP_COLUMNS))
        modeled_physical = timing.steps.select(STEP_COLUMNS).unique()
        if modeled_physical.select(STEP_KEY_COLUMNS).n_unique() != modeled_physical.height:
            return None
        candidates = CompactCandidateSteps(
            steps=modeled_physical, membership=history,
            columns=tuple(CandidatePlanStepsAsset.STRUCTURAL_COLUMNS + CandidatePlanStepsAsset.RETENTION_COLUMNS),
        )

        # Keep this compatibility view lazy: allocations can filter plans
        # before their physical steps are attached to scoring contexts.
        class_plans = scored_context_plans.unique(subset=PLAN_KEY_COLS, keep="first").sort(PLAN_KEY_COLS)
        # Map keys to their IDs even when scoring excluded some plans.
        class_plans = class_plans.join(plan_id_index, on=PLAN_KEY_COLS, maintain_order="left")
        del scored_context_plans
        details = SharedSectionStepDetails(
            class_plans, timing.steps.drop(PLAN_COLUMNS + ["time"]), lookup, history, plan_id_index, stay_home_plan,
        )
        steps = details.select(self)
        return CandidatePlanEvaluation(
            candidate_plan_steps=candidates, possible_plan_steps=steps, possible_plan_utility=utilities,
            plan_id_index=plan_id_index, current_plans=current, time_sequence_steps=timing.time_sequence_steps,
            scoring_contexts=contexts, time_mapping=time_mapping,
            shared_plan_ids=class_plans.select("plan_id", "_timing_plan"),
            shared_steps=timing.steps.select("_timing_plan", "from", "to", "mode", "activity"),
            shared_step_details=details,
        )

    @trace_simulation_step("candidate evaluation")
    def evaluate_candidate_pool(
        self,
        *,
        current_plans: pl.DataFrame,
        current_plan_steps: pl.DataFrame | None,
        candidate_plan_steps: pl.DataFrame | CompactCandidateSteps | None,
        shared_candidate_plan_steps: pl.DataFrame | pl.LazyFrame | None,
        shared_active_plan_steps: pl.DataFrame | None = None,
        factored_candidate_steps: ScoringCandidateSteps | None = None,
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
        activity_time_rigidity_by_activity: dict[str, float],
        destination_sequences: DestinationSequences,
        mode_sequences: ModeSequences,
        previous_plan_id_index: pl.DataFrame,
        parameters: Any,
        compact_contexts: bool = False,
        scoring_contexts: ScoringContexts | None = None,
        compact_current_plans: pl.DataFrame | None = None,
        compact_stay_home: pl.DataFrame | None = None,
        compact_plan_id_index: pl.DataFrame | None = None,
        scoring_lookup: ScoringLookup | None = None,
        whole_plan_cache: WholePlanCache | None = None,
        time_index_cache: TimeSequenceIndexCache | None = None,
        reachable_scoring_scope: BehaviorChangeScope | None = None,
        forced_scoring_contexts: pl.DataFrame | None = None,
    ) -> CandidatePlanEvaluation:
        """Score all current candidates without changing the behavioral state."""
        if compact_contexts and scoring_contexts is None:
            scoring_contexts = ScoringContexts.build(
                demand_groups, transport_costs, mode_sequences.population_segments,
            )
        candidate_memory = (
            shared_candidate_plan_steps
            if shared_candidate_plan_steps is not None
            else candidate_plan_steps
        )
        compact_candidates = None
        factored_parts = None
        can_score_sections = (
            compact_contexts and scoring_lookup is not None
            and type(self.schedule_updater) is PlanScheduleUpdater
            and not parameters.outputs.cache_step_events
            and not (parameters.plan_update.enable_transition_distance_model
                     and math.isfinite(parameters.plan_update.transition_distance_threshold))
        )
        context_filter = None
        if compact_contexts and reachable_scoring_scope is not None:
            compact_current_plans = (
                compact_current_plans if compact_current_plans is not None
                else scoring_contexts.collapse_allocations(current_plans)
            )
            context_filter = lambda members: self._reachable_scoring_candidates(
                members, compact_current_plans, survey_plan_steps,
                reachable_scoring_scope, forced_scoring_contexts,
            )
        if (
            compact_contexts and factored_candidate_steps is not None
            and (candidate_plan_steps is None or isinstance(candidate_plan_steps, CompactCandidateSteps))
        ):
            collapsed = scoring_contexts.collapse_factored_step_rows(
                factored_candidate_steps, shared_active_plan_steps, candidate_plan_steps, current_plans,
                context_filter=context_filter, return_parts=True, expand=not can_score_sections,
            )
            if collapsed is not None:
                compact_candidates, physical_candidates, candidate_memberships = collapsed
                factored_parts = (physical_candidates, candidate_memberships)
        if isinstance(candidate_memory, CompactCandidateSteps):
            candidate_memory = candidate_memory.lazy()
        candidate_parts = [candidate_memory.lazy()] if candidate_memory is not None else []
        if factored_parts is None and shared_active_plan_steps is not None and shared_active_plan_steps.height:
            candidate_parts.append(shared_active_plan_steps.lazy())
        if factored_parts is None and shared_candidate_plan_steps is not None and candidate_plan_steps is not None:
            # Inner timing updates may have replaced the occupied keys. Preserve
            # the real outer origins so they can be rescored and remapped here.
            active_keys = current_plans.lazy().select(PLAN_KEY_COLS)
            if isinstance(candidate_plan_steps, CompactCandidateSteps):
                # Select occupied memberships before joining their shared steps.
                # Otherwise the compact history is expanded for every unit.
                active_candidates = (
                    candidate_plan_steps.memberships_for_plans(active_keys)
                    .join(candidate_plan_steps.physical_steps(active_keys), on=PLAN_COLUMNS, how="inner")
                    .select(candidate_plan_steps.columns)
                )
            else:
                active_candidates = candidate_plan_steps.lazy().join(
                    active_keys, on=PLAN_KEY_COLS, how="semi",
                )
            candidate_parts.append(active_candidates)
        if factored_parts is None and len(candidate_parts) > 1:
            candidate_memory = pl.concat(candidate_parts, how="vertical_relaxed").unique(
                subset=CandidatePlanStepsAsset.DEDUPE_COLUMNS, keep="last"
            )
        if compact_contexts:
            if factored_parts is None:
                candidate_memory = scoring_contexts.collapse_step_rows(candidate_memory)
            elif compact_candidates is not None:
                candidate_memory = compact_candidates
            log_memory_checkpoint(
                f"plan_updater:iteration:{iteration}:scoring_context_candidates",
                candidate_steps=candidate_memory,
            )
            current_plans = (
                compact_current_plans if compact_current_plans is not None
                else scoring_contexts.collapse_allocations(current_plans)
            )
            stay_home_plan = (
                compact_stay_home if compact_stay_home is not None
                else scoring_contexts.collapse_stay_home(stay_home_plan)
            )
            demand_groups = scoring_contexts.demand_groups
            # Context ids are temporary; the outer update creates persistent
            # demand-unit plan ids separately.
            previous_plan_id_index = compact_plan_id_index
            current_plan_steps = None
            if reachable_scoring_scope is not None and factored_parts is None:
                candidate_memory = self._reachable_scoring_candidates(
                    candidate_memory, current_plans, survey_plan_steps,
                    reachable_scoring_scope, forced_scoring_contexts,
                )
        if factored_parts is not None and can_score_sections:
            physical, memberships = factored_parts
            _, utility_profiles = transport_costs.get_utility_profiles(
                demand_groups, mode_sequences.population_segments,
            )
            score_keys = ["country", "csp", "activity", "from", "to", "mode", "utility_profile_id"]
            # Equivalent contexts share the same trip inputs. Join each
            # physical plan to its distinct profiles, not to every context.
            classes = (memberships.filter(pl.col("mode_seq_id") != 0).select(PLAN_KEY_COLS)
                       .join(scoring_contexts.demand_groups.select(
                           DEMAND_UNIT_COLS + ["country", "csp", "utility_profile_id"]), on=DEMAND_UNIT_COLS)
                       .select(PLAN_COLUMNS + ["country", "csp", "utility_profile_id"]).unique())
            prepared_reuse = None
            if whole_plan_cache is not None and whole_plan_cache.state is not None:
                prepared_reuse = prepare_reuse(
                    whole_plan_cache.state, classes, physical.filter(pl.col("mode_seq_id") != 0),
                    scoring_lookup.scores, survey_plan_steps, home_night_dur,
                    resolved_activity_parameters["home"].value_of_time_stay_home,
                    parameters.plan_update.min_activity_time_constant, activity_time_rigidity_by_activity,
                    parameters.plan_update.update_plan_timings_from_modeled_travel_times,
                )
            score_classes = prepared_reuse[1] if prepared_reuse is not None else classes
            score_inputs = (score_classes.join(physical.select(PLAN_COLUMNS + ["activity", "from", "to", "mode"])
                                         .unique(), on=PLAN_COLUMNS)
                            .select(score_keys).unique())
            del classes
            compact_scores = self.compute_plan_steps_candidates_utility(
                candidates=None, compact_score_inputs=score_inputs, utility_profiles=utility_profiles,
                transport_costs=transport_costs,
                population_segments=mode_sequences.population_segments,
                destination_saturation=destination_saturation, activity_dur=activity_dur,
                transport_zones=transport_zones, resolved_activity_parameters=resolved_activity_parameters,
                min_activity_time_constant=parameters.plan_update.min_activity_time_constant,
                allow_missing_costs_for_current_plans=True,
                use_destination_shadow_prices=parameters.plan_update.use_destination_shadow_prices,
                scoring_lookup=scoring_lookup,
            )
            active_lookup = scoring_lookup
            if scoring_lookup.scores is None and prepared_reuse is not None:
                # A miss-only lookup cannot supply the full fallback evaluator.
                # Rebuild all inputs through the established expanded path.
                whole_plan_cache.retain(None)
                active_lookup = None
            elif scoring_lookup.scores is None:
                # The lookup is needed now even when it is too large to keep
                # between batches. Do not retain its section scores either.
                if whole_plan_cache is not None:
                    whole_plan_cache.retain(None)
                active_lookup = ScoringLookup(
                    budget=ScoringMemoryBudget(max_bytes=0),
                    scores=compact_scores.collect(engine="streaming"),
                )
            section_evaluation = self._evaluate_shared_sections(
                physical=physical, memberships=memberships, contexts=scoring_contexts,
                current_plans=current_plans, stay_home_plan=stay_home_plan, schedule_steps=survey_plan_steps,
                home_night_dur=home_night_dur,
                home_value=resolved_activity_parameters["home"].value_of_time_stay_home,
                min_activity_time_constant=parameters.plan_update.min_activity_time_constant,
                rigidity=activity_time_rigidity_by_activity,
                enabled=parameters.plan_update.update_plan_timings_from_modeled_travel_times,
                scoring_lookup=active_lookup,
                time_index_cache=time_index_cache, needed_score_keys=score_inputs,
                whole_plan_cache=whole_plan_cache if scoring_lookup.scores is not None else None,
                prepared_reuse=prepared_reuse,
            ) if active_lookup is not None else None
            if section_evaluation is not None:
                return section_evaluation
            # An incomplete lookup or conflicting modeled plan needs the
            # established expanded evaluator. Expand only in that case.
            compact_candidates = (memberships.join(physical, on=PLAN_COLUMNS)
                                  .select(CandidatePlanStepsAsset.STRUCTURAL_COLUMNS
                                          + CandidatePlanStepsAsset.RETENTION_COLUMNS))
            candidate_memory = compact_candidates
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
            activity_time_rigidity_by_activity,
            parameters.plan_update.min_activity_time_constant,
            destination_sequences,
            mode_sequences,
            parameters,
            shared_candidate_pool=(shared_candidate_plan_steps is not None),
            scoring_lookup=scoring_lookup,
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
        scored_steps = possible_plan_steps.collect(engine="streaming")
        share_physical_timing = (
            compact_candidates is not None and scoring_contexts is not None
            and scored_steps.height > 0
            and "utility_profile_id" in scoring_contexts.demand_groups.columns
            and type(self.schedule_updater) is PlanScheduleUpdater
        )
        del possible_plan_steps, candidate_memory, compact_candidates, candidate_parts
        with simulation_step("check candidate coverage"):
            # Input schedules can acquire a different identity after modeled
            # travel times are applied. Check coverage now, then index only the
            # resulting plans, which are the ones used for choices and state.
            observed_plans = scored_steps.select(PLAN_KEY_COLS).unique()
        self._assert_current_plans_covered_by_possible_plan_steps(
            current_plans,
            observed_plans.lazy(),
            iteration,
        )
        del observed_plans
        # Timing changes create new schedule identities. Remap the current
        # distribution too, so transition origins still match the scored plans.
        with simulation_step("materialize and index modeled timings"):
            if share_physical_timing:
                # Contexts with the same physical plan and travel times have
                # the same timing, even if their activity scores differ.
                profiles = scoring_contexts.demand_groups.select(DEMAND_UNIT_COLS + ["utility_profile_id"])
                physical, memberships = factored_parts
                physical = physical.filter(pl.col("mode_seq_id") != 0).select(STEP_COLUMNS)
                memberships = memberships.filter(pl.col("mode_seq_id") != 0).select(PLAN_KEY_COLS)
                expected_count = (memberships.select(PLAN_COLUMNS)
                                  .join(physical.group_by(PLAN_COLUMNS).len(), on=PLAN_COLUMNS)
                                  .select(pl.col("len").sum()).item())
                if expected_count != scored_steps.height:
                    # A scoring join can omit individual steps. Such a partial
                    # schedule must retain the existing context-level timing.
                    share_physical_timing = False
            if share_physical_timing:
                if scoring_lookup is not None and scoring_lookup.scores is not None:
                    travel = scoring_lookup.scores.select("utility_profile_id", "from", "to", "mode", "time").unique()
                else:
                    travel = (scored_steps.select(DEMAND_UNIT_COLS + ["from", "to", "mode", "time"])
                              .join(profiles, on=DEMAND_UNIT_COLS)
                              .select("utility_profile_id", "from", "to", "mode", "time").unique())
                factored = time_factored_candidates(
                    physical, memberships, profiles, travel, survey_plan_steps,
                    activity_time_rigidity_by_activity=activity_time_rigidity_by_activity,
                    enabled=parameters.plan_update.update_plan_timings_from_modeled_travel_times,
                    index_cache=time_index_cache,
                    schedule_updater=self.schedule_updater,
                )
                context_mapping = factored.context_mapping
                time_mapping = context_mapping.drop("_timing_plan")
                time_sequence_steps = factored.time_sequence_steps
                time_columns = ["departure_time", "arrival_time", "next_departure_time", "duration_per_pers"]
                timing_values = factored.steps.select(
                    ["_timing_plan", "seq_step_index"]
                    + [pl.col(name).alias("_modeled_" + name) for name in time_columns]
                )
                origin_keys = DEMAND_UNIT_COLS + [
                    "activity_seq_id", "previous_time_seq_id", "dest_seq_id", "mode_seq_id",
                ]
                timed_steps = (scored_steps.rename({"time_seq_id": "previous_time_seq_id"})
                               .join(context_mapping, on=origin_keys, how="inner")
                               .join(timing_values, on=["_timing_plan", "seq_step_index"], how="inner")
                               .drop(["previous_time_seq_id", "_timing_plan"] + time_columns)
                               .rename({"_modeled_" + name: name for name in time_columns}))
                if timed_steps.height != scored_steps.height:
                    raise ValueError("A scoring context lost candidate steps while attaching shared timings.")
                del scored_steps, physical, memberships, travel, factored, timing_values
            else:
                possible_plan_steps = self.schedule_updater.update_plan_timings(
                    scored_steps.lazy(),
                    activity_time_rigidity_by_activity=activity_time_rigidity_by_activity,
                    enabled=parameters.plan_update.update_plan_timings_from_modeled_travel_times,
                )
                timed_scored_steps = possible_plan_steps.collect(engine="streaming")
                del possible_plan_steps, scored_steps
                log_memory_checkpoint(
                    f"plan_updater:iteration:{iteration}:timed_scored_steps", scored_steps=timed_scored_steps,
                )
                timed_steps, time_sequence_steps, time_mapping = assign_time_sequence_ids(
                    timed_scored_steps, survey_plan_steps, index_cache=time_index_cache,
                )
                del timed_scored_steps
            # A person cannot leave an intermediate activity before arriving.
            # Check the indexed, second-rounded times so plans merged under one
            # schedule ID have the same feasibility. The final home step has
            # no subsequent departure and its end time is only a placeholder.
            timed_steps = timed_steps.with_columns(
                is_feasible=(
                    pl.col("is_feasible")
                    & (
                        (pl.col("seq_step_index") == pl.col("seq_step_index").max().over(PLAN_KEY_COLS))
                        | (pl.col("next_departure_time") >= pl.col("arrival_time"))
                    )
                ).fill_null(False)
            )
            timed_steps, plan_id_index = add_plan_id(
                timed_steps, previous_index=previous_plan_id_index,
                additional_keys=stay_home_plan.select(PLAN_KEY_COLS),
            )
        current_plans = (
            current_plans.rename({"time_seq_id": "previous_time_seq_id"})
            .join(time_mapping, on=["previous_time_seq_id" if col == "time_seq_id" else col for col in PLAN_KEY_COLS], how="left")
            .with_columns(time_seq_id=pl.col("time_seq_id").fill_null(pl.col("previous_time_seq_id")))
            .group_by(PLAN_KEY_COLS)
            .agg(n_persons=pl.col("n_persons").sort().implode().list.sum(), utility=pl.col("utility").first())
            .join(plan_id_index, on=PLAN_KEY_COLS, how="left")
        )
        timed_steps = self._merge_modeled_plan_history(timed_steps)
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
            plan_ids_already_indexed=True,
        )
        possible_plan_steps = possible_plan_steps.drop(
            "_missing_cost_row",
            "_invalid_cost_row",
        )
        possible_plan_steps, plan_id_index = self.add_stay_home_plan_steps(
            possible_plan_steps,
            stay_home_plan,
            plan_id_index=plan_id_index,
            plan_id_index_complete=True,
        )
        with simulation_step("collect plan utilities"):
            possible_plan_utility = possible_plan_utility.collect(engine="streaming")
        evaluation = CandidatePlanEvaluation(
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
        return evaluation

    @staticmethod
    @trace_simulation_step("restrict intermediate candidate scoring")
    def _reachable_scoring_candidates(
        candidates: pl.DataFrame, origins: pl.DataFrame, schedule_steps: pl.DataFrame,
        scope: BehaviorChangeScope, forced_contexts: pl.DataFrame,
    ) -> pl.DataFrame:
        """Keep intermediate search candidates that can affect a transition.

        The complete shared pool is kept separately. Contexts with infeasible
        origins retain every candidate because reassignment bypasses the scope.
        """
        if scope == BehaviorChangeScope.FULL_REPLANNING:
            return candidates
        candidate_keys = candidates.select(PLAN_KEY_COLS).unique()
        origin_keys = origins.select(PLAN_KEY_COLS).unique()
        if scope == BehaviorChangeScope.NO_TRANSITIONS:
            reachable = candidate_keys.join(origin_keys, on=PLAN_KEY_COLS, how="semi")
        else:
            references = schedule_steps.select(
                "time_seq_id",
                (pl.col("reference_time_seq_id") if "reference_time_seq_id" in schedule_steps.columns
                 else pl.col("time_seq_id")).alias("_reference_time"),
            ).unique()
            group_keys = DEMAND_UNIT_COLS + ["activity_seq_id", "_reference_time"]
            if scope == BehaviorChangeScope.MODE_REPLANNING:
                group_keys += ["dest_seq_id"]
            active = (
                origins.filter(pl.col("mode_seq_id") != 0)
                .join(references, on="time_seq_id", how="left")
                .with_columns(_reference_time=pl.col("_reference_time").fill_null(pl.col("time_seq_id")))
                .select(group_keys).unique()
            )
            reachable = (
                candidate_keys.join(references, on="time_seq_id", how="left")
                .with_columns(_reference_time=pl.col("_reference_time").fill_null(pl.col("time_seq_id")))
                .join(active, on=group_keys, how="semi")
                .drop("_reference_time")
            )
        # Keep every occupied plan, even if its schedule is unusual, and
        # every option in contexts requiring unrestricted reassignment.
        kept = pl.concat([
            reachable,
            candidate_keys.join(origin_keys, on=PLAN_KEY_COLS, how="semi"),
            candidate_keys.join(forced_contexts, on=DEMAND_UNIT_COLS, how="semi"),
        ]).unique()
        return candidates.join(kept, on=PLAN_KEY_COLS, how="semi")

    @trace_simulation_step("fresh MNL allocation")
    def estimate_mnl_allocation(
        self,
        evaluation: CandidatePlanEvaluation,
        demand_groups: pl.DataFrame,
        *,
        logit_scale: float,
        include_plan_steps: bool = True,
    ) -> tuple[pl.DataFrame, pl.DataFrame | None]:
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
                        / pl.col("_choice_weight").sort().implode().list.sum().over("_choice_context_id")
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
        current_plan_steps = (
            self.get_current_plan_steps(allocation, evaluation.possible_plan_steps.lazy())
            if include_plan_steps else None
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
        activity_time_rigidity_by_activity: dict[str, float],
        min_activity_time_constant,
        destination_sequences: DestinationSequences,
        mode_sequences: ModeSequences,
        parameters: Any,
        *,
        shared_candidate_pool: bool = False,
        scoring_lookup: ScoringLookup | None = None,
    ):
        """Enumerate candidate plan steps and compute per-step utilities."""
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
            profile_assignments, utility_profiles = transport_costs.get_utility_profiles(
                demand_groups,
                mode_sequences.population_segments,
            )
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
                activity_time_rigidity_by_activity=activity_time_rigidity_by_activity,
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
            scoring_lookup=scoring_lookup,
        )

    def _prune_shared_candidate_contexts(
        self,
        *,
        candidates: pl.LazyFrame | None,
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
        activity_time_rigidity_by_activity: dict[str, float],
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
            activity_time_rigidity_by_activity=activity_time_rigidity_by_activity,
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
                utility=pl.col("utility").sort().implode().list.sum(),
                # Use a fixed order for floating-point sums, and convert step
                # times to Float64 before deriving the home-night utility.
                home_night_per_pers=(
                    24.0
                    - pl.col("duration_per_pers").cast(pl.Float64).sort().implode().list.sum()
                    - pl.col("time").fill_null(0.0).cast(pl.Float64).sort().implode().list.sum()
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
        scoring_lookup: ScoringLookup | None = None,
        compact_score_inputs: pl.DataFrame | None = None,
    ) -> pl.LazyFrame:
        """Score plan-step candidates under current costs and destination saturation."""

        if utility_profiles is not None:
            profile_assignments = None
            cost_join_columns = ["utility_profile_id", "from", "to", "mode"]
        elif demand_groups is None:
            profile_assignments = None
            cost_join_columns = ["from", "to", "mode"]
        else:
            profile_assignments, utility_profiles = transport_costs.get_utility_profiles(
                demand_groups, population_segments or [],
            )
            cost_join_columns = ["utility_profile_id", "from", "to", "mode"]
        candidate_cost_inputs = (candidates.with_columns(
            activity=pl.col("activity").cast(activity_dur.schema["activity"])
        ) if candidates is not None else compact_score_inputs.lazy())
        if profile_assignments is not None:
            candidate_cost_inputs = candidate_cost_inputs.join(
                profile_assignments.lazy(),
                on=DEMAND_UNIT_COLS,
            )

        # Cost and activity parameters depend on these values, not on the
        # enclosing plan or its reference schedule. Keep the original timing and
        # plan columns on the candidate rows for the later schedule update.
        score_columns = ["country", "csp", "activity", "from", "to", "mode"]
        if "utility_profile_id" in candidate_cost_inputs.collect_schema().names():
            score_columns.append("utility_profile_id")
        candidate_columns = CandidatePlanStepsAsset.STRUCTURAL_COLUMNS + CandidatePlanStepsAsset.RETENTION_COLUMNS
        score_values = [
            "cost", "is_feasible", "_missing_cost_row", "_invalid_cost_row", "distance", "time",
            "mean_duration_per_pers", "value_of_time", "k_saturation_utility",
            "destination_shadow_price", "min_activity_time", "activity_utility_scale",
        ]
        compact_inputs = candidate_cost_inputs.select(score_columns).unique()
        previous_scores = scoring_lookup.scores if scoring_lookup is not None else None
        if previous_scores is not None:
            # The lookup belongs to one fixed set of scoring inputs. Only new
            # combinations need their cost and activity joins again.
            compact_inputs = compact_inputs.join(
                previous_scores.select(score_columns).lazy(),
                on=score_columns, how="anti", nulls_equal=True,
            )
        compact_inputs = compact_inputs.collect(engine="streaming")
        if previous_scores is not None and compact_inputs.is_empty():
            if compact_score_inputs is not None:
                return previous_scores.lazy()
            return candidate_cost_inputs.select(
                candidate_columns + (["utility_profile_id"] if "utility_profile_id" in score_columns else [])
            ).join(
                previous_scores.lazy(), on=score_columns, how="inner", nulls_equal=True,
            ).select(candidate_columns + score_values)
        if utility_profiles is None:
            cost_by_od_and_modes = transport_costs.get_costs_by_od_and_mode(
                ["cost", "distance", "time"], detail_distances=False,
            ).join(compact_inputs.select("from", "to").unique(), on=["from", "to"], how="semi")
        else:
            cost_by_od_and_modes = transport_costs.get_profile_costs_by_od_and_mode(
                utility_profiles, ["cost", "distance", "time"],
                od_pairs=compact_inputs.select("utility_profile_id", "from", "to").unique(),
            )
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
        scored_candidates = (
            compact_inputs.lazy()
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
            .select(score_columns + score_values)
        )

        # Materialize the compact scores once. Keeping this entire cost query
        # lazy would retain all profile OD tables during demand-unit expansion.
        compact_scores = scored_candidates.collect(engine="streaming")
        if previous_scores is not None:
            compact_scores = pl.concat([previous_scores, compact_scores], how="vertical_relaxed")
        if scoring_lookup is not None:
            scoring_lookup.retain(compact_scores)
        log_memory_checkpoint("plan_updater:compact_scores", compact_scores=compact_scores)
        if compact_score_inputs is not None:
            return compact_scores.lazy()
        return candidate_cost_inputs.select(
            candidate_columns + (["utility_profile_id"] if "utility_profile_id" in score_columns else [])
        ).join(
            compact_scores.lazy(), on=score_columns, how="inner", nulls_equal=True,
        ).select(candidate_columns + score_values)

    def get_possible_plan_utility(
        self,
        possible_plan_steps,
        home_night_dur,
        value_of_time_stay_home,
        stay_home_plan,
        min_activity_time_constant,
        *,
        plan_id_index: pl.DataFrame,
        plan_ids_already_indexed: bool = False,
    ):
        """Aggregate per-step utilities to plan-level utilities."""

        possible_plan_utility = (
            possible_plan_steps.filter(pl.col("mode_seq_id") != 0).group_by(
                ["plan_id"] + DEMAND_UNIT_COLS + ["country", "csp", "activity_seq_id", "time_seq_id", "dest_seq_id", "mode_seq_id"]
            )
            .agg(
                utility=pl.col("utility").sort().implode().list.sum(),
                is_feasible=pl.col("is_feasible").all(),
                missing_cost_row_count=pl.col("_missing_cost_row").sum(),
                invalid_cost_row_count=pl.col("_invalid_cost_row").sum(),
                # Keep this calculation consistent with candidate pruning.
                home_night_per_pers=(
                    24.0 - pl.col("duration_per_pers").cast(pl.Float64).sort().implode().list.sum()
                    - pl.col("time").fill_null(0.0).cast(pl.Float64).sort().implode().list.sum()
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
                + (["plan_id"] if plan_ids_already_indexed else [])
            )
        )

        stay_home_utility = (
            stay_home_plan.lazy()
            .with_columns(
                min_activity_time=pl.col("mean_home_night_per_pers") * math.exp(-min_activity_time_constant),
            )
            .with_columns(
                utility=(
                    value_of_time_stay_home
                    * pl.col("mean_home_night_per_pers")
                    * (pl.lit(24.0) / pl.col("min_activity_time")).log().clip(0.0)
                ),
                is_feasible=pl.lit(True),
                missing_cost_row_count=pl.lit(0, dtype=pl.UInt32),
                invalid_cost_row_count=pl.lit(0, dtype=pl.UInt32),
            )
            .select(
                DEMAND_UNIT_COLS
                + [
                    "activity_seq_id", "time_seq_id", "mode_seq_id", "dest_seq_id", "utility",
                    "is_feasible", "missing_cost_row_count", "invalid_cost_row_count",
                ]
            )
        )
        if plan_ids_already_indexed:
            # Mobile and stay-home IDs were assigned together before scoring.
            stay_home_utility = stay_home_utility.join(plan_id_index.lazy(), on=PLAN_KEY_COLS, how="inner")
            return pl.concat([possible_plan_utility, stay_home_utility]), plan_id_index
        return add_plan_id(pl.concat([possible_plan_utility, stay_home_utility]), previous_index=plan_id_index)

    def add_stay_home_plan_steps(
        self,
        possible_plan_steps: pl.LazyFrame,
        stay_home_plan: pl.DataFrame,
        *,
        plan_id_index: pl.DataFrame,
        plan_id_index_complete: bool = False,
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

        if plan_id_index_complete:
            # Mobile steps were indexed before utility scoring, and scoring
            # added stay-home IDs to the same index. Reuse those IDs directly.
            indexed_mobile = possible_plan_steps.filter(pl.col("mode_seq_id") != 0).select(
                step_columns + ["plan_id"]
            )
            indexed_stay_home = stay_home_steps.join(plan_id_index.lazy(), on=PLAN_KEY_COLS, how="inner")
            return pl.concat([indexed_mobile, indexed_stay_home], how="vertical_relaxed"), plan_id_index

        return add_plan_id(
            pl.concat([possible_plan_steps_no_id, stay_home_steps], how="vertical_relaxed"),
            previous_index=plan_id_index,
        )

    @trace_simulation_step("behavior transition probabilities")
    def get_transition_probabilities(
        self,
        current_plans: pl.DataFrame,
        possible_plan_utility: pl.LazyFrame,
        possible_plan_steps: pl.DataFrame | pl.LazyFrame,
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

        # Origins with the same demand unit and fixed sequence choices see the
        # same feasible targets. Find the best target before forming pairs so
        # the existing pruning and gain rules can be applied inside the join.
        # Forced reassignment bypasses these rules and can also use stay-home.
        if behavior_change_scope == BehaviorChangeScope.NO_TRANSITIONS:
            # Only self-transitions and forced reassignments can pass this
            # scope; both bypass the utility and gain filters.
            targets = possible_plan_utility_for_transitions
            pair_filter = scope_pair_constraint
            drop_columns = ["demand_group_id_trans", "demand_subgroup_id_trans"]
        else:
            target_group = list(DEMAND_UNIT_COLS)
            target_utility = pl.col("utility")
            if behavior_change_scope in (BehaviorChangeScope.DESTINATION_REPLANNING, BehaviorChangeScope.MODE_REPLANNING):
                target_group += ["activity_seq_id", timing_constraint_column]
                if behavior_change_scope == BehaviorChangeScope.MODE_REPLANNING:
                    target_group.append("dest_seq_id")
                target_utility = pl.when(pl.col("mode_seq_id") != 0).then(target_utility)
            targets = possible_plan_utility_for_transitions.with_columns(
                max_utility_trans=target_utility.max().over(target_group)
            )
            pair_filter = scope_pair_constraint & utility_filter & gain_filter
            drop_columns = ["demand_group_id_trans", "demand_subgroup_id_trans", "max_utility_trans"]
        return (
            current_plans_for_transitions
            .select(plan_cols + ["utility"])
            .rename({"utility": "utility_prev_from"})
            .join(possible_plan_utility, on=plan_cols)
            .rename({"plan_id": "plan_id_from"})
            .join_where(
                targets,
                (
                    (pl.col("demand_group_id") == pl.col("demand_group_id_trans"))
                    & (pl.col("demand_subgroup_id") == pl.col("demand_subgroup_id_trans"))
                    & pair_filter
                ),
                suffix="_trans",
            )
            .drop(drop_columns)
            .rename({"plan_id": "plan_id_trans"})
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

        # Sorting before summation keeps transition probabilities independent
        # of the order in which equivalent plan rows reached this calculation.
        q_transition = (
            pl.col("utility_weight")
            / pl.col("utility_weight").sort().implode().list.sum().over(plan_cols)
        )
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
        min_plan_persons: float = 0.0,
        plan_pruning_start_step: int = 2,
        stay_home_plans: pl.DataFrame | pl.LazyFrame | None = None,
        behavior_change_scope: BehaviorChangeScope | None = None,
        include_events: bool = True,
    ) -> tuple[pl.DataFrame, pl.LazyFrame | None]:
        """Apply transition probabilities and emit transition events."""
        plan_cols = PLAN_KEY_COLS
        # A no-transition step must not move people merely to shrink the state.
        if behavior_change_scope == BehaviorChangeScope.NO_TRANSITIONS:
            min_plan_persons = 0.0

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
        if include_events:
            # Choice probabilities describe the original target, even when a
            # small allocation is subsequently reassigned to stay home.
            transitions = transitions.with_columns(
                **{"chosen_" + column: pl.col(column + "_trans") for column in PLAN_COLUMNS}
            )

        if min_plan_persons > 0 and iteration >= plan_pruning_start_step:
            if stay_home_plans is None:
                raise ValueError("Stay-home plans are required to merge small allocations.")
            target_volumes = transitions.group_by(DEMAND_UNIT_COLS + ["plan_id_trans"]).agg(
                pl.col("n_persons_moved").sort().implode().list.sum().alias("_target_persons")
            )
            home = stay_home_plans.lazy().select(
                DEMAND_UNIT_COLS + [pl.col("plan_id").alias("_home_id"), pl.col("utility").alias("_home_utility")]
            )
            transitions = transitions.join(target_volumes, on=DEMAND_UNIT_COLS + ["plan_id_trans"]).join(
                home, on=DEMAND_UNIT_COLS, how="left"
            ).with_columns(
                _small_target=(pl.col("mode_seq_id_trans") != 0) & (pl.col("_target_persons") < min_plan_persons)
            ).with_columns(
                plan_id_trans=pl.when(pl.col("_small_target")).then(pl.col("_home_id")).otherwise(pl.col("plan_id_trans")),
                utility_trans=pl.when(pl.col("_small_target")).then(pl.col("_home_utility")).otherwise(pl.col("utility_trans")),
                **{column + "_trans": pl.when(pl.col("_small_target")).then(pl.lit(0)).otherwise(pl.col(column + "_trans"))
                   for column in PLAN_COLUMNS},
            ).drop("_target_persons", "_home_id", "_home_utility", "_small_target")
        transition_events = None
        if include_events:
            event_transitions = self.attach_previous_target_utility(current_plans, transitions)
            transition_events = build_transition_events_lazy(event_transitions, iteration=iteration)

        new_states = (
            transitions.group_by(
                ["plan_id_trans"] + DEMAND_UNIT_COLS + ["activity_seq_id_trans", "time_seq_id_trans", "dest_seq_id_trans", "mode_seq_id_trans"]
            )
            .agg(
                n_persons=pl.col("n_persons_moved").sort().implode().list.sum(),
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

    get_destination_saturation = staticmethod(get_destination_saturation)
    _ensure_destination_saturation_columns = staticmethod(ensure_destination_saturation_columns)
