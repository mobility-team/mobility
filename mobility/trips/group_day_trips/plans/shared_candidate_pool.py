from __future__ import annotations


import logging
from collections import Counter
from dataclasses import dataclass
from typing import Any

import polars as pl

from mobility.trips.group_day_trips.core.performance_trace import simulation_step, trace_simulation_step

from mobility.runtime.assets.asset import Asset
from mobility.runtime.assets.graph import asset_graph_key

from .candidate_plan_steps import CandidatePlanStepsAsset
from .compact_candidate_steps import (
    CompactCandidateSteps, ScoringCandidateSteps, MEMBERSHIP_COLUMNS, PLAN_COLUMNS, STEP_COLUMNS,
    STEP_KEY_COLUMNS,
)
from .plan_catalogue import PlanCatalogue
from .demand_subgroups import DEMAND_UNIT_COLS
from .plan_ids import PLAN_KEY_COLS
from .stable_key_index import StableKeyIndex
from .time_sequences import TimeSequenceIndexCache, assign_time_sequence_ids
from .activity_profiles import get_schedules_by_activity_profile, get_survey_schedule_activities
from ..core.multi_scenario import CANDIDATE_ID_COLS, add_candidate_ids


@dataclass(frozen=True)
class ScenarioCandidateSource:
    """Assets needed to add one generation batch for a run context."""

    run_key: int
    scenario: str
    run: Any
    previous_state: Asset
    destination_sequences: Any
    mode_sequences: Any
    iteration: int


@dataclass(frozen=True)
class SharedCandidatePoolResult:
    """Shared content and run-local ids after one generation batch."""

    candidate_plan_steps_by_run_key: dict[int, pl.DataFrame | pl.LazyFrame]
    destination_sequence_indices: dict[int, pl.DataFrame]
    mode_sequence_indices: dict[int, pl.DataFrame]
    generated_mode_sequences_by_run_key: dict[int, int]
    candidate_ids: pl.DataFrame
    time_sequence_steps_by_run_key: dict[int, pl.DataFrame]
    candidate_replications: pl.DataFrame
    candidate_catalogue: PlanCatalogue | None = None
    # Keep the attachment and active rows separate for the inner loop so it
    # can combine both with the occupied plans in one deduplication.
    candidate_step_parts_by_run_key: dict[int, tuple[pl.LazyFrame, pl.DataFrame | None]] | None = None
    scoring_candidate_steps_by_run_key: dict[int, ScoringCandidateSteps] | None = None


class SharedCandidatePool:
    """Merge complete plans from every run context and translate local ids."""

    def __init__(
        self,
        *,
        sources: list[ScenarioCandidateSource],
        iteration: int,
        max_shared_candidates: int | None = None,
        max_inactive_age: int | None = None,
        previous_candidates_by_run_key: dict[int, pl.DataFrame | CompactCandidateSteps | None] | None = None,
        previous_catalogue: PlanCatalogue | None = None,
        previous_destination_indices: dict[int, pl.DataFrame | None] | None = None,
        previous_mode_indices: dict[int, pl.DataFrame | None] | None = None,
        previous_time_sequence_steps: dict[int, pl.DataFrame] | None = None,
        compact: bool = False,
        build_catalogue: bool = True,
        time_index_caches: dict[int, TimeSequenceIndexCache] | None = None,
    ) -> None:
        if not sources:
            raise ValueError("A shared candidate pool needs at least one run source.")
        if any(not isinstance(source.run_key, int) for source in sources):
            raise TypeError("Candidate sources must use integer run IDs.")
        run_keys = {source.run_key for source in sources}
        if len(run_keys) != len(sources):
            raise ValueError("A pool merge accepts one generation source per run context.")
        self.sources = sources
        self.iteration = iteration
        self.max_shared_candidates = max_shared_candidates
        self.max_inactive_age = max_inactive_age
        self.previous_candidates_by_run_key = previous_candidates_by_run_key or {}
        self.previous_catalogue = previous_catalogue
        self.previous_destination_indices = previous_destination_indices or {}
        self.previous_mode_indices = previous_mode_indices or {}
        self.previous_time_sequence_steps = previous_time_sequence_steps or {}
        self.time_sequence_steps_by_run_key: dict[int, pl.DataFrame] = {}
        self.compact = compact
        self.build_catalogue = build_catalogue
        self.time_index_caches = time_index_caches or {}
        self.compact_candidate_steps_by_run_key: dict[int, pl.DataFrame] = {}
        self.unordered_candidate_steps_by_run_key: dict[int, pl.LazyFrame] = {}
        self.scoring_candidate_steps_by_run_key: dict[int, ScoringCandidateSteps] = {}

    @trace_simulation_step("shared pool generation and attachment")
    def build(self) -> SharedCandidatePoolResult:
        """Collect candidate plans and merge them by canonical content."""
        candidate_frames: list[pl.DataFrame] = []
        merged_chunks: list[pl.DataFrame] = []
        generated_mode_sequences = {source.run_key: 0 for source in self.sources}
        empty_candidates_by_run_key: dict[int, pl.DataFrame] = {}
        active_candidate_steps_by_run_key: dict[int, pl.DataFrame] = {}
        previous_index_by_run_key: dict[int, tuple[pl.DataFrame | None, pl.DataFrame | None]] = {}
        active_candidate_id_frames: list[pl.DataFrame] = []
        candidate_replication_frames: list[pl.DataFrame] = []
        profile_assignments_by_run_key: dict[int, pl.DataFrame] = {}
        history_by_run_key: dict[int, pl.DataFrame] = {}
        replication_history_frames: list[pl.DataFrame] = []
        generated_by_replication: list[pl.DataFrame] = []
        candidate_catalogues: list[PlanCatalogue] = []
        history_keys = CANDIDATE_ID_COLS + [
            "activity_seq_id", "time_seq_id",
            "destination_sequence_key_1", "destination_sequence_key_2",
            "mode_sequence_key_1", "mode_sequence_key_2",
        ]
        # Equivalent scenarios can use exactly the same generation assets.
        # Keep each generated table only until the last such source uses it.
        generation_keys = {
            source.run_key: (
                source.run.replication, id(source.run.population),
                asset_graph_key(source.previous_state),
                asset_graph_key(source.destination_sequences),
                asset_graph_key(source.mode_sequences),
            )
            for source in self.sources
        }
        remaining_generation = Counter(generation_keys.values())
        generated_by_source = {}
        state_keys = {
            source.run_key: (source.run.replication, asset_graph_key(source.previous_state))
            for source in self.sources
        }
        remaining_states = Counter(state_keys.values())
        active_by_state = {}
        previous_by_state = {}
        history_by_state = {}
        restored_catalogue = {}
        catalogue_groups = (
            self.previous_catalogue.equivalent_runs or {}
            if self.previous_catalogue is not None else {}
        )
        catalogue_group_sizes = Counter(catalogue_groups.values())

        for source in self.sources:
            source_candidate_frames: list[pl.DataFrame] = []
            source_history_frames: list[pl.DataFrame] = []
            previous_state = source.previous_state.get()
            source.destination_sequences.get()
            source.mode_sequences.get()
            scenario = source.scenario
            run_key = source.run_key

            if run_key not in previous_index_by_run_key:
                previous_destination_index = self.previous_destination_indices.get(
                    run_key,
                    previous_state.destination_sequence_index,
                )
                previous_mode_index = self.previous_mode_indices.get(
                    run_key,
                    previous_state.mode_sequence_index,
                )
                previous_index_by_run_key[run_key] = (
                    previous_destination_index,
                    previous_mode_index,
                )

                if self.previous_catalogue is not None:
                    restore_key = (
                        catalogue_groups.get(run_key, run_key),
                        id(previous_destination_index), id(previous_mode_index),
                    )
                    previous_candidates = restored_catalogue.get(restore_key)
                    if previous_candidates is None:
                        previous_candidates = self.previous_catalogue.candidates_for_run(
                            run_key, previous_destination_index, previous_mode_index,
                        )
                        if catalogue_group_sizes[restore_key[0]] > 1:
                            restored_catalogue[restore_key] = previous_candidates
                else:
                    previous_candidates = self.previous_candidates_by_run_key.get(
                        run_key,
                        previous_state.candidate_plan_steps,
                    )
                compact_previous = previous_candidates if isinstance(previous_candidates, CompactCandidateSteps) else None
                state_key = state_keys[run_key]
                previous_inputs = (
                    compact_previous if compact_previous is not None else previous_candidates,
                    previous_destination_index, previous_mode_index,
                    self.previous_time_sequence_steps.get(run_key, previous_state.schedule_steps),
                    previous_state.current_plans,
                )
                shared_previous = compact_previous is not None or self.previous_catalogue is not None
                previous_group = previous_by_state.setdefault(state_key, []) if shared_previous else []
                previous_result = next((
                    result for inputs, result in previous_group
                    if all(left is right for left, right in zip(inputs, previous_inputs))
                ), None)
                if compact_previous is not None and previous_result is None:
                    # History is ultimately reduced by plan variant, not by
                    # demand unit. Reduce membership before joining steps so
                    # identical plans are not expanded once per person group.
                    if compact_previous.unit_map is not None:
                        # All units in a context have this same history. Only
                        # the first unit is needed for the plan-level merge.
                        representatives = (
                            compact_previous.unit_map.sort(DEMAND_UNIT_COLS)
                            .unique(subset=["_score_context_id"], keep="first", maintain_order=True)
                        )
                        source_membership = (
                            compact_previous.membership.drop("demand_subgroup_id")
                            .rename({"demand_group_id": "_score_context_id"})
                            .join(representatives, on="_score_context_id", how="inner")
                        )
                    else:
                        source_membership = compact_previous.membership
                    membership = (
                        source_membership.sort(DEMAND_UNIT_COLS)
                        .group_by(PLAN_COLUMNS, maintain_order=True)
                        .agg(
                            [pl.col(column).first() for column in DEMAND_UNIT_COLS + ["country", "csp"]]
                            + [
                                pl.col("iteration").min(),
                                pl.col("first_seen_iteration").min(),
                                pl.col("last_seen_iteration").max(),
                                pl.col("last_active_iteration").max(),
                            ]
                        )
                    )
                    previous_candidates = (
                        membership.lazy()
                        .join(compact_previous.steps.lazy(), on=PLAN_COLUMNS, how="inner")
                        .select(compact_previous.columns)
                        .sort(CandidatePlanStepsAsset.DEDUPE_COLUMNS)
                        .collect(engine="streaming")
                    )
                if previous_candidates is not None:
                    empty_candidates_by_run_key[run_key] = (
                        previous_result[0] if previous_result is not None else previous_candidates.select(
                            CandidatePlanStepsAsset.STRUCTURAL_COLUMNS + CandidatePlanStepsAsset.RETENTION_COLUMNS
                        ).head(0)
                    )
                    active_source = previous_state.candidate_plan_steps
                    if active_source is None:
                        active_source = compact_previous if compact_previous is not None else previous_candidates
                    state_key = state_keys[run_key]
                    active_inputs = (
                        active_source, previous_state.current_plans,
                        previous_state.destination_sequence_index, previous_state.mode_sequence_index,
                        previous_state.schedule_steps,
                    )
                    # The state's own history is already held for the whole
                    # batch. A locally reconstructed fallback can differ by
                    # run and should not be retained just for reuse.
                    shared_history = (
                        previous_state.candidate_plan_steps is not None
                        or self.previous_catalogue is not None
                    )
                    state_active = active_by_state.setdefault(state_key, []) if shared_history else []
                    active_result = next((
                        result for inputs, result in state_active
                        if all(left is right for left, right in zip(inputs, active_inputs))
                    ), None)
                    if active_result is None:
                        active_keys = (
                            previous_state.current_plans.lazy()
                            .filter(pl.col("mode_seq_id") != 0)
                            .select(PLAN_KEY_COLS)
                        )
                        if isinstance(active_source, CompactCandidateSteps):
                            active_rows = (
                                active_source.memberships_for_plans(active_keys)
                                .join(active_source.steps.lazy(), on=PLAN_COLUMNS, how="inner")
                                .select(active_source.columns)
                                .sort(CandidatePlanStepsAsset.DEDUPE_COLUMNS)
                            )
                        else:
                            active_rows = active_source.lazy().join(active_keys, on=PLAN_KEY_COLS, how="inner")
                        active_steps = (
                            active_rows
                            .select(
                                CandidatePlanStepsAsset.STRUCTURAL_COLUMNS
                                + CandidatePlanStepsAsset.RETENTION_COLUMNS
                            )
                            .with_columns(last_active_iteration=pl.lit(self.iteration, dtype=pl.UInt16))
                            .collect(engine="streaming")
                        )
                        active_ids = active_history = None
                        if active_steps.height:
                            # Preserve the occupied timing even if the sampled
                            # pool still contains its original survey timing.
                            active_content = self._attach_sequence_content_keys(
                                add_candidate_ids(active_steps),
                                previous_state.destination_sequence_index,
                                previous_state.mode_sequence_index,
                                previous_state.schedule_steps,
                            )
                            candidate_frames.append(active_content)
                            active_ids = active_content.select(CANDIDATE_ID_COLS).unique()
                            active_history = active_content.select(
                                history_keys + CandidatePlanStepsAsset.RETENTION_COLUMNS
                            )
                            active_candidate_id_frames.append(active_ids)
                        active_result = (active_steps, active_ids, active_history)
                        if shared_history and remaining_states[state_key] > 1:
                            state_active.append((active_inputs, active_result))
                    active_steps, active_ids, active_history = active_result
                    active_candidate_steps_by_run_key[run_key] = active_steps
                    if active_ids is not None:
                        source_candidate_frames.append(active_ids)
                        source_history_frames.append(active_history)
                if previous_result is not None:
                    _, previous_ids, previous_history, active_ids, active_history = previous_result
                    source_candidate_frames.append(previous_ids)
                    source_history_frames.append(previous_history)
                    if active_ids is not None:
                        active_candidate_id_frames.append(active_ids)
                        source_history_frames.append(active_history)
                elif previous_candidates is not None and previous_candidates.height:
                    # The catalogue already stores the physical identity and
                    # sequence keys. Only restored outer-step rows need them
                    # computed again; inner batches can carry them forward.
                    previous_content = (
                        previous_candidates if self.previous_catalogue is not None
                        else self._attach_sequence_content_keys(
                            add_candidate_ids(previous_candidates),
                            previous_destination_index,
                            previous_mode_index,
                            self.previous_time_sequence_steps.get(run_key, previous_state.schedule_steps),
                        )
                    )
                    candidate_frames.append(previous_content)
                    previous_ids = previous_content.select(CANDIDATE_ID_COLS).unique()
                    previous_history = previous_content.select(history_keys + CandidatePlanStepsAsset.RETENTION_COLUMNS)
                    source_candidate_frames.append(previous_ids)
                    source_history_frames.append(previous_history)

                    previous_active_ids = previous_active_history = None
                    if previous_state.current_plans.height:
                        # Only membership matters here: history is unique by
                        # content, even if several current rows match a plan.
                        active_keys = previous_state.current_plans.lazy().select(PLAN_KEY_COLS)
                        active_join_columns = PLAN_KEY_COLS
                        if compact_previous is not None:
                            active_keys = (
                                compact_previous.memberships_for_plans(active_keys)
                                .select(PLAN_COLUMNS)
                                .unique()
                            )
                            active_join_columns = PLAN_COLUMNS
                        active = (
                            previous_content.lazy()
                            .join(
                                active_keys,
                                on=active_join_columns,
                                how="semi",
                            )
                            .select(history_keys)
                            .unique()
                            .collect(engine="streaming")
                        )
                        previous_active_ids = active.select(CANDIDATE_ID_COLS).unique()
                        previous_active_history = active.with_columns(
                            first_seen_iteration=pl.lit(None, dtype=pl.UInt16),
                            last_seen_iteration=pl.lit(None, dtype=pl.UInt16),
                            last_active_iteration=pl.lit(self.iteration, dtype=pl.UInt16),
                        )
                        active_candidate_id_frames.append(previous_active_ids)
                        source_history_frames.append(previous_active_history)
                    if shared_previous and remaining_states[state_key] > 1:
                        previous_group.append((previous_inputs, (
                            empty_candidates_by_run_key[run_key], previous_ids, previous_history,
                            previous_active_ids, previous_active_history,
                        )))

            with simulation_step("generate scenario candidates", scenario=scenario):
                generation_key = generation_keys[run_key]
                cached = generated_by_source.get(generation_key)
                if cached is None:
                    profile_assignments, _ = source.mode_sequences.transport_costs.get_utility_profiles(
                        previous_state.demand_groups,
                        source.run.population.population_segments,
                    )
                    with simulation_step("generate and attach source candidates"):
                        generated = CandidatePlanStepsAsset.build_iteration_candidates(
                            destination_sequences=source.destination_sequences,
                            mode_sequences=source.mode_sequences,
                            survey_plan_steps=previous_state.schedule_steps,
                            demand_groups=previous_state.demand_groups,
                            profile_assignments=profile_assignments,
                            compact_for_shared_pool=True,
                        ).collect(engine="streaming")
                    if logging.getLogger().isEnabledFor(logging.DEBUG):
                        logging.debug(
                            "Generated compact candidate steps | scenario=%s | step_rows=%d | "
                            "plan_variants=%d | home_zones=%d",
                            scenario, generated.height,
                            generated.select(PLAN_KEY_COLS).unique().height,
                            generated.filter(pl.col("seq_step_index") == 1)
                            .select(pl.col("from").n_unique()).item(),
                        )
                    generated = generated.with_columns(
                        first_seen_iteration=pl.lit(self.iteration, dtype=pl.UInt16),
                        last_seen_iteration=pl.lit(self.iteration, dtype=pl.UInt16),
                        last_active_iteration=pl.lit(None, dtype=pl.UInt16),
                    )
                    if generated.height:
                        generated = self._attach_sequence_content_keys(
                            add_candidate_ids(generated),
                            source.destination_sequences.get_index(),
                            source.mode_sequences.get_index(),
                            previous_state.schedule_steps,
                        )
                        candidate_frames.append(generated)
                        generated_ids = generated.select(CANDIDATE_ID_COLS).unique()
                        generated_by_replication.append(
                            generated_ids.with_columns(replication=pl.lit(source.run.replication, dtype=pl.UInt32))
                        )
                        generated_history = generated.select(history_keys + CandidatePlanStepsAsset.RETENTION_COLUMNS)
                    else:
                        generated_ids = generated_history = None
                    cached = (profile_assignments, generated, generated_ids, generated_history,
                              int(source.mode_sequences.generated_mode_sequences))
                    generated_by_source[generation_key] = cached
                profile_assignments, generated, generated_ids, generated_history, generated_count = cached
                profile_assignments_by_run_key[run_key] = profile_assignments
                empty_candidates_by_run_key.setdefault(
                    run_key,
                    generated.select(
                        CandidatePlanStepsAsset.STRUCTURAL_COLUMNS + CandidatePlanStepsAsset.RETENTION_COLUMNS
                    ).head(0),
                )
                if generated_ids is not None:
                    source_candidate_frames.append(generated_ids)
                    source_history_frames.append(generated_history)
                generated_mode_sequences[run_key] += generated_count
                remaining_generation[generation_key] -= 1
                if not remaining_generation[generation_key]:
                    del generated_by_source[generation_key]

            if source_candidate_frames:
                # Scenarios share plans within a replication. Other replications
                # retain their own sampled choice sets, even for identical plans.
                candidate_replication_frames.append(
                    pl.concat(source_candidate_frames).unique().with_columns(
                        replication=pl.lit(source.run.replication, dtype=pl.UInt32)
                    )
                )
            if source_history_frames:
                state_key = state_keys[run_key]
                state_histories = history_by_state.setdefault(state_key, [])
                history = next((
                    result for inputs, result in state_histories
                    if len(inputs) == len(source_history_frames)
                    and all(left is right for left, right in zip(inputs, source_history_frames))
                ), None)
                if history is None:
                    history = (
                        pl.concat(source_history_frames, how="vertical_relaxed")
                        .group_by(history_keys)
                        .agg(
                            pl.col("first_seen_iteration").min(),
                            pl.col("last_seen_iteration").max(),
                            pl.col("last_active_iteration").max(),
                        )
                    )
                    if remaining_states[state_key] > 1:
                        state_histories.append((tuple(source_history_frames), history))
                history_by_run_key[run_key] = history
                if self.max_inactive_age is not None:
                    replication_history_frames.append(
                        history_by_run_key[run_key].select(
                            CANDIDATE_ID_COLS + CandidatePlanStepsAsset.RETENTION_COLUMNS
                        ).with_columns(replication=pl.lit(source.run.replication, dtype=pl.UInt32))
                    )

            # The merged history and replication IDs now own what this source
            # contributes. Keep its candidate rows only in candidate_frames
            # until the shared content has been merged.
            source_candidate_frames.clear()
            source_history_frames.clear()
            active_content = previous_content = previous_candidates = generated = None
            cached = generated_ids = generated_history = None
            active_result = active_steps = active_ids = active_history = state_active = None
            active_inputs = active_source = None
            previous_inputs = previous_result = previous_group = None
            previous_ids = previous_history = previous_active_ids = previous_active_history = None
            state_histories = history = None
            active = active_rows = membership = None
            state_key = state_keys[run_key]
            remaining_states[state_key] -= 1
            if not remaining_states[state_key]:
                active_by_state.pop(state_key, None)
                previous_by_state.pop(state_key, None)
                history_by_state.pop(state_key, None)

            # Merge small groups before collecting the whole cohort. Equivalent
            # plans from different runs then occupy one row in each chunk.
            if len(candidate_frames) >= 5:
                merged_chunks.append(self._merge_candidate_content(candidate_frames))
                candidate_frames.clear()

        restored_catalogue.clear()
        candidate_frames = merged_chunks + candidate_frames
        if candidate_frames:
            shared_candidates = self._merge_candidate_content(candidate_frames)
        else:
            shared_candidates = self._empty_candidate_content()
        # Per-scenario attachment only needs the merged rows. Drop the source
        # frames before building those outputs, which can themselves be large.
        candidate_frames.clear()
        merged_chunks.clear()

        if shared_candidates.height and active_candidate_id_frames:
            active_candidate_ids = pl.concat(active_candidate_id_frames).unique()
            shared_candidates = (
                shared_candidates
                .join(
                    active_candidate_ids.with_columns(_is_active=pl.lit(True)),
                    on=CANDIDATE_ID_COLS,
                    how="left",
                )
                .with_columns(
                    last_active_iteration=pl.when(pl.col("_is_active").fill_null(False))
                    .then(
                        pl.max_horizontal(
                            pl.col("last_active_iteration").fill_null(0),
                            pl.lit(self.iteration, dtype=pl.UInt16),
                        )
                    )
                    .otherwise(pl.col("last_active_iteration"))
                )
                .drop("_is_active")
            )
            active_candidate_ids = None
        active_candidate_id_frames.clear()

        if candidate_replication_frames:
            candidate_replications = (
                pl.concat(candidate_replication_frames)
                .unique()
                .join(shared_candidates.select(CANDIDATE_ID_COLS).unique(), on=CANDIDATE_ID_COLS, how="semi")
                .sort(["replication"] + CANDIDATE_ID_COLS)
            )
        else:
            candidate_replications = shared_candidates.select(CANDIDATE_ID_COLS).head(0).with_columns(
                replication=pl.lit(None, dtype=pl.UInt32)
            )
        if self.max_inactive_age is not None and replication_history_frames:
            # Scenarios share a physical pool within a replication, but one
            # seed's recent activity must not keep another seed's plan alive.
            history = pl.concat(replication_history_frames).group_by(
                ["replication"] + CANDIDATE_ID_COLS
            ).agg(
                pl.col("first_seen_iteration").min(),
                pl.col("last_seen_iteration").max(),
                pl.col("last_active_iteration").max(),
            )
            cutoff = max(0, self.iteration - self.max_inactive_age)
            retained = history.filter(
                pl.max_horizontal(
                    "first_seen_iteration", "last_seen_iteration", "last_active_iteration"
                ) >= cutoff
            ).select(["replication"] + CANDIDATE_ID_COLS)
            candidate_replications = candidate_replications.join(
                retained, on=["replication"] + CANDIDATE_ID_COLS, how="semi"
            )
            shared_candidates = shared_candidates.join(
                candidate_replications.select(CANDIDATE_ID_COLS).unique(),
                on=CANDIDATE_ID_COLS, how="semi",
            )
            del history, retained
        replication_history_frames.clear()
        if self.max_shared_candidates is not None:
            shared_candidates = self._limit_candidate_pool(shared_candidates)
            candidate_replications = candidate_replications.join(
                shared_candidates.select(CANDIDATE_ID_COLS).unique(), on=CANDIDATE_ID_COLS, how="semi"
            )
        candidate_replications = candidate_replications.sort(["replication"] + CANDIDATE_ID_COLS)
        generated_candidates = (
            pl.concat(generated_by_replication).unique()
            if generated_by_replication else candidate_replications.head(0)
        )
        candidate_replication_frames.clear()
        generated_by_replication.clear()

        candidate_steps_by_run_key: dict[int, pl.DataFrame | pl.LazyFrame] = {}
        candidate_step_parts_by_run_key = {}
        destination_indices: dict[int, pl.DataFrame] = {}
        mode_indices: dict[int, pl.DataFrame] = {}
        candidates_by_replication: dict[int, pl.DataFrame] = {}
        refreshed_by_replication: dict[int, pl.DataFrame] = {}
        remaining_sources = Counter(source.run.replication for source in self.sources)
        history_keys_by_run = {
            source.run_key: (source.run.replication, id(history_by_run_key.get(source.run_key)))
            for source in self.sources
        }
        remaining_histories = Counter(history_keys_by_run.values())
        candidates_by_history = {}
        attachment_keys = {}
        for source in self.sources:
            run_key = source.run_key
            previous_destination, previous_mode = previous_index_by_run_key[run_key]
            attachment_keys[run_key] = (
                history_keys_by_run[run_key], id(previous_destination), id(previous_mode),
                state_keys[run_key], id(self.previous_time_sequence_steps.get(run_key)),
                id(profile_assignments_by_run_key[run_key]),
            )
        remaining_attachments = Counter(attachment_keys.values())
        shared_attachments = {}
        shared_catalogues = {}
        catalogue_group_by_attachment = {}
        catalogue_equivalent_runs = {}

        for source in self.sources:
            scenario = source.scenario
            run_key = source.run_key
            replication = source.run.replication
            if replication not in candidates_by_replication:
                # Scenarios in one replication draw from the same physical pool.
                candidates_by_replication[replication] = shared_candidates.join(
                    candidate_replications.filter(pl.col("replication") == replication)
                    .select(CANDIDATE_ID_COLS),
                    on=CANDIDATE_ID_COLS,
                    how="semi",
                )
            replication_candidates = candidates_by_replication[replication]
            remaining_sources[replication] -= 1
            if not remaining_sources[replication]:
                del candidates_by_replication[replication]
            local_key = history_keys_by_run[run_key]
            local_candidates = candidates_by_history.get(local_key)
            if local_candidates is not None:
                replication_candidates = local_candidates
            elif replication_candidates.height:
                # A shared plan can be active in one scenario and inactive in
                # another. Keep its retention history local to this run.
                replication_candidates = replication_candidates.drop(CandidatePlanStepsAsset.RETENTION_COLUMNS)
                local_history = history_by_run_key.get(run_key)
                if local_history is not None:
                    replication_candidates = replication_candidates.join(
                        local_history, on=history_keys, how="left"
                    )
                else:
                    replication_candidates = replication_candidates.with_columns([
                        pl.lit(None, dtype=pl.UInt16).alias(column)
                        for column in CandidatePlanStepsAsset.RETENTION_COLUMNS
                    ])
                if generated_candidates.height:
                    if replication not in refreshed_by_replication:
                        refreshed_by_replication[replication] = (
                            generated_candidates.filter(pl.col("replication") == replication)
                            .select(CANDIDATE_ID_COLS).unique()
                            .with_columns(_refreshed=pl.lit(True))
                        )
                    refreshed = refreshed_by_replication[replication]
                    replication_candidates = replication_candidates.join(refreshed, on=CANDIDATE_ID_COLS, how="left")
                else:
                    replication_candidates = replication_candidates.with_columns(_refreshed=pl.lit(False))
                replication_candidates = replication_candidates.with_columns(
                    first_seen_iteration=pl.col("first_seen_iteration").fill_null(self.iteration),
                    last_seen_iteration=pl.when(pl.col("_refreshed").fill_null(False))
                    .then(pl.lit(self.iteration, dtype=pl.UInt16))
                    .otherwise(pl.col("last_seen_iteration").fill_null(self.iteration)),
                ).drop("_refreshed")
                if remaining_histories[local_key] > 1:
                    candidates_by_history[local_key] = replication_candidates
            remaining_histories[local_key] -= 1
            if not remaining_histories[local_key]:
                candidates_by_history.pop(local_key, None)
            if not remaining_sources[replication]:
                refreshed_by_replication.pop(replication, None)
            self.time_sequence_steps_by_run_key[run_key] = self.previous_time_sequence_steps.get(
                run_key, source.previous_state.get().schedule_steps
            )
            previous_destination_index, previous_mode_index = previous_index_by_run_key[run_key]
            attachment_key = attachment_keys[run_key]
            attachment = shared_attachments.get(attachment_key)
            destination_key_cols = [
                "destination_sequence_key_1",
                "destination_sequence_key_2",
            ]
            mode_key_cols = ["mode_sequence_key_1", "mode_sequence_key_2"]
            if attachment is None:
                destination_index_rows = replication_candidates.select(destination_key_cols).unique()
                _, destination_index = StableKeyIndex(
                    key_cols=destination_key_cols, index_col="dest_seq_id", first_new_id=1,
                ).extend(destination_index_rows, previous_index=previous_destination_index)
                mode_index_rows = replication_candidates.select(mode_key_cols).unique()
                _, mode_index = StableKeyIndex(
                    key_cols=mode_key_cols, index_col="mode_seq_id", first_new_id=1,
                ).extend(mode_index_rows, previous_index=previous_mode_index)
            else:
                destination_index, mode_index = attachment[:2]
            destination_indices[run_key] = destination_index
            mode_indices[run_key] = mode_index

            if replication_candidates.height == 0:
                candidate_steps_by_run_key[run_key] = empty_candidates_by_run_key[run_key]
            else:
                if attachment is None:
                    candidate_steps = self._build_scenario_candidate_steps(
                        shared_candidates=replication_candidates,
                        destination_index=destination_index,
                        mode_index=mode_index,
                        source=source,
                        profile_assignments=profile_assignments_by_run_key[run_key] if self.compact else None,
                    )
                    if remaining_attachments[attachment_key] > 1:
                        shared_attachments[attachment_key] = (
                            destination_index, mode_index, candidate_steps,
                            self.time_sequence_steps_by_run_key[run_key],
                            self.unordered_candidate_steps_by_run_key.get(run_key),
                            self.scoring_candidate_steps_by_run_key.get(run_key),
                            self.compact_candidate_steps_by_run_key.get(run_key),
                        )
                else:
                    _, _, candidate_steps, schedules, unordered, factored, compact_steps = attachment
                    self.time_sequence_steps_by_run_key[run_key] = schedules
                    if unordered is not None:
                        self.unordered_candidate_steps_by_run_key[run_key] = unordered
                    if factored is not None:
                        self.scoring_candidate_steps_by_run_key[run_key] = factored
                    if compact_steps is not None:
                        self.compact_candidate_steps_by_run_key[run_key] = compact_steps
                if logging.getLogger().isEnabledFor(logging.DEBUG):
                    logging.debug(
                        "Shared pool candidate attachment | scenario=%s | "
                        "shared_candidate_ids=%d | shared_step_rows=%d",
                        scenario,
                        replication_candidates.select(CANDIDATE_ID_COLS).unique().height,
                        replication_candidates.height,
                    )
                active_candidate_steps = active_candidate_steps_by_run_key.get(run_key)
                if self.compact:
                    candidate_step_parts_by_run_key[run_key] = (
                        self.unordered_candidate_steps_by_run_key.pop(run_key),
                        active_candidate_steps,
                    )
                if active_candidate_steps is not None and active_candidate_steps.height:
                    # Keep active plans addressable by their existing local ids.
                    candidate_steps = (
                        pl.concat(
                            [candidate_steps.lazy(), active_candidate_steps.lazy()],
                            how="vertical_relaxed",
                        )
                        .unique(
                            subset=CandidatePlanStepsAsset.DEDUPE_COLUMNS,
                            keep="last",
                            maintain_order=True,
                        )
                        .sort(PLAN_KEY_COLS + ["seq_step_index"])
                    )
                    if not self.compact:
                        candidate_steps = candidate_steps.collect(engine="streaming")
                candidate_steps_by_run_key[run_key] = candidate_steps

            if self.compact and run_key in self.compact_candidate_steps_by_run_key:
                compact_steps = self.compact_candidate_steps_by_run_key.pop(run_key)
                catalogue_equivalent_runs[run_key] = catalogue_group_by_attachment.setdefault(
                    attachment_key, run_key,
                )
                shared_catalogue = shared_catalogues.get(attachment_key)
                if shared_catalogue is not None:
                    catalogue = PlanCatalogue(
                        plans=shared_catalogue.plans, steps=shared_catalogue.steps,
                        variants=shared_catalogue.variants,
                        membership=shared_catalogue.membership.with_columns(
                            run_id=pl.lit(run_key, dtype=pl.Int64),
                        ),
                    )
                else:
                    content = compact_steps.with_columns(
                        run_id=pl.lit(run_key, dtype=pl.Int64),
                        replication=pl.lit(source.run.replication, dtype=pl.UInt32),
                    )
                    catalogue = PlanCatalogue.from_candidates(content)
                    if candidate_catalogues:
                        first = candidate_catalogues[0]
                        # Share physical tables across other equal attachments.
                        if catalogue.steps.equals(first.steps):
                            catalogue = PlanCatalogue(
                                plans=first.plans,
                                steps=first.steps,
                                variants=(
                                    first.variants if catalogue.variants.equals(first.variants)
                                    else catalogue.variants
                                ),
                                membership=catalogue.membership,
                            )
                    if remaining_attachments[attachment_key] > 1:
                        shared_catalogues[attachment_key] = catalogue
                candidate_catalogues.append(catalogue)

            remaining_attachments[attachment_key] -= 1
            if not remaining_attachments[attachment_key]:
                shared_attachments.pop(attachment_key, None)
                shared_catalogues.pop(attachment_key, None)

        candidate_ids = (
            candidate_replications.select(CANDIDATE_ID_COLS)
            .unique()
            .sort(CANDIDATE_ID_COLS)
        )
        catalogue = PlanCatalogue.merge(candidate_catalogues) if candidate_catalogues else None
        if catalogue is not None:
            catalogue = PlanCatalogue(
                catalogue.plans, catalogue.steps, catalogue.variants, catalogue.membership,
                catalogue_equivalent_runs,
            )
        return SharedCandidatePoolResult(
            candidate_plan_steps_by_run_key=candidate_steps_by_run_key,
            destination_sequence_indices=destination_indices,
            mode_sequence_indices=mode_indices,
            generated_mode_sequences_by_run_key=generated_mode_sequences,
            candidate_ids=candidate_ids,
            time_sequence_steps_by_run_key=self.time_sequence_steps_by_run_key,
            candidate_replications=candidate_replications,
            candidate_catalogue=catalogue,
            candidate_step_parts_by_run_key=candidate_step_parts_by_run_key,
            scoring_candidate_steps_by_run_key=self.scoring_candidate_steps_by_run_key,
        )

    @staticmethod
    def _attach_sequence_content_keys(
        candidates: pl.DataFrame,
        destination_index: pl.DataFrame | None,
        mode_index: pl.DataFrame | None,
        schedule_steps: pl.DataFrame,
    ) -> pl.DataFrame:
        """Replace run-local sequence ids with their ordered content keys."""
        if destination_index is None or mode_index is None:
            raise ValueError(
                "Shared candidates need destination and mode sequence indexes to translate ids."
            )
        if "reference_time_seq_id" not in schedule_steps.columns:
            schedule_steps = schedule_steps.with_columns(reference_time_seq_id=pl.col("time_seq_id"))
        return (
            candidates.join(
                schedule_steps.select("activity_seq_id", "time_seq_id", "reference_time_seq_id").unique(),
                on=["activity_seq_id", "time_seq_id"],
                how="left",
            )
            .join(
                destination_index.select(
                    "dest_seq_id", "destination_sequence_key_1", "destination_sequence_key_2"
                ),
                on="dest_seq_id",
                how="inner",
            )
            .join(
                mode_index.select("mode_seq_id", "mode_sequence_key_1", "mode_sequence_key_2"),
                on="mode_seq_id",
                how="inner",
            )
        )

    @trace_simulation_step("merge shared content")
    def _merge_candidate_content(self, candidate_frames: list[pl.DataFrame]) -> pl.DataFrame:
        """Keep one structural row per shared candidate variant and trip leg."""
        candidates = pl.concat(
            [frame.select(candidate_frames[0].columns) for frame in candidate_frames],
            how="vertical_relaxed",
        )
        # Identical step content can belong to distinct activity/time sequence
        # ids. Keep each sequence variant so active plan keys survive a pool
        # merge and can be mapped back to each scenario's local indexes.
        sequence_columns = [
            "activity_seq_id",
            "time_seq_id",
            "destination_sequence_key_1",
            "destination_sequence_key_2",
            "mode_sequence_key_1",
            "mode_sequence_key_2",
        ]
        group_columns = (
            CANDIDATE_ID_COLS
            + sequence_columns
            + ["seq_step_index"]
        )
        first_columns = [
            column
            for column in CandidatePlanStepsAsset.STRUCTURAL_COLUMNS
            if column
            not in (
                set(group_columns)
                | {"dest_seq_id", "mode_seq_id", "iteration"}
            )
        ]
        first_columns.append("reference_time_seq_id")
        return (
            candidates
            .group_by(group_columns)
            .agg(
                [pl.col(column).first() for column in first_columns]
                + [
                    pl.col("iteration").min(),
                    pl.col("first_seen_iteration").min(),
                    pl.col("last_seen_iteration").max(),
                    pl.col("last_active_iteration").max(),
                ]
            )
            .sort(group_columns)
        )

    @trace_simulation_step("attach shared candidates")
    def _build_scenario_candidate_steps(
        self,
        *,
        shared_candidates: pl.DataFrame,
        destination_index: pl.DataFrame,
        mode_index: pl.DataFrame,
        source: ScenarioCandidateSource,
        profile_assignments: pl.DataFrame | None = None,
    ) -> pl.DataFrame | pl.LazyFrame:
        """Attach every shared plan to each compatible demand unit in a scenario."""
        previous_state = source.previous_state.get()
        run_key = source.run_key
        schedule_steps = self.previous_time_sequence_steps.get(run_key, previous_state.schedule_steps)

        # Keep each complete candidate intact. Splitting destination and mode
        # rows here would mix schedules that share sequence ids but differ in time.
        plan_rows = (
            shared_candidates
            .unique(subset=CANDIDATE_ID_COLS + ["activity_seq_id", "time_seq_id", "seq_step_index"], keep="first")
            .join(
                destination_index,
                on=["destination_sequence_key_1", "destination_sequence_key_2"],
                how="inner",
            )
            .join(
                mode_index,
                on=["mode_sequence_key_1", "mode_sequence_key_2"],
                how="inner",
            )
            # Catalog rows have no owning person. A fixed internal unit key
            # keeps all legs together when timing and content ids are built.
            .with_columns([
                pl.lit(0).cast(shared_candidates.schema[column]).alias(column)
                for column in DEMAND_UNIT_COLS
            ])
        )
        plan_rows, schedule_steps, _ = assign_time_sequence_ids(
            plan_rows, schedule_steps, index_cache=getattr(self, "time_index_caches", {}).get(run_key),
        )
        self.time_sequence_steps_by_run_key[run_key] = schedule_steps
        plan_rows = plan_rows.with_columns(
            home_zone_id=pl.col("from").sort_by("seq_step_index").first().over(CANDIDATE_ID_COLS)
        )

        # Check activity eligibility once per profile, then attach the original
        # candidate rows to the represented demand units in that home zone.
        units = previous_state.demand_groups.select(
            DEMAND_UNIT_COLS + ["country", "csp", "home_zone_id", "allowed_activities"]
        ).with_columns(activity_profile_id=pl.col("allowed_activities").list.join("|"))
        if profile_assignments is not None:
            # Inner scoring immediately groups equivalent units. Attach one
            # representative per scoring context instead of expanding every
            # plan to every unit and then discarding those duplicate rows.
            units = (
                units.join(profile_assignments, on=DEMAND_UNIT_COLS, how="inner")
                .with_columns(_sorted_activities=pl.col("allowed_activities").list.sort().list.join("|"))
                .sort(DEMAND_UNIT_COLS)
                .unique(
                    subset=["home_zone_id", "country", "csp", "_sorted_activities", "utility_profile_id"],
                    keep="first", maintain_order=True,
                )
                .drop("_sorted_activities", "utility_profile_id")
            )
        schedules = (
            plan_rows.select("home_zone_id", "activity_seq_id", "time_seq_id").unique()
            .join(units.select("country", "home_zone_id").unique(), on="home_zone_id")
            .join(get_survey_schedule_activities(schedule_steps), on=["activity_seq_id", "time_seq_id"])
            .with_columns(plan_weight_mass=pl.lit(1.0))
        )
        eligible = get_schedules_by_activity_profile(
            schedules=schedules,
            profiles=units.select("country", "activity_profile_id", "allowed_activities"),
        ).select("country", "activity_profile_id", "activity_seq_id", "time_seq_id")
        if getattr(self, "compact", False):
            # Keep one physical step table and attach demand units to plan
            # membership, so context grouping does not repeat every step.
            # Different content or history for the same plan key cannot be
            # represented this way and uses the ordinary row-based path.
            physical = plan_rows.select(STEP_COLUMNS).unique()
            plan_members = plan_rows.select(
                PLAN_COLUMNS + ["home_zone_id", "iteration"] + CandidatePlanStepsAsset.RETENTION_COLUMNS
            ).unique()
            unique_physical = physical.select(STEP_KEY_COLUMNS).n_unique() == physical.height
            unique_members = (
                plan_members.select(PLAN_COLUMNS + ["home_zone_id"]).n_unique() == plan_members.height
                if unique_physical else None
            )
            if unique_physical and not unique_members:
                # Different sampled histories can become the same modeled plan.
                # Merge their dates before attaching demand units, using the
                # same earliest/latest rules as modeled-plan history.
                history_keys = PLAN_COLUMNS + ["home_zone_id"]
                plan_rows = plan_rows.with_columns(
                    pl.col("iteration").min().over(history_keys),
                    pl.col("first_seen_iteration").min().over(history_keys),
                    pl.col("last_seen_iteration").max().over(history_keys),
                    pl.col("last_active_iteration").max().over(history_keys),
                )
                plan_members = plan_rows.select(
                    history_keys + ["iteration"] + CandidatePlanStepsAsset.RETENTION_COLUMNS
                ).unique(subset=history_keys, keep="first")
                unique_members = True
            if unique_physical and unique_members:
                membership = (
                    plan_members.lazy()
                    .join(units.lazy(), on="home_zone_id", how="inner")
                    .join(eligible.lazy(), on=["country", "activity_profile_id", "activity_seq_id", "time_seq_id"])
                    .select(MEMBERSHIP_COLUMNS)
                    .unique(subset=PLAN_KEY_COLS, keep="first")
                )
                self.scoring_candidate_steps_by_run_key[run_key] = ScoringCandidateSteps(physical, membership)
            else:
                logging.getLogger(__name__).debug(
                    "Factored candidates unavailable: scenario=%s step=%s "
                    "unique_physical=%s unique_members=%s physical_rows=%d membership_rows=%d",
                    source.scenario, self.iteration, unique_physical, unique_members,
                    physical.height, plan_members.height,
                )
        candidate_steps = (
            plan_rows.lazy().drop(DEMAND_UNIT_COLS + ["country", "csp"])
            .join(units.lazy(), on="home_zone_id", how="inner")
            .join(eligible.lazy(), on=["country", "activity_profile_id", "activity_seq_id", "time_seq_id"])
        )
        # Validate eligibility on narrow keys, without materializing demand-unit
        # step rows or hashing the same content for every compatible person.
        attached_ids = candidate_steps.select(CANDIDATE_ID_COLS).unique().collect(engine="streaming")
        missing = shared_candidates.select(CANDIDATE_ID_COLS).unique().join(
            attached_ids, on=CANDIDATE_ID_COLS, how="anti"
        )
        candidate_steps = (
            candidate_steps
            .select(CandidatePlanStepsAsset.STRUCTURAL_COLUMNS + CandidatePlanStepsAsset.RETENTION_COLUMNS)
        )
        # If each physical step is unique before attachment, the joins above
        # cannot duplicate a unit/plan/step: units have one row per demand unit
        # and eligibility has one row per profile and schedule. Check the
        # smaller physical table instead of deduplicating every attached row.
        physical_keys = PLAN_COLUMNS + ["home_zone_id", "seq_step_index"]
        unique_attachment = (
            getattr(self, "compact", False)
            and plan_rows.select(physical_keys).n_unique() == plan_rows.height
            and units.select(DEMAND_UNIT_COLS).n_unique() == units.height
        )
        if not unique_attachment:
            candidate_steps = candidate_steps.unique(
                subset=CandidatePlanStepsAsset.DEDUPE_COLUMNS, keep="first"
            )
        if missing.height:
            raise ValueError(
                f"{missing.height} complete shared plans disappeared during attachment "
                f"to scenario '{source.scenario}'."
            )
        if getattr(self, "compact", False):
            # Different reference schedules can become the same modeled time
            # sequence. Use the reference retained by the time catalogue, as
            # later behavioral restrictions also use that catalogue.
            if getattr(self, "build_catalogue", True):
                self.compact_candidate_steps_by_run_key[run_key] = (
                    plan_rows.drop("reference_time_seq_id").join(
                        schedule_steps.select(
                            "activity_seq_id", "time_seq_id", "reference_time_seq_id"
                        ).unique(), on=["activity_seq_id", "time_seq_id"], how="left",
                    ).select(
                        CandidatePlanStepsAsset.STRUCTURAL_COLUMNS + CandidatePlanStepsAsset.RETENTION_COLUMNS
                        + CANDIDATE_ID_COLS + [
                            "reference_time_seq_id", "destination_sequence_key_1", "destination_sequence_key_2",
                            "mode_sequence_key_1", "mode_sequence_key_2",
                        ]
                    )
                )
            self.unordered_candidate_steps_by_run_key[run_key] = candidate_steps
            return candidate_steps.sort(PLAN_KEY_COLS + ["seq_step_index"])
        # The eager path remains available to standalone callers and tests.
        return candidate_steps.sort(PLAN_KEY_COLS + ["seq_step_index"]).collect(engine="streaming")

    def _limit_candidate_pool(self, candidates: pl.DataFrame) -> pl.DataFrame:
        """Keep active plans first, then the most recently generated candidates."""
        if candidates.height == 0:
            return candidates
        candidate_summary = (
            candidates
            .group_by(CANDIDATE_ID_COLS)
            .agg(
                last_seen_iteration=pl.col("last_seen_iteration").max(),
                last_active_iteration=pl.col("last_active_iteration").max(),
            )
        )
        active_candidates = candidate_summary.filter(
            pl.col("last_active_iteration") == self.iteration
        ).sort(
            ["last_active_iteration", "last_seen_iteration"] + CANDIDATE_ID_COLS,
            descending=[True, True, False, False],
        )
        if active_candidates.height > self.max_shared_candidates:
            raise ValueError(
                "max_shared_candidates is smaller than the number of plans active "
                f"in iteration {self.iteration}: {active_candidates.height} active plans, "
                f"limit {self.max_shared_candidates}."
            )
        recent_candidates = (
            candidate_summary
            .filter(
                pl.col("last_active_iteration").fill_null(0) != self.iteration
            )
            .sort(["last_seen_iteration"] + CANDIDATE_ID_COLS, descending=[True, False, False])
            .head(max(0, self.max_shared_candidates - active_candidates.height))
        )
        retained_ids = pl.concat(
            [active_candidates, recent_candidates],
            how="vertical_relaxed",
        ).select(CANDIDATE_ID_COLS)
        return candidates.join(retained_ids, on=CANDIDATE_ID_COLS, how="inner")

    @staticmethod
    def _empty_candidate_content() -> pl.DataFrame:
        """Return an empty frame matching the candidate-content schema."""
        return pl.DataFrame(
            schema={
                **CandidatePlanStepsAsset.REQUIRED_SCHEMA,
                **{
                    column: pl.UInt64
                    for column in (
                        *CANDIDATE_ID_COLS,
                        "destination_sequence_key_1",
                        "destination_sequence_key_2",
                        "mode_sequence_key_1",
                        "mode_sequence_key_2",
                    )
                },
            }
        )
