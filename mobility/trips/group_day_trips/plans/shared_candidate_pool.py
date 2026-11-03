from __future__ import annotations


import hashlib
import logging
from dataclasses import dataclass
from typing import Any

import polars as pl

from mobility.trips.group_day_trips.core.performance_trace import simulation_step, trace_simulation_step

from mobility.runtime.assets.asset import Asset
from mobility.runtime.assets.in_memory_asset import InMemoryAsset

from .candidate_plan_steps import CandidatePlanStepsAsset
from .demand_subgroups import DEMAND_UNIT_COLS
from .plan_ids import PLAN_KEY_COLS
from .stable_key_index import StableKeyIndex
from .time_sequences import assign_time_sequence_ids
from .activity_profiles import get_schedules_by_activity_profile, get_survey_schedule_activities
from ..core.multi_scenario import CANDIDATE_ID_COLS, add_candidate_ids


def _frame_hash(frame: pl.DataFrame | None) -> str | None:
    """Return a compact value hash for transient Polars inputs."""
    if frame is None:
        return None
    digest = hashlib.sha256()
    schema = tuple((column, str(dtype)) for column, dtype in frame.schema.items())
    digest.update(repr(schema).encode())
    digest.update(frame.hash_rows(seed=0).to_numpy().tobytes())
    return digest.hexdigest()


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
    candidate_ids: tuple[tuple[int, int], ...]
    time_sequence_steps_by_run_key: dict[int, pl.DataFrame]
    compact_candidate_steps_by_run_key: dict[int, pl.DataFrame] | None = None


class SharedCandidatePoolAsset(InMemoryAsset):
    """Merge complete plans from every run context and translate local ids."""

    def __init__(
        self,
        *,
        sources: list[ScenarioCandidateSource],
        iteration: int,
        max_shared_candidates: int | None = None,
        previous_candidates_by_run_key: dict[int, pl.DataFrame | None] | None = None,
        previous_destination_indices: dict[int, pl.DataFrame | None] | None = None,
        previous_mode_indices: dict[int, pl.DataFrame | None] | None = None,
        previous_time_sequence_steps: dict[int, pl.DataFrame] | None = None,
        compact: bool = False,
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
        self.previous_candidates_by_run_key = previous_candidates_by_run_key or {}
        self.previous_destination_indices = previous_destination_indices or {}
        self.previous_mode_indices = previous_mode_indices or {}
        self.previous_time_sequence_steps = previous_time_sequence_steps or {}
        self.time_sequence_steps_by_run_key: dict[int, pl.DataFrame] = {}
        self.compact = compact
        self.compact_candidate_steps_by_run_key: dict[int, pl.DataFrame] = {}
        super().__init__(
            {
                "version": 10,
                "compact": compact,
                "iteration": iteration,
                "time_sequence_hashes": {
                    run_key: _frame_hash(frame)
                    for run_key, frame in self.previous_time_sequence_steps.items()
                },
                "max_shared_candidates": max_shared_candidates,
                "previous_candidate_hashes": {
                    run_key: _frame_hash(frame)
                    for run_key, frame in self.previous_candidates_by_run_key.items()
                },
                "previous_destination_index_hashes": {
                    run_key: _frame_hash(frame)
                    for run_key, frame in self.previous_destination_indices.items()
                },
                "previous_mode_index_hashes": {
                    run_key: _frame_hash(frame)
                    for run_key, frame in self.previous_mode_indices.items()
                },
                "run_sources": [
                    {
                        "run_key": source.run_key,
                        "scenario": source.scenario,
                        "sensitivity_case": source.run.sensitivity_case_id,
                        "replication": source.run.replication,
                        "run_context_hash": source.run.run_context.inputs_hash,
                        "previous_state": source.previous_state,
                        "destination_sequences": source.destination_sequences,
                        "mode_sequences": source.mode_sequences,
                    }
                    for source in sources
                ],
            }
        )

    @trace_simulation_step("shared pool generation and attachment")
    def create_and_get_asset(self) -> SharedCandidatePoolResult:
        """Collect candidate plans and merge them by canonical content."""
        candidate_frames: list[pl.DataFrame] = []
        generated_mode_sequences = {source.run_key: 0 for source in self.sources}
        empty_candidates_by_run_key: dict[int, pl.DataFrame] = {}
        active_candidate_steps_by_run_key: dict[int, pl.DataFrame] = {}
        previous_index_by_run_key: dict[int, tuple[pl.DataFrame | None, pl.DataFrame | None]] = {}
        active_candidate_id_frames: list[pl.DataFrame] = []

        for source in self.sources:
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

                previous_candidates = self.previous_candidates_by_run_key.get(
                    run_key,
                    previous_state.candidate_plan_steps,
                )
                if previous_candidates is not None:
                    empty_candidates_by_run_key[run_key] = previous_candidates.head(0)
                    active_source = previous_state.candidate_plan_steps
                    if active_source is None:
                        active_source = previous_candidates
                    active_candidate_steps_by_run_key[run_key] = (
                        active_source
                        .join(
                            previous_state.current_plans
                            .filter(pl.col("mode_seq_id") != 0)
                            .select(PLAN_KEY_COLS),
                            on=PLAN_KEY_COLS,
                            how="inner",
                        )
                        .select(
                            CandidatePlanStepsAsset.STRUCTURAL_COLUMNS
                            + CandidatePlanStepsAsset.RETENTION_COLUMNS
                        )
                    )
                    active_steps = active_candidate_steps_by_run_key[run_key]
                    if active_steps.height:
                        # Keep the actual occupied timings, even when the last
                        # inner pool still contains the original survey timings.
                        active_content = self._attach_sequence_content_keys(
                            add_candidate_ids(active_steps),
                            previous_state.destination_sequence_index,
                            previous_state.mode_sequence_index,
                            previous_state.schedule_steps,
                        )
                        candidate_frames.append(active_content)
                        active_candidate_id_frames.append(
                            active_content.select(CANDIDATE_ID_COLS).unique()
                        )
                if previous_candidates is not None and previous_candidates.height:
                    previous_content = self._attach_sequence_content_keys(
                        add_candidate_ids(previous_candidates),
                        previous_destination_index,
                        previous_mode_index,
                        self.previous_time_sequence_steps.get(run_key, previous_state.schedule_steps),
                    )
                    candidate_frames.append(previous_content)

                    if previous_state.current_plans.height:
                        active = (
                            previous_content
                            .join(
                                previous_state.current_plans.select(PLAN_KEY_COLS),
                                on=PLAN_KEY_COLS,
                                how="inner",
                            )
                            .select(CANDIDATE_ID_COLS)
                            .unique()
                        )
                        active_candidate_id_frames.append(active)

            with simulation_step("generate scenario candidates", scenario=scenario):
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
                    ).collect(engine="streaming")
                if logging.getLogger().isEnabledFor(logging.DEBUG):
                    logging.debug(
                        "Generated candidate steps | scenario=%s | step_rows=%d | "
                        "plan_variants=%d | demand_units=%d | home_zones=%d",
                        scenario,
                        generated.height,
                        generated.select(PLAN_KEY_COLS).unique().height,
                        generated.select(DEMAND_UNIT_COLS).unique().height,
                        generated
                        .filter(pl.col("seq_step_index") == 1)
                        .select(pl.col("from").n_unique())
                        .item(),
                    )
                generated = generated.with_columns(
                    first_seen_iteration=pl.lit(self.iteration, dtype=pl.UInt16),
                    last_seen_iteration=pl.lit(self.iteration, dtype=pl.UInt16),
                    last_active_iteration=pl.lit(None, dtype=pl.UInt16),
                )
                empty_candidates_by_run_key.setdefault(run_key, generated.head(0))
                if generated.height:
                    generated = add_candidate_ids(generated)
                    generated = self._attach_sequence_content_keys(
                        generated,
                        source.destination_sequences.get_index(),
                        source.mode_sequences.get_index(),
                        previous_state.schedule_steps,
                    )
                    candidate_frames.append(generated)
                generated_mode_sequences[run_key] += int(
                    source.mode_sequences.generated_mode_sequences
                )

        if candidate_frames:
            shared_candidates = self._merge_candidate_content(candidate_frames)
        else:
            shared_candidates = self._empty_candidate_content()

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

        if self.max_shared_candidates is not None:
            shared_candidates = self._limit_candidate_pool(shared_candidates)

        candidate_steps_by_run_key: dict[int, pl.DataFrame | pl.LazyFrame] = {}
        destination_indices: dict[int, pl.DataFrame] = {}
        mode_indices: dict[int, pl.DataFrame] = {}

        for source in self.sources:
            scenario = source.scenario
            run_key = source.run_key
            self.time_sequence_steps_by_run_key[run_key] = self.previous_time_sequence_steps.get(
                run_key, source.previous_state.get().schedule_steps
            )
            previous_destination_index, previous_mode_index = previous_index_by_run_key[run_key]
            destination_key_cols = [
                "destination_sequence_key_1",
                "destination_sequence_key_2",
            ]
            destination_index_rows = shared_candidates.select(destination_key_cols).unique()
            _, destination_index = StableKeyIndex(
                key_cols=destination_key_cols,
                index_col="dest_seq_id",
                first_new_id=1,
            ).extend(
                destination_index_rows,
                previous_index=previous_destination_index,
            )
            mode_key_cols = ["mode_sequence_key_1", "mode_sequence_key_2"]
            mode_index_rows = shared_candidates.select(mode_key_cols).unique()
            _, mode_index = StableKeyIndex(
                key_cols=mode_key_cols,
                index_col="mode_seq_id",
                first_new_id=1,
            ).extend(
                mode_index_rows,
                previous_index=previous_mode_index,
            )
            destination_indices[run_key] = destination_index
            mode_indices[run_key] = mode_index

            if shared_candidates.height == 0:
                candidate_steps_by_run_key[run_key] = empty_candidates_by_run_key[run_key]
            else:
                candidate_steps = self._build_scenario_candidate_steps(
                    shared_candidates=shared_candidates,
                    destination_index=destination_index,
                    mode_index=mode_index,
                    source=source,
                )
                if logging.getLogger().isEnabledFor(logging.DEBUG):
                    debug_steps = candidate_steps.collect(engine="streaming") if isinstance(candidate_steps, pl.LazyFrame) else candidate_steps
                    logging.debug(
                        "Shared pool candidate attachment | scenario=%s | "
                        "shared_candidate_ids=%d | step_rows=%d | plan_variants=%d | "
                        "demand_units=%d",
                        scenario,
                        shared_candidates.select(CANDIDATE_ID_COLS).unique().height,
                        debug_steps.height,
                        debug_steps.select(PLAN_KEY_COLS).unique().height,
                        debug_steps.select(DEMAND_UNIT_COLS).unique().height,
                    )
                active_candidate_steps = active_candidate_steps_by_run_key.get(run_key)
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

        candidate_ids = tuple(
            shared_candidates.select(CANDIDATE_ID_COLS)
            .unique()
            .sort(CANDIDATE_ID_COLS)
            .iter_rows()
        )
        return SharedCandidatePoolResult(
            candidate_plan_steps_by_run_key=candidate_steps_by_run_key,
            destination_sequence_indices=destination_indices,
            mode_sequence_indices=mode_indices,
            generated_mode_sequences_by_run_key=generated_mode_sequences,
            candidate_ids=candidate_ids,
            time_sequence_steps_by_run_key=self.time_sequence_steps_by_run_key,
            compact_candidate_steps_by_run_key=self.compact_candidate_steps_by_run_key,
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
        plan_rows, schedule_steps, _ = assign_time_sequence_ids(plan_rows, schedule_steps)
        self.time_sequence_steps_by_run_key[run_key] = schedule_steps
        plan_rows = plan_rows.with_columns(
            home_zone_id=pl.col("from").sort_by("seq_step_index").first().over(CANDIDATE_ID_COLS)
        )

        # Check activity eligibility once per profile, then attach the original
        # candidate rows to the represented demand units in that home zone.
        units = previous_state.demand_groups.select(
            DEMAND_UNIT_COLS + ["country", "csp", "home_zone_id", "allowed_activities"]
        ).with_columns(activity_profile_id=pl.col("allowed_activities").list.join("|"))
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
            .unique(subset=CandidatePlanStepsAsset.DEDUPE_COLUMNS, keep="first")
            .sort(PLAN_KEY_COLS + ["seq_step_index"])
        )
        if missing.height:
            raise ValueError(
                f"{missing.height} complete shared plans disappeared during attachment "
                f"to scenario '{source.scenario}'."
            )
        if getattr(self, "compact", False):
            self.compact_candidate_steps_by_run_key[run_key] = plan_rows.select(
                CandidatePlanStepsAsset.STRUCTURAL_COLUMNS + CandidatePlanStepsAsset.RETENTION_COLUMNS
            )
            return candidate_steps
        # The eager path remains available to standalone callers and tests.
        return candidate_steps.collect(engine="streaming")

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
