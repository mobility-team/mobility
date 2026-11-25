"""Keep modeled schedules distinct without losing complete shared plans."""

from dataclasses import replace
import logging
import random
import weakref
from pathlib import Path
from types import SimpleNamespace

import polars as pl
import pytest
from polars.testing import assert_frame_equal

import mobility.trips.group_day_trips.plans.time_sequences as time_sequences
import mobility.trips.group_day_trips.plans.shared_candidate_pool as shared_pool_module
from mobility.trips.group_day_trips.core.multi_scenario import CANDIDATE_ID_COLS, add_candidate_ids, candidate_content_ids
from mobility.trips.group_day_trips.plans.shared_candidate_pool import SharedCandidatePool, ScenarioCandidateSource
from mobility.trips.group_day_trips.plans.candidate_plan_steps import CandidatePlanStepsAsset
from mobility.runtime.assets.in_memory_asset import InMemoryAsset
from mobility.runtime.temporary import TemporaryDiskLimitExceeded
from mobility.trips.group_day_trips.plans.time_sequences import TimeSequenceIndexCache, assign_time_sequence_ids
from mobility.trips.group_day_trips.plans.plan_ids import PLAN_KEY_COLS, add_plan_id
from mobility.trips.group_day_trips.plans.plan_updater import PlanUpdater
from mobility.trips.group_day_trips.plans.plan_catalogue import PlanCatalogue
from mobility.trips.group_day_trips.plans.compact_candidate_steps import (
    CompactCandidateSteps, SectionedPhysicalSteps, ScoringCandidateSteps,
    MEMBERSHIP_COLUMNS, PLAN_COLUMNS, STEP_COLUMNS,
)
from mobility.trips.group_day_trips.plans.scoring_contexts import ScoringContexts, ScoringLookup, ScoringMemoryBudget
from mobility.trips.group_day_trips.core.parameters import GroupDayTripsParameters, BehaviorChangeScope
from mobility.trips.group_day_trips.core.run_state import RunState
from mobility.trips.group_day_trips.iterations.iteration_assets import (
    CandidateCheckpointAsset, InitialIterationStateAsset, IterationStateAsset,
)


def _schedule():
    return pl.DataFrame({
        "activity_seq_id": [1, 1], "time_seq_id": [1, 1], "seq_step_index": [1, 2],
        "activity": ["work", "home"], "is_anchor": [True, True], "step_count": [2, 2],
        "departure_time": [8.0, 17.0], "arrival_time": [9.0, 18.0],
        "next_departure_time": [17.0, 24.0], "duration_per_pers": [8.0, 6.0],
    }).with_columns(
        pl.col("activity_seq_id", "time_seq_id").cast(pl.UInt32),
        pl.col("seq_step_index", "step_count").cast(pl.UInt8),
    )


def _plan_steps():
    return _schedule().drop("is_anchor", "step_count").with_columns(
        demand_group_id=pl.lit(1, dtype=pl.UInt32), demand_subgroup_id=pl.lit(0, dtype=pl.UInt32),
        dest_seq_id=pl.lit(1, dtype=pl.UInt32), mode_seq_id=pl.lit(1, dtype=pl.UInt32),
        country=pl.lit("fr"), csp=pl.lit("worker"), mode=pl.lit("car"),
        iteration=pl.lit(1, dtype=pl.UInt16), first_seen_iteration=pl.lit(1, dtype=pl.UInt16),
        last_seen_iteration=pl.lit(1, dtype=pl.UInt16), last_active_iteration=pl.lit(None, dtype=pl.UInt16),
    ).with_columns(pl.Series("from", [1, 2], dtype=pl.Int32), pl.Series("to", [2, 1], dtype=pl.Int32))


@pytest.mark.parametrize("storage", ["expanded", "lazy", "compact", "sectioned", "mapped"])
def test_candidate_checkpoint_restores_candidate_history(tmp_path, storage):
    rows = _plan_steps()
    if storage == "expanded":
        candidates = rows
    elif storage == "lazy":
        candidates = rows.lazy()
    else:
        steps = rows.select(STEP_COLUMNS).unique()
        if storage == "sectioned":
            steps = SectionedPhysicalSteps.from_steps(steps, {"work"})
        membership = rows.select(MEMBERSHIP_COLUMNS).unique()
        unit_map = None
        if storage == "mapped":
            unit_map = membership.select("demand_group_id", "demand_subgroup_id").with_columns(
                _score_context_id=pl.col("demand_group_id")
            )
        candidates = CompactCandidateSteps(steps, membership, tuple(rows.columns), unit_map)
    args = dict(run_key="test", is_weekday=True, iteration=1, base_folder=tmp_path)
    saved = CandidateCheckpointAsset(**args, candidate_plan_steps=candidates)
    saved.get()
    restored = CandidateCheckpointAsset(**args).get()
    actual = restored.lazy().collect() if isinstance(restored, CompactCandidateSteps) else restored
    assert_frame_equal(actual.sort(CandidatePlanStepsAsset.DEDUPE_COLUMNS),
                       rows.sort(CandidatePlanStepsAsset.DEDUPE_COLUMNS))
    if storage in {"compact", "sectioned", "mapped"}:
        assert pl.read_parquet(saved.cache_path["expanded"]).is_empty()
        assert isinstance(restored, CompactCandidateSteps)
        assert (restored.unit_map is not None) == (storage == "mapped")
    saved.cache_path["membership"].unlink()
    assert CandidateCheckpointAsset(**args).assets_missing()


def test_physical_candidate_ids_match_ids_repeated_across_contexts():
    first = _plan_steps()
    second = first.with_columns(mode_seq_id=pl.lit(2, dtype=pl.UInt32), mode=pl.lit("walk"))
    rows = pl.concat([first, second, first.with_columns(demand_group_id=pl.lit(2, dtype=pl.UInt32)),
                      second.with_columns(demand_group_id=pl.lit(2, dtype=pl.UInt32))])
    expanded = add_candidate_ids(rows).select(PLAN_KEY_COLS + CANDIDATE_ID_COLS).unique()
    physical = rows.select(STEP_COLUMNS).unique()
    shared = (rows.select(PLAN_KEY_COLS).unique()
              .join(candidate_content_ids(physical, PLAN_COLUMNS), on=PLAN_COLUMNS))
    assert_frame_equal(shared.sort(PLAN_KEY_COLS), expanded.sort(PLAN_KEY_COLS), check_exact=True)


def test_time_index_reuse_matches_rebuilding_and_rejects_a_different_catalogue(monkeypatch):
    original = time_sequences.add_ordered_key_hashes
    catalogue_hashes = []

    def record_hashes(*args, **kwargs):
        if kwargs["group_cols"] == ["time_seq_id"]:
            catalogue_hashes.append(True)
        return original(*args, **kwargs)

    monkeypatch.setattr(time_sequences, "add_ordered_key_hashes", record_hashes)
    cache = TimeSequenceIndexCache()
    changed_steps = _plan_steps().with_columns(pl.col("arrival_time") + 0.5)
    steps, catalogue, _ = assign_time_sequence_ids(changed_steps, _schedule(), index_cache=cache)
    assert catalogue["time_seq_id"].n_unique() == 2
    assert len(catalogue_hashes) == 1
    reused = assign_time_sequence_ids(steps, catalogue, index_cache=cache)
    assert len(catalogue_hashes) == 1
    rebuilt = assign_time_sequence_ids(steps, catalogue)
    for actual, expected in zip(reused, rebuilt):
        assert_frame_equal(actual, expected)
    assert len(catalogue_hashes) == 2
    # A different object, even with the same values, must rebuild its index.
    assign_time_sequence_ids(steps, catalogue.clone(), index_cache=cache)
    assert len(catalogue_hashes) == 3


def test_timing_hashes_match_materialized_seconds(monkeypatch):
    original = time_sequences.add_ordered_key_hashes
    checked = []
    columns = ["departure_time", "arrival_time", "next_departure_time", "duration_per_pers"]

    def compare_hashes(rows, **kwargs):
        actual = original(rows, **kwargs)
        seconds = rows.with_columns([
            (pl.col(column).cast(pl.Float64) * 3600.0).round(0).cast(pl.Int64)
            .alias(f"_{column}_seconds") for column in columns
        ])
        reference_content = pl.struct(
            pl.col("activity").cast(pl.String),
            *[pl.col(f"_{column}_seconds") for column in columns],
        )
        reference = original(seconds, **{**kwargs, "step_content": reference_content})
        assert_frame_equal(actual.sort(kwargs["group_cols"]), reference.sort(kwargs["group_cols"]))
        checked.append(True)
        return actual

    monkeypatch.setattr(time_sequences, "add_ordered_key_hashes", compare_hashes)
    assign_time_sequence_ids(_plan_steps(), _schedule())
    assert len(checked) == 2


def test_new_timed_schedule_preserves_existing_sparse_ids():
    """An existing ID gap must not make an old schedule look newly created."""
    existing = _schedule().with_columns(
        time_seq_id=pl.lit(10, dtype=pl.UInt32),
        arrival_time=pl.col("arrival_time") + 0.25,
    )
    catalogue = pl.concat([_schedule(), existing])
    incoming = pl.concat([
        _plan_steps(),
        _plan_steps().with_columns(
            demand_group_id=pl.lit(2, dtype=pl.UInt32),
            arrival_time=pl.col("arrival_time") + 0.5,
        ),
    ])
    updated, result_catalogue, _ = assign_time_sequence_ids(incoming, catalogue)
    assert result_catalogue["time_seq_id"].unique().sort().to_list() == [1, 10, 11]
    assert updated.group_by("demand_group_id").agg(pl.col("time_seq_id").first()).sort(
        "demand_group_id"
    )["time_seq_id"].to_list() == [1, 11]
    assert_frame_equal(
        result_catalogue.filter(pl.col("time_seq_id") == 10).drop("reference_time_seq_id"),
        existing,
    )


def test_current_plan_coverage_accepts_plan_keys_and_rejects_missing_active_plan():
    current = _plan_steps().select(PLAN_KEY_COLS).unique()
    updater = PlanUpdater()
    updater._assert_current_plans_covered_by_possible_plan_steps(current, current.lazy(), 1)
    with pytest.raises(ValueError, match="Current non-stay-home plans are missing"):
        updater._assert_current_plans_covered_by_possible_plan_steps(current, current.head(0).lazy(), 1)
    stay_home = current.with_columns(mode_seq_id=pl.lit(0, dtype=pl.UInt32))
    updater._assert_current_plans_covered_by_possible_plan_steps(stay_home, current.head(0).lazy(), 1)


def test_shared_generation_keeps_plan_content_without_expanding_demand_units(caplog):
    """Compact generation retains the same complete plans as unit expansion."""
    steps = _plan_steps()
    mode_rows = steps.select(
        "activity_seq_id", "time_seq_id", "dest_seq_id", "mode_seq_id",
        "seq_step_index", "mode", "iteration",
    ).with_columns(home_zone_id=pl.lit(1), utility_profile_id=pl.lit(0))
    destination_rows = steps.select(
        "activity_seq_id", "time_seq_id", "dest_seq_id", "seq_step_index", "from", "to",
    ).with_columns(home_zone_id=pl.lit(1))
    units = pl.DataFrame({
        "demand_group_id": [1, 2, 3], "demand_subgroup_id": [0, 0, 0],
        "country": ["fr"] * 3, "csp": ["worker"] * 3,
        "home_zone_id": [1] * 3, "allowed_activities": [["work"]] * 3,
    }).with_columns(pl.col("demand_group_id", "demand_subgroup_id").cast(pl.UInt32))
    profiles = units.select("demand_group_id", "demand_subgroup_id").with_columns(
        utility_profile_id=pl.lit(0),
    )
    arguments = dict(
        mode_rows=mode_rows, destination_rows=destination_rows,
        survey_plan_steps=_schedule().select(
            "activity_seq_id", "time_seq_id", "seq_step_index", "activity",
            "duration_per_pers", "departure_time", "arrival_time", "next_departure_time",
        ),
        demand_groups=units, profile_assignments=profiles,
        candidate_pruner=None, stage="test",
    )
    expanded = CandidatePlanStepsAsset._attach_candidates_to_demand_units(**arguments).collect()
    with caplog.at_level(logging.INFO):
        compact = CandidatePlanStepsAsset._attach_candidates_to_demand_units(
            **arguments, compact_for_shared_pool=True,
        ).collect()
    with caplog.at_level(logging.DEBUG):
        debug_compact = CandidatePlanStepsAsset._attach_candidates_to_demand_units(
            **arguments, compact_for_shared_pool=True,
        ).collect()
    assert_frame_equal(compact.sort(compact.columns), debug_compact.sort(compact.columns))
    assert "Plan attachment contexts" in caplog.text
    assert expanded.height == 6
    assert compact.height == 2
    expanded_ids = add_candidate_ids(expanded).select(CANDIDATE_ID_COLS).unique()
    compact_ids = add_candidate_ids(compact).select(CANDIDATE_ID_COLS).unique()
    assert_frame_equal(expanded_ids.sort(CANDIDATE_ID_COLS), compact_ids.sort(CANDIDATE_ID_COLS))


def test_merging_candidate_chunks_preserves_complete_pool_and_history():
    first = add_candidate_ids(_plan_steps()).with_columns(
        destination_sequence_key_1=pl.lit(21, dtype=pl.UInt64),
        destination_sequence_key_2=pl.lit(22, dtype=pl.UInt64),
        mode_sequence_key_1=pl.lit(31, dtype=pl.UInt64),
        mode_sequence_key_2=pl.lit(32, dtype=pl.UInt64),
        reference_time_seq_id=pl.lit(1, dtype=pl.UInt32),
    )
    later = first.with_columns(
        first_seen_iteration=pl.lit(2, dtype=pl.UInt16),
        last_seen_iteration=pl.lit(3, dtype=pl.UInt16),
        last_active_iteration=pl.lit(3, dtype=pl.UInt16),
    )
    pool = object.__new__(SharedCandidatePool)
    expected = pool._merge_candidate_content([first, later, first])
    chunked = pool._merge_candidate_content([
        pool._merge_candidate_content([first, later]), first,
    ])
    assert_frame_equal(expected, chunked)
    assert expected.height == first.height
    assert expected["first_seen_iteration"].to_list() == [1, 1]
    assert expected["last_seen_iteration"].to_list() == [3, 3]
    assert expected["last_active_iteration"].to_list() == [3, 3]
    assert_frame_equal(
        expected.select("seq_step_index", "activity", "from", "to", "mode").sort("seq_step_index"),
        first.select("seq_step_index", "activity", "from", "to", "mode").sort("seq_step_index"),
    )


def test_compact_outer_candidates_preserve_demand_membership_and_history():
    steps = _plan_steps()
    expanded = pl.concat([
        steps.with_columns(
            demand_group_id=pl.lit(index, dtype=pl.UInt32),
            last_active_iteration=pl.lit(index % 3, dtype=pl.UInt16),
        )
        for index in range(30)
    ])
    compact = CompactCandidateSteps.from_frame(expanded)
    assert isinstance(compact, CompactCandidateSteps)
    assert compact.steps.height == 2
    assert compact.membership.height == 30
    assert compact.estimated_size() < expanded.estimated_size()
    assert_frame_equal(
        compact.lazy().collect().sort(CandidatePlanStepsAsset.DEDUPE_COLUMNS),
        expanded.sort(CandidatePlanStepsAsset.DEDUPE_COLUMNS),
        check_exact=True,
    )


def test_compact_outer_candidates_keep_inconsistent_rows_without_loss():
    first = _plan_steps()
    different = first.with_columns(
        demand_group_id=pl.lit(2, dtype=pl.UInt32), mode=pl.lit("walk"),
    )
    inconsistent = pl.concat([first, different])
    assert CompactCandidateSteps.from_frame(inconsistent) is inconsistent
    assert CompactCandidateSteps.from_frame(inconsistent, prefer_compact=True) is inconsistent

    incomplete = pl.concat([first, first.head(1).with_columns(demand_group_id=pl.lit(2, dtype=pl.UInt32))])
    assert CompactCandidateSteps.from_frame(incomplete) is incomplete
    assert CompactCandidateSteps.from_frame(incomplete, prefer_compact=True) is incomplete


def test_physical_sections_share_equal_parts_without_losing_plan_steps():
    first = _plan_steps().select(
        PLAN_COLUMNS + ["seq_step_index", "activity", "from", "to", "mode", "duration_per_pers",
                        "departure_time", "arrival_time", "next_departure_time"]
    )
    second = first.with_columns(
        mode_seq_id=pl.lit(2, dtype=pl.UInt32),
        mode=pl.when(pl.col("activity") == "home").then(pl.lit("walk")).otherwise(pl.col("mode")),
    )
    physical = pl.concat([first, second]).sort(PLAN_COLUMNS + ["seq_step_index"])
    sectioned = SectionedPhysicalSteps.from_steps(physical, {"work"})
    assert sectioned.sections.height == 3
    assert sectioned.references.height == 4
    assert_frame_equal(sectioned.lazy().collect().sort(PLAN_COLUMNS + ["seq_step_index"]), physical, check_exact=True)
    selected = physical.filter(pl.col("mode_seq_id") == 2)
    assert_frame_equal(
        sectioned.lazy(selected.lazy()).collect().sort(PLAN_COLUMNS + ["seq_step_index"]), selected, check_exact=True,
    )


def test_large_candidate_history_uses_sections_when_they_save_space():
    variants = pl.DataFrame({"mode_seq_id": range(25_000)}, schema={"mode_seq_id": pl.UInt32})
    expanded = _plan_steps().drop("mode_seq_id").join(variants, how="cross")
    compact = CompactCandidateSteps.from_frame(expanded, prefer_compact=True, rigid_activities={"work"})
    assert isinstance(compact, CompactCandidateSteps)
    assert isinstance(compact.steps, SectionedPhysicalSteps)
    assert compact.steps.sections.height == 2
    assert_frame_equal(
        compact.lazy().collect().sort(CandidatePlanStepsAsset.DEDUPE_COLUMNS),
        expanded.sort(CandidatePlanStepsAsset.DEDUPE_COLUMNS), check_exact=True,
    )


@pytest.mark.parametrize("shared_context", [False, True])
@pytest.mark.parametrize("sectioned", [False, True])
def test_compact_outer_candidates_survive_state_spill_and_restore(tmp_path, monkeypatch, shared_context, sectioned):
    expanded = pl.concat([
        _plan_steps().with_columns(demand_group_id=pl.lit(index, dtype=pl.UInt32))
        for index in range(30)
    ])
    compact = CompactCandidateSteps.from_frame(expanded)
    assert isinstance(compact, CompactCandidateSteps)
    if sectioned:
        sections = SectionedPhysicalSteps.from_steps(compact.steps, {"work"})
        assert_frame_equal(
            sections.lazy().collect().sort(PLAN_COLUMNS + ["seq_step_index"]),
            compact.steps.sort(PLAN_COLUMNS + ["seq_step_index"]), check_exact=True,
        )
        compact = replace(compact, steps=sections)
    if shared_context:
        compact = CompactCandidateSteps(
            steps=compact.steps,
            membership=compact.membership.filter(pl.col("demand_group_id") == 0),
            columns=compact.columns,
            unit_map=expanded.select("demand_group_id", "demand_subgroup_id").unique().with_columns(
                _score_context_id=pl.lit(0, dtype=pl.UInt32)
            ),
        )
    empty = pl.DataFrame()
    rng = random.Random(732)
    rng.random()
    indices = pl.DataFrame({"key": [2, 1], "id": [17, 29]})
    initial = RunState(
        survey_plans=empty, survey_plan_steps=_schedule(), demand_groups=empty,
        activity_dur=empty, home_night_dur=empty, stay_home_plan=empty,
        opportunities=empty, current_plans=empty, candidate_plan_steps=compact,
        plan_id_index=indices, destination_saturation=empty, costs=empty, start_step=2,
        rng_state=rng.getstate(), destination_sequence_index=indices,
        mode_sequence_index=indices, time_sequence_steps=_schedule(),
    )
    asset = object.__new__(IterationStateAsset)
    asset.iteration = 1
    asset.initial_state = SimpleNamespace(get=lambda: initial)
    asset.transport_costs = SimpleNamespace(get_costs_by_od=lambda metrics: empty)
    asset.spill_execution_value(initial, tmp_path / "state")
    with pytest.raises(FileExistsError):
        asset.spill_execution_value(initial, tmp_path / "state")
    assert (tmp_path / "state" / "candidate_steps.parquet").exists() is not sectioned
    assert (tmp_path / "state" / "candidate_sections.parquet").exists() is sectioned
    assert (tmp_path / "state" / "candidate_section_references.parquet").exists() is sectioned
    assert (tmp_path / "state" / "candidate_membership.parquet").exists()
    assert (tmp_path / "state" / "candidate_unit_map.parquet").exists() == shared_context
    assert not (tmp_path / "state" / "candidate_plan_steps.parquet").exists()
    restored = asset.load_execution_value(tmp_path / "state")
    assert isinstance(restored.candidate_plan_steps, CompactCandidateSteps)
    InitialIterationStateAsset.spill_execution_value(initial, tmp_path / "initial-state")
    restored_initial = InitialIterationStateAsset.load_execution_value(tmp_path / "initial-state")
    assert isinstance(restored_initial.candidate_plan_steps, CompactCandidateSteps)
    assert_frame_equal(restored_initial.candidate_plan_steps.lazy().collect(), compact.lazy().collect(),
                       check_exact=True)
    assert_frame_equal(restored_initial.time_sequence_steps, initial.time_sequence_steps, check_exact=True)
    assert restored.rng_state == initial.rng_state
    resumed_rng = random.Random()
    resumed_rng.setstate(restored.rng_state)
    assert [resumed_rng.random() for _ in range(5)] == [rng.random() for _ in range(5)]
    for name in ("plan_id_index", "destination_sequence_index", "mode_sequence_index", "time_sequence_steps"):
        assert_frame_equal(getattr(restored, name), getattr(initial, name), check_exact=True)
    assert restored.start_step == initial.start_step
    assert_frame_equal(
        add_candidate_ids(restored.candidate_plan_steps.lazy().collect()).select(CANDIDATE_ID_COLS).unique().sort(CANDIDATE_ID_COLS),
        add_candidate_ids(compact.lazy().collect()).select(CANDIDATE_ID_COLS).unique().sort(CANDIDATE_ID_COLS),
        check_exact=True,
    )
    assert_frame_equal(
        restored.candidate_plan_steps.lazy().collect().sort(CandidatePlanStepsAsset.DEDUPE_COLUMNS),
        expanded.sort(CandidatePlanStepsAsset.DEDUPE_COLUMNS), check_exact=True,
    )
    checkpoint = CandidatePlanStepsAsset(
        run_key="compact-test", is_weekday=True, iteration=1, base_folder=tmp_path,
        candidate_plan_steps=compact,
    ).create_and_get_asset()
    assert_frame_equal(
        checkpoint.sort(CandidatePlanStepsAsset.DEDUPE_COLUMNS),
        expanded.sort(CandidatePlanStepsAsset.DEDUPE_COLUMNS), check_exact=True,
    )
    monkeypatch.setenv("MOBILITY_PROJECT_DATA_FOLDER", str(tmp_path))
    managed = tmp_path / "temporary" / "mobility-shared-assets-test"
    managed.mkdir(parents=True)
    monkeypatch.setenv("MOBILITY_TEMPORARY_DISK_LIMIT_BYTES", "100000000")
    asset.spill_execution_value(initial, managed / "state")
    limited_restore = asset.load_execution_value(managed / "state")
    assert limited_restore.rng_state == initial.rng_state
    monkeypatch.setenv("MOBILITY_TEMPORARY_DISK_LIMIT_BYTES", "1")
    with pytest.raises(TemporaryDiskLimitExceeded):
        asset.spill_execution_value(initial, managed / "failed")
    assert not (managed / "failed.tmp").exists()
    assert not (managed / "failed").exists()
    assert (managed / "state").exists()
    assert not list(managed.glob(".failed-*"))

    # Publishing failure or cancellation must remove staging data without
    # touching an already available state spill.
    monkeypatch.delenv("MOBILITY_TEMPORARY_DISK_LIMIT_BYTES")
    for spill in (asset.spill_execution_value, InitialIterationStateAsset.spill_execution_value):
        for error in (OSError("publish failed"), KeyboardInterrupt()):
            def fail_publish(source, destination):
                raise error
            with monkeypatch.context() as patch:
                patch.setattr(Path, "rename", fail_publish)
                with pytest.raises(type(error)):
                    spill(initial, managed / "unpublished")
            assert not (managed / "unpublished").exists()
            assert not list(managed.glob(".unpublished-*"))
            assert (managed / "state" / "metadata.json").exists()


def test_catalogue_separates_plan_content_variants_and_run_history():
    """Two runs can share the same plan without sharing their history."""
    first = add_candidate_ids(_plan_steps()).with_columns(
        destination_sequence_key_1=pl.lit(21, dtype=pl.UInt64),
        destination_sequence_key_2=pl.lit(22, dtype=pl.UInt64),
        mode_sequence_key_1=pl.lit(31, dtype=pl.UInt64),
        mode_sequence_key_2=pl.lit(32, dtype=pl.UInt64),
        reference_time_seq_id=pl.lit(1, dtype=pl.UInt32),
        run_id=pl.lit(0, dtype=pl.UInt32), replication=pl.lit(0, dtype=pl.UInt32),
    )
    second = first.with_columns(
        activity_seq_id=pl.lit(2, dtype=pl.UInt32),
        reference_time_seq_id=pl.lit(2, dtype=pl.UInt32),
        run_id=pl.lit(1, dtype=pl.UInt32), replication=pl.lit(1, dtype=pl.UInt32),
        first_seen_iteration=pl.lit(3, dtype=pl.UInt16),
        last_seen_iteration=pl.lit(4, dtype=pl.UInt16),
        last_active_iteration=pl.lit(4, dtype=pl.UInt16),
    )
    catalogue = PlanCatalogue.from_candidates(pl.concat([first, second]))
    assert catalogue.plans.height == 1
    assert catalogue.steps.height == 2
    assert catalogue.variants.height == 2
    assert catalogue.membership.height == 2
    assert catalogue.membership["first_seen_iteration"].to_list() == [1, 3]
    assert catalogue.membership["last_active_iteration"].to_list() == [None, 4]
    assert catalogue.variants["reference_time_seq_id"].to_list() == [1, 2]
    assert not {"run_id", "replication", "first_seen_iteration"}.intersection(catalogue.steps.columns)
    float32_catalogue = PlanCatalogue.from_candidates(first.with_columns(
        pl.col("duration_per_pers", "departure_time", "arrival_time", "next_departure_time").cast(pl.Float32)
    ))
    assert float32_catalogue.steps.schema["duration_per_pers"] == pl.Float32

    # Batches may reuse their local variant IDs; merging must reassign them
    # while keeping both runs attached to the correct survey reference.
    merged = PlanCatalogue.merge([
        PlanCatalogue.from_candidates(second), PlanCatalogue.from_candidates(first),
    ])
    for name in ("plans", "steps", "variants", "membership"):
        assert_frame_equal(getattr(merged, name), getattr(catalogue, name))

    later = first.with_columns(
        last_seen_iteration=pl.lit(5, dtype=pl.UInt16),
        last_active_iteration=pl.lit(4, dtype=pl.UInt16),
    )
    repeated = PlanCatalogue.merge([
        PlanCatalogue.from_candidates(first), PlanCatalogue.from_candidates(later),
    ])
    assert repeated.membership.height == 1
    assert repeated.membership["first_seen_iteration"].item() == 1
    assert repeated.membership["last_seen_iteration"].item() == 5
    assert repeated.membership["last_active_iteration"].item() == 4

    left = PlanCatalogue.from_candidates(first)
    right = PlanCatalogue.from_candidates(later)
    expected = PlanCatalogue.merge([left, right])
    shared = PlanCatalogue(left.plans, left.steps, left.variants, right.membership)
    merged_shared = PlanCatalogue.merge([left, shared])
    assert merged_shared.steps is left.steps
    for name in ("plans", "steps", "variants", "membership"):
        assert_frame_equal(getattr(merged_shared, name), getattr(expected, name))


def test_catalogue_rejects_conflicting_content_for_the_same_candidate_id():
    steps = add_candidate_ids(_plan_steps()).with_columns(
        destination_sequence_key_1=pl.lit(21, dtype=pl.UInt64),
        destination_sequence_key_2=pl.lit(22, dtype=pl.UInt64),
        mode_sequence_key_1=pl.lit(31, dtype=pl.UInt64),
        mode_sequence_key_2=pl.lit(32, dtype=pl.UInt64),
        reference_time_seq_id=pl.lit(1, dtype=pl.UInt32),
        run_id=pl.lit(0, dtype=pl.UInt32), replication=pl.lit(0, dtype=pl.UInt32),
    )
    conflicting = steps.with_columns(pl.lit("walk").alias("mode"))
    with pytest.raises(ValueError, match="different physical plan steps"):
        PlanCatalogue.from_candidates(pl.concat([steps, conflicting]))
    with pytest.raises(ValueError, match="different physical plan steps"):
        PlanCatalogue.merge([
            PlanCatalogue.from_candidates(steps), PlanCatalogue.from_candidates(conflicting),
        ])


def test_changed_timings_get_stable_ids_and_keep_the_survey_reference():
    steps = _plan_steps().with_columns(arrival_time=pl.col("arrival_time") + 0.25)
    updated, catalog, _ = assign_time_sequence_ids(steps, _schedule())
    new_id = updated["time_seq_id"][0]
    assert new_id > 1
    assert catalog.filter(pl.col("time_seq_id") == new_id)["reference_time_seq_id"].unique().to_list() == [1]

    repeated, repeated_catalog, _ = assign_time_sequence_ids(updated, catalog)
    assert_frame_equal(updated.sort("seq_step_index"), repeated.sort("seq_step_index"))
    assert_frame_equal(catalog, repeated_catalog)


def test_unchanged_timings_keep_the_existing_id():
    updated, catalog, _ = assign_time_sequence_ids(_plan_steps(), _schedule())
    assert updated["time_seq_id"].unique().to_list() == [1]
    assert catalog.height == 2


def test_stored_activity_durations_use_whole_seconds():
    steps = _plan_steps().with_columns(duration_per_pers=pl.lit(5.609027862548828))
    updated, catalog, _ = assign_time_sequence_ids(steps, _schedule())
    expected = 20193 / 3600
    assert updated["duration_per_pers"].to_list() == [expected, expected]
    modeled = catalog.filter(pl.col("time_seq_id") == updated["time_seq_id"][0])
    assert modeled["duration_per_pers"].to_list() == [expected, expected]
    repeated, _, _ = assign_time_sequence_ids(updated, catalog)
    assert_frame_equal(updated.sort("seq_step_index"), repeated.sort("seq_step_index"))


@pytest.mark.parametrize("duration_only", [False, True])
@pytest.mark.parametrize("compact", [False, True])
@pytest.mark.parametrize("eligible", [False, True])
def test_attachment_preserves_two_timings_of_the_same_route_and_modes(duration_only, compact, eligible):
    original = _plan_steps()
    if duration_only:
        # These durations straddle a rounded-second boundary although their
        # difference is less than a tenth of a second. Clock times are equal.
        original = original.with_columns(duration_per_pers=pl.lit(5.609011173248291))
        changed = original.with_columns(duration_per_pers=pl.lit(5.609027862548828))
    else:
        changed = original.with_columns(
            arrival_time=pl.col("arrival_time") + 0.25,
            duration_per_pers=pl.col("duration_per_pers") - 0.25,
        )
    shared = pl.concat([add_candidate_ids(original), add_candidate_ids(changed)]).with_columns(
        destination_sequence_key_1=pl.lit(21, dtype=pl.UInt64),
        destination_sequence_key_2=pl.lit(22, dtype=pl.UInt64),
        mode_sequence_key_1=pl.lit(31, dtype=pl.UInt64),
        mode_sequence_key_2=pl.lit(32, dtype=pl.UInt64),
        # One modeled schedule already exists under reference 1.
        reference_time_seq_id=pl.lit(99, dtype=pl.UInt32),
    ).drop("dest_seq_id", "mode_seq_id")
    units = original.select("demand_group_id", "demand_subgroup_id", "country", "csp").unique().with_columns(
        home_zone_id=pl.lit(1, dtype=pl.Int32), allowed_activities=pl.lit(["work"] if eligible else ["shop"]),
    )
    state = SimpleNamespace(schedule_steps=_schedule(), demand_groups=units)
    source = SimpleNamespace(
        run_key=1,
        scenario="default",
        previous_state=SimpleNamespace(get=lambda: state),
    )
    pool = object.__new__(SharedCandidatePool)
    pool.previous_time_sequence_steps = {}
    pool.time_sequence_steps_by_run_key = {}
    pool.compact = compact
    pool.compact_candidate_steps_by_run_key = {}
    pool.unordered_candidate_steps_by_run_key = {}
    pool.scoring_candidate_steps_by_run_key = {}

    # Physical catalog legs may have come from different original people.
    # Those representative ids must not split a complete plan during indexing.
    shared = shared.with_columns(
        demand_group_id=pl.Series([1, 2, 1, 2], dtype=pl.UInt32),
    )

    attachment_inputs = dict(
        shared_candidates=shared,
        destination_index=pl.DataFrame({
            "destination_sequence_key_1": [21], "destination_sequence_key_2": [22], "dest_seq_id": [1],
        }, schema_overrides={
            "destination_sequence_key_1": pl.UInt64,
            "destination_sequence_key_2": pl.UInt64,
            "dest_seq_id": pl.UInt32,
        }),
        mode_index=pl.DataFrame({
            "mode_sequence_key_1": [31], "mode_sequence_key_2": [32], "mode_seq_id": [1],
        }, schema_overrides={
            "mode_sequence_key_1": pl.UInt64,
            "mode_sequence_key_2": pl.UInt64,
            "mode_seq_id": pl.UInt32,
        }),
        source=source,
    )
    if not eligible:
        pytest.raises(
            ValueError, pool._build_scenario_candidate_steps, **attachment_inputs,
        ).match("disappeared during attachment")
        return
    attached = pool._build_scenario_candidate_steps(**attachment_inputs)
    if compact:
        assert isinstance(attached, pl.LazyFrame)
        assert pool.compact_candidate_steps_by_run_key[1].height == 4
        catalog = pool.compact_candidate_steps_by_run_key[1]
        references = pool.time_sequence_steps_by_run_key[1].select(
            "activity_seq_id", "time_seq_id", "reference_time_seq_id"
        ).unique()
        actual_references = catalog.select(
            "activity_seq_id", "time_seq_id", "reference_time_seq_id"
        ).unique().sort("time_seq_id")
        expected_references = actual_references.drop("reference_time_seq_id").join(
            references, on=["activity_seq_id", "time_seq_id"], how="left"
        ).sort("time_seq_id")
        assert_frame_equal(actual_references, expected_references)
        catalog_ids = set(add_candidate_ids(catalog).select(CANDIDATE_ID_COLS).unique().iter_rows())
        shared_ids = set(shared.select(CANDIDATE_ID_COLS).unique().iter_rows())
        assert catalog_ids == shared_ids
        attached = attached.collect(engine="streaming")
    assert attached.height == 4
    assert attached["time_seq_id"].n_unique() == 2
    attached_ids = set(add_candidate_ids(attached).select(CANDIDATE_ID_COLS).unique().iter_rows())
    shared_ids = set(shared.select(CANDIDATE_ID_COLS).unique().iter_rows())
    assert attached_ids == shared_ids


def test_shared_attachment_uses_one_representative_per_scoring_context():
    original = _plan_steps()
    shared = add_candidate_ids(original).with_columns(
        destination_sequence_key_1=pl.lit(21, dtype=pl.UInt64),
        destination_sequence_key_2=pl.lit(22, dtype=pl.UInt64),
        mode_sequence_key_1=pl.lit(31, dtype=pl.UInt64),
        mode_sequence_key_2=pl.lit(32, dtype=pl.UInt64),
        reference_time_seq_id=pl.lit(1, dtype=pl.UInt32),
    ).drop("dest_seq_id", "mode_seq_id")
    units = pl.DataFrame({
        "demand_group_id": [1, 2, 3], "demand_subgroup_id": [0, 0, 0],
        "country": ["fr"] * 3, "csp": ["worker"] * 3,
        "home_zone_id": [1] * 3, "allowed_activities": [["work"]] * 3,
        "n_persons": [10.0, 20.0, 30.0],
    }).with_columns(pl.col("demand_group_id", "demand_subgroup_id").cast(pl.UInt32),
                    pl.col("home_zone_id").cast(pl.Int32))
    assignments = units.select("demand_group_id", "demand_subgroup_id").with_columns(
        utility_profile_id=pl.Series([0, 0, 1], dtype=pl.UInt32),
    )
    source = SimpleNamespace(
        run_key=1, scenario="default",
        previous_state=SimpleNamespace(get=lambda: SimpleNamespace(schedule_steps=_schedule(), demand_groups=units)),
    )
    pool = object.__new__(SharedCandidatePool)
    pool.previous_time_sequence_steps = {}
    pool.time_sequence_steps_by_run_key = {}
    pool.compact = True
    pool.compact_candidate_steps_by_run_key = {}
    pool.unordered_candidate_steps_by_run_key = {}
    pool.scoring_candidate_steps_by_run_key = {}
    attached = pool._build_scenario_candidate_steps(
        shared_candidates=shared,
        destination_index=pl.DataFrame({"destination_sequence_key_1": [21], "destination_sequence_key_2": [22], "dest_seq_id": [1]},
                                     schema_overrides={"destination_sequence_key_1": pl.UInt64, "destination_sequence_key_2": pl.UInt64, "dest_seq_id": pl.UInt32}),
        mode_index=pl.DataFrame({"mode_sequence_key_1": [31], "mode_sequence_key_2": [32], "mode_seq_id": [1]},
                              schema_overrides={"mode_sequence_key_1": pl.UInt64, "mode_sequence_key_2": pl.UInt64, "mode_seq_id": pl.UInt32}),
        source=source, profile_assignments=assignments,
    ).collect()
    assert attached.height == 4
    assert attached["demand_group_id"].unique().sort().to_list() == [1, 3]
    factored = pool.scoring_candidate_steps_by_run_key[1]
    reconstructed = factored.membership.join(
        factored.steps.lazy(), on=PLAN_COLUMNS, how="inner"
    ).select(attached.columns).collect()
    assert_frame_equal(
        reconstructed.sort(CandidatePlanStepsAsset.DEDUPE_COLUMNS),
        attached.sort(CandidatePlanStepsAsset.DEDUPE_COLUMNS),
    )
    contexts = ScoringContexts.build(
        units, SimpleNamespace(get_utility_profiles=lambda demand, segments: (assignments, {})), None,
    )
    restored = contexts.expand_step_rows(contexts.collapse_step_rows(attached)).collect()
    assert restored.height == 6
    assert restored["demand_group_id"].unique().sort().to_list() == [1, 2, 3]


def test_shared_attachment_merges_histories_for_identical_modeled_plans():
    """Keep both source IDs while merging the resulting plan's history."""
    first = add_candidate_ids(_plan_steps()).with_columns(
        destination_sequence_key_1=pl.lit(21, dtype=pl.UInt64),
        destination_sequence_key_2=pl.lit(22, dtype=pl.UInt64),
        mode_sequence_key_1=pl.lit(31, dtype=pl.UInt64),
        mode_sequence_key_2=pl.lit(32, dtype=pl.UInt64),
        reference_time_seq_id=pl.lit(1, dtype=pl.UInt32),
        first_seen_iteration=pl.lit(1, dtype=pl.UInt16),
        last_seen_iteration=pl.lit(2, dtype=pl.UInt16),
        last_active_iteration=pl.lit(1, dtype=pl.UInt16),
    ).drop("dest_seq_id", "mode_seq_id")
    second = first.with_columns(
        candidate_id_1=pl.col("candidate_id_1") ^ pl.lit(1, dtype=pl.UInt64),
        iteration=pl.lit(3, dtype=pl.UInt16),
        first_seen_iteration=pl.lit(2, dtype=pl.UInt16),
        last_seen_iteration=pl.lit(4, dtype=pl.UInt16),
        last_active_iteration=pl.lit(3, dtype=pl.UInt16),
    )
    units = pl.DataFrame({
        "demand_group_id": [1], "demand_subgroup_id": [0], "country": ["fr"],
        "csp": ["worker"], "home_zone_id": [1], "allowed_activities": [["work"]],
    }).with_columns(pl.col("demand_group_id", "demand_subgroup_id").cast(pl.UInt32),
                    pl.col("home_zone_id").cast(pl.Int32))
    source = SimpleNamespace(
        run_key=1, scenario="default",
        previous_state=SimpleNamespace(get=lambda: SimpleNamespace(schedule_steps=_schedule(), demand_groups=units)),
    )
    pool = object.__new__(SharedCandidatePool)
    pool.previous_time_sequence_steps = {}
    pool.time_sequence_steps_by_run_key = {}
    pool.compact = True
    pool.compact_candidate_steps_by_run_key = {}
    pool.unordered_candidate_steps_by_run_key = {}
    pool.scoring_candidate_steps_by_run_key = {}
    destination_index = pl.DataFrame({
        "destination_sequence_key_1": [21], "destination_sequence_key_2": [22], "dest_seq_id": [1],
    }, schema_overrides={"destination_sequence_key_1": pl.UInt64, "destination_sequence_key_2": pl.UInt64,
                         "dest_seq_id": pl.UInt32})
    mode_index = pl.DataFrame({
        "mode_sequence_key_1": [31], "mode_sequence_key_2": [32], "mode_seq_id": [1],
    }, schema_overrides={"mode_sequence_key_1": pl.UInt64, "mode_sequence_key_2": pl.UInt64,
                         "mode_seq_id": pl.UInt32})
    for candidates in (pl.concat([first, second]), pl.concat([second, first])):
        attached = pool._build_scenario_candidate_steps(
            shared_candidates=candidates, destination_index=destination_index,
            mode_index=mode_index, source=source,
        ).collect(engine="streaming")
        factored = pool.scoring_candidate_steps_by_run_key[1]
        reconstructed = factored.membership.join(factored.steps.lazy(), on=PLAN_COLUMNS).select(attached.columns).collect()
        assert_frame_equal(attached.sort("seq_step_index"), reconstructed.sort("seq_step_index"))
        assert attached.height == 2
        assert attached["iteration"].to_list() == [1, 1]
        assert attached["first_seen_iteration"].to_list() == [1, 1]
        assert attached["last_seen_iteration"].to_list() == [4, 4]
        assert attached["last_active_iteration"].to_list() == [3, 3]


@pytest.mark.parametrize("travel_time, feasible", [(2.0, True), (10.0, False)])
def test_evaluation_remaps_current_plans_after_modeled_travel_changes_timings(monkeypatch, travel_time, feasible):
    """New schedule and plan ids must still identify every transition origin."""
    steps = _plan_steps().with_columns(
        activity_utility_scale=pl.lit(1.0), min_activity_time=pl.lit(1.0),
        cost=pl.lit(0.0), time=pl.lit(travel_time), distance=pl.lit(1.0),
        is_feasible=pl.lit(True), _missing_cost_row=pl.lit(False), _invalid_cost_row=pl.lit(False),
        mean_duration_per_pers=pl.col("duration_per_pers"), value_of_time=pl.lit(1.0),
        k_saturation_utility=pl.lit(1.0), destination_shadow_price=pl.lit(0.0),
    )
    current, index = add_plan_id(steps.select(PLAN_KEY_COLS).unique(), previous_index=None)
    current = current.with_columns(n_persons=pl.lit(100.0), utility=pl.lit(0.0))
    stay_home = steps.head(1).with_columns(
        pl.lit(0, dtype=pl.UInt32).alias("activity_seq_id"),
        pl.lit(0, dtype=pl.UInt32).alias("time_seq_id"),
        pl.lit(0, dtype=pl.UInt32).alias("dest_seq_id"),
        pl.lit(0, dtype=pl.UInt32).alias("mode_seq_id"),
        activity=pl.lit("home"), mode=pl.lit("stay_home"),
        departure_time=pl.lit(0.0), arrival_time=pl.lit(0.0), next_departure_time=pl.lit(24.0),
        duration_per_pers=pl.lit(24.0), mean_home_night_per_pers=pl.lit(10.0),
        utility=pl.lit(0.0),
    )
    updater = PlanUpdater()
    monkeypatch.setattr(updater, "get_possible_plan_steps", lambda *args, **kwargs: steps.lazy())
    evaluation = updater.evaluate_candidate_pool(
        current_plans=current, current_plan_steps=None, candidate_plan_steps=None,
        shared_candidate_plan_steps=None, demand_groups=pl.DataFrame(), survey_plan_steps=_schedule(),
        transport_costs=None, destination_saturation=pl.DataFrame(), activity_dur=pl.DataFrame(),
        home_night_dur=pl.DataFrame({"country": ["fr"], "csp": ["worker"], "mean_home_night_per_pers": [10.0]}),
        stay_home_plan=stay_home, transport_zones=None, iteration=1,
        resolved_activity_parameters={"home": SimpleNamespace(value_of_time_stay_home=1.0)},
        activity_time_rigidity_by_activity={"work": 0.0, "home": 0.0},
        destination_sequences=None, mode_sequences=None, previous_plan_id_index=index,
        parameters=GroupDayTripsParameters(plan_update={"update_plan_timings_from_modeled_travel_times": True}),
    )
    assert evaluation.current_plans["time_seq_id"].item() > 1
    assert evaluation.current_plans["n_persons"].sum() == 100.0
    assert evaluation.current_plans.join(
        evaluation.possible_plan_utility, on=PLAN_KEY_COLS, how="anti"
    ).height == 0
    assert evaluation.candidate_plan_steps["time_seq_id"].n_unique() == 1
    mobile = evaluation.possible_plan_utility.filter(pl.col("mode_seq_id") != 0)
    assert mobile["is_feasible"].item() is feasible
    demand = pl.DataFrame({"demand_group_id": [1], "demand_subgroup_id": [0], "n_persons": [100.0]}).with_columns(
        pl.col("demand_group_id", "demand_subgroup_id").cast(pl.UInt32)
    )
    allocation, _ = updater.estimate_mnl_allocation(evaluation, demand, logit_scale=1.0, include_plan_steps=False)
    assert allocation["n_persons"].sum() == pytest.approx(100.0)
    assert (allocation.filter(pl.col("mode_seq_id") != 0).height > 0) is feasible


def test_modeled_plan_history_merges_when_schedules_converge(monkeypatch):
    """Equivalent modeled schedules keep the earliest and latest history."""
    first = _plan_steps()
    added = first.with_columns(
        pl.col("to") + 10,
        dest_seq_id=pl.lit(2, dtype=pl.UInt32),
    )
    current, index = add_plan_id(first.select(PLAN_KEY_COLS).unique(), previous_index=None)
    current = current.with_columns(n_persons=pl.lit(100.0), utility=pl.lit(0.0))
    stay_home = first.head(1).with_columns(
        pl.lit(0, dtype=pl.UInt32).alias("activity_seq_id"),
        pl.lit(0, dtype=pl.UInt32).alias("time_seq_id"),
        pl.lit(0, dtype=pl.UInt32).alias("dest_seq_id"),
        pl.lit(0, dtype=pl.UInt32).alias("mode_seq_id"),
        activity=pl.lit("home"), mode=pl.lit("stay_home"),
        departure_time=pl.lit(0.0), arrival_time=pl.lit(0.0), next_departure_time=pl.lit(24.0),
        duration_per_pers=pl.lit(24.0), mean_home_night_per_pers=pl.lit(10.0),
        n_persons=pl.lit(100.0), utility=pl.lit(0.0),
    )
    contexts = ScoringContexts(
        demand_groups=pl.DataFrame({"demand_group_id": [1], "demand_subgroup_id": [0], "utility_profile_id": [1],
                                   "country": ["fr"], "csp": ["worker"]}),
        unit_map=pl.DataFrame({"demand_group_id": [1], "demand_subgroup_id": [0], "_score_context_id": [1]}),
    )
    updater = PlanUpdater()
    def score(_current, _current_steps, candidates, *args, **kwargs):
        return candidates.lazy().with_columns(
            activity_utility_scale=pl.lit(1.0), min_activity_time=pl.lit(1.0),
            cost=pl.col("to").cast(pl.Float64) / 100.0,
            time=pl.col("to").cast(pl.Float64) / 10.0 + 1.0,
            distance=pl.col("to").cast(pl.Float64),
            is_feasible=pl.col("to") != 11, _missing_cost_row=pl.col("to") == 11,
            _invalid_cost_row=pl.lit(False),
            mean_duration_per_pers=pl.col("duration_per_pers"), value_of_time=pl.lit(1.0),
            k_saturation_utility=pl.lit(1.0), destination_shadow_price=pl.lit(0.0),
        )

    monkeypatch.setattr(updater, "get_possible_plan_steps", score)
    parameters = GroupDayTripsParameters(plan_update={"update_plan_timings_from_modeled_travel_times": True})
    args = dict(
        current_plans=current, current_plan_steps=None, candidate_plan_steps=None,
        demand_groups=contexts.demand_groups, transport_costs=None, destination_saturation=pl.DataFrame(),
        activity_dur=pl.DataFrame(), home_night_dur=pl.DataFrame({
            "country": ["fr"], "csp": ["worker"], "mean_home_night_per_pers": [10.0],
        }), stay_home_plan=stay_home, transport_zones=None, iteration=1,
        resolved_activity_parameters={"home": SimpleNamespace(value_of_time_stay_home=1.0)},
        activity_time_rigidity_by_activity={"work": 0.0, "home": 0.0},
        destination_sequences=None, mode_sequences=SimpleNamespace(population_segments=[]),
        previous_plan_id_index=index, parameters=parameters, compact_contexts=True,
        scoring_contexts=contexts,
    )
    # Two distinct reference schedules can become the same modeled schedule.
    # Their histories must collapse to one result.
    other_schedule = _schedule().with_columns(
        pl.col("departure_time") - 0.25, pl.col("arrival_time") + 0.25,
        time_seq_id=pl.lit(2, dtype=pl.UInt32),
    )
    other_plan = first.with_columns(
        pl.col("departure_time") - 0.25, pl.col("arrival_time") + 0.25,
        time_seq_id=pl.lit(2, dtype=pl.UInt32),
        iteration=pl.lit(2, dtype=pl.UInt16),
        first_seen_iteration=pl.lit(2, dtype=pl.UInt16),
        last_seen_iteration=pl.lit(2, dtype=pl.UInt16),
    )
    occupied = first.with_columns(
        last_seen_iteration=pl.lit(2, dtype=pl.UInt16),
        last_active_iteration=pl.lit(2, dtype=pl.UInt16),
    )
    collision = updater.evaluate_candidate_pool(
        **args, shared_candidate_plan_steps=pl.concat([occupied, other_plan, added]),
        survey_plan_steps=pl.concat([_schedule(), other_schedule]),
    )
    combined = pl.concat([occupied, other_plan, added])
    factored = ScoringCandidateSteps(
        steps=combined.select(STEP_COLUMNS).unique(),
        membership=combined.select(MEMBERSHIP_COLUMNS).unique().lazy(),
    )
    shared_timing = updater.evaluate_candidate_pool(
        **args, shared_candidate_plan_steps=combined, factored_candidate_steps=factored,
        survey_plan_steps=pl.concat([_schedule(), other_schedule]),
    )
    assert_frame_equal(
        shared_timing.possible_plan_utility.sort(PLAN_KEY_COLS), collision.possible_plan_utility.sort(PLAN_KEY_COLS),
        check_exact=True,
    )
    assert_frame_equal(
        shared_timing.candidate_plan_steps.sort(PLAN_KEY_COLS + ["seq_step_index"]),
        collision.candidate_plan_steps.sort(PLAN_KEY_COLS + ["seq_step_index"]),
        check_exact=True, check_column_order=False,
    )
    _, physical, membership = contexts.collapse_factored_step_rows(
        factored, None, None, current, return_parts=True,
    )
    score_rows = (score(None, None, combined).collect()
                  .with_columns(utility_profile_id=pl.lit(1, dtype=pl.Int64))
                  .select("country", "csp", "activity", "from", "to", "mode", "utility_profile_id",
                          "cost", "time", "distance", "is_feasible", "_missing_cost_row", "_invalid_cost_row",
                          "min_activity_time", "activity_utility_scale").unique())
    lookup = ScoringLookup(ScoringMemoryBudget(100_000_000))
    lookup.retain(score_rows)
    section_evaluation = updater._evaluate_shared_sections(
        physical=physical, memberships=membership, contexts=contexts,
        current_plans=contexts.collapse_allocations(current),
        stay_home_plan=contexts.collapse_stay_home(stay_home),
        schedule_steps=pl.concat([_schedule(), other_schedule]), home_night_dur=args["home_night_dur"],
        home_value=1.0, min_activity_time_constant=parameters.plan_update.min_activity_time_constant,
        rigidity={"work": 0.0, "home": 0.0}, enabled=True,
        scoring_lookup=lookup, time_index_cache=None,
    )
    assert section_evaluation is not None
    # Two scenarios can share trip scores while retaining different complete
    # plan scores. In particular, home-night values must remain run-local.
    from mobility.trips.group_day_trips.plans.whole_plan_cache import WholePlanCache, WholePlanMemoryBudget, prepare_reuse
    budget = WholePlanMemoryBudget()
    cache_a, cache_b = WholePlanCache(budget), WholePlanCache(budget)
    shared_args = dict(physical=physical, memberships=membership, contexts=contexts,
        current_plans=contexts.collapse_allocations(current),
        stay_home_plan=contexts.collapse_stay_home(stay_home),
        schedule_steps=pl.concat([_schedule(), other_schedule]), home_value=1.0,
        min_activity_time_constant=parameters.plan_update.min_activity_time_constant,
        rigidity={"work": 0.0, "home": 0.0}, enabled=True, scoring_lookup=lookup, time_index_cache=None)
    original_home = args["home_night_dur"]
    changed_home = original_home.with_columns(mean_home_night_per_pers=pl.col("mean_home_night_per_pers") + 1.0)
    scenario_a = updater._evaluate_shared_sections(**shared_args, home_night_dur=original_home, whole_plan_cache=cache_a)
    scenario_b = updater._evaluate_shared_sections(**shared_args, home_night_dur=changed_home, whole_plan_cache=cache_b)
    assert cache_a.state is not None and cache_b.state is not None
    assert not scenario_a.possible_plan_utility.sort(PLAN_KEY_COLS).equals(
        scenario_b.possible_plan_utility.sort(PLAN_KEY_COLS))
    profiles = contexts.demand_groups.select("demand_group_id", "demand_subgroup_id", "country", "csp", "utility_profile_id")
    classes = (membership.select(PLAN_KEY_COLS).join(profiles, on=["demand_group_id", "demand_subgroup_id"])
               .select(PLAN_COLUMNS + ["country", "csp", "utility_profile_id"]).unique())
    prepared_b = prepare_reuse(cache_b.state, classes, physical, lookup.scores, scenario_b.time_sequence_steps, changed_home, 1.0,
                               parameters.plan_update.min_activity_time_constant, {"work": 0.0, "home": 0.0}, True)
    assert prepared_b is not None and prepared_b[1].is_empty()
    reused_b = updater._evaluate_shared_sections(**{**shared_args, "schedule_steps": scenario_b.time_sequence_steps},
                                                  home_night_dur=changed_home, whole_plan_cache=cache_b, prepared_reuse=prepared_b)
    assert_frame_equal(reused_b.possible_plan_utility.sort(PLAN_KEY_COLS),
                       scenario_b.possible_plan_utility.sort(PLAN_KEY_COLS), check_exact=True)
    assert_frame_equal(section_evaluation.possible_plan_utility.sort(PLAN_KEY_COLS),
                       collision.possible_plan_utility.sort(PLAN_KEY_COLS), check_exact=True,
                       check_column_order=False)
    assert_frame_equal(section_evaluation.candidate_plan_steps.lazy().collect().sort(PLAN_KEY_COLS + ["seq_step_index"]),
                       collision.candidate_plan_steps.sort(PLAN_KEY_COLS + ["seq_step_index"]), check_exact=True,
                       check_column_order=False)
    assert_frame_equal(section_evaluation.possible_plan_steps.collect().sort(PLAN_KEY_COLS + ["seq_step_index"]),
                       collision.possible_plan_steps.collect().sort(PLAN_KEY_COLS + ["seq_step_index"]),
                       check_exact=True, check_column_order=False)
    all_steps = section_evaluation.possible_plan_steps.collect()
    selected_ids = (section_evaluation.possible_plan_utility.sort("mode_seq_id")
                    .select("plan_id").gather_every(2))
    selected_steps = section_evaluation.shared_step_details.select(updater, selected_ids).collect()
    assert_frame_equal(selected_steps.sort(PLAN_KEY_COLS + ["seq_step_index"]),
                       all_steps.join(selected_ids, on="plan_id", how="semi")
                       .sort(PLAN_KEY_COLS + ["seq_step_index"]),
                       check_exact=True, check_column_order=False)
    stored = section_evaluation.shared_step_details.for_storage(selected_ids)
    assert_frame_equal(stored.sort(PLAN_KEY_COLS + ["seq_step_index"]),
                       selected_steps.select(stored.columns).sort(PLAN_KEY_COLS + ["seq_step_index"]),
                       check_exact=True)
    # A lookup that cannot be retained should still be used for this batch,
    # without falling back to the expanded candidate evaluator.
    monkeypatch.setattr(updater, "compute_plan_steps_candidates_utility", lambda **kwargs: score_rows.lazy())
    monkeypatch.setattr(updater, "get_possible_plan_steps",
                        lambda *args, **kwargs: pytest.fail("Unexpected expanded candidate evaluation"))
    transport = SimpleNamespace(get_utility_profiles=lambda *args: (pl.DataFrame(), {}))
    small_lookup = ScoringLookup(ScoringMemoryBudget(0))
    unretained_cache = WholePlanCache(WholePlanMemoryBudget())
    temporary = updater.evaluate_candidate_pool(
        **{**args, "transport_costs": transport}, shared_candidate_plan_steps=combined,
        factored_candidate_steps=factored, scoring_lookup=small_lookup, whole_plan_cache=unretained_cache,
        survey_plan_steps=pl.concat([_schedule(), other_schedule]),
    )
    assert small_lookup.scores is None
    assert unretained_cache.state is None and unretained_cache.budget.used_bytes == 0
    # In an all-hit batch, old alternatives stay available downstream without
    # expanding their physical steps into new scoring requests.
    requested_rows = []
    def record_score_inputs(**kwargs):
        requested_rows.append(kwargs["compact_score_inputs"].height)
        return score_rows.lazy()
    monkeypatch.setattr(updater, "compute_plan_steps_candidates_utility", record_score_inputs)
    warm_lookup = ScoringLookup(ScoringMemoryBudget(100_000_000))
    warm_lookup.retain(score_rows)
    warm_cache = WholePlanCache(WholePlanMemoryBudget())
    warm_args = dict(**{**args, "transport_costs": transport}, shared_candidate_plan_steps=combined,
                     factored_candidate_steps=factored, scoring_lookup=warm_lookup, whole_plan_cache=warm_cache)
    first_batch = updater.evaluate_candidate_pool(**warm_args, survey_plan_steps=pl.concat([_schedule(), other_schedule]))
    assert warm_cache.state is not None
    repeated_batch = updater.evaluate_candidate_pool(**warm_args, survey_plan_steps=first_batch.time_sequence_steps)
    assert requested_rows[0] > 0 and requested_rows[1] == 0
    assert_frame_equal(repeated_batch.possible_plan_utility.sort(PLAN_KEY_COLS),
                       first_batch.possible_plan_utility.sort(PLAN_KEY_COLS), check_exact=True)
    assert_frame_equal(temporary.possible_plan_utility.sort(PLAN_KEY_COLS),
                       section_evaluation.possible_plan_utility.sort(PLAN_KEY_COLS), check_exact=True,
                       check_column_order=False)
    assert collision.possible_plan_utility.filter(pl.col("mode_seq_id") != 0).height == 2
    merged = collision.candidate_plan_steps.filter(pl.col("dest_seq_id") == 1)
    assert merged["iteration"].unique().to_list() == [1]
    assert merged["first_seen_iteration"].unique().to_list() == [1]
    assert merged["last_seen_iteration"].unique().to_list() == [2]
    assert merged["last_active_iteration"].unique().to_list() == [2]


def test_modeled_schedule_ids_survive_state_spill_and_restore(tmp_path):
    """Restoring a state must not forget ids already given to modeled timings."""
    steps, catalog, _ = assign_time_sequence_ids(
        _plan_steps().with_columns(arrival_time=pl.col("arrival_time") + 0.25), _schedule()
    )
    empty = pl.DataFrame()
    initial = RunState(
        survey_plans=empty, survey_plan_steps=_schedule(), demand_groups=empty,
        activity_dur=empty, home_night_dur=empty, stay_home_plan=empty,
        opportunities=empty, current_plans=empty, candidate_plan_steps=steps,
        plan_id_index=empty, destination_saturation=empty, costs=empty, start_step=1,
    )
    state = RunState(**{**vars(initial), "time_sequence_steps": catalog, "current_plan_steps": steps})
    asset = object.__new__(IterationStateAsset)
    asset.iteration = 1
    asset.initial_state = SimpleNamespace(get=lambda: initial)
    asset.transport_costs = SimpleNamespace(get_costs_by_od=lambda metrics: empty)

    asset.spill_execution_value(state, tmp_path / "state")
    restored = asset.load_execution_value(tmp_path / "state")
    repeated, repeated_catalog, _ = assign_time_sequence_ids(steps, restored.schedule_steps)

    assert_frame_equal(catalog, restored.time_sequence_steps)
    assert_frame_equal(catalog, repeated_catalog)
    assert set(steps["time_seq_id"]) == set(repeated["time_seq_id"])


def test_pool_keeps_occupied_timing_when_previous_pool_has_only_survey_timing(monkeypatch):
    """A fresh mode search must not discard the occupied timing variant."""
    original = _plan_steps()
    occupied, catalog, _ = assign_time_sequence_ids(
        original.with_columns(arrival_time=pl.col("arrival_time") + 0.25), _schedule()
    )
    dest_index = pl.DataFrame({
        "destination_sequence_key_1": [21], "destination_sequence_key_2": [22], "dest_seq_id": [1],
    }, schema_overrides={
        "destination_sequence_key_1": pl.UInt64,
        "destination_sequence_key_2": pl.UInt64,
        "dest_seq_id": pl.UInt32,
    })
    mode_index = pl.DataFrame({
        "mode_sequence_key_1": [31], "mode_sequence_key_2": [32], "mode_seq_id": [1],
    }, schema_overrides={
        "mode_sequence_key_1": pl.UInt64,
        "mode_sequence_key_2": pl.UInt64,
        "mode_seq_id": pl.UInt32,
    })
    units = original.select("demand_group_id", "demand_subgroup_id", "country", "csp").unique().with_columns(
        home_zone_id=pl.lit(1, dtype=pl.Int32), allowed_activities=pl.lit(["work"]),
    )
    state = SimpleNamespace(
        schedule_steps=catalog, current_plans=occupied.select(PLAN_KEY_COLS).unique(),
        candidate_plan_steps=occupied, demand_groups=units,
        destination_sequence_index=dest_index, mode_sequence_index=mode_index,
    )
    previous = InMemoryAsset({"test": "previous"})
    monkeypatch.setattr(previous, "get", lambda: state)
    destinations = InMemoryAsset({"test": "destinations"})
    monkeypatch.setattr(destinations, "get", lambda: pl.DataFrame())
    monkeypatch.setattr(destinations, "get_index", lambda: dest_index, raising=False)
    modes = InMemoryAsset({"test": "modes"})
    monkeypatch.setattr(modes, "get", lambda: pl.DataFrame())
    monkeypatch.setattr(modes, "get_index", lambda: mode_index, raising=False)
    modes.generated_mode_sequences = 0
    modes.transport_costs = SimpleNamespace(get_utility_profiles=lambda demand, segments: (
        demand.select("demand_group_id", "demand_subgroup_id").with_columns(utility_profile_id=pl.lit(0)), {}
    ))
    source = ScenarioCandidateSource(
        run_key=1, scenario="default", previous_state=previous, destination_sequences=destinations,
        mode_sequences=modes, iteration=2,
        run=SimpleNamespace(
            run_context=SimpleNamespace(inputs_hash="test"),
            population=SimpleNamespace(population_segments=[]),
            sensitivity_case_id="base",
            replication=0,
        ),
    )
    other_state = SimpleNamespace(
        schedule_steps=catalog, current_plans=occupied.select(PLAN_KEY_COLS).head(0),
        candidate_plan_steps=None, demand_groups=units,
        destination_sequence_index=None, mode_sequence_index=None,
    )
    other_previous = InMemoryAsset({"test": "other previous"})
    monkeypatch.setattr(other_previous, "get", lambda: other_state)
    other_source = replace(source, run_key=2, scenario="other", previous_state=other_previous)
    monkeypatch.setattr(CandidatePlanStepsAsset, "build_iteration_candidates", lambda **kwargs: original.head(0).lazy())
    merge = SharedCandidatePool._merge_candidate_content
    attach = SharedCandidatePool._build_scenario_candidate_steps
    source_frames = []
    checked_release = []

    def remember_source_frames(pool, frames):
        source_frames[:] = [weakref.ref(frame) for frame in frames]
        return merge(pool, frames)

    def check_source_frames_released(pool, *args, **kwargs):
        assert source_frames and all(frame() is None for frame in source_frames)
        checked_release.append(True)
        return attach(pool, *args, **kwargs)

    monkeypatch.setattr(SharedCandidatePool, "_merge_candidate_content", remember_source_frames)
    monkeypatch.setattr(SharedCandidatePool, "_build_scenario_candidate_steps", check_source_frames_released)
    pool = SharedCandidatePool(
        sources=[source, other_source], iteration=2, previous_candidates_by_run_key={1: original},
        previous_time_sequence_steps={1: catalog},
    ).build()
    assert checked_release

    keys = pool.candidate_plan_steps_by_run_key[1].select(PLAN_KEY_COLS).unique()
    assert state.current_plans.join(keys, on=PLAN_KEY_COLS, how="anti").height == 0
    assert len(pool.candidate_ids) == 2
    other_steps = pool.candidate_plan_steps_by_run_key[2]
    assert other_steps["last_active_iteration"].null_count() == other_steps.height
    assert other_steps["first_seen_iteration"].unique().to_list() == [2]
    active_steps = pool.candidate_plan_steps_by_run_key[1].join(state.current_plans, on=PLAN_KEY_COLS)
    assert active_steps["last_active_iteration"].unique().to_list() == [2]

    # A no-transition step reuses history without touching sampling assets.
    original_mode_get, original_destination_get = modes.get, destinations.get
    def reject_sampling():
        raise AssertionError("A no-transition step must not generate alternatives")
    monkeypatch.setattr(modes, "get", reject_sampling)
    monkeypatch.setattr(destinations, "get", reject_sampling)
    frozen_pool = SharedCandidatePool(
        sources=[source], iteration=2, generate_candidates=False,
        previous_candidates_by_run_key={1: original},
        previous_time_sequence_steps={1: catalog},
    ).build()
    monkeypatch.setattr(modes, "get", original_mode_get)
    monkeypatch.setattr(destinations, "get", original_destination_get)
    assert frozen_pool.generated_mode_sequences_by_run_key[1] == 0
    frozen_keys = frozen_pool.candidate_plan_steps_by_run_key[1].select(PLAN_KEY_COLS).unique()
    assert state.current_plans.join(frozen_keys, on=PLAN_KEY_COLS, how="anti").height == 0

    # The first step may also have no candidate history at all.
    other_state.stay_home_plan = original
    empty_pool = SharedCandidatePool(
        sources=[other_source], iteration=1, generate_candidates=False,
    ).build()
    assert empty_pool.candidate_plan_steps_by_run_key[2].is_empty()
    assert empty_pool.generated_mode_sequences_by_run_key[2] == 0

    # The same occupied state can occur in scenarios whose sampled histories
    # differ. Reconstruct its active plans once without merging those histories.
    same_state_source = replace(source, run_key=3, scenario="same-state")
    recent = original.with_columns(last_seen_iteration=pl.lit(3, dtype=pl.UInt16))
    calls = []
    add_ids = shared_pool_module.add_candidate_ids

    def count_ids(frame):
        calls.append(True)
        return add_ids(frame)

    monkeypatch.setattr(shared_pool_module, "add_candidate_ids", count_ids)
    shared_active = SharedCandidatePool(
        sources=[source, same_state_source], iteration=4,
        previous_candidates_by_run_key={1: original, 3: recent},
        previous_time_sequence_steps={1: catalog, 3: catalog},
    ).build()
    assert len(calls) == 3  # One active reconstruction and two local histories.
    calls.clear()
    original_key = shared_pool_module.asset_graph_key
    unique_keys = iter(range(100))
    monkeypatch.setattr(
        shared_pool_module, "asset_graph_key", lambda asset: (original_key(asset), next(unique_keys)),
    )
    independent_active = SharedCandidatePool(
        sources=[source, same_state_source], iteration=4,
        previous_candidates_by_run_key={1: original, 3: recent},
        previous_time_sequence_steps={1: catalog, 3: catalog},
    ).build()
    monkeypatch.setattr(shared_pool_module, "asset_graph_key", original_key)
    monkeypatch.setattr(shared_pool_module, "add_candidate_ids", add_ids)
    assert len(calls) == 4
    assert_frame_equal(shared_active.candidate_ids, independent_active.candidate_ids)
    for run_key in (1, 3):
        assert_frame_equal(
            shared_active.candidate_plan_steps_by_run_key[run_key],
            independent_active.candidate_plan_steps_by_run_key[run_key],
        )

    # Many demand groups can share the same plan. In particular, the occupied
    # group need not be the first group retained for shared plan content.
    def for_group(frame, group):
        return frame.with_columns(
            demand_group_id=pl.lit(group, dtype=pl.UInt32),
            csp=pl.lit("worker" if group % 2 else "other"),
            first_seen_iteration=pl.lit(1 + group % 2, dtype=pl.UInt16),
            last_seen_iteration=pl.lit(2 + group % 3, dtype=pl.UInt16),
            last_active_iteration=pl.lit(group % 3, dtype=pl.UInt16),
        )

    expanded_previous = pl.concat([for_group(original, group) for group in range(1, 31)])
    expanded_occupied = pl.concat([for_group(occupied, group) for group in range(1, 31)])
    state.demand_groups = expanded_previous.select(
        "demand_group_id", "demand_subgroup_id", "country", "csp"
    ).unique().with_columns(home_zone_id=pl.lit(1, dtype=pl.Int32), allowed_activities=pl.lit(["work"]))
    state.current_plans = expanded_occupied.filter(pl.col("demand_group_id") == 17).select(PLAN_KEY_COLS).unique()
    state.candidate_plan_steps = expanded_occupied
    expanded_pool = SharedCandidatePool(
        sources=[source], iteration=2, compact=True, previous_candidates_by_run_key={1: expanded_previous},
        previous_time_sequence_steps={1: catalog},
    ).build()
    compact_previous = CompactCandidateSteps.from_frame(expanded_previous)
    compact_occupied = CompactCandidateSteps.from_frame(expanded_occupied)
    assert isinstance(compact_previous, CompactCandidateSteps)
    assert isinstance(compact_occupied, CompactCandidateSteps)
    state.candidate_plan_steps = compact_occupied
    compact_pool = SharedCandidatePool(
        sources=[source], iteration=2, compact=True, previous_candidates_by_run_key={1: compact_previous},
        previous_time_sequence_steps={1: catalog},
    ).build()
    assert_frame_equal(compact_pool.candidate_ids, expanded_pool.candidate_ids)
    for name in ("candidate_replications",):
        left, right = getattr(compact_pool, name), getattr(expanded_pool, name)
        assert_frame_equal(left.sort(left.columns), right.sort(right.columns), check_exact=True)
    left = compact_pool.candidate_plan_steps_by_run_key[1].collect().sort(CandidatePlanStepsAsset.DEDUPE_COLUMNS)
    right = expanded_pool.candidate_plan_steps_by_run_key[1].collect().sort(CandidatePlanStepsAsset.DEDUPE_COLUMNS)
    assert_frame_equal(left, right, check_exact=True)
    shared, active = compact_pool.candidate_step_parts_by_run_key[1]
    combined = pl.concat([shared, active.lazy()], how="vertical_relaxed").unique(
        subset=CandidatePlanStepsAsset.DEDUPE_COLUMNS, keep="last"
    ).collect().sort(CandidatePlanStepsAsset.DEDUPE_COLUMNS)
    assert_frame_equal(combined, left, check_exact=True)

    # Shared compact history is reduced and assigned content IDs once. The
    # result must match building both scenario histories independently.
    history_calls = []
    add_ids = shared_pool_module.add_candidate_ids

    def count_history_ids(frame):
        history_calls.append(True)
        return add_ids(frame)

    monkeypatch.setattr(shared_pool_module, "add_candidate_ids", count_history_ids)
    histories = {1: compact_previous, 3: compact_previous}
    shared_history = SharedCandidatePool(
        sources=[source, same_state_source], iteration=2, compact=True,
        previous_candidates_by_run_key=histories, previous_time_sequence_steps={1: catalog, 3: catalog},
    ).build()
    assert len(history_calls) == 2
    history_calls.clear()
    original_key = shared_pool_module.asset_graph_key
    unique_keys = iter(range(100))
    monkeypatch.setattr(
        shared_pool_module, "asset_graph_key", lambda asset: (original_key(asset), next(unique_keys)),
    )
    independent_history = SharedCandidatePool(
        sources=[source, same_state_source], iteration=2, compact=True,
        previous_candidates_by_run_key=histories, previous_time_sequence_steps={1: catalog, 3: catalog},
    ).build()
    monkeypatch.setattr(shared_pool_module, "asset_graph_key", original_key)
    monkeypatch.setattr(shared_pool_module, "add_candidate_ids", add_ids)
    assert len(history_calls) == 4
    assert_frame_equal(shared_history.candidate_ids, independent_history.candidate_ids)
    for run_key in (1, 3):
        assert_frame_equal(
            shared_history.candidate_plan_steps_by_run_key[run_key].collect().sort(CandidatePlanStepsAsset.DEDUPE_COLUMNS),
            independent_history.candidate_plan_steps_by_run_key[run_key].collect().sort(CandidatePlanStepsAsset.DEDUPE_COLUMNS),
        )
    changed_history = replace(
        compact_previous,
        membership=compact_previous.membership.with_columns(
            last_seen_iteration=(pl.col("last_seen_iteration") + 3).cast(pl.UInt16),
        ),
    )
    history_calls.clear()
    monkeypatch.setattr(shared_pool_module, "add_candidate_ids", count_history_ids)
    separate_histories = SharedCandidatePool(
        sources=[source, same_state_source], iteration=2, compact=True,
        previous_candidates_by_run_key={1: compact_previous, 3: changed_history},
        previous_time_sequence_steps={1: catalog, 3: catalog},
    ).build()
    monkeypatch.setattr(shared_pool_module, "add_candidate_ids", add_ids)
    assert len(history_calls) == 3
    first_dates = separate_histories.candidate_plan_steps_by_run_key[1].collect()["last_seen_iteration"].max()
    second_dates = separate_histories.candidate_plan_steps_by_run_key[3].collect()["last_seen_iteration"].max()
    assert second_dates > first_dates


def test_pool_shares_scenarios_but_keeps_replication_choice_sets_separate(monkeypatch):
    """The same stored plan can be available to several, but not all, seeds."""
    original = _plan_steps()
    units = original.select("demand_group_id", "demand_subgroup_id", "country", "csp").unique().with_columns(
        home_zone_id=pl.lit(1, dtype=pl.Int32), allowed_activities=pl.lit(["work"]),
    )
    state = SimpleNamespace(
        schedule_steps=_schedule(), current_plans=original.select(PLAN_KEY_COLS).head(0),
        candidate_plan_steps=None, demand_groups=units,
        destination_sequence_index=None, mode_sequence_index=None,
    )
    previous = InMemoryAsset({"test": "choice set state"})
    monkeypatch.setattr(previous, "get", lambda: state)
    mode_index = pl.DataFrame({
        "mode_sequence_key_1": [31], "mode_sequence_key_2": [32], "mode_seq_id": [1],
    }, schema_overrides={"mode_sequence_key_1": pl.UInt64, "mode_sequence_key_2": pl.UInt64, "mode_seq_id": pl.UInt32})
    sources = []
    # Seed 0 gets A and B from two scenarios; seed 1 gets B and C.
    for run_key, replication, destination in [(1, 0, 2), (2, 0, 3), (3, 1, 3), (4, 1, 4)]:
        destinations = InMemoryAsset({"test": "destinations", "run": run_key})
        destinations.test_destination = destination
        index = pl.DataFrame({
            "destination_sequence_key_1": [destination], "destination_sequence_key_2": [0],
            "dest_seq_id": [destination],
        }, schema_overrides={"destination_sequence_key_1": pl.UInt64, "destination_sequence_key_2": pl.UInt64, "dest_seq_id": pl.UInt32})
        monkeypatch.setattr(destinations, "get", lambda: pl.DataFrame())
        monkeypatch.setattr(destinations, "get_index", lambda index=index: index, raising=False)
        modes = InMemoryAsset({"test": "modes", "run": run_key})
        monkeypatch.setattr(modes, "get", lambda: pl.DataFrame())
        monkeypatch.setattr(modes, "get_index", lambda: mode_index, raising=False)
        modes.generated_mode_sequences = 1
        modes.transport_costs = SimpleNamespace(get_utility_profiles=lambda demand, segments: (
            demand.select("demand_group_id", "demand_subgroup_id").with_columns(utility_profile_id=pl.lit(0)), {}
        ))
        sources.append(ScenarioCandidateSource(
            run_key=run_key, scenario=f"scenario-{run_key % 2}", previous_state=previous,
            destination_sequences=destinations, mode_sequences=modes, iteration=1,
            run=SimpleNamespace(run_context=SimpleNamespace(inputs_hash=str(run_key)),
                                population=SimpleNamespace(population_segments=[]),
                                sensitivity_case_id="base", replication=replication),
        ))

    def generate(**kwargs):
        destination = kwargs["destination_sequences"].test_destination
        return original.with_columns(
            pl.lit(destination, dtype=pl.UInt32).alias("dest_seq_id"),
            pl.Series("from", [1, destination], dtype=pl.Int32),
            pl.Series("to", [destination, 1], dtype=pl.Int32),
        ).lazy()

    monkeypatch.setattr(CandidatePlanStepsAsset, "build_iteration_candidates", generate)
    original_catalogue_builder = PlanCatalogue.from_candidates
    previous_steps = {}

    def capture_catalogue_input(cls, candidates):
        run_id = candidates["run_id"][0]
        # Schedule indexing rounds times to seconds, the same precision used
        # for candidate IDs. Carrying the IDs must still describe these steps.
        for candidate in candidates.partition_by(CANDIDATE_ID_COLS):
            recomputed = add_candidate_ids(candidate).select(CANDIDATE_ID_COLS).unique()
            assert_frame_equal(recomputed, candidate.select(CANDIDATE_ID_COLS).unique())
        previous_steps[run_id] = candidates.select(
            CandidatePlanStepsAsset.STRUCTURAL_COLUMNS + CandidatePlanStepsAsset.RETENTION_COLUMNS
        )
        return original_catalogue_builder(candidates)

    monkeypatch.setattr(PlanCatalogue, "from_candidates", classmethod(capture_catalogue_input))
    merge = SharedCandidatePool._merge_candidate_content
    attach = SharedCandidatePool._build_scenario_candidate_steps
    source_frames = []
    checked_release = []

    def remember_source_frames(pool, frames):
        source_frames[:] = [weakref.ref(frame) for frame in frames]
        return merge(pool, frames)

    def check_source_frames_released(pool, *args, **kwargs):
        assert source_frames and all(frame() is None for frame in source_frames)
        checked_release.append(True)
        return attach(pool, *args, **kwargs)

    monkeypatch.setattr(SharedCandidatePool, "_merge_candidate_content", remember_source_frames)
    monkeypatch.setattr(SharedCandidatePool, "_build_scenario_candidate_steps", check_source_frames_released)
    result = SharedCandidatePool(sources=sources, iteration=1, compact=True).build()
    assert checked_release
    monkeypatch.setattr(SharedCandidatePool, "_merge_candidate_content", merge)
    monkeypatch.setattr(SharedCandidatePool, "_build_scenario_candidate_steps", attach)
    monkeypatch.setattr(PlanCatalogue, "from_candidates", original_catalogue_builder)
    assert len(result.candidate_ids) == 3
    assert result.candidate_replications.group_by("replication").len().sort("replication")["len"].to_list() == [2, 2]
    assert result.candidate_catalogue.plans.height == 3
    assert result.candidate_catalogue.steps.height == 6
    assert result.candidate_catalogue.membership.height == 8
    # Identical generation assets contribute once to the physical pool while
    # both scenarios retain their own available plans and generation counts.
    generation_calls = []

    def count_generation(**kwargs):
        generation_calls.append(True)
        return generate(**kwargs)

    monkeypatch.setattr(CandidatePlanStepsAsset, "build_iteration_candidates", count_generation)
    same_source = replace(sources[0], run_key=5, scenario="same-inputs")
    shared_generation = SharedCandidatePool(
        sources=[sources[0], same_source], iteration=1, compact=True, build_catalogue=False,
    ).build()
    assert len(generation_calls) == 1
    assert shared_generation.generated_mode_sequences_by_run_key[1] == 1
    assert shared_generation.generated_mode_sequences_by_run_key[5] == 1
    assert shared_generation.scoring_candidate_steps_by_run_key[1] is shared_generation.scoring_candidate_steps_by_run_key[5]
    assert_frame_equal(
        shared_generation.candidate_plan_steps_by_run_key[1].collect().sort(CandidatePlanStepsAsset.DEDUPE_COLUMNS),
        shared_generation.candidate_plan_steps_by_run_key[5].collect().sort(CandidatePlanStepsAsset.DEDUPE_COLUMNS),
    )
    catalogue_calls = []
    build_catalogue = PlanCatalogue.from_candidates

    def count_catalogue(cls, candidates):
        catalogue_calls.append(True)
        return build_catalogue(candidates)

    monkeypatch.setattr(PlanCatalogue, "from_candidates", classmethod(count_catalogue))
    shared_catalogue = SharedCandidatePool(
        sources=[sources[0], same_source], iteration=1, compact=True,
    ).build().candidate_catalogue
    assert len(catalogue_calls) == 1
    assert shared_catalogue.membership["run_id"].unique().sort().to_list() == [1, 5]
    # Shared membership remains shared in the next generation batch.
    first_batch = SharedCandidatePool(
        sources=[sources[0], same_source], iteration=1, compact=True,
    ).build()
    second_batch = SharedCandidatePool(
        sources=[sources[0], same_source], iteration=2, compact=True,
        previous_catalogue=first_batch.candidate_catalogue,
        previous_destination_indices=first_batch.destination_sequence_indices,
        previous_mode_indices=first_batch.mode_sequence_indices,
        previous_time_sequence_steps=first_batch.time_sequence_steps_by_run_key,
    ).build()
    assert (second_batch.scoring_candidate_steps_by_run_key[1]
            is second_batch.scoring_candidate_steps_by_run_key[5])
    assert second_batch.candidate_catalogue.equivalent_runs == {1: 1, 5: 1}
    catalogue_calls.clear()
    original_key = shared_pool_module.asset_graph_key
    unique_keys = iter(range(100))
    monkeypatch.setattr(
        shared_pool_module, "asset_graph_key", lambda asset: (original_key(asset), next(unique_keys)),
    )
    separate_catalogue = SharedCandidatePool(
        sources=[sources[0], same_source], iteration=1, compact=True,
    ).build().candidate_catalogue
    monkeypatch.setattr(shared_pool_module, "asset_graph_key", original_key)
    monkeypatch.setattr(PlanCatalogue, "from_candidates", build_catalogue)
    assert len(catalogue_calls) == 2
    for name in ("plans", "steps", "variants", "membership"):
        assert_frame_equal(getattr(shared_catalogue, name), getattr(separate_catalogue, name))
    generation_calls.clear()
    different_history = SharedCandidatePool(
        sources=[sources[0], same_source], iteration=2, compact=True, build_catalogue=False,
        previous_catalogue=result.candidate_catalogue,
        previous_destination_indices={1: result.destination_sequence_indices[1], 5: result.destination_sequence_indices[1]},
        previous_mode_indices={1: result.mode_sequence_indices[1], 5: result.mode_sequence_indices[1]},
        previous_time_sequence_steps={1: result.time_sequence_steps_by_run_key[1], 5: result.time_sequence_steps_by_run_key[1]},
    ).build()
    assert len(generation_calls) == 1
    assert different_history.scoring_candidate_steps_by_run_key[1] is not different_history.scoring_candidate_steps_by_run_key[5]
    for run_key, first_seen in [(1, 1), (5, 2)]:
        rows = different_history.candidate_plan_steps_by_run_key[run_key].collect()
        plan = rows.filter((pl.col("seq_step_index") == 1) & (pl.col("to") == 3))
        assert plan["first_seen_iteration"].unique().to_list() == [first_seen]
    generation_calls.clear()
    original_key = shared_pool_module.asset_graph_key
    unique_key_counter = iter(range(100))
    monkeypatch.setattr(
        shared_pool_module, "asset_graph_key", lambda asset: (original_key(asset), next(unique_key_counter)),
    )
    separately_generated = SharedCandidatePool(
        sources=[sources[0], same_source], iteration=2, compact=True, build_catalogue=False,
        previous_catalogue=result.candidate_catalogue,
        previous_destination_indices={1: result.destination_sequence_indices[1], 5: result.destination_sequence_indices[1]},
        previous_mode_indices={1: result.mode_sequence_indices[1], 5: result.mode_sequence_indices[1]},
        previous_time_sequence_steps={1: result.time_sequence_steps_by_run_key[1], 5: result.time_sequence_steps_by_run_key[1]},
    ).build()
    monkeypatch.setattr(shared_pool_module, "asset_graph_key", original_key)
    assert len(generation_calls) == 2
    assert_frame_equal(different_history.candidate_ids, separately_generated.candidate_ids)
    for run_key in (1, 5):
        assert_frame_equal(
            different_history.candidate_plan_steps_by_run_key[run_key].collect().sort(CandidatePlanStepsAsset.DEDUPE_COLUMNS),
            separately_generated.candidate_plan_steps_by_run_key[run_key].collect().sort(CandidatePlanStepsAsset.DEDUPE_COLUMNS),
        )
    generation_calls.clear()
    second_seed_run = SimpleNamespace(**{**vars(sources[0].run), "replication": 1})
    second_seed = replace(same_source, run=second_seed_run)
    separated_seeds = SharedCandidatePool(
        sources=[sources[0], second_seed], iteration=1, compact=True, build_catalogue=False,
    ).build()
    assert len(generation_calls) == 2
    assert separated_seeds.candidate_replications["replication"].unique().sort().to_list() == [0, 1]
    monkeypatch.setattr(CandidatePlanStepsAsset, "build_iteration_candidates", generate)
    last_batch = SharedCandidatePool(
        sources=sources, iteration=1, compact=True, build_catalogue=False,
    ).build()
    assert last_batch.candidate_catalogue is None
    assert_frame_equal(last_batch.candidate_ids, result.candidate_ids)
    assert_frame_equal(last_batch.candidate_replications, result.candidate_replications)
    # The same pool must retain different IDs when scenarios start from
    # different indexes.
    old_index = result.destination_sequence_indices[1]
    different_index = old_index.with_columns(pl.col("dest_seq_id") + 10)
    different = SharedCandidatePool(
        sources=sources[:2], iteration=1, compact=True, build_catalogue=False,
        previous_destination_indices={1: old_index, 2: different_index},
    ).build()
    assert_frame_equal(different.destination_sequence_indices[1], old_index)
    assert_frame_equal(different.destination_sequence_indices[2], different_index)
    different_local_indices = SharedCandidatePool(
        sources=[sources[0], same_source], iteration=1, compact=True, build_catalogue=False,
        previous_destination_indices={1: old_index, 5: different_index},
    ).build()
    assert_frame_equal(different_local_indices.destination_sequence_indices[1], old_index)
    assert_frame_equal(different_local_indices.destination_sequence_indices[5], different_index)
    assert different_local_indices.scoring_candidate_steps_by_run_key[1] is not different_local_indices.scoring_candidate_steps_by_run_key[5]
    for run_key, expected in [(1, {2, 3}), (2, {2, 3}), (3, {3, 4}), (4, {3, 4})]:
        steps = result.candidate_plan_steps_by_run_key[run_key].collect()
        assert set(steps.filter(pl.col("seq_step_index") == 1)["to"]) == expected
        assert_frame_equal(
            steps.sort(PLAN_KEY_COLS + ["seq_step_index"]),
            last_batch.candidate_plan_steps_by_run_key[run_key].collect().sort(PLAN_KEY_COLS + ["seq_step_index"]),
        )

    # With no new generation, each replication must carry forward its own pool.
    monkeypatch.setattr(CandidatePlanStepsAsset, "build_iteration_candidates", lambda **kwargs: original.head(0).lazy())
    later = SharedCandidatePool(
        sources=sources, iteration=2, compact=True,
        previous_catalogue=result.candidate_catalogue,
        previous_destination_indices=result.destination_sequence_indices,
        previous_mode_indices=result.mode_sequence_indices,
        previous_time_sequence_steps=result.time_sequence_steps_by_run_key,
    ).build()
    old_carry_forward = SharedCandidatePool(
        sources=sources, iteration=2, compact=True,
        previous_candidates_by_run_key=previous_steps,
        previous_destination_indices=result.destination_sequence_indices,
        previous_mode_indices=result.mode_sequence_indices,
        previous_time_sequence_steps=result.time_sequence_steps_by_run_key,
    ).build()
    for run_key, expected in [(1, {2, 3}), (2, {2, 3}), (3, {3, 4}), (4, {3, 4})]:
        steps = later.candidate_plan_steps_by_run_key[run_key].collect()
        assert set(steps.filter(pl.col("seq_step_index") == 1)["to"]) == expected
        baseline = old_carry_forward.candidate_plan_steps_by_run_key[run_key].collect()
        assert_frame_equal(steps, baseline, check_exact=True)

    # A freshly generated plan refreshes both scenarios of the same seed,
    # without changing the other seed's history.
    monkeypatch.setattr(
        CandidatePlanStepsAsset, "build_iteration_candidates",
        lambda **kwargs: generate(**kwargs) if kwargs["destination_sequences"].test_destination == 2
        else original.head(0).lazy(),
    )
    refreshed = SharedCandidatePool(
        sources=sources, iteration=3, compact=True,
        previous_catalogue=later.candidate_catalogue,
        previous_destination_indices=later.destination_sequence_indices,
        previous_mode_indices=later.mode_sequence_indices,
        previous_time_sequence_steps=later.time_sequence_steps_by_run_key,
    ).build()
    second_scenario = refreshed.candidate_plan_steps_by_run_key[2].collect().filter(pl.col("seq_step_index") == 1)
    assert second_scenario.filter(pl.col("to") == 2)["last_seen_iteration"].to_list() == [3]
    assert second_scenario.filter(pl.col("to") == 3)["last_seen_iteration"].to_list() == [1]
    other_seed = refreshed.candidate_plan_steps_by_run_key[3].collect().filter(pl.col("seq_step_index") == 1)
    assert other_seed["last_seen_iteration"].unique().to_list() == [1]

    # At step 4, the plan generated in step 3 remains in both scenarios of
    # seed 0; older unused plans expire independently in each seed.
    monkeypatch.setattr(CandidatePlanStepsAsset, "build_iteration_candidates", lambda **kwargs: original.head(0).lazy())
    pruned = SharedCandidatePool(
        sources=sources, iteration=4, max_inactive_age=1, compact=True,
        previous_catalogue=refreshed.candidate_catalogue,
        previous_destination_indices=refreshed.destination_sequence_indices,
        previous_mode_indices=refreshed.mode_sequence_indices,
        previous_time_sequence_steps=refreshed.time_sequence_steps_by_run_key,
    ).build()
    assert pruned.candidate_replications["replication"].to_list() == [0]
    for run_key, expected in [(1, {2}), (2, {2}), (3, set()), (4, set())]:
        steps = pruned.candidate_plan_steps_by_run_key[run_key]
        if isinstance(steps, pl.LazyFrame):
            steps = steps.collect()
        assert set(steps.filter(pl.col("seq_step_index") == 1)["to"]) == expected

    # Occupancy in one scenario protects the physical plan for that seed,
    # but does not protect the same plan in another seed.
    previous_rows = refreshed.candidate_plan_steps_by_run_key[2].collect()
    occupied_keys = previous_rows.filter(
        (pl.col("seq_step_index") == 1) & (pl.col("to") == 3)
    ).select(PLAN_KEY_COLS)
    occupied_steps = previous_rows.join(occupied_keys, on=PLAN_KEY_COLS, how="semi")
    occupied_state = SimpleNamespace(**vars(state))
    occupied_state.current_plans = occupied_steps.select(PLAN_KEY_COLS).unique()
    occupied_state.candidate_plan_steps = occupied_steps
    occupied_state.destination_sequence_index = refreshed.destination_sequence_indices[2]
    occupied_state.mode_sequence_index = refreshed.mode_sequence_indices[2]
    occupied_asset = InMemoryAsset({"test": "occupied choice set state"})
    monkeypatch.setattr(occupied_asset, "get", lambda: occupied_state)
    occupied_sources = [replace(source, previous_state=occupied_asset) if source.run_key == 2 else source
                        for source in sources]
    occupied = SharedCandidatePool(
        sources=occupied_sources, iteration=4, max_inactive_age=1, compact=True,
        previous_catalogue=refreshed.candidate_catalogue,
        previous_destination_indices=refreshed.destination_sequence_indices,
        previous_mode_indices=refreshed.mode_sequence_indices,
        previous_time_sequence_steps=refreshed.time_sequence_steps_by_run_key,
    ).build()
    for run_key, expected in [(1, {2, 3}), (2, {2, 3}), (3, set()), (4, set())]:
        steps = occupied.candidate_plan_steps_by_run_key[run_key]
        if isinstance(steps, pl.LazyFrame):
            steps = steps.collect()
        assert set(steps.filter(pl.col("seq_step_index") == 1)["to"]) == expected

    capped = SharedCandidatePool(
        sources=occupied_sources, iteration=4, max_inactive_age=1, max_shared_candidates=1, compact=True,
        previous_catalogue=refreshed.candidate_catalogue,
        previous_destination_indices=refreshed.destination_sequence_indices,
        previous_mode_indices=refreshed.mode_sequence_indices,
        previous_time_sequence_steps=refreshed.time_sequence_steps_by_run_key,
    ).build()
    assert len(capped.candidate_ids) == 1
    for run_key in (1, 2):
        steps = capped.candidate_plan_steps_by_run_key[run_key].collect()
        assert set(steps.filter(pl.col("seq_step_index") == 1)["to"]) == {3}


def test_mode_replanning_allows_timing_adjustments_of_the_same_survey_schedule():
    """A new modeled timing id must not prevent an allowed mode change."""
    current = _plan_steps().select(PLAN_KEY_COLS).unique().with_columns(
        time_seq_id=pl.lit(2, dtype=pl.UInt32), plan_id=pl.lit(11, dtype=pl.UInt32),
        utility=pl.lit(0.0), n_persons=pl.lit(100.0),
    )
    target = current.with_columns(
        time_seq_id=pl.lit(3, dtype=pl.UInt32), mode_seq_id=pl.lit(2, dtype=pl.UInt32),
        plan_id=pl.lit(12, dtype=pl.UInt32), utility=pl.lit(5.0),
    )
    utilities = pl.concat([current, target]).with_columns(
        reference_time_seq_id=pl.lit(1, dtype=pl.UInt32), is_feasible=pl.lit(True),
    )
    allowed = PlanUpdater().build_allowed_plan_transitions(
        current, utilities.lazy(), BehaviorChangeScope.MODE_REPLANNING,
    ).collect()
    assert 12 in allowed["plan_id_trans"].to_list()
