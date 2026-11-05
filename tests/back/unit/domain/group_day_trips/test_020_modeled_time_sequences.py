"""Keep modeled schedules distinct without losing complete shared plans."""

from dataclasses import replace
from types import SimpleNamespace

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from mobility.trips.group_day_trips.core.multi_scenario import CANDIDATE_ID_COLS, add_candidate_ids
from mobility.trips.group_day_trips.plans.shared_candidate_pool import SharedCandidatePoolAsset, ScenarioCandidateSource
from mobility.trips.group_day_trips.plans.candidate_plan_steps import CandidatePlanStepsAsset
from mobility.runtime.assets.in_memory_asset import InMemoryAsset
from mobility.trips.group_day_trips.plans.time_sequences import assign_time_sequence_ids
from mobility.trips.group_day_trips.plans.plan_ids import PLAN_KEY_COLS, add_plan_id
from mobility.trips.group_day_trips.plans.plan_updater import PlanUpdater
from mobility.trips.group_day_trips.plans.plan_catalogue import PlanCatalogue
from mobility.trips.group_day_trips.plans.compact_candidate_steps import CompactCandidateSteps
from mobility.trips.group_day_trips.plans.scoring_contexts import ScoringContexts
from mobility.trips.group_day_trips.core.parameters import GroupDayTripsParameters, BehaviorChangeScope
from mobility.trips.group_day_trips.core.run_state import RunState
from mobility.trips.group_day_trips.iterations.iteration_assets import IterationStateAsset


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


def test_shared_generation_keeps_plan_content_without_expanding_demand_units():
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
    compact = CandidatePlanStepsAsset._attach_candidates_to_demand_units(
        **arguments, compact_for_shared_pool=True,
    ).collect()
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
    pool = object.__new__(SharedCandidatePoolAsset)
    expected = pool._merge_candidate_content([first, later, first])
    chunked = pool._merge_candidate_content([
        pool._merge_candidate_content([first, later]), first,
    ])
    assert_frame_equal(expected, chunked)


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

    incomplete = pl.concat([first, first.head(1).with_columns(demand_group_id=pl.lit(2, dtype=pl.UInt32))])
    assert CompactCandidateSteps.from_frame(incomplete) is incomplete


def test_compact_outer_candidates_survive_state_spill_and_restore(tmp_path):
    expanded = pl.concat([
        _plan_steps().with_columns(demand_group_id=pl.lit(index, dtype=pl.UInt32))
        for index in range(30)
    ])
    compact = CompactCandidateSteps.from_frame(expanded)
    assert isinstance(compact, CompactCandidateSteps)
    empty = pl.DataFrame()
    initial = RunState(
        survey_plans=empty, survey_plan_steps=_schedule(), demand_groups=empty,
        activity_dur=empty, home_night_dur=empty, stay_home_plan=empty,
        opportunities=empty, current_plans=empty, candidate_plan_steps=compact,
        plan_id_index=empty, destination_saturation=empty, costs=empty, start_iteration=1,
    )
    asset = object.__new__(IterationStateAsset)
    asset.iteration = 1
    asset.initial_state = SimpleNamespace(get=lambda: initial)
    asset.transport_costs = SimpleNamespace(get_costs_by_od=lambda metrics: empty)
    asset._spill_state(initial, tmp_path / "state")
    assert (tmp_path / "state" / "candidate_steps.parquet").exists()
    assert (tmp_path / "state" / "candidate_membership.parquet").exists()
    assert not (tmp_path / "state" / "candidate_plan_steps.parquet").exists()
    restored = asset._load_spilled_state(tmp_path / "state")
    assert isinstance(restored.candidate_plan_steps, CompactCandidateSteps)
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
def test_attachment_preserves_two_timings_of_the_same_route_and_modes(duration_only, compact):
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
        reference_time_seq_id=pl.lit(1, dtype=pl.UInt32),
    ).drop("dest_seq_id", "mode_seq_id")
    units = original.select("demand_group_id", "demand_subgroup_id", "country", "csp").unique().with_columns(
        home_zone_id=pl.lit(1, dtype=pl.Int32), allowed_activities=pl.lit(["work"]),
    )
    state = SimpleNamespace(schedule_steps=_schedule(), demand_groups=units)
    source = SimpleNamespace(
        run_key=1,
        scenario="default",
        previous_state=SimpleNamespace(get=lambda: state),
    )
    pool = object.__new__(SharedCandidatePoolAsset)
    pool.previous_time_sequence_steps = {}
    pool.time_sequence_steps_by_run_key = {}
    pool.compact = compact
    pool.compact_candidate_steps_by_run_key = {}

    # Physical catalog legs may have come from different original people.
    # Those representative ids must not split a complete plan during indexing.
    shared = shared.with_columns(
        demand_group_id=pl.Series([1, 2, 1, 2], dtype=pl.UInt32),
    )

    attached = pool._build_scenario_candidate_steps(
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
    if compact:
        assert isinstance(attached, pl.LazyFrame)
        assert pool.compact_candidate_steps_by_run_key[1].height == 4
        catalog = pool.compact_candidate_steps_by_run_key[1]
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
    pool = object.__new__(SharedCandidatePoolAsset)
    pool.previous_time_sequence_steps = {}
    pool.time_sequence_steps_by_run_key = {}
    pool.compact = True
    pool.compact_candidate_steps_by_run_key = {}
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
    contexts = ScoringContexts.build(
        units, SimpleNamespace(get_utility_profiles=lambda demand, segments: (assignments, {})), None,
    )
    restored = contexts.expand_step_rows(contexts.collapse_step_rows(attached)).collect()
    assert restored.height == 6
    assert restored["demand_group_id"].unique().sort().to_list() == [1, 2, 3]


def test_evaluation_remaps_current_plans_after_modeled_travel_changes_timings(monkeypatch):
    """New schedule and plan ids must still identify every transition origin."""
    steps = _plan_steps().with_columns(
        activity_utility_scale=pl.lit(1.0), min_activity_time=pl.lit(1.0),
        cost=pl.lit(0.0), time=pl.lit(2.0), distance=pl.lit(1.0),
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
        arrival_time_rigidity_by_activity={"work": 0.0, "home": 0.0},
        destination_sequences=None, mode_sequences=None, previous_plan_id_index=index,
        parameters=GroupDayTripsParameters(plan_update={"update_plan_timings_from_modeled_travel_times": True}),
    )
    assert evaluation.current_plans["time_seq_id"].item() > 1
    assert evaluation.current_plans["n_persons"].sum() == 100.0
    assert evaluation.current_plans.join(
        evaluation.possible_plan_utility, on=PLAN_KEY_COLS, how="anti"
    ).height == 0
    assert evaluation.candidate_plan_steps["time_seq_id"].n_unique() == 1


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
        plan_id_index=empty, destination_saturation=empty, costs=empty, start_iteration=1,
    )
    state = RunState(**{**vars(initial), "time_sequence_steps": catalog, "current_plan_steps": steps})
    asset = object.__new__(IterationStateAsset)
    asset.iteration = 1
    asset.initial_state = SimpleNamespace(get=lambda: initial)
    asset.transport_costs = SimpleNamespace(get_costs_by_od=lambda metrics: empty)

    asset._spill_state(state, tmp_path / "state")
    restored = asset._load_spilled_state(tmp_path / "state")
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
    pool = SharedCandidatePoolAsset(
        sources=[source, other_source], iteration=2, previous_candidates_by_run_key={1: original},
        previous_time_sequence_steps={1: catalog},
    ).create_and_get_asset()

    keys = pool.candidate_plan_steps_by_run_key[1].select(PLAN_KEY_COLS).unique()
    assert state.current_plans.join(keys, on=PLAN_KEY_COLS, how="anti").height == 0
    assert len(pool.candidate_ids) == 2
    other_steps = pool.candidate_plan_steps_by_run_key[2]
    assert other_steps["last_active_iteration"].null_count() == other_steps.height
    assert other_steps["first_seen_iteration"].unique().to_list() == [2]
    active_steps = pool.candidate_plan_steps_by_run_key[1].join(state.current_plans, on=PLAN_KEY_COLS)
    assert active_steps["last_active_iteration"].unique().to_list() == [2]


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
        previous_steps[run_id] = candidates.select(
            CandidatePlanStepsAsset.STRUCTURAL_COLUMNS + CandidatePlanStepsAsset.RETENTION_COLUMNS
        )
        return original_catalogue_builder(candidates)

    monkeypatch.setattr(PlanCatalogue, "from_candidates", classmethod(capture_catalogue_input))
    result = SharedCandidatePoolAsset(sources=sources, iteration=1, compact=True).create_and_get_asset()
    monkeypatch.setattr(PlanCatalogue, "from_candidates", original_catalogue_builder)
    assert len(result.candidate_ids) == 3
    assert result.candidate_replications.group_by("replication").len().sort("replication")["len"].to_list() == [2, 2]
    assert result.candidate_catalogue.plans.height == 3
    assert result.candidate_catalogue.steps.height == 6
    assert result.candidate_catalogue.membership.height == 8
    for run_key, expected in [(1, {2, 3}), (2, {2, 3}), (3, {3, 4}), (4, {3, 4})]:
        steps = result.candidate_plan_steps_by_run_key[run_key].collect()
        assert set(steps.filter(pl.col("seq_step_index") == 1)["to"]) == expected

    # With no new generation, each replication must carry forward its own pool.
    monkeypatch.setattr(CandidatePlanStepsAsset, "build_iteration_candidates", lambda **kwargs: original.head(0).lazy())
    later = SharedCandidatePoolAsset(
        sources=sources, iteration=2, compact=True,
        previous_catalogue=result.candidate_catalogue,
        previous_destination_indices=result.destination_sequence_indices,
        previous_mode_indices=result.mode_sequence_indices,
        previous_time_sequence_steps=result.time_sequence_steps_by_run_key,
    ).create_and_get_asset()
    old_carry_forward = SharedCandidatePoolAsset(
        sources=sources, iteration=2, compact=True,
        previous_candidates_by_run_key=previous_steps,
        previous_destination_indices=result.destination_sequence_indices,
        previous_mode_indices=result.mode_sequence_indices,
        previous_time_sequence_steps=result.time_sequence_steps_by_run_key,
    ).create_and_get_asset()
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
    refreshed = SharedCandidatePoolAsset(
        sources=sources, iteration=3, compact=True,
        previous_catalogue=later.candidate_catalogue,
        previous_destination_indices=later.destination_sequence_indices,
        previous_mode_indices=later.mode_sequence_indices,
        previous_time_sequence_steps=later.time_sequence_steps_by_run_key,
    ).create_and_get_asset()
    second_scenario = refreshed.candidate_plan_steps_by_run_key[2].collect().filter(pl.col("seq_step_index") == 1)
    assert second_scenario.filter(pl.col("to") == 2)["last_seen_iteration"].to_list() == [3]
    assert second_scenario.filter(pl.col("to") == 3)["last_seen_iteration"].to_list() == [1]
    other_seed = refreshed.candidate_plan_steps_by_run_key[3].collect().filter(pl.col("seq_step_index") == 1)
    assert other_seed["last_seen_iteration"].unique().to_list() == [1]


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
