"""Keep modeled schedules distinct without losing complete shared plans."""

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
    modes.transport_costs = SimpleNamespace(get_utility_profiles=lambda demand, segments: (pl.DataFrame(), {}))
    source = ScenarioCandidateSource(
        run_key=1, scenario="default", previous_state=previous, destination_sequences=destinations,
        mode_sequences=modes, iteration=2,
        run=SimpleNamespace(run_context=SimpleNamespace(inputs_hash="test"), population=SimpleNamespace(population_segments=[])),
    )
    monkeypatch.setattr(CandidatePlanStepsAsset, "build_iteration_candidates", lambda **kwargs: original.head(0).lazy())
    pool = SharedCandidatePoolAsset(
        sources=[source], iteration=2, previous_candidates_by_run_key={1: original},
        previous_time_sequence_steps={1: catalog},
    ).create_and_get_asset()

    keys = pool.candidate_plan_steps_by_run_key[1].select(PLAN_KEY_COLS).unique()
    assert state.current_plans.join(keys, on=PLAN_KEY_COLS, how="anti").height == 0
    assert len(pool.candidate_ids) == 2


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
