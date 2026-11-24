"""Search estimates must never replace the actual behavioral allocation."""

from types import SimpleNamespace
from contextlib import contextmanager, nullcontext
import json
import gc
import weakref

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from mobility.trips.group_day_trips.core.multi_scenario_run import MultiScenarioRunAsset
from mobility.trips.group_day_trips.core.coordinated_run_result import current_coordinated_result
from mobility.trips.group_day_trips.plans.shared_candidate_pool import SharedCandidatePoolResult
import mobility.trips.group_day_trips.core.multi_scenario_run as coordinated_module
import mobility.trips.group_day_trips.results.simulation_runner as runner_module
from mobility.trips.group_day_trips.core.parameters import GroupDayTripsParameters
from mobility.trips.group_day_trips.core.run_state import RunState
from mobility.trips.group_day_trips.core.run import Run
from mobility.trips.group_day_trips.iterations.iterations import Iterations
from mobility.trips.group_day_trips.iterations.iteration_assets import CurrentPlansAsset, IterationStateAsset
import mobility.trips.group_day_trips.iterations.iteration_assets as iteration_assets_module
from mobility.trips.group_day_trips.transitions.transition_schema import TRANSITION_EVENT_SCHEMA
from mobility.runtime.assets.resolver import AssetResolver, asset_resolution_context
from mobility.runtime.assets.in_memory_asset import InMemoryAsset


@pytest.mark.parametrize("coordinated", [True, False])
def test_previous_state_lifetime_accounts_for_consumed_precomputed_evaluation(monkeypatch, coordinated):
    """A scenario must not release a prior state still needed by its cohort."""
    asset = object.__new__(IterationStateAsset)
    asset.coordinated_candidate_plan_steps = None
    asset.coordinated_evaluation = object() if coordinated else None
    asset.previous_state = object()
    asset.seeds = object()
    asset.activity_sequences = object()
    asset.destination_sequences = SimpleNamespace(sequence_index=None, previous_sequence_index=None)
    asset.mode_sequences = SimpleNamespace(sequence_index=None, previous_sequence_index=None)
    cleared = []
    asset.transport_costs = SimpleNamespace(release_profile_costs=lambda: cleared.append(True))
    asset.iteration = 1
    asset.parameters = SimpleNamespace(run=SimpleNamespace(n_steps=1))
    asset.checkpoint_callback = None
    released = []

    def get_state(current, *args, **kwargs):
        # The actual state builder consumes this evaluation before returning.
        current.coordinated_evaluation = None
        return SimpleNamespace(rng_state=(1, 2, 3))

    resolver = SimpleNamespace(
        execution_cost_consumer=lambda current: nullcontext(),
        get_in_memory_asset=get_state,
        release_in_memory_asset=released.append,
        consume_execution_cost_state=lambda current: None,
    )
    monkeypatch.setattr(iteration_assets_module, "get_current_asset_resolver", lambda: resolver)
    asset.get()
    assert cleared == [True]
    assert (asset.previous_state in released) is (not coordinated)


def test_restored_cached_state_does_not_rewrite_checkpoint(monkeypatch):
    """A resolver may return an already restored state without running its loader again."""
    asset = object.__new__(IterationStateAsset)
    asset.coordinated_candidate_plan_steps = None
    asset.coordinated_evaluation = None
    asset.previous_state = asset.seeds = asset.activity_sequences = object()
    asset.destination_sequences = SimpleNamespace(sequence_index=None, previous_sequence_index=None)
    asset.mode_sequences = SimpleNamespace(sequence_index=None, previous_sequence_index=None)
    asset.transport_costs = SimpleNamespace()
    asset.iteration = 1
    asset.parameters = SimpleNamespace(run=SimpleNamespace(n_steps=1))
    asset._checkpoint_saved = False
    asset._loaded_checkpoint = False
    asset.resume_checkpoint_loader = lambda: None
    writes = []
    asset.checkpoint_callback = lambda *args: writes.append(args)
    resolver = SimpleNamespace(execution_cost_consumer=lambda current: nullcontext(),
        get_in_memory_asset=lambda *args, **kwargs: SimpleNamespace(rng_state=(1, 2, 3)),
        release_in_memory_asset=lambda value: None, consume_execution_cost_state=lambda value: None)
    monkeypatch.setattr(iteration_assets_module, "get_current_asset_resolver", lambda: resolver)
    asset.get()
    assert writes == []
    assert asset._checkpoint_saved


def test_transition_history_scans_recorded_files(tmp_path):
    """A requested transition history must not load every step at once."""
    run = object.__new__(Run)
    run.parameters = SimpleNamespace(outputs=SimpleNamespace(cache_step_events=True))
    paths = [tmp_path / "step-1.parquet", tmp_path / "step-2.parquet"]
    for path in paths:
        pl.DataFrame(schema=TRANSITION_EVENT_SCHEMA).write_parquet(path)
    run.iteration_state_assets = [
        SimpleNamespace(transition_events_asset=SimpleNamespace(cache_path=path))
        for path in paths
    ]
    table = run._build_transitions()
    assert isinstance(table, pl.LazyFrame)
    assert table.collect().schema == pl.Schema(TRANSITION_EVENT_SCHEMA)
    paths[1].unlink()
    with pytest.raises(RuntimeError, match=r"missing.*\[2\]"):
        run._build_transitions()
    # During a live step, future event files are not expected yet.
    run.coordinated_run_asset = SimpleNamespace()
    with asset_resolution_context() as resolver:
        resolver.simulation_coordinator = run.coordinated_run_asset
        resolver.simulation_step_result = SimpleNamespace(current_iteration=1)
        assert run._build_transitions().collect().schema == pl.Schema(TRANSITION_EVENT_SCHEMA)


def test_direct_run_get_requests_only_the_final_state(monkeypatch):
    """A direct final-state request should not retain the run history."""
    expected = behavioral_state(20)
    seen = []

    class Coordinator(InMemoryAsset):
        def __init__(self):
            super().__init__({"version": 1})
            self.run_keys = [0]
            self._cached_result = None
            self._is_running = False
            self.parameters = SimpleNamespace(run=SimpleNamespace(n_steps=2),
                                              outputs=SimpleNamespace(checkpoint_each_step=False))

        def get(self, *, step_consumer=None, requirements=None):
            assert step_consumer is None
            assert requirements.steps == frozenset()
            assert requirements.mnl_flows is False
            for iteration in (1, 2):
                seen.append(iteration)
            return SimpleNamespace(final_states_by_run_key={0: expected})

    run = object.__new__(Run)
    run.enabled = True
    run.is_weekday = True
    run.parameters = SimpleNamespace(run=SimpleNamespace(n_steps=2))
    run.coordinated_run_asset = Coordinator()
    run.coordinated_run_key = 0
    monkeypatch.setattr(
        "mobility.trips.group_day_trips.core.run.is_group_day_trips_progress_active",
        lambda: True,
    )
    assert run._execute() is expected
    assert seen == [1, 2]
    assert run.coordinated_run_asset._cached_result is None


def test_run_get_explains_how_to_access_results():
    run = object.__new__(Run)
    with pytest.raises(RuntimeError, match=r"run\.table.*run\.results"):
        run.get()


@pytest.mark.parametrize("contents,expected_resume", [
    ('{"version": 4, "cohort_hash": "cohort", "iteration": 1, "diagnostics": [], "mnl_flows": {}}', True),
    ('{"version": 4, "cohort_hash": "other", "iteration": 1, "diagnostics": [], "mnl_flows": {}}', False),
    ('{"version": 4, "cohort_hash": "cohort", "iteration": 2, "diagnostics": [], "mnl_flows": {}}', False),
    ('{"version": 4, "cohort_hash": "cohort", "iteration": true, "diagnostics": [], "mnl_flows": {}}', False),
    ('{"version": 4, "cohort_hash": "cohort", "iteration": 1, "diagnostics": {}, "mnl_flows": {}}', False),
    ('{"cohort_hash":', False),
])
def test_checkpoint_manifest_must_match_complete_cohort(tmp_path, contents, expected_resume):
    manifest = tmp_path / "manifest.json"
    manifest.write_text(contents, encoding="utf-8")
    loaders = [lambda: None, lambda: None]
    runs = {
        key: SimpleNamespace(
            iterations=SimpleNamespace(
                iteration=lambda step: SimpleNamespace(
                    checkpoint_assets=lambda: {"state": SimpleNamespace(is_update_needed=lambda: False)},
                    load_state=lambda: None,
                )
            ),
            iteration_state_assets=[SimpleNamespace(iteration=1, resume_checkpoint_loader=loaders[key])],
        )
        for key in range(2)
    }
    coordinator = object.__new__(MultiScenarioRunAsset)
    coordinator.runs = runs
    coordinator.parameters = SimpleNamespace(
        outputs=SimpleNamespace(checkpoint_each_step=True, cache_step_events=False), run=SimpleNamespace(n_steps=2)
    )
    coordinator._checkpoint_cohort_hash = "cohort"
    coordinator._checkpoint_manifest_path = lambda iteration: manifest
    coordinator._prepare_checkpoint_resume()
    assert all(run.iteration_state_assets[0].resume_checkpoint_loader is not None
               for run in runs.values()) is expected_resume


def test_checkpoint_manifest_is_published_atomically(tmp_path, monkeypatch):
    coordinator = object.__new__(MultiScenarioRunAsset)
    coordinator.parameters = SimpleNamespace(outputs=SimpleNamespace(checkpoint_each_step=True, cache_step_events=False))
    coordinator._checkpoint_cohort_hash = "cohort"
    target = tmp_path / "manifest.json"
    coordinator._checkpoint_manifest_path = lambda iteration: target
    coordinator._write_checkpoint_manifest(1)
    assert json.loads(target.read_text(encoding="utf-8")) == {
        "version": 4, "cohort_hash": "cohort", "iteration": 1, "diagnostics": [], "mnl_flows": {}
    }

    diagnostics = [{"flow_change_by_mode": [{"category": "car", "trips": 2.0, "flow_change_index": 0.1}]}]
    coordinator._write_checkpoint_manifest(1, diagnostics=diagnostics, mnl_flows={0: pl.DataFrame({"volume": [2.0]})})
    saved = json.loads(target.read_text(encoding="utf-8"))
    assert list(saved["diagnostics"][0]["flow_change_by_mode"][0]) == ["category", "trips", "flow_change_index"]
    assert pl.read_parquet(saved["mnl_flows"]["0"]["path"])["volume"].to_list() == [2.0]
    original = target.read_bytes()

    def fail_replace(source, destination):
        raise OSError("publication failed")

    monkeypatch.setattr("mobility.trips.group_day_trips.core.multi_scenario_run.os.replace", fail_replace)
    with pytest.raises(OSError, match="publication failed"):
        coordinator._write_checkpoint_manifest(1)
    assert target.read_bytes() == original
    assert sorted(path.name for path in tmp_path.iterdir()) == ["manifest.json", "manifest_mnl_0.parquet"]


def test_recomputing_a_step_invalidates_later_cohort_markers(tmp_path):
    coordinator = object.__new__(MultiScenarioRunAsset)
    coordinator._checkpoint_cohort_hash = "cohort"
    coordinator._checkpoint_manifest_path = lambda step: tmp_path / f"cohort_iteration_{step}.json"
    for step in (1, 2, 3):
        coordinator._checkpoint_manifest_path(step).write_text("{}", encoding="utf-8")
    (tmp_path / "other_iteration_3.json").write_text("{}", encoding="utf-8")
    coordinator._invalidate_checkpoint_manifests_from(2)
    assert sorted(path.name for path in tmp_path.iterdir()) == ["cohort_iteration_1.json", "other_iteration_3.json"]


def test_resume_requires_final_inner_flows_only_when_requested(tmp_path):
    coordinator = object.__new__(MultiScenarioRunAsset)
    coordinator.parameters = SimpleNamespace(
        outputs=SimpleNamespace(cache_step_events=False), run=SimpleNamespace(n_steps=1),
    )
    coordinator.runs = {0: object()}
    coordinator._checkpoint_cohort_hash = "cohort"
    manifest = tmp_path / "manifest.json"
    coordinator._checkpoint_manifest_path = lambda step: manifest
    manifest.write_text(json.dumps({
        "version": 4, "cohort_hash": "cohort", "iteration": 1,
        "diagnostics": [], "mnl_flows": {"0": {"path": str(tmp_path / "missing.parquet")}},
    }), encoding="utf-8")
    assert coordinator._checkpoint_manifest_matches(1, needs_mnl_flows=False)
    assert not coordinator._checkpoint_manifest_matches(1, needs_mnl_flows=True)


@pytest.mark.parametrize("error", [OSError("disk failure"), KeyboardInterrupt()])
def test_interrupted_checkpoint_rewrite_removes_old_input_marker(tmp_path, monkeypatch, error):
    iterations = Iterations(run_inputs_hash="run", is_weekday=True, base_folder=tmp_path)
    checkpoint = iterations.iteration(1)
    saved = checkpoint.checkpoint_assets()["current_plans"]
    saved.cache_path.write_bytes(b"previous state")
    saved.update_hash(saved.inputs_hash)
    assert not saved.is_update_needed()

    def fail_after_partial_write(asset):
        asset.cache_path.parent.mkdir(parents=True, exist_ok=True)
        asset.cache_path.write_bytes(b"incomplete")
        raise error

    monkeypatch.setattr(CurrentPlansAsset, "create_and_get_asset", fail_after_partial_write)
    expected = KeyboardInterrupt if isinstance(error, KeyboardInterrupt) else RuntimeError
    with pytest.raises(expected):
        checkpoint.save_state(SimpleNamespace(current_plans=pl.DataFrame()), rng_state={})
    assert saved.is_update_needed()
    assert not iterations.has_complete_checkpoints(1)


def test_missing_transition_events_disable_resume_for_the_whole_cohort(tmp_path):
    coordinator = object.__new__(MultiScenarioRunAsset)
    coordinator.parameters = SimpleNamespace(
        outputs=SimpleNamespace(checkpoint_each_step=True, cache_step_events=True),
        run=SimpleNamespace(n_steps=1),
    )
    coordinator._checkpoint_cohort_hash = "cohort"
    coordinator._checkpoint_manifest_path = lambda iteration: tmp_path / "manifest.json"
    states = []
    coordinator.runs = {}
    for key in range(2):
        state_path = tmp_path / f"state-{key}"
        event_path = tmp_path / f"events-{key}"
        state_path.write_bytes(b"state")
        event_path.write_bytes(b"events")
        state = SimpleNamespace(
            iteration=1, resume_checkpoint_loader=lambda: None,
            transition_events_asset=SimpleNamespace(cache_path=event_path, is_update_needed=lambda path=event_path: not path.exists()),
        )
        states.append(state)
        checkpoint = SimpleNamespace(checkpoint_assets=lambda path=state_path: {
            "state": SimpleNamespace(cache_path=path, is_update_needed=lambda: False),
        }, load_state=lambda: None)
        coordinator.runs[key] = SimpleNamespace(
            iteration_state_assets=[state],
            iterations=SimpleNamespace(
                iteration=lambda iteration, checkpoint=checkpoint: checkpoint,
            ),
        )

    coordinator._write_checkpoint_manifest(1, mnl_flows={key: pl.DataFrame() for key in coordinator.runs})
    coordinator._prepare_checkpoint_resume()
    assert all(state.resume_checkpoint_loader is not None for state in states)
    states[0].transition_events_asset.cache_path.unlink()
    coordinator._prepare_checkpoint_resume()
    assert all(state.resume_checkpoint_loader is None for state in states)


@pytest.mark.parametrize("missing_kind,missing_step,expected", [
    ("scenario", 10, 9), ("manifest", 10, 9), ("events", 7, 6),
    ("scenario", 1, 0), ("manifest", 1, 0),
])
def test_resume_uses_last_complete_shared_step(tmp_path, missing_kind, missing_step, expected):
    coordinator = object.__new__(MultiScenarioRunAsset)
    coordinator.parameters = SimpleNamespace(
        outputs=SimpleNamespace(checkpoint_each_step=True, cache_step_events=True),
        run=SimpleNamespace(n_steps=10),
    )
    coordinator._checkpoint_cohort_hash = "cohort"
    coordinator._checkpoint_manifest_path = lambda step: tmp_path / f"{step}.json"
    coordinator.runs = {}
    for key in (0, 1):
        states = [SimpleNamespace(
            iteration=step, resume_checkpoint_loader=lambda: "stale",
            transition_events_asset=SimpleNamespace(
                is_update_needed=lambda step=step: missing_kind == "events" and step == missing_step,
            ),
        ) for step in range(1, 11)]
        def checkpoint(step, key=key):
            incomplete = missing_kind == "scenario" and key == 1 and step == missing_step
            return SimpleNamespace(
                checkpoint_assets=lambda: {"state": SimpleNamespace(is_update_needed=lambda: incomplete)},
                load_state=lambda: None,
            )
        coordinator.runs[key] = SimpleNamespace(
            iteration_state_assets=states, iterations=SimpleNamespace(iteration=checkpoint),
        )
    for step in range(1, 11):
        if missing_kind == "manifest" and step == missing_step:
            continue
        flows = {}
        if step == 10:
            for key in (0, 1):
                path = tmp_path / f"flow-{key}.parquet"
                path.touch()
                flows[str(key)] = {"path": str(path)}
        coordinator._checkpoint_manifest_path(step).write_text(json.dumps({
            "version": 4, "cohort_hash": "cohort", "iteration": step,
            "diagnostics": [], "mnl_flows": flows,
        }), encoding="utf-8")
    assert coordinator._prepare_checkpoint_resume() == expected
    for run in coordinator.runs.values():
        assert [state.iteration for state in run.iteration_state_assets
                if state.resume_checkpoint_loader is not None] == list(range(1, expected + 1))


def behavioral_state(active_people):
    plans = pl.DataFrame({
        "demand_group_id": [1, 1], "demand_subgroup_id": [0, 0],
        "activity_seq_id": [1, 0], "time_seq_id": [1, 0],
        "dest_seq_id": [1, 0], "mode_seq_id": [1, 0],
        "n_persons": [float(active_people), 100.0 - active_people],
        "utility": [1.0, 0.0], "plan_id": [1, 0],
    })
    steps = pl.DataFrame({
        "from": [1], "to": [2], "mode": ["car"], "activity": ["work"],
        "mode_seq_id": [1], "n_persons": [float(active_people)],
    })
    return RunState(
        survey_plans=pl.DataFrame(), survey_plan_steps=pl.DataFrame(),
        demand_groups=pl.DataFrame({"n_persons": [100.0]}),
        activity_dur=pl.DataFrame(), home_night_dur=pl.DataFrame(),
        stay_home_plan=pl.DataFrame(), opportunities=pl.DataFrame(),
        current_plans=plans, candidate_plan_steps=None, plan_id_index=pl.DataFrame(),
        destination_saturation=pl.DataFrame(), costs=pl.DataFrame(), start_step=1,
        current_plan_steps=steps,
    )


def test_coordinated_resume_restores_diagnostics_without_repeating_search(tmp_path, monkeypatch):
    """A saved first step supplies both state and history to the next step."""
    parameters = GroupDayTripsParameters(run={"n_steps": 2}, outputs={"checkpoint_each_step": True})
    initial = behavioral_state(20)
    assets = [SimpleNamespace(iteration=i, resume_checkpoint_loader=lambda: None,
                              get=lambda i=i: behavioral_state(20 + 10 * i)) for i in (1, 2)]
    run = SimpleNamespace(parameters=parameters, scenario="default", sensitivity_case_id="base", replication=0,
                          initial_iteration_state=SimpleNamespace(get=lambda: initial), iteration_state_assets=assets,
                          population=SimpleNamespace(parameters=SimpleNamespace(sample_size=1)))
    coordinator = SimpleNamespace(parameters=parameters, runs={0: run}, run_keys=[0])
    coordinator._run_steps = lambda **kwargs: MultiScenarioRunAsset._run_steps(coordinator, **kwargs)
    coordinator._log_plan_distribution = lambda **kwargs: None
    coordinator._clear_step_inputs = lambda asset: None
    coordinator._checkpoint_manifest_path = lambda step: tmp_path / f"{step}.json"
    coordinator._invalidate_checkpoint_manifests_from = lambda step: None
    calls = []

    def advance(step, states, **kwargs):
        calls.append(step)
        assert_frame_equal(states[0].current_plans, behavioral_state(20 + 10 * (step - 1)).current_plans)
        state = behavioral_state(20 + 10 * step)
        inner = {"diagnostics": [], "search_batches": 1, "generation_effort_used": 1,
                 "activity_profile_cell_count_by_scenario": {0: 0},
                 "covered_activity_profile_cell_count_by_scenario": {0: 0},
                 "pool": SimpleNamespace(candidate_ids=[1]), "scored_candidate_scope_by_scenario": {0: "all"},
                 "infeasible_counts": {0: 0}, "missing_cost_row_counts": {0: 0}, "invalid_cost_row_counts": {0: 0},
                 "convergence_by_run_key": {0: SimpleNamespace(flow_changes={}, base_gap_changes={}, converged=False)},
                 "mnl_od_mode_flows": {}}
        return {0: assets[step - 1]}, {0: state}, inner

    coordinator._advance_cohort_step = advance
    coordinator._write_checkpoint_manifest = lambda step, diagnostics, mnl_flows: (
        coordinator._checkpoint_manifest_path(step).write_text(
            json.dumps({"diagnostics": diagnostics, "mnl_flows": {}}), encoding="utf-8")
    )
    monkeypatch.setattr(runner_module.SimulationRunner, "_final_requirements", lambda *args: (False, False))
    runner = runner_module.SimulationRunner(requests=[], resolver=None)
    coordinator._prepare_checkpoint_resume = lambda **kwargs: 0
    baseline = coordinator._run_steps(resolver=None, step_consumer=lambda step, result: None,
                                      requirements=runner._simulation_requirements(coordinator))
    baseline_diagnostics = baseline.final_states_by_run_key[0].adaptive_search_diagnostics
    assert calls == [1, 2]

    calls.clear()
    coordinator._prepare_checkpoint_resume = lambda **kwargs: 1
    consumed = []
    resumed = coordinator._run_steps(resolver=None, step_consumer=lambda step, result: consumed.append(step),
                                     requirements=runner._simulation_requirements(coordinator))
    assert calls == [2]
    assert consumed == [1, 2]
    assert_frame_equal(resumed.final_states_by_run_key[0].current_plans, baseline.final_states_by_run_key[0].current_plans)
    assert_frame_equal(resumed.final_states_by_run_key[0].adaptive_search_diagnostics, baseline_diagnostics)


@pytest.mark.parametrize("resume_step,requested,expected_loads,expected_steps", [
    (10, set(), [10], []),
    (7, {3, 10}, [3, 7], [3, 10]),
])
def test_resume_loads_only_requested_states_and_restart_state(
    tmp_path, monkeypatch, resume_step, requested, expected_loads, expected_steps,
):
    parameters = GroupDayTripsParameters(run={"n_steps": 10}, outputs={"checkpoint_each_step": True})
    loaded = {0: [], 1: []}
    advanced = []
    runs = {}
    for key in (0, 1):
        def load(step, key=key):
            loaded[key].append(step)
            return behavioral_state(20 + step)
        assets = [SimpleNamespace(iteration=step, resume_checkpoint_loader=None,
                                  get=lambda step=step, load=load: load(step)) for step in range(1, 11)]
        runs[key] = SimpleNamespace(
            parameters=parameters, scenario=str(key), sensitivity_case_id="base", replication=0,
            initial_iteration_state=SimpleNamespace(get=lambda: behavioral_state(20)),
            iteration_state_assets=assets, population=SimpleNamespace(parameters=SimpleNamespace(sample_size=1)),
        )
    coordinator = SimpleNamespace(parameters=parameters, runs=runs, run_keys=[0, 1])
    coordinator._run_steps = lambda **kwargs: MultiScenarioRunAsset._run_steps(coordinator, **kwargs)
    coordinator._prepare_checkpoint_resume = lambda **kwargs: resume_step
    coordinator._log_plan_distribution = lambda **kwargs: None
    coordinator._clear_step_inputs = lambda asset: None
    coordinator._invalidate_checkpoint_manifests_from = lambda step: None
    coordinator._checkpoint_manifest_path = lambda step: tmp_path / f"{step}.json"
    for step in range(1, resume_step + 1):
        coordinator._checkpoint_manifest_path(step).write_text(json.dumps({
            "diagnostics": [{"_run_key": key, "outer_step": step} for key in runs], "mnl_flows": {},
        }), encoding="utf-8")

    def advance(step, states, **kwargs):
        advanced.append(step)
        assert all(states[key].current_plans.equals(behavioral_state(19 + step).current_plans) for key in runs)
        values = {key: behavioral_state(20 + step) for key in runs}
        convergence = SimpleNamespace(flow_changes={}, base_gap_changes={}, converged=False)
        search = {"diagnostics": [], "search_batches": 1, "generation_effort_used": 1,
                  "activity_profile_cell_count_by_scenario": {key: 0 for key in runs},
                  "covered_activity_profile_cell_count_by_scenario": {key: 0 for key in runs},
                  "pool": SimpleNamespace(candidate_ids=[1]),
                  "scored_candidate_scope_by_scenario": {key: "all" for key in runs},
                  "infeasible_counts": {key: 0 for key in runs},
                  "missing_cost_row_counts": {key: 0 for key in runs},
                  "invalid_cost_row_counts": {key: 0 for key in runs},
                  "convergence_by_run_key": {key: convergence for key in runs}, "mnl_od_mode_flows": {}}
        return {key: runs[key].iteration_state_assets[step - 1] for key in runs}, values, search

    coordinator._advance_cohort_step = advance
    coordinator._write_checkpoint_manifest = lambda *args, **kwargs: None
    monkeypatch.setattr(runner_module.SimulationRunner, "_final_requirements", lambda *args: (False, False))
    consumed = []
    runner = runner_module.SimulationRunner(requests=[], resolver=None)
    result = coordinator._run_steps(
        resolver=None, step_consumer=lambda step, result: consumed.append(step),
        requirements=runner._simulation_requirements(coordinator, required_iterations=requested),
    )
    assert all(steps == expected_loads for steps in loaded.values())
    assert consumed == expected_steps
    assert advanced == list(range(resume_step + 1, 11))
    assert all(result.final_states_by_run_key[key].adaptive_search_diagnostics.height == 10 for key in runs)


def test_cohort_step_uses_all_pre_step_states_before_updating_scenarios(monkeypatch):
    """Search once for the cohort, then update each scenario from its own state."""
    states = {0: behavioral_state(20), 1: behavioral_state(40)}
    assets = {key: SimpleNamespace(transport_costs=object()) for key in states}
    runs = {
        key: SimpleNamespace(
            scenario=str(key), replication=0, sensitivity_case_id="base",
            iteration_state_assets=[assets[key]],
        )
        for key in states
    }
    coordinator = object.__new__(MultiScenarioRunAsset)
    coordinator.runs = runs
    coordinator.run_keys = list(runs)
    coordinator.parameters = GroupDayTripsParameters(run={"n_steps": 1})
    seen = []
    released = {}
    for key, asset in assets.items():
        def update(key=key, asset=asset):
            # Both members must be attached before either update begins.
            assert all(hasattr(value, "coordinated_rng_state") for value in assets.values())
            assert asset.coordinated_current_plans is states[key].current_plans
            assert asset.coordinated_rng_state == ("rng", key)
            if key == 1 and "first_evaluation" in released:
                assert released["first_evaluation"]() is None
            seen.append(key)
            return behavioral_state(50 + key)
        asset.get = update

    pool = SharedCandidatePoolResult(
        candidate_plan_steps_by_run_key={}, generated_mode_sequences_by_run_key={}, candidate_ids=pl.DataFrame(),
        time_sequence_steps_by_run_key={}, candidate_replications=pl.DataFrame(),
        destination_sequence_indices={key: None for key in states},
        mode_sequence_indices={key: None for key in states},
    )
    inner_result = {
        "pool": pool,
        "candidate_plan_steps": {key: None for key in states},
        "plan_id_indices": {key: None for key in states},
        "rng_states": {key: ("rng", key) for key in states},
        "time_sequence_steps": {key: None for key in states},
        "flows_by_scenario": {},
    }
    class Evaluation:
        pass
    inner_result["evaluations"] = {key: Evaluation() for key in states}
    released["first_evaluation"] = weakref.ref(inner_result["evaluations"][0])
    def search(**kwargs):
        assert kwargs["fixed_states"] is states
        assert kwargs["state_assets"] == assets
        assert not seen
        inner_result["flows_by_scenario"] = {}
        inner_result["mnl_flows_by_scenario"] = {}
        return inner_result

    monkeypatch.setattr(coordinator, "_run_inner_loop", search)
    monkeypatch.setattr(
        "mobility.trips.group_day_trips.core.multi_scenario_run.prepare_shared_congested_costs",
        lambda costs: None,
    )
    returned_assets, next_states, result = coordinator._advance_cohort_step(1, states)
    assert returned_assets == assets
    assert seen == [0, 1]
    assert [next_states[key].current_plans["n_persons"][0] for key in states] == [50, 51]
    assert result is inner_result

    # A failed scenario must not leave scoring tables attached to other assets.
    seen.clear()
    inner_result["evaluations"] = {key: Evaluation() for key in states}
    released["first_evaluation"] = weakref.ref(inner_result["evaluations"][0])
    def fail_update():
        raise RuntimeError("scenario failed")
    assets[1].get = fail_update
    with pytest.raises(RuntimeError, match="scenario failed"):
        coordinator._advance_cohort_step(1, states)
    assert all(asset.coordinated_evaluation is None for asset in assets.values())
    assert inner_result["evaluations"] == {}


@pytest.mark.parametrize("iterations", [1, 2])
def test_outer_origins_and_final_trips_keep_behavioral_population(iterations, monkeypatch, tmp_path):
    monkeypatch.setattr(coordinated_module, "prepare_shared_congested_costs", lambda assets: None)
    parameters = GroupDayTripsParameters(run={"n_steps": iterations})
    initial = behavioral_state(20)
    origins = []
    inner_calls = []
    previous_search_values = []
    final_cost_iterations = []
    active_outer_step = []
    diagnostic_steps = []
    state_assets = []

    @contextmanager
    def track_step(name, **kwargs):
        if name == "simulation step":
            active_outer_step.append(kwargs["step"])
        try:
            yield
        finally:
            if name == "simulation step":
                active_outer_step.pop()

    original_flow_reducer = coordinated_module.represented_od_mode_flows
    def reduce_flows(plan_steps, **kwargs):
        if not kwargs.get("include_activity"):
            diagnostic_steps.append(active_outer_step[-1] if active_outer_step else None)
        return original_flow_reducer(plan_steps, **kwargs)

    monkeypatch.setattr(coordinated_module, "simulation_step", track_step)
    monkeypatch.setattr(coordinated_module, "represented_od_mode_flows", reduce_flows)

    for iteration in range(1, iterations + 1):
        asset = SimpleNamespace(iteration=iteration, inputs_hash=f"state-{iteration}", transport_costs=None)

        def update(asset=asset, iteration=iteration):
            expected = behavioral_state(20 + 10 * (iteration - 1))
            assert_frame_equal(asset.coordinated_current_plans, expected.current_plans)
            assert_frame_equal(asset.coordinated_current_plan_steps, expected.current_plan_steps)
            origins.append(asset.coordinated_current_plans["n_persons"][0])
            return behavioral_state(20 + 10 * iteration)

        asset.get = update
        state_assets.append(asset)

    run = SimpleNamespace(
        parameters=parameters, run_context=SimpleNamespace(inputs_hash="behavior-test"),
        scenario="default", sensitivity_case_id="default", replication=1,
        iterations=SimpleNamespace(run_inputs_hash="behavior-test", base_folder=tmp_path),
        initial_iteration_state=SimpleNamespace(get=lambda: initial),
        iteration_state_assets=state_assets,
        population=SimpleNamespace(parameters=SimpleNamespace(sample_size=1)),
    )
    coordinator = MultiScenarioRunAsset({0: run})
    pool = SharedCandidatePoolResult(
        candidate_plan_steps_by_run_key={}, generated_mode_sequences_by_run_key={},
        time_sequence_steps_by_run_key={}, candidate_replications=pl.DataFrame(),
        candidate_ids=pl.DataFrame({"candidate_id_1": [1], "candidate_id_2": [2]}),
        destination_sequence_indices={0: pl.DataFrame()},
        mode_sequence_indices={0: pl.DataFrame()},
    )

    def inner(**kwargs):
        # No unused search result from the preceding step should be pinned
        # while this step creates its candidates.
        gc.collect()
        assert all(value() is None for value in previous_search_values)

        class SearchIntermediate:
            pass

        intermediate = SearchIntermediate()
        previous_search_values.append(weakref.ref(intermediate))
        inner_calls.append(kwargs)
        estimate = behavioral_state(89 + kwargs["iteration"])
        flows = {0: coordinated_module.represented_od_mode_flows(estimate.current_plan_steps)}
        return {
            "unused_search_intermediate": intermediate,
            "pool": pool, "diagnostics": [], "search_batches": 1,
            "candidate_plan_steps": {0: None},
            "flows_by_scenario": flows,
            "mnl_flows_by_scenario": flows if kwargs["keep_inner_flows"] else {},
            "plan_id_indices": {0: pl.DataFrame()},
            "time_sequence_steps": {0: pl.DataFrame()}, "rng_states": {0: None},
            "infeasible_counts": {0: 0}, "missing_cost_row_counts": {0: 0},
            "scored_candidate_scope_by_scenario": {0: "all"},
            "invalid_cost_row_counts": {0: 0}, "converged": False,
            "generation_effort_used": 0,
            "activity_profile_cell_count_by_scenario": {0: 0},
            "covered_activity_profile_cell_count_by_scenario": {0: 0},
            "convergence_by_run_key": {
                0: SimpleNamespace(
                    flow_changes={0: None},
                    base_gap_changes={0: 0.0},
                    converged=False,
                )
            },
        }

    monkeypatch.setattr(coordinator, "_run_inner_loop", inner)
    monkeypatch.setattr(coordinator, "_write_checkpoint_manifest", lambda iteration, **kwargs: None)
    monkeypatch.setattr(coordinator, "_log_plan_distribution", lambda **kwargs: None)

    def final_cost_asset(run, iteration, previous_state):
        gc.collect()
        assert all(value() is None for value in previous_search_values)
        assert active_outer_step == [iterations]
        assert previous_state is state_assets[-1]
        final_cost_iterations.append(iteration)
        def get_costs(metrics):
            assert active_outer_step == [iterations]
            return pl.DataFrame()
        return SimpleNamespace(get_costs_by_od=get_costs)

    monkeypatch.setattr(coordinator, "_final_transport_cost_asset", final_cost_asset)
    result = coordinator.get()
    assert len(inner_calls) == iterations
    assert [call["iteration"] for call in inner_calls] == list(range(1, iterations + 1))
    assert all(call.get("cost_assets") is None for call in inner_calls)
    assert final_cost_iterations == [iterations + 1]
    assert diagnostic_steps == list(range(1, iterations + 1))
    assert origins == [20 + 10 * iteration for iteration in range(iterations)]
    assert_frame_equal(
        result.final_states_by_run_key[0].current_plans,
        behavioral_state(20 + 10 * iterations).current_plans,
    )
    assert result.mnl_od_mode_flows_by_run_key[0]["volume"].item() == 89 + iterations

    # A failing live result consumer must not leave search tables attached to
    # the run or make a later request reuse a partial result.
    monkeypatch.setattr(AssetResolver, "release_in_memory_asset", lambda *args: None)
    def fail_consumer(iteration):
        current = current_coordinated_result(coordinator)
        assert current is not None
        assert not hasattr(current, "states_by_run_key")
        assert current.current_iteration == iteration
        if iteration != iterations:
            return
        if iterations > 1:
            assert state_assets[0].coordinated_current_plans is None
        raise RuntimeError("result consumer failed")

    with pytest.raises(RuntimeError, match="result consumer failed"):
        coordinator.get(step_consumer=fail_consumer)
    assert current_coordinated_result(coordinator) is None
    assert not hasattr(coordinator, "_cached_result")
    assert not coordinator._is_running
    assert all(asset.coordinated_current_plans is None for asset in state_assets)

    # Once a live request has discarded history, a later direct run must
    # execute the complete cohort again instead of returning that history.
    monkeypatch.setattr(AssetResolver, "retain_assets_shared_by_roots", lambda *args: None)
    before = len(inner_calls)
    coordinator.get(step_consumer=lambda iteration, **kwargs: None)
    assert not hasattr(coordinator, "_cached_result")
    later = coordinator.get()
    assert len(inner_calls) == before + 2 * iterations
    assert later.current_iteration == iterations
    assert not hasattr(later, "states_by_run_key")



def test_stable_replications_complete_the_configured_outer_iterations(monkeypatch, tmp_path):
    """A stable allocation must not shorten the modeled time horizon."""
    monkeypatch.setattr(coordinated_module, "prepare_shared_congested_costs", lambda assets: None)
    parameters = GroupDayTripsParameters(
        run={"n_steps": 3},
    )

    def make_coordinator(replication, increment):
        states = [SimpleNamespace(iteration=i, inputs_hash=f"{replication}-{i}", transport_costs=None) for i in range(1, 4)]
        for asset in states:
            asset.get = lambda asset=asset: behavioral_state(20 + increment * asset.iteration)
        run = SimpleNamespace(
            parameters=parameters, run_context=SimpleNamespace(inputs_hash=str(replication)),
            scenario="default", sensitivity_case_id="base", replication=replication,
            iterations=SimpleNamespace(run_inputs_hash=str(replication), base_folder=tmp_path),
            initial_iteration_state=SimpleNamespace(get=lambda: behavioral_state(20)),
            iteration_state_assets=states,
            population=SimpleNamespace(parameters=SimpleNamespace(sample_size=1)),
        )
        coordinator = MultiScenarioRunAsset({replication: run})
        key = replication
        pool = SharedCandidatePoolResult(
            candidate_plan_steps_by_run_key={}, generated_mode_sequences_by_run_key={}, candidate_ids=pl.DataFrame(),
            time_sequence_steps_by_run_key={}, candidate_replications=pl.DataFrame(),
            destination_sequence_indices={key: None}, mode_sequence_indices={key: None},
        )

        def inner(**kwargs):
            return {
                "pool": pool, "diagnostics": [], "search_batches": 1,
                "candidate_plan_steps": {key: None}, "flows_by_scenario": {}, "mnl_flows_by_scenario": {},
                "plan_id_indices": {key: pl.DataFrame()}, "time_sequence_steps": {key: pl.DataFrame()},
                "rng_states": {key: None}, "infeasible_counts": {key: 0},
                "scored_candidate_scope_by_scenario": {key: "all"},
                "missing_cost_row_counts": {key: 0}, "invalid_cost_row_counts": {key: 0},
                "generation_effort_used": 0, "activity_profile_cell_count_by_scenario": {key: 0},
                "covered_activity_profile_cell_count_by_scenario": {key: 0},
                "convergence_by_run_key": {key: SimpleNamespace(flow_changes={}, base_gap_changes={}, converged=True)},
            }

        monkeypatch.setattr(coordinator, "_run_inner_loop", inner)
        monkeypatch.setattr(coordinator, "_write_checkpoint_manifest", lambda iteration, **kwargs: None)
        monkeypatch.setattr(coordinator, "_log_plan_distribution", lambda **kwargs: None)
        monkeypatch.setattr(coordinator, "_final_transport_cost_asset", lambda *args: SimpleNamespace(
            get_costs_by_od=lambda metrics: pl.DataFrame(),
        ))
        return coordinator

    fast = make_coordinator(0, 0).get()
    slow = make_coordinator(1, 10).get()
    assert fast.current_iteration == 3
    assert slow.current_iteration == 3
    diagnostics = fast.final_states_by_run_key[0].adaptive_search_diagnostics
    assert "outer_converged" not in diagnostics.columns
    assert diagnostics["stop_reason"].to_list() == ["step_continues", "step_continues", "completed_steps"]
    assert diagnostics["flow_change_index"].to_list() == [None, 0.0, 0.0]
    changing = slow.final_states_by_run_key[1].adaptive_search_diagnostics["flow_change_index"].to_list()
    assert changing[0] is None
    assert changing[1:] == pytest.approx([10 / 70, 10 / 90])
    assert fast.final_states_by_run_key[0].current_plans["n_persons"][0] == 20
    assert slow.final_states_by_run_key[1].current_plans["n_persons"][0] == 50
