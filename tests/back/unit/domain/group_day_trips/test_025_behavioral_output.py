"""Search estimates must never replace the actual behavioral allocation."""

from types import SimpleNamespace
from contextlib import contextmanager
import json
import gc
import weakref

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from mobility.trips.group_day_trips.core.multi_scenario_run import MultiScenarioRunAsset
import mobility.trips.group_day_trips.core.multi_scenario_run as coordinated_module
import mobility.trips.group_day_trips.results.simulation_runner as runner_module
from mobility.trips.group_day_trips.core.parameters import GroupDayTripsParameters
from mobility.trips.group_day_trips.core.run_state import RunState
from mobility.trips.group_day_trips.core.run import Run
from mobility.trips.group_day_trips.iterations.iterations import Iterations
from mobility.trips.group_day_trips.iterations.iteration_assets import CurrentPlansAsset
from mobility.trips.group_day_trips.transitions.transition_schema import TRANSITION_EVENT_SCHEMA
from mobility.runtime.assets.resolver import AssetResolver
from mobility.runtime.assets.in_memory_asset import InMemoryAsset


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
    run.coordinated_run_asset = SimpleNamespace(_active_result=SimpleNamespace(current_iteration=1))
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

        def get(self, *, step_consumer=None, runner=None):
            assert step_consumer is not None
            for iteration in (1, 2):
                seen.append(iteration)
                step_consumer(iteration)
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
    assert run.get() is expected
    assert seen == [1, 2]
    assert run.coordinated_run_asset._cached_result is None


@pytest.mark.parametrize("contents,expected_resume", [
    ('{"version": 2, "cohort_hash": "cohort", "iteration": 1, "files": {}}', True),
    ('{"version": 2, "cohort_hash": "other", "iteration": 1, "files": {}}', False),
    ('{"version": 2, "cohort_hash": "cohort", "iteration": 2, "files": {}}', False),
    ('{"version": 2, "cohort_hash": "cohort", "iteration": true, "files": {}}', False),
    ('{"version": 2, "cohort_hash": "cohort", "iteration": 1, "files": {"bad": {}}}', False),
    ('{"cohort_hash":', False),
])
def test_checkpoint_manifest_must_match_complete_cohort(tmp_path, contents, expected_resume):
    manifest = tmp_path / "manifest.json"
    manifest.write_text(contents, encoding="utf-8")
    loaders = [lambda: None, lambda: None]
    runs = {
        key: SimpleNamespace(
            iterations=SimpleNamespace(
                get_resume_iteration=lambda n_steps: 1, has_complete_checkpoints=lambda n_steps: True
            ),
            iteration_state_assets=[SimpleNamespace(iteration=1, resume_checkpoint_loader=loaders[key])],
        )
        for key in range(2)
    }
    coordinator = object.__new__(MultiScenarioRunAsset)
    coordinator.runs = runs
    coordinator.parameters = SimpleNamespace(
        outputs=SimpleNamespace(checkpoint_each_step=True), run=SimpleNamespace(n_steps=2)
    )
    coordinator._checkpoint_cohort_hash = "cohort"
    coordinator._checkpoint_manifest_path = lambda iteration: manifest
    coordinator._checkpoint_file_hashes = lambda iteration: {}
    coordinator._prepare_checkpoint_resume()
    assert all(run.iteration_state_assets[0].resume_checkpoint_loader is not None
               for run in runs.values()) is expected_resume


def test_checkpoint_manifest_is_published_atomically(tmp_path, monkeypatch):
    coordinator = object.__new__(MultiScenarioRunAsset)
    coordinator.parameters = SimpleNamespace(outputs=SimpleNamespace(checkpoint_each_step=True))
    coordinator._checkpoint_cohort_hash = "cohort"
    target = tmp_path / "manifest.json"
    coordinator._checkpoint_manifest_path = lambda iteration: target
    coordinator._checkpoint_file_hashes = lambda iteration: {}
    coordinator._write_checkpoint_manifest(1)
    assert json.loads(target.read_text(encoding="utf-8")) == {
        "version": 2, "cohort_hash": "cohort", "iteration": 1, "files": {}
    }
    original = target.read_bytes()

    def fail_replace(source, destination):
        raise OSError("publication failed")

    monkeypatch.setattr("mobility.trips.group_day_trips.core.multi_scenario_run.os.replace", fail_replace)
    with pytest.raises(OSError, match="publication failed"):
        coordinator._write_checkpoint_manifest(1)
    assert target.read_bytes() == original
    assert sorted(path.name for path in tmp_path.iterdir()) == ["manifest.json"]


@pytest.mark.parametrize("error", [OSError("disk failure"), KeyboardInterrupt()])
def test_interrupted_checkpoint_rewrite_removes_old_completion_marker(tmp_path, monkeypatch, error):
    iterations = Iterations(run_inputs_hash="run", is_weekday=True, base_folder=tmp_path)
    checkpoint = iterations.iteration(1)
    completion = checkpoint.checkpoint_assets()["completion_marker"]
    completion.get()
    assert completion.cache_path.exists()

    def fail_after_partial_write(asset):
        asset.cache_path.parent.mkdir(parents=True, exist_ok=True)
        asset.cache_path.write_bytes(b"incomplete")
        raise error

    monkeypatch.setattr(CurrentPlansAsset, "create_and_get_asset", fail_after_partial_write)
    expected = KeyboardInterrupt if isinstance(error, KeyboardInterrupt) else RuntimeError
    with pytest.raises(expected):
        checkpoint.save_state(SimpleNamespace(current_plans=pl.DataFrame()), rng_state={})
    assert not completion.cache_path.exists()
    assert not iterations.has_complete_checkpoints(1)


def test_changed_transition_events_disable_resume_for_the_whole_cohort(tmp_path):
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
            transition_events_asset=SimpleNamespace(cache_path=event_path),
        )
        states.append(state)
        checkpoint = SimpleNamespace(checkpoint_assets=lambda path=state_path: {
            "state": SimpleNamespace(cache_path=path),
        })
        coordinator.runs[key] = SimpleNamespace(
            iteration_state_assets=[state],
            iterations=SimpleNamespace(
                iteration=lambda iteration, checkpoint=checkpoint: checkpoint,
                get_resume_iteration=lambda n_steps: 1,
                has_complete_checkpoints=lambda n_steps: True,
            ),
        )

    coordinator._write_checkpoint_manifest(1)
    coordinator._prepare_checkpoint_resume()
    assert all(state.resume_checkpoint_loader is not None for state in states)
    states[0].transition_events_asset.cache_path.write_bytes(b"altered")
    coordinator._prepare_checkpoint_resume()
    assert all(state.resume_checkpoint_loader is None for state in states)


def test_incomplete_cohort_checkpoint_cannot_resume_one_scenario(tmp_path):
    manifest = tmp_path / "manifest.json"
    manifest.write_text('{"version": 2, "cohort_hash": "cohort", "iteration": 1, "files": {}}', encoding="utf-8")
    first = SimpleNamespace(iteration=1, resume_checkpoint_loader=lambda: None)
    second = SimpleNamespace(iteration=1, resume_checkpoint_loader=None)
    coordinator = object.__new__(MultiScenarioRunAsset)
    coordinator.parameters = SimpleNamespace(
        outputs=SimpleNamespace(checkpoint_each_step=True), run=SimpleNamespace(n_steps=2)
    )
    coordinator.runs = {
        key: SimpleNamespace(
            iterations=SimpleNamespace(
                get_resume_iteration=lambda n_steps: 1, has_complete_checkpoints=lambda n_steps: True
            ),
            iteration_state_assets=[state],
        )
        for key, state in enumerate((first, second))
    }
    coordinator._checkpoint_cohort_hash = "cohort"
    coordinator._checkpoint_manifest_path = lambda iteration: manifest
    coordinator._checkpoint_file_hashes = lambda iteration: {}
    coordinator._prepare_checkpoint_resume()
    assert first.resume_checkpoint_loader is None
    assert second.resume_checkpoint_loader is None


def test_missing_checkpoint_file_disables_resume_for_entire_cohort(tmp_path):
    manifest = tmp_path / "manifest.json"
    manifest.write_text('{"version": 2, "cohort_hash": "cohort", "iteration": 1, "files": {}}', encoding="utf-8")
    states = [SimpleNamespace(iteration=1, resume_checkpoint_loader=lambda: None) for _ in range(2)]
    coordinator = object.__new__(MultiScenarioRunAsset)
    coordinator.parameters = SimpleNamespace(
        outputs=SimpleNamespace(checkpoint_each_step=True), run=SimpleNamespace(n_steps=2)
    )
    coordinator.runs = {
        key: SimpleNamespace(
            iterations=SimpleNamespace(
                get_resume_iteration=lambda n_steps: 1,
                has_complete_checkpoints=lambda n_steps, key=key: key == 0,
            ),
            iteration_state_assets=[state],
        )
        for key, state in enumerate(states)
    }
    coordinator._checkpoint_cohort_hash = "cohort"
    coordinator._checkpoint_manifest_path = lambda iteration: manifest
    coordinator._checkpoint_file_hashes = lambda iteration: {}
    coordinator._prepare_checkpoint_resume()
    assert all(state.resume_checkpoint_loader is None for state in states)


def behavioral_state(active_people):
    plans = pl.DataFrame({
        "demand_group_id": [1, 1], "demand_subgroup_id": [0, 0],
        "activity_seq_id": [1, 0], "time_seq_id": [1, 0],
        "dest_seq_id": [1, 0], "mode_seq_id": [1, 0],
        "n_persons": [float(active_people), 100.0 - active_people],
        "utility": [1.0, 0.0], "plan_id": [1, 0],
    })
    steps = pl.DataFrame({
        "from": [1], "to": [2], "mode": ["car"],
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

    pool = SimpleNamespace(
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
    def reduce_flows(plan_steps):
        diagnostic_steps.append(active_outer_step[-1] if active_outer_step else None)
        return original_flow_reducer(plan_steps)

    monkeypatch.setattr(runner_module, "simulation_step", track_step)
    monkeypatch.setattr(coordinated_module, "represented_od_mode_flows", reduce_flows)

    for iteration in range(1, iterations + 1):
        asset = SimpleNamespace(iteration=iteration, inputs_hash=f"state-{iteration}")

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
    pool = SimpleNamespace(
        candidate_ids=((1, 2),), destination_sequence_indices={0: pl.DataFrame()},
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
        return {
            "unused_search_intermediate": intermediate,
            "pool": pool, "diagnostics": [], "search_batches": 1,
            "candidate_plan_steps": {0: None},
            "flows_by_scenario": {0: coordinated_module.represented_od_mode_flows(estimate.current_plan_steps)},
            "plan_id_indices": {0: pl.DataFrame()},
            "time_sequence_steps": {0: pl.DataFrame()}, "rng_states": {0: None},
            "infeasible_counts": {0: 0}, "missing_cost_row_counts": {0: 0},
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
    monkeypatch.setattr(coordinator, "_write_checkpoint_manifest", lambda iteration: None)
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
        assert coordinator._active_result is not None
        assert not hasattr(coordinator._active_result, "states_by_run_key")
        assert coordinator._active_result.current_iteration == iteration
        if iteration != iterations:
            return
        if iterations > 1:
            assert state_assets[0].coordinated_current_plans is None
        raise RuntimeError("result consumer failed")

    with pytest.raises(RuntimeError, match="result consumer failed"):
        coordinator.get(step_consumer=fail_consumer)
    assert coordinator._active_result is None
    assert not hasattr(coordinator, "_cached_result")
    assert not coordinator._is_running
    assert all(asset.coordinated_current_plans is None for asset in state_assets)

    # Once a live request has discarded history, a later direct run must
    # execute the complete cohort again instead of returning that history.
    monkeypatch.setattr(AssetResolver, "retain_assets_shared_by_roots", lambda *args: None)
    before = len(inner_calls)
    coordinator.get(step_consumer=lambda iteration: None)
    assert not hasattr(coordinator, "_cached_result")
    later = coordinator.get()
    assert len(inner_calls) == before + 2 * iterations
    assert later.current_iteration == iterations
    assert not hasattr(later, "states_by_run_key")



def test_stable_replications_complete_the_configured_outer_iterations(monkeypatch, tmp_path):
    """A stable allocation must not shorten the modeled time horizon."""
    parameters = GroupDayTripsParameters(
        run={"n_steps": 3},
    )

    def make_coordinator(replication, increment):
        states = [SimpleNamespace(iteration=i, inputs_hash=f"{replication}-{i}") for i in range(1, 4)]
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
        pool = SimpleNamespace(candidate_ids=(), destination_sequence_indices={key: None}, mode_sequence_indices={key: None})

        def inner(**kwargs):
            return {
                "pool": pool, "diagnostics": [], "search_batches": 1,
                "candidate_plan_steps": {key: None}, "flows_by_scenario": {},
                "plan_id_indices": {key: pl.DataFrame()}, "time_sequence_steps": {key: pl.DataFrame()},
                "rng_states": {key: None}, "infeasible_counts": {key: 0},
                "missing_cost_row_counts": {key: 0}, "invalid_cost_row_counts": {key: 0},
                "generation_effort_used": 0, "activity_profile_cell_count_by_scenario": {key: 0},
                "covered_activity_profile_cell_count_by_scenario": {key: 0},
                "convergence_by_run_key": {key: SimpleNamespace(flow_changes={}, base_gap_changes={}, converged=True)},
            }

        monkeypatch.setattr(coordinator, "_run_inner_loop", inner)
        monkeypatch.setattr(coordinator, "_write_checkpoint_manifest", lambda iteration: None)
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
    assert fast.final_states_by_run_key[0].current_plans["n_persons"][0] == 20
    assert slow.final_states_by_run_key[1].current_plans["n_persons"][0] == 50
