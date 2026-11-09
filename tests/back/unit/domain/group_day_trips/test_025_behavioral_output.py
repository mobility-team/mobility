"""Search estimates must never replace the actual behavioral allocation."""

from types import SimpleNamespace

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from mobility.trips.group_day_trips.core.multi_scenario_run import MultiScenarioRunAsset
from mobility.trips.group_day_trips.core.parameters import GroupDayTripsParameters
from mobility.trips.group_day_trips.core.run_state import RunState


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


@pytest.mark.parametrize("iterations", [1, 2])
def test_outer_origins_and_final_trips_keep_behavioral_population(iterations, monkeypatch, tmp_path):
    parameters = GroupDayTripsParameters(run={"n_steps": iterations})
    initial = behavioral_state(20)
    origins = []
    inner_calls = []
    final_cost_iterations = []
    state_assets = []

    for iteration in range(1, iterations + 1):
        asset = SimpleNamespace(iteration=iteration)

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
        inner_calls.append(kwargs)
        estimate = behavioral_state(89 + kwargs["iteration"])
        return {
            "pool": pool, "diagnostics": [], "search_batches": 1,
            "candidate_plan_steps": {0: None},
            "allocations": {0: estimate.current_plans},
            "plan_steps": {0: estimate.current_plan_steps},
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
        final_cost_iterations.append(iteration)
        return SimpleNamespace(get_costs_by_od=lambda metrics: pl.DataFrame())

    monkeypatch.setattr(coordinator, "_final_transport_cost_asset", final_cost_asset)
    result = coordinator.create_and_get_asset()
    assert len(inner_calls) == iterations
    assert [call["iteration"] for call in inner_calls] == list(range(1, iterations + 1))
    assert all(call.get("cost_assets") is None for call in inner_calls)
    assert final_cost_iterations == [iterations + 1]
    assert origins == [20 + 10 * iteration for iteration in range(iterations)]
    assert_frame_equal(
        result.final_states_by_run_key[0].current_plans,
        behavioral_state(20 + 10 * iterations).current_plans,
    )
    assert result.mnl_od_mode_flows_by_run_key[0]["volume"].item() == 89 + iterations


def test_stable_replications_complete_the_configured_outer_iterations(monkeypatch, tmp_path):
    """A stable allocation must not shorten the modeled time horizon."""
    parameters = GroupDayTripsParameters(
        run={"n_steps": 3},
    )

    def make_coordinator(replication, increment):
        states = [SimpleNamespace(iteration=i) for i in range(1, 4)]
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
                "candidate_plan_steps": {key: None}, "plan_steps": {key: behavioral_state(20).current_plan_steps},
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

    fast = make_coordinator(0, 0).create_and_get_asset()
    slow = make_coordinator(1, 10).create_and_get_asset()
    assert len(fast.states_by_run_key[0]) == 3
    assert len(slow.states_by_run_key[1]) == 3
    diagnostics = fast.final_states_by_run_key[0].adaptive_search_diagnostics
    assert "outer_converged" not in diagnostics.columns
    assert diagnostics["stop_reason"].to_list() == ["step_continues", "step_continues", "completed_steps"]
    assert fast.final_states_by_run_key[0].current_plans["n_persons"][0] == 20
    assert slow.final_states_by_run_key[1].current_plans["n_persons"][0] == 50
