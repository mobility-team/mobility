"""Check that shared search waits for stable flows in every scenario."""

from types import SimpleNamespace

import polars as pl
import pytest

from mobility.trips.group_day_trips.core.multi_scenario import (
    InnerLoopConvergence,
    normalized_base_gap_change,
)
from mobility.trips.group_day_trips.core.parameters import GroupDayTripsParameters
from mobility.trips.group_day_trips.core.multi_scenario_run import MultiScenarioRunAsset
from mobility.trips.group_day_trips.plans.plan_updater import CandidatePlanEvaluation, PlanUpdater
from mobility.trips.group_day_trips.core.run import RunTableAsset
from mobility.runtime.assets.in_memory_asset import InMemoryAsset
from mobility.runtime.assets.resolver import asset_resolution_context


def test_inner_convergence_requires_consecutive_passes_from_every_scenario():
    """Reset the pass count when either scenario changes too much."""
    convergence = InnerLoopConvergence(
        flow_tolerance=0.01,
        base_gap_tolerance=0.01,
        required_consecutive_passes=2,
    )
    flows = pl.DataFrame({"from": [1], "to": [2], "mode": ["car"], "volume": [10.0]})
    scenarios = {0: flows, 1: flows}
    population = {0: 100.0, 1: 100.0}

    first = convergence.evaluate(scenarios, population, base_scenario=0)
    assert first.flow_changes[0] is None
    assert not first.converged
    assert convergence.evaluate(scenarios, population, base_scenario=0).consecutive_passes == 1

    scenarios[1] = flows.with_columns(volume=pl.lit(20.0))
    changed = convergence.evaluate(scenarios, population, base_scenario=0)
    assert changed.consecutive_passes == 0
    assert not changed.converged
    assert not convergence.evaluate(scenarios, population, base_scenario=0).converged
    assert convergence.evaluate(scenarios, population, base_scenario=0).converged


def test_base_gap_change_ignores_equal_changes_in_both_scenarios():
    """A common flow change leaves the scenario-to-base difference unchanged."""
    previous = pl.DataFrame({"from": [1], "to": [2], "mode": ["car"], "volume": [10.0]})
    current = previous.with_columns(volume=pl.lit(20.0))

    assert normalized_base_gap_change(previous, previous, current, current, 100.0) == 0.0


def test_low_effort_defaults_keep_a_finite_inner_limit():
    """Keep the intended sampling defaults and reject an empty inner loop."""
    parameters = GroupDayTripsParameters()
    assert parameters.destination_sequences.k_destination_sequences == 1
    assert parameters.adaptive_search.max_search_batches == 3

    with pytest.raises(ValueError):
        GroupDayTripsParameters(adaptive_search={"max_search_batches": 0})


def test_inner_allocation_preserves_plan_ids_for_the_behavior_update(monkeypatch):
    """Keep stable plan ids and conserve the represented population."""
    utilities = pl.DataFrame({
        "plan_id": [11, 12, 13],
        "demand_group_id": [1, 1, 1],
        "demand_subgroup_id": [0, 0, 0],
        "activity_seq_id": [1, 1, 1],
        "time_seq_id": [1, 1, 1],
        "dest_seq_id": [1, 1, 1],
        "mode_seq_id": [1, 2, 3],
        "utility": [0.0, 0.0, 100.0],
        "is_feasible": [True, True, False],
    })
    evaluation = CandidatePlanEvaluation(pl.DataFrame(), pl.DataFrame(), utilities, pl.DataFrame())
    demand = pl.DataFrame({"demand_group_id": [1], "demand_subgroup_id": [0], "n_persons": [100.0]})
    updater = PlanUpdater()
    monkeypatch.setattr(updater, "get_current_plan_steps", lambda allocation, steps: pl.DataFrame())

    allocation, _ = updater.estimate_mnl_allocation(evaluation, demand, logit_scale=1.0)

    assert sorted(allocation["plan_id"].to_list()) == [11, 12]
    assert allocation["n_persons"].to_list() == pytest.approx([50.0, 50.0])


@pytest.mark.parametrize("table_name", ["plan_steps", "costs", "iteration_metrics", "adaptive_search_diagnostics"])
def test_coordinated_result_tables_keep_separate_scenario_cache_entries(monkeypatch, table_name):
    """Sharing a simulation must not mix scenario or sensitivity-case rows."""
    coordinator = InMemoryAsset({"cohort": "shared"})
    costs = InMemoryAsset({"costs": "shared"})
    runs = [
        SimpleNamespace(
            scenario=scenario,
            sensitivity_case=case,
            replication=0,
            run_context=SimpleNamespace(inputs_hash="shared-context"),
            coordinated_run_key=index,
            coordinated_run_asset=coordinator,
            iteration_transport_cost_assets=[costs],
        )
        for index, (scenario, case) in enumerate([
            ("default", None), ("project", None),
            ("default", "changed"), ("project", "changed"),
        ])
    ]
    tables = [RunTableAsset(run, table_name=table_name) for run in runs]
    monkeypatch.setattr(
        RunTableAsset,
        "create_and_get_asset",
        lambda table: pl.DataFrame({"value": [table.run.coordinated_run_key]}),
    )

    with asset_resolution_context():
        assert [table.get()["value"].item() for table in tables] == [0, 1, 2, 3]


@pytest.mark.parametrize("manifest_ready,ready_scenarios", [(False, 2), (True, 1), (True, 2)])
def test_checkpoint_resume_requires_the_complete_scenario_cohort(
    tmp_path, manifest_ready, ready_scenarios, monkeypatch
):
    """Resume every scenario together, or clear every checkpoint loader."""
    parameters = GroupDayTripsParameters(outputs={"checkpoint_each_step": True})
    runs = {
        run_key: SimpleNamespace(
            parameters=parameters,
            scenario=scenario,
            sensitivity_case_id="default",
            replication=1,
            run_context=SimpleNamespace(inputs_hash=scenario),
            iterations=SimpleNamespace(
                run_inputs_hash=scenario,
                base_folder=tmp_path,
                get_resume_iteration=lambda maximum: 1,
                has_complete_checkpoints=lambda count: True,
            ),
            iteration_state_assets=[SimpleNamespace(iteration=1, resume_checkpoint_loader=None)],
        )
        for run_key, scenario in enumerate(["default", "project"])
    }
    coordinator = MultiScenarioRunAsset(runs)
    monkeypatch.setattr(coordinator, "_checkpoint_file_hashes", lambda iteration: {})

    # A cohort marker is written only after both scenario states are saved.
    if manifest_ready:
        coordinator._write_checkpoint_manifest(1)
    for index, run in enumerate(runs.values()):
        run.iteration_state_assets[0].resume_checkpoint_loader = (
            object() if index < ready_scenarios else None
        )
    coordinator._prepare_checkpoint_resume()

    for run in runs.values():
        loader = run.iteration_state_assets[0].resume_checkpoint_loader
        assert (loader is not None) == (manifest_ready and ready_scenarios == 2)
