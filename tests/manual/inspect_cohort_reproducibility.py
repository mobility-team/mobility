"""Inspect cohort differences without deleting existing run caches.

Run explicitly with:
    pixi run python -m pytest --local --use-truststore tests/manual/inspect_cohort_reproducibility.py

This diagnostic retains detailed search inputs and runs the cohort three times.
It is intentionally not collected by the normal test suite.
"""

import polars as pl
import pytest
from polars.testing import assert_frame_equal

import mobility
from mobility.activities import HomeActivity, OtherActivity, WorkActivity
from mobility.surveys.france import EMPMobilitySurvey
from mobility.runtime.scenarios import Scenario, Scenarios
from mobility.runtime.assets.resolver import asset_resolution_context, get_current_asset_resolver
from mobility.runtime.assets.graph import asset_graph_key
from mobility.trips.group_day_trips import (
    GroupDayTripsDestinationSequenceParameters,
    GroupDayTripsModeSequenceParameters,
    GroupDayTripsParameters,
    GroupDayTripsRunParameters,
    PopulationGroupDayTrips,
)
from mobility.trips.group_day_trips.core.multi_scenario import CANDIDATE_ID_COLS, add_candidate_ids
from mobility.trips.group_day_trips.core.multi_scenario_run import MultiScenarioRunAsset
from mobility.trips.group_day_trips.plans.compact_candidate_steps import CompactCandidateSteps
from mobility.trips.group_day_trips.plans.plan_updater import PlanUpdater


def test_independent_cohort_executions_keep_state_and_candidate_ids(test_data, monkeypatch, tmp_path):
    """Two experiments with one seed follow the same two-step model path."""
    zones = mobility.TransportZones(
        local_admin_unit_id=test_data["transport_zones_local_admin_unit_id"],
        radius=test_data["transport_zones_radius"],
    )
    population = mobility.Population(zones, sample_size=test_data["population_sample_size"])
    setup = PopulationGroupDayTrips(
        population=population,
        modes=[mobility.CarMode(
            zones,
            generalized_cost_parameters=mobility.GeneralizedCostParameters(
                cost_of_distance=mobility.ParameterValue.by_scenario(default=0.1, project=0.2),
            ),
        )],
        activities=[HomeActivity(), WorkActivity(), OtherActivity(population=population)],
        surveys=[EMPMobilitySurvey()],
        scenarios=Scenarios([Scenario(name="project")]),
        parameters=GroupDayTripsParameters(
            run=GroupDayTripsRunParameters(n_steps=2, n_steps_per_cost_update=0, seed=108),
            destination_sequences=GroupDayTripsDestinationSequenceParameters(
                dest_prob_cutoff=0.9, cost_uncertainty_sd=1.0,
            ),
            mode_sequences=GroupDayTripsModeSequenceParameters(
                k_mode_sequences=3, mode_sequence_search_parallel=False,
            ),
        ),
    )

    step_records = []
    step_frames = []
    search_records = []
    scoring_inputs = []
    final_cohorts = []
    advance = MultiScenarioRunAsset._advance_cohort_step
    search_loop = MultiScenarioRunAsset._run_inner_loop
    finalize = MultiScenarioRunAsset._finalize_cohort
    evaluate = PlanUpdater.evaluate_candidate_pool

    def record_inputs(updater, **kwargs):
        scoring_inputs.append((kwargs["iteration"], {
            name: kwargs[name].clone()
            for name in ("current_plans", "destination_saturation", "activity_dur", "home_night_dur")
        }))
        return evaluate(updater, **kwargs)

    def record_search(coordinator, **kwargs):
        result = search_loop(coordinator, **kwargs)
        search_records.append((kwargs["iteration"], {
            key: evaluation.possible_plan_utility.clone()
            for key, evaluation in result.get("evaluations", {}).items()
        }))
        return result

    def record_step(coordinator, iteration, current_states, **kwargs):
        state_assets, states, search = advance(coordinator, iteration, current_states, **kwargs)
        step_records.append((
            iteration,
            tuple(sorted(search["pool"].candidate_ids)),
            {key: (state.rng_state, state.current_plans.clone()) for key, state in states.items()},
        ))
        step_frames.append({key: state.current_plan_steps.clone() for key, state in states.items()})
        return state_assets, states, search

    monkeypatch.setattr(MultiScenarioRunAsset, "_advance_cohort_step", record_step)
    monkeypatch.setattr(MultiScenarioRunAsset, "_run_inner_loop", record_search)
    monkeypatch.setattr(PlanUpdater, "evaluate_candidate_pool", record_inputs)
    def record_final(coordinator, *args):
        if not final_cohorts:
            resolver = get_current_asset_resolver()
            for member in coordinator.runs.values():
                previous = member.iteration_state_assets[0]
                assert asset_graph_key(previous) not in resolver.in_memory_values
                assert previous.coordinated_candidate_plan_steps is None
        states = finalize(coordinator, *args)
        final_cohorts.append(states)
        return states
    monkeypatch.setattr(MultiScenarioRunAsset, "_finalize_cohort", record_final)
    experiment = setup.experiment("weekday", scenarios=["default", "project"], replication=0)
    run = experiment.run("weekday")
    results = experiment.results()
    requests = {
        "trips": results.metrics.trip_count(by_variable="mode"),
        "filtered": results.metrics.trip_count(by_variable="mode", modes=["car"]),
        "per_person": results.metrics.trip_count(
            by_variable="mode", normalize_by="person_count", normalize_scope="study_area",
        ),
        "gap": results.metrics.trip_count(by_variable="mode", reference=("scenario", "default")),
        "project_only": results.metrics.trip_count(by_variable="mode", scenarios=["project"]),
        "distance": results.metrics.travel_distance(by_variable="mode"),
        "time": results.metrics.travel_time(by_variable="mode", reference=("scenario", "default")),
        "cost": results.metrics.cost(by_variable="mode", modes=["car"]),
        "emissions": results.metrics.ghg_emissions(by_variable="mode"),
        "history": results.metrics.trip_count(by_variable="mode", steps="all"),
        "history_time": results.metrics.travel_time(by_variable="mode", steps=[1, 2]),
        "first_time": results.metrics.travel_time(by_variable="mode", steps=1),
        "external": results.metrics.trip_count(by_variable="mode", reference="external"),
    }
    consumed_steps = []
    for name, request in requests.items():
        # Use fresh metric caches so every consumer runs during the step.
        request.cache_path = tmp_path / f"{name}.parquet"
        request.hash_path = tmp_path / f"{name}.inputs-hash"
        compute_iteration = request.compute_iteration
        def check_live_consumption(iteration, name=name, compute=compute_iteration):
            assert run.coordinated_run_asset._active_result is not None
            consumed_steps.append((name, iteration))
            return compute(iteration)
        request.compute_iteration = check_live_consumption
    with asset_resolution_context() as resolver:
        live_values = results.get_all(**requests)
        assert asset_graph_key(run.coordinated_run_asset) not in resolver.in_memory_values
        assert not hasattr(run.coordinated_run_asset, "_cached_result")
        for member in run.coordinated_run_asset.runs.values():
            for asset in member.iteration_state_assets:
                assert asset.coordinated_candidate_plan_steps is None
                assert asset.coordinated_current_plans is None
        # An unretained later request starts the full cohort again, even when
        # the caller keeps the same resolver open.
        later = run.coordinated_run_asset.get().final_states_by_run_key
        assert len(step_records) == 4
    assert consumed_steps == [
        (name, iteration)
        for iteration in (1, 2)
        for name, request in requests.items()
        if iteration in request.selected_iterations
    ]
    first = final_cohorts[0]
    other_experiment = setup.experiment("weekday", scenarios=["default", "project"], replication=0)
    other_run = other_experiment.run("weekday")
    second = other_run.coordinated_run_asset.get().final_states_by_run_key
    metric = other_experiment.results().metrics._trip_count(
        output_column="trip_count", group_columns=["mode"], per_person=False, iterations="last",
    )
    with asset_resolution_context():
        live_plan_steps = metric.plan_steps.get()
        live_demand_groups = metric.demand_groups.get()
        direct_metric = metric.reduce_tables(live_plan_steps, live_demand_groups)
        existing_metric = metric.create_and_get_asset()
    assert_frame_equal(direct_metric, existing_metric, check_exact=False, rel_tol=1e-12, abs_tol=1e-9)
    assert_frame_equal(direct_metric, live_values["trips"], check_exact=False, rel_tol=1e-12, abs_tol=1e-9)
    comparison_results = other_experiment.results()
    comparisons = {
        "filtered": comparison_results.metrics.trip_count(by_variable="mode", modes=["car"]),
        "per_person": comparison_results.metrics.trip_count(
            by_variable="mode", normalize_by="person_count", normalize_scope="study_area",
        ),
        "gap": comparison_results.metrics.trip_count(by_variable="mode", reference=("scenario", "default")),
        "project_only": comparison_results.metrics.trip_count(by_variable="mode", scenarios=["project"]),
        "distance": comparison_results.metrics.travel_distance(by_variable="mode"),
        "time": comparison_results.metrics.travel_time(by_variable="mode", reference=("scenario", "default")),
        "cost": comparison_results.metrics.cost(by_variable="mode", modes=["car"]),
        "emissions": comparison_results.metrics.ghg_emissions(by_variable="mode"),
        "external": comparison_results.metrics.trip_count(by_variable="mode", reference="external"),
    }
    with asset_resolution_context():
        for name, request in comparisons.items():
            expected = request.compute_iteration(2)
            assert_frame_equal(live_values[name], expected, check_exact=False, rel_tol=1e-12, abs_tol=1e-9)

    assert_frame_equal(
        live_values["history_time"].filter(pl.col("step") == 1),
        live_values["first_time"], check_exact=False, rel_tol=1e-12, abs_tol=1e-9,
    )
    for iteration, frames in enumerate(step_frames[:2], start=1):
        for key, frame in frames.items():
            scenario = run.coordinated_run_asset.runs[key].scenario
            expected_trips = frame.filter(pl.col("activity_seq_id") != 0)["n_persons"].sum()
            actual_trips = live_values["history"].filter(
                (pl.col("step") == iteration) & (pl.col("scenario") == scenario)
            )["trip_count"].sum()
            assert actual_trips == pytest.approx(expected_trips, rel=1e-12, abs=1e-9)

    assert len(step_records) == 6
    assert len(scoring_inputs) % 3 == 0
    third = len(scoring_inputs) // 3
    for index, ((iteration, inputs), (other_iteration, other_inputs)) in enumerate(zip(scoring_inputs[:third], scoring_inputs[2 * third:])):
        assert iteration == other_iteration
        for name, frame in inputs.items():
            other_frame = other_inputs[name]
            try:
                assert_frame_equal(frame.sort(frame.columns), other_frame.sort(other_frame.columns), check_exact=False, rel_tol=1e-12, abs_tol=1e-9)
            except AssertionError as exc:
                raise AssertionError(f"Scoring input {name} differs at step {iteration}, call {index}") from exc
    for (iteration, first_scores), (_, second_scores) in zip(search_records[:2], search_records[4:]):
        for key, scores in first_scores.items():
            other_scores = second_scores[key]
            try:
                assert_frame_equal(
                    scores.sort(scores.columns), other_scores.sort(other_scores.columns),
                    check_exact=False, rel_tol=1e-12, abs_tol=1e-9,
                )
            except AssertionError as exc:
                left = scores.sort(scores.columns).with_row_index("_row")
                right = other_scores.sort(other_scores.columns).with_row_index("_row")
                differences = (
                    left.join(right.select("_row", pl.col("utility").alias("other_utility")), on="_row")
                    .with_columns(delta=(pl.col("utility") - pl.col("other_utility")).abs())
                    .filter(pl.col("delta") > 1e-9)
                    .sort("delta", descending=True)
                    .head(3)
                )
                raise AssertionError(f"Search scores differ at step {iteration}, run {key}: {differences.to_dicts()}") from exc
    for first_step, second_step in zip(step_records[:2], step_records[4:]):
        iteration, candidates, states = first_step
        assert iteration == second_step[0]
        assert candidates == second_step[1], f"Candidate IDs differ at step {iteration}"
        for key, (rng_state, plans) in states.items():
            other_rng, other_plans = second_step[2][key]
            assert rng_state == other_rng, f"RNG differs at step {iteration}, run {key}"
            try:
                assert_frame_equal(
                    plans.sort(plans.columns), other_plans.sort(other_plans.columns),
                    check_exact=False, rel_tol=1e-12, abs_tol=1e-9,
                )
            except AssertionError as exc:
                raise AssertionError(f"Plans differ at step {iteration}, run {key}") from exc

    def candidate_ids(state):
        candidates = state.candidate_plan_steps
        if isinstance(candidates, CompactCandidateSteps):
            candidates = candidates.lazy().collect()
        assert isinstance(candidates, pl.DataFrame)
        return add_candidate_ids(candidates).select(CANDIDATE_ID_COLS).unique().sort(CANDIDATE_ID_COLS)

    for key, first_state in first.items():
        other = second[key]
        assert first_state.rng_state == other.rng_state
        assert first_state.rng_state == later[key].rng_state
        for name in ("current_plans", "current_plan_steps", "plan_id_index", "costs"):
            left = getattr(first_state, name)
            right = getattr(other, name)
            # Row order is not part of these outputs. Sorting all columns
            # still checks every value and stable ID, including duplicates.
            assert_frame_equal(
                left.sort(left.columns), right.sort(right.columns),
                check_exact=False, rel_tol=1e-12, abs_tol=1e-9,
            )
            later_frame = getattr(later[key], name)
            assert_frame_equal(
                left.sort(left.columns), later_frame.sort(later_frame.columns),
                check_exact=False, rel_tol=1e-12, abs_tol=1e-9,
            )
        assert_frame_equal(candidate_ids(first_state), candidate_ids(other), check_exact=True)
        assert_frame_equal(candidate_ids(first_state), candidate_ids(later[key]), check_exact=True)
