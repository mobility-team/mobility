"""Compare live results with an independent complete cohort execution."""

import weakref
import json
from pathlib import Path
from types import SimpleNamespace

import polars as pl
import pytest
from polars.testing import assert_frame_equal

import mobility
from mobility.activities import HomeActivity, OtherActivity, WorkActivity
from mobility.runtime.assets.graph import asset_graph_key
from mobility.runtime.assets.file_asset import FileAsset
from mobility.runtime.assets.in_memory_asset import InMemoryAsset
from mobility.runtime.assets.resolver import asset_resolution_context, get_current_asset_resolver
from mobility.runtime.scenarios import Scenario, Scenarios
from mobility.surveys.france import EMPMobilitySurvey
from mobility.trips.group_day_trips import (
    GroupDayTripsDestinationSequenceParameters,
    GroupDayTripsModeSequenceParameters,
    GroupDayTripsParameters,
    GroupDayTripsRunParameters,
    PopulationGroupDayTrips,
)
from mobility.trips.group_day_trips.core.multi_scenario_run import MultiScenarioRunAsset
from mobility.trips.group_day_trips.core.metrics import RunMetrics
from mobility.trips.group_day_trips.core.run import Run
from mobility.trips.group_day_trips.evaluation.iteration_metrics import IterationMetricsHistory
from mobility.trips.group_day_trips.iterations.iterations import Iterations
from mobility.trips.group_day_trips.plans.compact_candidate_steps import CompactCandidateSteps
from mobility.trips.group_day_trips.plans.plan_updater import PlanUpdater
from mobility.trips.group_day_trips.results.assets.metric import Metric
from mobility.trips.group_day_trips.results.result_table import ResultTable
from mobility.trips.group_day_trips.sensitivity import collect_group_day_trips_sensitivity_cases
from mobility.trips.group_day_trips.transitions.transition_schema import TRANSITION_EVENT_SCHEMA
from mobility.transport.graphs.modified.modifiers.speed_modifier import NewRoadModifier


def make_setup(test_data, *, n_steps=2, n_replications=1, record_events=False, sensitivity=False):
    zones = mobility.TransportZones(
        local_admin_unit_id=test_data["transport_zones_local_admin_unit_id"],
        radius=test_data["transport_zones_radius"],
    )
    population = mobility.Population(zones, sample_size=test_data["population_sample_size"])
    return PopulationGroupDayTrips(
        population=population,
        modes=[mobility.CarMode(
            zones, generalized_cost_parameters=mobility.GeneralizedCostParameters(
                cost_of_distance=mobility.ParameterValue.by_scenario(
                    default=(mobility.SensitivityValue.relative(0.1, [0.5], name="car_cost")
                             if sensitivity else 0.1),
                    project=(mobility.SensitivityValue.relative(0.2, [0.5], name="car_cost")
                             if sensitivity else 0.2),
                ),
            ),
        )],
        activities=[HomeActivity(), WorkActivity(), OtherActivity(population=population)],
        surveys=[EMPMobilitySurvey()],
        scenarios=Scenarios([Scenario(name="project")]),
        parameters=GroupDayTripsParameters(
            outputs={"cache_step_events": record_events},
            run=GroupDayTripsRunParameters(
                n_steps=n_steps, n_replications=n_replications,
                n_steps_per_cost_update=0,
                seed=108 if n_replications == 1 else 0,
                seeds=None if n_replications == 1 else [108, 109],
            ),
            destination_sequences=GroupDayTripsDestinationSequenceParameters(
                dest_prob_cutoff=0.9, cost_uncertainty_sd=1.0,
            ),
            mode_sequences=GroupDayTripsModeSequenceParameters(
                k_mode_sequences=3, mode_sequence_search_parallel=False,
            ),
        ),
    )


def test_result_and_cohort_identity_include_other_scenario_timetable(test_data, tmp_path):
    """A filtered result still depends on every scenario sharing its search."""
    feed = tmp_path / "project-service.zip"
    feed.write_bytes(b"first timetable")
    setup = make_setup(test_data)
    # The file is an identity input only: constructing these requests does not
    # open it as a timetable or run the simulation.
    setup.modes[0].routing_parameters = SimpleNamespace(
        additional_gtfs_files=mobility.ParameterValue.by_scenario(
            default=None, project=[str(feed)]
        )
    )

    def identities():
        experiment = setup.experiment("weekday", scenarios=["default", "project"], replication=0)
        run = experiment.run("weekday", scenario="default")
        result = experiment.results(scenarios=["default"])
        metric = result.metrics.trip_count(by_variable="mode")
        return (
            run.run_context.inputs_hash,
            run.coordinated_run_asset.inputs_hash,
            run.coordinated_run_asset._checkpoint_cohort_hash,
            metric.inputs_hash,
        )

    before = identities()
    experiment = setup.experiment("weekday", scenarios=["default", "project"], replication=0)
    filtered = experiment.results(scenarios=["default"])
    old_request = filtered.metrics.trip_count(by_variable="mode")
    feed.write_bytes(b"revised timetable")
    with pytest.raises(RuntimeError, match="Create a new experiment"):
        filtered.explain(trips=old_request)
    with pytest.raises(RuntimeError, match="Create a new experiment"):
        old_request.get()
    with pytest.raises(RuntimeError, match="Create a new experiment"):
        experiment.run("weekday", scenario="default").get()
    after = identities()
    assert before[0] == after[0]
    assert all(old != new for old, new in zip(before[1:], after[1:]))


def test_result_and_cohort_identity_include_other_scenario_road(test_data, tmp_path):
    """Editing a shared cohort member's road file invalidates filtered results."""
    road = tmp_path / "project-road.geojson"
    road.write_text("first geometry", encoding="utf-8")
    default_road = tmp_path / "default-road.geojson"
    default_road.write_text("default geometry", encoding="utf-8")
    setup = make_setup(test_data)
    modifier = NewRoadModifier(mobility.ParameterValue.by_scenario(
        default=str(default_road), project=str(road),
    ))
    setup.modes[0].inputs["travel_costs"].inputs["speed_modifiers"].append(modifier)

    def identities():
        experiment = setup.experiment("weekday", scenarios=["default", "project"], replication=0)
        run = experiment.run("weekday", scenario="default")
        metric = experiment.results(scenarios=["default"]).metrics.trip_count(by_variable="mode")
        return run.run_context.inputs_hash, run.coordinated_run_asset._checkpoint_cohort_hash, metric.inputs_hash

    before = identities()
    experiment = setup.experiment("weekday", scenarios=["default", "project"], replication=0)
    filtered = experiment.results(scenarios=["default"])
    old_request = filtered.metrics.trip_count(by_variable="mode")
    road.write_text("changed geometry", encoding="utf-8")
    with pytest.raises(RuntimeError, match="Create a new experiment"):
        old_request.get()
    after = identities()
    assert before[0] == after[0]
    assert all(old != new for old, new in zip(before[1:], after[1:]))


def test_live_results_match_complete_cohort(test_data, monkeypatch, tmp_path):
    """Selected results must preserve the cohort's behavior and random path."""
    setup = make_setup(test_data, record_events=True)
    steps = []
    final_states = []
    advance = MultiScenarioRunAsset._advance_cohort_step
    finalize = MultiScenarioRunAsset._finalize_cohort
    expand = PlanUpdater.expand_context_evaluation
    compact = CompactCandidateSteps.from_frame
    expanded_evaluations = []
    checked_compactions = []
    compacted_candidates = []

    def record_evaluation(updater, *args):
        evaluation = expand(updater, *args)
        expanded_evaluations.append(weakref.ref(evaluation))
        return evaluation

    def check_compaction(cls, frame):
        if expanded_evaluations:
            assert expanded_evaluations[-1]() is None
            checked_compactions.append(True)
        result = compact(frame)
        compacted_candidates.append(isinstance(result, CompactCandidateSteps))
        return result

    def record_step(coordinator, iteration, current_states, **kwargs):
        assets, states, search = advance(coordinator, iteration, current_states, **kwargs)
        steps.append((
            tuple(sorted(search["pool"].candidate_ids)),
            {key: (state.rng_state, state.current_plans.clone(), state.current_plan_steps.clone())
             for key, state in states.items()},
        ))
        return assets, states, search

    def record_final(coordinator, *args):
        if not final_states:
            resolver = get_current_asset_resolver()
            for member in coordinator.runs.values():
                previous = member.iteration_state_assets[0]
                assert asset_graph_key(previous) not in resolver.in_memory_values
                assert previous.coordinated_candidate_plan_steps is None
        result = finalize(coordinator, *args)
        final_states.append(result)
        return result

    monkeypatch.setattr(MultiScenarioRunAsset, "_advance_cohort_step", record_step)
    monkeypatch.setattr(MultiScenarioRunAsset, "_finalize_cohort", record_final)
    monkeypatch.setattr(PlanUpdater, "expand_context_evaluation", record_evaluation)
    monkeypatch.setattr(CompactCandidateSteps, "from_frame", classmethod(check_compaction))
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
        "external_history": results.metrics.trip_count(by_variable="mode", reference="external", steps="all"),
        "external_first": results.metrics.trip_count(by_variable="mode", reference="external", steps=1),
        "by_demand": results.metrics.trip_count_by_demand_group(steps="all"),
        "by_demand_first": results.metrics.trip_count_by_demand_group(steps=1),
        "immobility": results.metrics.immobility(steps="all"),
        "immobility_first": results.metrics.immobility(steps=1),
        "immobility_survey": results.metrics.immobility(reference="external"),
        "occupation": results.metrics.opportunity_occupation(steps="all", inner_zone_residents_only=False),
        "occupation_first": results.metrics.opportunity_occupation(steps=1, inner_zone_residents_only=False),
        "durations": results.metrics.activity_duration_distribution(steps="all"),
        "durations_first": results.metrics.activity_duration_distribution(steps=1),
        "activity_time": results.metrics.activity_time_series(steps="all", interval_minutes=60),
        "activity_time_first": results.metrics.activity_time_series(steps=1, interval_minutes=60),
        "step_diagnostics": results.tables.step_metrics(steps="all"),
        "first_diagnostics": results.tables.step_metrics(steps=1),
        "search_diagnostics": results.tables.adaptive_search_diagnostics(
            columns=["scenario", "phase", "outer_step"],
        ),
        "plan_steps": results.tables.plan_steps(
            steps="all", columns=["scenario", "step", "mode", "n_persons"]
        ),
        "cost_history": results.tables.costs(steps="all", columns=[
            "scenario", "step", "from", "to", "mode", "cost"
        ]),
        "final_costs": results.tables.costs(columns=[
            "scenario", "step", "from", "to", "mode", "cost"
        ]),
        "transitions": results.tables.transitions(columns=[
            "scenario", "step", "demand_group_id", "n_persons_moved"
        ]),
        "demand_groups": results.tables.demand_groups(columns=[
            "scenario", "step", "replication", "demand_group_id", "n_persons"
        ]),
        "mnl_flows": results.tables.mnl_od_mode_flows(),
    }
    consumed = []
    for name, request in requests.items():
        if hasattr(request, "hash_path"):
            request.cache_path = tmp_path / f"{name}.parquet"
            request.hash_path = tmp_path / f"{name}.inputs-hash"
        method = "compute_lane" if getattr(request, "compute_lane", None) is not None else "compute_iteration"
        compute = getattr(request, method)
        def check_live(iteration, name=name, compute=compute, **kwargs):
            assert run.coordinated_run_asset._active_result is not None
            consumed.append((name, iteration))
            return compute(iteration, **kwargs)
        setattr(request, method, check_live)

    with asset_resolution_context() as resolver:
        values = results.get_all(**requests)
        assert asset_graph_key(run.coordinated_run_asset) not in resolver.in_memory_values
    assert expanded_evaluations and checked_compactions and any(compacted_candidates)
    assert not hasattr(run.coordinated_run_asset, "_cached_result")
    assert consumed == [
        (name, iteration) for iteration in (1, 2) for name, request in requests.items()
        if iteration in request.selected_iterations
    ]
    for member in run.coordinated_run_asset.runs.values():
        for asset in member.iteration_state_assets:
            assert asset.coordinated_candidate_plan_steps is None
            assert asset.coordinated_current_plans is None

    # Compare both supported candidate-history representations in the same
    # model: lossless compaction and the full-table fallback.
    monkeypatch.setattr(CompactCandidateSteps, "from_frame", classmethod(lambda cls, frame: frame))
    other = setup.experiment("weekday", scenarios=["default", "project"], replication=0)
    reference = other.results()
    comparisons = {
        "trips": reference.metrics.trip_count(by_variable="mode"),
        "filtered": reference.metrics.trip_count(by_variable="mode", modes=["car"]),
        "per_person": reference.metrics.trip_count(
            by_variable="mode", normalize_by="person_count", normalize_scope="study_area",
        ),
        "gap": reference.metrics.trip_count(by_variable="mode", reference=("scenario", "default")),
        "project_only": reference.metrics.trip_count(by_variable="mode", scenarios=["project"]),
        "distance": reference.metrics.travel_distance(by_variable="mode"),
        "time": reference.metrics.travel_time(by_variable="mode", reference=("scenario", "default")),
        "cost": reference.metrics.cost(by_variable="mode", modes=["car"]),
        "emissions": reference.metrics.ghg_emissions(by_variable="mode"),
        "external": reference.metrics.trip_count(by_variable="mode", reference="external"),
        "immobility_survey": reference.metrics.immobility(reference="external"),
    }
    reference_steps = reference.tables.plan_steps(
        steps="all", columns=["scenario", "step", "mode", "n_persons"]
    )
    cost_history = reference.tables.costs(steps="all", columns=[
        "scenario", "step", "from", "to", "mode", "cost"
    ])
    expected_step_parts = []
    expected_cost_parts = []
    expected_tables = {}
    expected_metrics = {}

    def consume_reference(iteration):
        expected_step_parts.append(reference_steps.compute_iteration(iteration).collect())
        expected_cost_parts.append(cost_history.compute_iteration(iteration).collect())
        if iteration != 2:
            return
        for name, request in comparisons.items():
            expected_metrics[name] = request.compute_iteration(iteration)
        reference_tables = {
            "demand_groups": reference.tables.demand_groups(columns=[
                "scenario", "step", "replication", "demand_group_id", "n_persons"
            ]),
            "mnl_flows": reference.tables.mnl_od_mode_flows(),
            "final_costs": reference.tables.costs(columns=[
                "scenario", "step", "from", "to", "mode", "cost"
            ]),
            "transitions": reference.tables.transitions(columns=[
                "scenario", "step", "demand_group_id", "n_persons_moved"
            ]),
        }
        expected_tables.update({
            name: request.compute_iteration(iteration).collect()
            for name, request in reference_tables.items()
        })

    with asset_resolution_context():
        other.run("weekday").coordinated_run_asset.get(step_consumer=consume_reference)
        assert len(steps) == 4
        for (ids, states), (other_ids, other_states) in zip(steps[:2], steps[2:]):
            assert ids == other_ids
            for key, (rng, plans, _) in states.items():
                other_rng, other_plans, _ = other_states[key]
                assert rng == other_rng
                assert_frame_equal(plans.sort(plans.columns), other_plans.sort(other_plans.columns),
                                   check_exact=False, rel_tol=1e-12, abs_tol=1e-9)
    for name in comparisons:
        assert_frame_equal(values[name], expected_metrics[name],
                           check_exact=False, rel_tol=1e-12, abs_tol=1e-9)
    expected_steps = pl.concat(expected_step_parts)
    expected_tables["cost_history"] = pl.concat(expected_cost_parts)
    actual_steps = values["plan_steps"].collect()
    assert_frame_equal(actual_steps.sort(actual_steps.columns),
                       expected_steps.sort(expected_steps.columns),
                       check_exact=False, rel_tol=1e-12, abs_tol=1e-9)
    grouped = (
        values["plan_steps"].filter(pl.col("step") == 1)
        .group_by("scenario", "mode")
        .agg(people=pl.col("n_persons").sum())
        .sort("scenario", "mode")
        .collect()
    )
    expected_grouped = (
        expected_steps.filter(pl.col("step") == 1)
        .group_by("scenario", "mode")
        .agg(people=pl.col("n_persons").sum())
        .sort("scenario", "mode")
    )
    assert_frame_equal(grouped, expected_grouped, check_exact=False, rel_tol=1e-12, abs_tol=1e-9)
    values["plan_steps"].close()
    for name, expected in expected_tables.items():
        actual = values[name].collect()
        assert_frame_equal(actual.sort(actual.columns), expected.sort(expected.columns),
                           check_exact=False, rel_tol=1e-12, abs_tol=1e-9)
        values[name].close()
    assert_frame_equal(values["history_time"].filter(pl.col("step") == 1), values["first_time"],
                       check_exact=False, rel_tol=1e-12, abs_tol=1e-9)
    first_external = values["external_history"].filter(pl.col("step") == 1)
    assert_frame_equal(first_external.sort(first_external.columns),
                       values["external_first"].sort(first_external.columns),
                       check_exact=False, rel_tol=1e-12, abs_tol=1e-9)
    assert first_external["trip_count_reference"].null_count() == 0
    for history, selected in (
        ("by_demand", "by_demand_first"), ("immobility", "immobility_first"),
        ("occupation", "occupation_first"), ("durations", "durations_first"),
        ("activity_time", "activity_time_first"),
    ):
        left = values[history].filter(pl.col("step") == 1)
        right = values[selected]
        assert_frame_equal(left.sort(left.columns), right.sort(right.columns),
                           check_exact=False, rel_tol=1e-12, abs_tol=1e-9)
    diagnostics = values["step_diagnostics"].collect()
    first_diagnostics = values["first_diagnostics"].collect()
    first_rows = diagnostics.filter(pl.col("step") == 1)
    assert_frame_equal(first_rows.sort(first_rows.columns), first_diagnostics.sort(first_diagnostics.columns),
                       check_exact=False, rel_tol=1e-12, abs_tol=1e-9)
    search_diagnostics = values["search_diagnostics"].collect()
    assert set(search_diagnostics["scenario"].unique()) == {"default", "project"}
    assert set(search_diagnostics["outer_step"].unique()) == {1, 2}
    for iteration, (_, states) in enumerate(steps[:2], start=1):
        for key, (_, _, frame) in states.items():
            scenario = run.coordinated_run_asset.runs[key].scenario
            expected = frame.filter(pl.col("activity_seq_id") != 0)["n_persons"].sum()
            actual = values["history"].filter(
                (pl.col("step") == iteration) & (pl.col("scenario") == scenario)
            )["trip_count"].sum()
            assert actual == pytest.approx(expected, rel=1e-12, abs=1e-9)

    # An earlier-step-only request still advances the complete cohort, but
    # does not build post-update costs or diagnostics nobody requested.
    first_only = setup.experiment("weekday", scenarios=["default", "project"], replication=0).results()
    first_metric = first_only.metrics.travel_time(by_variable="mode", steps=1)
    first_metric.cache_path = tmp_path / "first-only.parquet"
    first_metric.hash_path = tmp_path / "first-only.inputs-hash"
    final_count = len(final_states)
    observed_count = len(steps)
    first_value = first_only.get_all(time=first_metric)["time"]
    assert_frame_equal(first_value, values["first_time"], check_exact=False, rel_tol=1e-12, abs_tol=1e-9)
    assert len(final_states) == final_count
    assert len(steps) == observed_count + 2
    for (expected_ids, expected_states), (actual_ids, actual_states) in zip(steps[:2], steps[observed_count:]):
        assert actual_ids == expected_ids
        assert {key: state[0] for key, state in actual_states.items()} == {
            key: state[0] for key, state in expected_states.items()
        }

    def unexpected_simulation(*args, **kwargs):
        raise AssertionError("Demand groups do not need a behavior simulation.")

    monkeypatch.setattr(MultiScenarioRunAsset, "_advance_cohort_step", unexpected_simulation)
    demand_request = reference.tables.demand_groups(columns=[
        "scenario", "step", "replication", "demand_group_id", "n_persons"
    ])
    assert "No simulation" in reference.explain(groups=demand_request)["simulation"]
    demand_table = reference.get_all(groups=demand_request)["groups"]
    demand_groups = demand_table.collect()
    demand_table.close()
    expected_demand = expected_tables["demand_groups"]
    assert_frame_equal(demand_groups.sort(demand_groups.columns), expected_demand.sort(expected_demand.columns))


def test_raw_tables_stream_independent_replication_lanes(test_data, monkeypatch, tmp_path):
    """Both seeds keep the full cohort and match separate full executions."""
    setup = make_setup(test_data, n_steps=1, n_replications=2, record_events=True)
    results = setup.results("weekday", scenarios=["default", "project"])
    lanes = results.explain(steps=results.tables.plan_steps())["lanes"]
    assert [(lane["replication"], lane["seed"]) for lane in lanes] == [(0, 108), (1, 109)]
    observed = []
    advance = MultiScenarioRunAsset._advance_cohort_step

    def record_step(coordinator, iteration, current_states, **kwargs):
        assets, states, search = advance(coordinator, iteration, current_states, **kwargs)
        observed.append((
            tuple(sorted(search["pool"].candidate_ids)),
            {key: state.rng_state for key, state in states.items()},
        ))
        return assets, states, search

    monkeypatch.setattr(MultiScenarioRunAsset, "_advance_cohort_step", record_step)
    requests = {
        "steps": results.tables.plan_steps(columns=[
            "scenario", "replication", "step", "mode", "n_persons"
        ]),
        "diagnostics": results.tables.step_metrics(columns=[
            "scenario", "replication", "step", "total_loss"
        ]),
        "transitions": results.tables.transitions(columns=[
            "scenario", "replication", "step", "demand_group_id", "n_persons_moved"
        ]),
        "trips": results.metrics.trip_count(by_variable="mode"),
        "gap": results.metrics.trip_count(by_variable="mode", reference=("scenario", "default")),
        "distance": results.metrics.travel_distance(by_variable="mode"),
        "share_gap": results.metrics.travel_distance(
            by_variable="mode", normalize_by="metric_total", normalize_scope="study_area",
            reference=("scenario", "default")
        ),
        "person_gap": results.metrics.trip_count(
            by_variable="mode", normalize_by="person_count", normalize_scope="study_area",
            reference=("scenario", "default")
        ),
        "filtered_gap": results.metrics.trip_count(
            by_variable="mode", modes=["car"], normalize_by="person_count",
            normalize_scope="study_area", reference=("scenario", "default")
        ),
        "external": results.metrics.trip_count(by_variable="mode", reference="external"),
        "external_step1": results.metrics.trip_count(by_variable="mode", reference="external", steps=1),
        "external_person": results.metrics.trip_count(
            by_variable="mode", normalize_by="person_count", normalize_scope="study_area",
            reference="external", reference_view="values",
        ),
        "by_demand": results.metrics.trip_count_by_demand_group(),
        "immobility": results.metrics.immobility(),
        "survey_immobility": results.metrics.immobility(reference="external"),
        "durations": results.metrics.activity_duration_distribution(),
        "survey_durations": results.metrics.activity_duration_distribution(reference="external"),
        "activity_time": results.metrics.activity_time_series(interval_minutes=60),
        "survey_activity_time": results.metrics.activity_time_series(reference="external", interval_minutes=60),
        "occupation": results.metrics.opportunity_occupation(inner_zone_residents_only=False),
    }
    for name, request in requests.items():
        if hasattr(request, "hash_path"):
            request.cache_path = tmp_path / f"{name}.parquet"
            request.hash_path = tmp_path / f"{name}.inputs-hash"
    description = results.explain(**requests)
    assert observed == []
    assert [lane["seed"] for lane in description["lanes"]] == [108, 109]
    assert all({member["scenario"] for member in lane["cohort"]} == {"default", "project"}
               for lane in description["lanes"])
    tables = results.get_all(**requests)
    assert len(observed) == 2
    for context in results.run_contexts:
        assert not hasattr(context.run.coordinated_run_asset, "_cached_result")

    reference = setup.results("weekday", scenarios=["default", "project"])
    reference_requests = {
            "steps": reference.tables.plan_steps(columns=[
                "scenario", "replication", "step", "mode", "n_persons"
            ]),
            "diagnostics": reference.tables.step_metrics(columns=[
                "scenario", "replication", "step", "total_loss"
            ]),
            "transitions": reference.tables.transitions(columns=[
                "scenario", "replication", "step", "demand_group_id", "n_persons_moved"
            ]),
            "trips": reference.metrics.trip_count(by_variable="mode"),
            "gap": reference.metrics.trip_count(
                by_variable="mode", reference=("scenario", "default")
            ),
            "distance": reference.metrics.travel_distance(by_variable="mode"),
            "share_gap": reference.metrics.travel_distance(
                by_variable="mode", normalize_by="metric_total", normalize_scope="study_area",
                reference=("scenario", "default")
            ),
            "person_gap": reference.metrics.trip_count(
                by_variable="mode", normalize_by="person_count", normalize_scope="study_area",
                reference=("scenario", "default")
            ),
            "filtered_gap": reference.metrics.trip_count(
                by_variable="mode", modes=["car"], normalize_by="person_count",
                normalize_scope="study_area", reference=("scenario", "default")
            ),
            "external": reference.metrics.trip_count(by_variable="mode", reference="external"),
            "external_step1": reference.metrics.trip_count(
                by_variable="mode", reference="external", steps=1
            ),
            "external_person": reference.metrics.trip_count(
                by_variable="mode", normalize_by="person_count", normalize_scope="study_area",
                reference="external", reference_view="values",
            ),
            "by_demand": reference.metrics.trip_count_by_demand_group(),
            "immobility": reference.metrics.immobility(),
            "survey_immobility": reference.metrics.immobility(reference="external"),
            "durations": reference.metrics.activity_duration_distribution(),
            "survey_durations": reference.metrics.activity_duration_distribution(reference="external"),
            "activity_time": reference.metrics.activity_time_series(interval_minutes=60),
            "survey_activity_time": reference.metrics.activity_time_series(reference="external", interval_minutes=60),
            "occupation": reference.metrics.opportunity_occupation(inner_zone_residents_only=False),
    }
    for name, request in reference_requests.items():
        if hasattr(request, "hash_path"):
            request.cache_path = tmp_path / f"reference-{name}.parquet"
            request.hash_path = tmp_path / f"reference-{name}.inputs-hash"
    expected = reference.get_all(**reference_requests)
    assert len(observed) == 4
    assert observed[:2] == observed[2:]
    for name, table in tables.items():
        actual = table.collect() if name in {"steps", "diagnostics", "transitions"} else table
        if name in {"steps", "diagnostics"}:
            assert set(actual["replication"].unique()) == {0, 1}
            assert set(actual["scenario"].unique()) == {"default", "project"}
        expected_table = expected[name]
        expected_value = expected_table.collect() if name in {"steps", "diagnostics", "transitions"} else expected_table
        assert_frame_equal(actual.sort(actual.columns), expected_value.sort(expected_value.columns),
                           check_exact=False, rel_tol=1e-12, abs_tol=1e-9)
        if name in {"steps", "diagnostics", "transitions"}:
            table.close()
            expected_table.close()


@pytest.mark.parametrize("table_name", ["plans", "plan_steps"])
def test_direct_diagnostics_read_step_from_full_cohort(test_data, monkeypatch, table_name):
    setup = make_setup(test_data)
    run = setup.experiment("weekday", scenarios=["default", "project"], replication=0).run("weekday")
    advance = MultiScenarioRunAsset._advance_cohort_step
    observed = []

    def record_step(coordinator, iteration, states, **kwargs):
        assets, next_states, search = advance(coordinator, iteration, states, **kwargs)
        if iteration == 1:
            state = next_states[run.coordinated_run_key]
            table = state.current_plans if table_name == "plans" else state.current_plan_steps
            observed.append((len(coordinator.runs), table.clone()))
        return assets, next_states, search

    monkeypatch.setattr(MultiScenarioRunAsset, "_advance_cohort_step", record_step)
    metrics = object.__new__(RunMetrics)
    actual = (
        metrics._saved_iteration_plans(run, 1) if table_name == "plans"
        else metrics._saved_iteration_plan_steps(run, 1)
    )
    assert len(observed) == 1 and observed[0][0] == 2
    expected = observed[0][1]
    sort_columns = [column for column, dtype in actual.schema.items() if dtype not in (pl.Float32, pl.Float64)]
    assert_frame_equal(actual.sort(sort_columns), expected.sort(sort_columns))


def test_direct_comparison_batches_selected_steps_and_scenarios(test_data, monkeypatch):
    setup = make_setup(test_data)
    run = setup.experiment("weekday", scenarios=["default", "project"], replication=0).run("weekday")
    reference = next(member for member in run.coordinated_run_asset.runs.values() if member.scenario == "project")
    advance = MultiScenarioRunAsset._advance_cohort_step
    append = ResultTable._append
    observed = []
    directories = []
    shares = {}

    def record_step(coordinator, iteration, states, **kwargs):
        observed.append((iteration, len(coordinator.runs)))
        assets, next_states, search = advance(coordinator, iteration, states, **kwargs)
        for key, state in next_states.items():
            rows = state.current_plan_steps.filter(pl.col("activity_seq_id") != 0)
            total = rows["n_persons"].sum()
            car = rows.filter(pl.col("mode").cast(pl.String) == "car")["n_persons"].sum()
            shares[(coordinator.runs[key].scenario, iteration)] = car / total if total else 0.0
        return assets, next_states, search

    def record_append(table, value):
        directories.append(table._directory.name)
        return append(table, value)

    monkeypatch.setattr(MultiScenarioRunAsset, "_advance_cohort_step", record_step)
    monkeypatch.setattr(ResultTable, "_append", record_append)
    analysis = run.results()
    assert observed == []
    result = analysis.metrics.modal_share_delta_evolution_by_step(
        reference, iterations=[1, 2], modes=["car"], plot=False,
    )
    assert set(result["step"]) == {1, 2}
    assert observed == [(1, 2), (2, 2)]
    assert len(directories) == 4
    assert all(not Path(directory).exists() for directory in directories)
    for row in result.iter_rows(named=True):
        assert row["modal_share"] == pytest.approx(shares[("default", row["step"])])
        assert row["modal_share_reference"] == pytest.approx(shares[("project", row["step"])])

    observed.clear()
    directories.clear()
    complete = analysis.metrics._complete_modal_share_evolution_table

    def interrupt_after_read(*args, **kwargs):
        raise KeyboardInterrupt()

    monkeypatch.setattr(analysis.metrics, "_complete_modal_share_evolution_table", interrupt_after_read)
    with pytest.raises(KeyboardInterrupt):
        analysis.metrics.modal_share_delta_evolution_by_step(
            reference, iterations=[1, 2], modes=["car"], plot=False,
        )
    assert observed == [(1, 2), (2, 2)]
    assert directories and all(not Path(directory).exists() for directory in directories)

    monkeypatch.setattr(analysis.metrics, "_complete_modal_share_evolution_table", complete)
    observed.clear()
    directories.clear()
    utility_delta = analysis.metrics.plot_delta_by_step(
        reference, iteration=1, value="mean_utility", zone="home", plot=False,
    )
    assert not utility_delta.is_empty()
    assert observed == [(1, 2), (2, 2)]
    assert len(directories) == 2 and all(not Path(directory).exists() for directory in directories)

    observed.clear()
    directories.clear()
    emissions_delta = analysis.metrics.plot_delta_by_step(
        reference, iteration=1, value="ghg_emissions", zone="home", plot=False,
    )
    assert not emissions_delta.is_empty()
    assert observed == [(1, 2), (2, 2)]
    assert len(directories) == 4 and all(not Path(directory).exists() for directory in directories)

    observed.clear()
    directories.clear()
    monkeypatch.setattr(analysis.metrics, "_ghg_emissions_by_zone_for_saved_iteration", interrupt_after_read)
    with pytest.raises(KeyboardInterrupt):
        analysis.metrics.plot_delta_by_step(
            reference, iteration=1, value="ghg_emissions", zone="home", plot=False,
        )
    assert observed == [(1, 2), (2, 2)]
    assert len(directories) == 4 and all(not Path(directory).exists() for directory in directories)


def test_direct_paired_diagnostic_runs_each_replication_once(test_data, monkeypatch):
    setup = make_setup(test_data, n_replications=2)
    contexts = setup.results("weekday", scenarios=["default", "project"]).run_contexts
    runs = {(context.replication, context.scenario): context.run for context in contexts}
    analysis = runs[(0, "default")].results()
    pairs = [(runs[(seed, "default")], runs[(seed, "project")]) for seed in (0, 1)]
    advance = MultiScenarioRunAsset._advance_cohort_step
    observed = []

    def record_step(coordinator, iteration, states, **kwargs):
        seed = coordinator.runs[coordinator.run_keys[0]].replication
        observed.append((seed, iteration, len(coordinator.runs)))
        return advance(coordinator, iteration, states, **kwargs)

    monkeypatch.setattr(MultiScenarioRunAsset, "_advance_cohort_step", record_step)
    result = analysis.metrics.plot_delta_by_step(
        None, iteration=1, value="mean_utility", zone="home", paired_runs=pairs, plot=False,
    )
    assert not result.is_empty()
    assert observed == [(0, 1, 2), (0, 2, 2), (1, 1, 2), (1, 2, 2)]
    observed.clear()
    emissions = analysis.metrics.plot_delta_by_step(
        None, iteration=1, value="ghg_emissions", zone="home", paired_runs=pairs, plot=False,
    )
    assert not emissions.is_empty()
    assert observed == [(0, 1, 2), (0, 2, 2), (1, 1, 2), (1, 2, 2)]


def test_direct_sensitivity_comparison_shares_complete_cohort(test_data, monkeypatch):
    setup = make_setup(test_data, n_steps=1, sensitivity=True)
    cases = collect_group_day_trips_sensitivity_cases(setup)
    experiment = setup.experiment(
        "weekday", scenarios=["default", "project"], sensitivity_cases=cases, replication=0,
    )
    run = experiment.run("weekday")
    reference = next(
        member for member in run.coordinated_run_asset.runs.values()
        if member.scenario == run.scenario and member.sensitivity_case_id != run.sensitivity_case_id
    )
    analysis = run.results()
    advance = MultiScenarioRunAsset._advance_cohort_step
    observed = []
    shares = {}

    def record_step(coordinator, iteration, states, **kwargs):
        observed.append((iteration, len(coordinator.runs)))
        assets, next_states, search = advance(coordinator, iteration, states, **kwargs)
        for key, state in next_states.items():
            member = coordinator.runs[key]
            if member.scenario == run.scenario:
                rows = state.current_plan_steps.filter(pl.col("activity_seq_id") != 0)
                total = rows["n_persons"].sum()
                car = rows.filter(pl.col("mode").cast(pl.String) == "car")["n_persons"].sum()
                shares[member.sensitivity_case_id] = car / total if total else 0.0
        return assets, next_states, search

    monkeypatch.setattr(MultiScenarioRunAsset, "_advance_cohort_step", record_step)
    result = analysis.metrics.modal_share_delta_evolution_by_step(
        reference, iterations=[1], modes=["car"], plot=False,
    )
    assert not result.is_empty()
    assert observed == [(1, 4)]
    assert result["modal_share"].item() == pytest.approx(shares[run.sensitivity_case_id])
    assert result["modal_share_reference"].item() == pytest.approx(shares[reference.sensitivity_case_id])


def test_custom_demand_group_metric_does_not_run_steps(test_data, monkeypatch, tmp_path):
    setup = make_setup(test_data)
    results = setup.experiment("weekday", scenarios=["default", "project"], replication=0).results()
    dependency = results.tables.demand_groups(steps="all")
    metric = Metric(
        name="custom_group_count", results=results, dependencies=[dependency],
        planned_outputs=dependency.planned_outputs, query={},
        compute=lambda: dependency.get().group_by(["scenario", "step"]).len(),
    )
    metric.cache_path = tmp_path / "groups.parquet"
    metric.hash_path = tmp_path / "groups.inputs-hash"

    def unexpected_step(*args, **kwargs):
        raise AssertionError("The demand-group metric must not run a simulation step.")

    monkeypatch.setattr(MultiScenarioRunAsset, "_advance_cohort_step", unexpected_step)
    opportunities = results.tables._table("opportunities", iterations="last")
    assert "No simulation" in results.explain(opportunities=opportunities)["simulation"]
    values = results.get_all(custom=metric, groups=dependency, opportunities=opportunities)
    expected = values["groups"].collect().group_by(["scenario", "step"]).len()
    assert_frame_equal(values["custom"].sort(expected.columns), expected.sort(expected.columns))
    assert set(values["opportunities"].collect()["scenario"]) == {"default", "project"}
    values["groups"].close()
    values["opportunities"].close()

    derived = Metric(
        name="groups_and_population", results=results, dependencies=[metric, dependency],
        planned_outputs=dependency.planned_outputs, query={},
        compute=lambda: metric.get().lazy().join(
            dependency.get().group_by("scenario", "step").agg(pl.col("n_persons").sum().alias("people")),
            on=["scenario", "step"],
        ),
    )
    derived.cache_path = tmp_path / "derived-groups.parquet"
    derived.hash_path = tmp_path / "derived-groups.inputs-hash"
    metric.hash_path.unlink()
    assert "No simulation" in results.explain(derived=derived)["simulation"]
    combined = results.get_all(derived=derived)["derived"]
    assert_frame_equal(combined.drop("people").sort(expected.columns), expected.sort(expected.columns))
    assert (combined["people"] > 0).all()
    derived.hash_path.unlink()
    shared = results.get_all(derived=derived, groups=dependency)
    assert_frame_equal(shared["derived"].sort(combined.columns), combined.sort(combined.columns))
    assert not shared["groups"].collect().is_empty()
    shared["groups"].close()


def test_custom_table_metric_shares_cohort_execution(test_data, monkeypatch, tmp_path):
    setup = make_setup(test_data, n_replications=2)
    results = setup.results("weekday", scenarios=["default", "project"])
    dependency = results.tables.plan_steps(steps="all")

    class Reference(FileAsset):
        def __init__(self):
            super().__init__({"version": 1}, tmp_path / "reference.parquet")

        def create_and_get_asset(self):
            pl.DataFrame({"factor": [2]}).write_parquet(self.cache_path)
            return self.get_cached_asset()

        def get_cached_asset(self):
            return pl.scan_parquet(self.cache_path)

    reference = Reference()
    custom = Metric(
        name="custom_step_rows", results=results, dependencies=[dependency, reference],
        planned_outputs=dependency.planned_outputs, query={},
        compute=lambda: dependency.get().group_by(["replication", "scenario", "step"]).len()
        .join(reference.get(), how="cross"),
    )
    custom.cache_path = tmp_path / "custom.parquet"
    custom.hash_path = tmp_path / "custom.inputs-hash"
    independent = Metric(
        name="reference_only", results=results, dependencies=[reference], query={},
        compute=lambda: reference.get().select(value=pl.col("factor") * 2),
    )
    independent.cache_path = tmp_path / "reference-only.parquet"
    independent.hash_path = tmp_path / "reference-only.inputs-hash"
    assert "No simulation" in results.explain(reference=independent)["simulation"]
    description = results.explain(custom=custom, reference=independent,
                                  table=results.tables.plan_steps(steps="all"))
    assert all(lane["final_costs"] == "not needed" and lane["inner_flow_diagnostics"] == "not needed"
               for lane in description["lanes"])
    steps = []
    advance = MultiScenarioRunAsset._advance_cohort_step

    def record_step(coordinator, iteration, *args, **kwargs):
        assert not kwargs["keep_inner_flows"]
        steps.append((coordinator.runs[coordinator.run_keys[0]].replication, iteration))
        return advance(coordinator, iteration, *args, **kwargs)

    def unexpected_final_costs(*args, **kwargs):
        raise AssertionError("Explicit step tables do not need final costs.")

    monkeypatch.setattr(MultiScenarioRunAsset, "_advance_cohort_step", record_step)
    monkeypatch.setattr(MultiScenarioRunAsset, "_finalize_cohort", unexpected_final_costs)
    values = results.get_all(
        custom=custom, reference=independent, table=results.tables.plan_steps(steps="all"),
    )
    expected = (values["table"].collect().group_by(["replication", "scenario", "step"]).len()
                .with_columns(factor=pl.lit(2, dtype=pl.Int64)))
    assert_frame_equal(values["custom"].sort(expected.columns), expected.sort(expected.columns))
    assert values["reference"]["value"].to_list() == [4]
    assert steps == [(0, 1), (0, 2), (1, 1), (1, 2)]
    values["table"].close()


def test_unknown_metric_dependency_does_not_split_supported_requests(test_data, monkeypatch, tmp_path):
    """Known tables and metrics still share a run alongside an unknown input."""
    setup = make_setup(test_data)
    results = setup.results("weekday", scenarios=["default", "project"])

    class CustomInput(InMemoryAsset):
        def __init__(self):
            super().__init__({"name": "independent custom input"})

        def create_and_get_asset(self):
            return pl.DataFrame({"value": [7]})

    source = CustomInput()
    custom = Metric(
        name="custom_unknown_input", results=results, dependencies=[source], query={},
        compute=lambda: source.get(),
    )
    custom.cache_path = tmp_path / "custom.parquet"
    custom.hash_path = tmp_path / "custom.inputs-hash"
    steps = []
    advance = MultiScenarioRunAsset._advance_cohort_step

    def record_step(coordinator, iteration, *args, **kwargs):
        steps.append(iteration)
        return advance(coordinator, iteration, *args, **kwargs)

    monkeypatch.setattr(MultiScenarioRunAsset, "_advance_cohort_step", record_step)
    values = results.get_all(
        custom=custom,
        trips=results.metrics.trip_count(by_variable="mode", steps="all"),
        table=results.tables.plan_steps(steps="all", columns=["scenario", "step", "mode"]),
    )
    assert steps == [1, 2]
    assert values["custom"]["value"].to_list() == [7]
    assert set(values["table"].collect()["step"].to_list()) == {1, 2}
    assert set(values["trips"]["step"].to_list()) == {1, 2}
    values["table"].close()


def test_requested_metric_and_derived_metric_share_the_cohort(test_data, monkeypatch, tmp_path):
    setup = make_setup(test_data)
    results = setup.results("weekday", scenarios=["default", "project"])
    trips = results.metrics.trip_count(by_variable="mode", steps="all")
    trips.cache_path = tmp_path / "trips.parquet"
    trips.hash_path = tmp_path / "trips.inputs-hash"
    derived = Metric(
        name="derived_trip_counts", results=results, dependencies=[trips],
        planned_outputs=trips.planned_outputs, query={},
        compute=lambda: trips.get().with_columns(doubled=pl.col("trip_count") * 2),
    )
    derived.cache_path = tmp_path / "derived.parquet"
    derived.hash_path = tmp_path / "derived.inputs-hash"
    outer = Metric(
        name="derived_again", results=results, dependencies=[derived],
        planned_outputs=trips.planned_outputs, query={},
        compute=lambda: derived.get().with_columns(added=pl.col("doubled") + 1),
    )
    outer.cache_path = tmp_path / "outer.parquet"
    outer.hash_path = tmp_path / "outer.inputs-hash"
    steps = []
    advance = MultiScenarioRunAsset._advance_cohort_step

    def record_step(coordinator, iteration, *args, **kwargs):
        steps.append(iteration)
        return advance(coordinator, iteration, *args, **kwargs)

    monkeypatch.setattr(MultiScenarioRunAsset, "_advance_cohort_step", record_step)
    values = results.get_all(
        outer=outer, derived=derived, trips=trips,
        table=results.tables.plan_steps(steps="all", columns=["scenario", "step", "mode"]),
    )
    assert steps == [1, 2]
    assert_frame_equal(values["derived"].drop("doubled"), values["trips"])
    assert values["derived"]["doubled"].to_list() == (values["trips"]["trip_count"] * 2).to_list()
    assert values["outer"]["added"].to_list() == (values["derived"]["doubled"] + 1).to_list()
    values["table"].close()
    steps.clear()
    cached = results.get_all(outer=outer)["outer"]
    assert steps == []
    assert_frame_equal(cached, values["outer"])

    # A missing upstream metric is scheduled with an unrequested chain, rather
    # than rebuilt separately by each derived metric's cache preparation.
    trips.hash_path.unlink()
    derived.hash_path.unlink()
    outer.hash_path.unlink()
    recomputed = results.get_all(outer=outer)["outer"]
    assert steps == [1, 2]
    assert_frame_equal(recomputed, values["outer"])


def test_derived_metric_shares_metric_and_table_dependencies(test_data, monkeypatch, tmp_path):
    setup = make_setup(test_data)
    results = setup.results("weekday", scenarios=["default", "project"])
    trips = results.metrics.trip_count(by_variable="mode", steps=1)
    trips.cache_path = tmp_path / "trips.parquet"
    trips.hash_path = tmp_path / "trips.inputs-hash"
    table = results.tables.plan_steps(steps=1, columns=["scenario", "step", "mode"])
    derived = Metric(
        name="trips_and_rows", results=results, dependencies=[trips, table],
        planned_outputs=[*trips.planned_outputs, *table.planned_outputs], query={},
        compute=lambda: trips.get().lazy().join(
            table.get().group_by("scenario", "step").len(), on=["scenario", "step"],
        ),
    )
    derived.cache_path = tmp_path / "mixed.parquet"
    derived.hash_path = tmp_path / "mixed.inputs-hash"
    steps = []
    advance = MultiScenarioRunAsset._advance_cohort_step

    def record_step(coordinator, iteration, *args, **kwargs):
        steps.append(iteration)
        assert not kwargs["keep_inner_flows"]
        return advance(coordinator, iteration, *args, **kwargs)

    def unexpected_final_costs(*args, **kwargs):
        raise AssertionError("Earlier-step dependencies do not need final costs.")

    monkeypatch.setattr(MultiScenarioRunAsset, "_advance_cohort_step", record_step)
    monkeypatch.setattr(MultiScenarioRunAsset, "_finalize_cohort", unexpected_final_costs)
    values = results.get_all(derived=derived)["derived"]
    assert steps == [1, 2]
    assert set(values["scenario"]) == {"default", "project"}
    assert (values["len"] > 0).all()


def test_derived_earlier_step_metric_does_not_build_final_outputs(test_data, monkeypatch, tmp_path):
    setup = make_setup(test_data)
    results = setup.results("weekday", scenarios=["default", "project"])
    trips = results.metrics.trip_count(by_variable="mode", steps=1)
    trips.cache_path = tmp_path / "first-trips.parquet"
    trips.hash_path = tmp_path / "first-trips.inputs-hash"
    derived = Metric(
        name="derived_first_step", results=results, dependencies=[trips],
        planned_outputs=trips.planned_outputs, query={},
        compute=lambda: trips.get().with_columns(doubled=pl.col("trip_count") * 2),
    )
    derived.cache_path = tmp_path / "derived.parquet"
    derived.hash_path = tmp_path / "derived.inputs-hash"
    assert results.explain(derived=derived)["lanes"][0]["final_costs"] == "not needed"
    retained = []
    advance = MultiScenarioRunAsset._advance_cohort_step

    def record_step(coordinator, iteration, *args, **kwargs):
        retained.append(kwargs["keep_inner_flows"])
        return advance(coordinator, iteration, *args, **kwargs)

    def unexpected_final_costs(*args, **kwargs):
        raise AssertionError("An earlier-step metric does not need final costs.")

    monkeypatch.setattr(MultiScenarioRunAsset, "_advance_cohort_step", record_step)
    monkeypatch.setattr(MultiScenarioRunAsset, "_finalize_cohort", unexpected_final_costs)
    result = results.get_all(derived=derived)["derived"]
    assert not result.is_empty()
    assert retained == [False, False]


def test_custom_inner_flow_metric_keeps_final_flows(test_data, monkeypatch, tmp_path):
    setup = make_setup(test_data)
    results = setup.experiment("weekday", scenarios=["default", "project"], replication=0).results()
    flows = results.tables.mnl_od_mode_flows()
    metric = Metric(
        name="custom_flow_rows", results=results, dependencies=[flows],
        planned_outputs=flows.planned_outputs, query={},
        compute=lambda: flows.get().group_by("scenario").len(),
    )
    metric.cache_path = tmp_path / "flow-rows.parquet"
    metric.hash_path = tmp_path / "flow-rows.inputs-hash"
    derived = Metric(
        name="derived_flow_rows", results=results, dependencies=[metric],
        planned_outputs=metric.planned_outputs, query={},
        compute=lambda: metric.get().with_columns(doubled=pl.col("len") * 2),
    )
    derived.cache_path = tmp_path / "derived-flow-rows.parquet"
    derived.hash_path = tmp_path / "derived-flow-rows.inputs-hash"
    lane = results.explain(derived=derived)["lanes"][0]
    assert lane["final_costs"] == "not needed"
    assert lane["inner_flow_diagnostics"] == "planned"
    advance = MultiScenarioRunAsset._advance_cohort_step
    retained = []

    def record_step(coordinator, iteration, *args, **kwargs):
        retained.append(kwargs["keep_inner_flows"])
        return advance(coordinator, iteration, *args, **kwargs)

    def unexpected_final_costs(*args, **kwargs):
        raise AssertionError("Inner-flow diagnostics do not need final costs.")

    monkeypatch.setattr(MultiScenarioRunAsset, "_advance_cohort_step", record_step)
    monkeypatch.setattr(MultiScenarioRunAsset, "_finalize_cohort", unexpected_final_costs)
    values = results.get_all(custom=metric, derived=derived)
    assert set(values["custom"]["scenario"]) == {"default", "project"}
    assert values["derived"]["doubled"].to_list() == (values["custom"]["len"] * 2).to_list()
    assert retained == [False, True]


def test_cached_metric_does_not_trigger_unneeded_final_costs(test_data, monkeypatch, tmp_path):
    setup = make_setup(test_data)
    results = setup.experiment("weekday", scenarios=["default", "project"], replication=0).results()
    metric = results.metrics.trip_count(by_variable="mode")
    metric.cache_path = tmp_path / "cached.parquet"
    metric.hash_path = tmp_path / "cached.inputs-hash"
    expected = metric.get()
    derived = Metric(
        name="derived_cached_trips", results=results, dependencies=[metric],
        planned_outputs=metric.planned_outputs, query={},
        compute=lambda: metric.get().with_columns(doubled=pl.col("trip_count") * 2),
    )
    derived.cache_path = tmp_path / "derived-cached.parquet"
    derived.hash_path = tmp_path / "derived-cached.inputs-hash"
    lane = results.explain(cached=metric, derived=derived, steps=results.tables.plan_steps(steps=2))["lanes"][0]
    assert lane["final_costs"] == "not needed"
    assert lane["inner_flow_diagnostics"] == "not needed"

    def unexpected_final_costs(*args, **kwargs):
        raise AssertionError("A cached metric must not cause final costs to be built.")

    advance = MultiScenarioRunAsset._advance_cohort_step
    steps = []

    def record_step(coordinator, iteration, states, **kwargs):
        assert not kwargs["keep_inner_flows"]
        steps.append((iteration, len(coordinator.runs)))
        return advance(coordinator, iteration, states, **kwargs)

    monkeypatch.setattr(MultiScenarioRunAsset, "_finalize_cohort", unexpected_final_costs)
    monkeypatch.setattr(MultiScenarioRunAsset, "_advance_cohort_step", record_step)
    values = results.get_all(cached=metric, derived=derived, steps=results.tables.plan_steps(steps=2))
    assert_frame_equal(values["cached"], expected)
    assert values["derived"]["doubled"].to_list() == (expected["trip_count"] * 2).to_list()
    assert steps == [(1, 2), (2, 2)]
    assert not values["steps"].collect().is_empty()
    values["steps"].close()


def test_step_and_inner_flow_tables_do_not_build_final_costs(test_data, monkeypatch):
    """These tables use live step data and do not need post-update costs."""
    setup = make_setup(test_data)
    finalized = []
    finalize = MultiScenarioRunAsset._finalize_cohort

    def record_final(coordinator, *args, **kwargs):
        finalized.append(True)
        return finalize(coordinator, *args, **kwargs)

    monkeypatch.setattr(MultiScenarioRunAsset, "_finalize_cohort", record_final)

    def request_tables(results, include_final_costs):
        requests = {
            "steps": results.tables.plan_steps(steps=2),
            "costs": results.tables.costs(steps=2),
            "diagnostics": results.tables.step_metrics(steps=2),
            "flows": results.tables.mnl_od_mode_flows(),
        }
        if include_final_costs:
            requests["final_costs"] = results.tables.costs()
        tables = results.get_all(**requests)
        values = {name: table.collect() for name, table in tables.items()}
        for table in tables.values():
            table.close()
        return values

    actual = request_tables(setup.experiment(
        "weekday", scenarios=["default", "project"], replication=0,
    ).results(), False)
    assert finalized == []
    expected = request_tables(setup.experiment(
        "weekday", scenarios=["default", "project"], replication=0,
    ).results(), True)
    assert finalized == [True]
    for name, value in actual.items():
        keys = [column for column, dtype in value.schema.items() if dtype not in (pl.Float32, pl.Float64)]
        assert_frame_equal(value.sort(keys), expected[name].sort(keys),
                           check_exact=False, rel_tol=1e-12, abs_tol=1e-9)


def test_direct_metric_batches_its_required_run_tables(test_data, monkeypatch):
    setup = make_setup(test_data)
    run = setup.experiment("weekday", scenarios=["default", "project"], replication=0).run("weekday")
    steps = []
    advance = MultiScenarioRunAsset._advance_cohort_step

    def record_step(coordinator, iteration, *args, **kwargs):
        steps.append(iteration)
        return advance(coordinator, iteration, *args, **kwargs)

    monkeypatch.setattr(MultiScenarioRunAsset, "_advance_cohort_step", record_step)
    analysis = run.results()
    assert steps == []
    result = analysis.metrics.metric_per_person("distance")
    assert steps == [1, 2]
    assert "distance_per_person" in result.columns
    steps.clear()
    reference = next(member for member in run.coordinated_run_asset.runs.values() if member.scenario == "project")
    comparison = analysis.metrics.metric_per_person("distance", compare_with=reference)
    assert steps == [1, 2]
    assert "distance_per_person_comp" in comparison.columns
    assert "delta" in comparison.columns
    steps.clear()
    reference_result = reference.results().metrics.metric_per_person("distance")
    assert steps == [1, 2]
    keys = ["transport_zone_id", "csp", "n_cars"]
    expected = (
        result.select(keys + ["distance_per_person"])
        .join(reference_result.select(keys + ["distance_per_person"]), on=keys, suffix="_comp")
        .with_columns(delta=pl.col("distance_per_person") - pl.col("distance_per_person_comp"))
        .sort(keys)
    )
    assert_frame_equal(comparison.select(expected.columns).sort(keys), expected,
                       check_exact=False, rel_tol=1e-12, abs_tol=1e-9)
    steps.clear()
    comparison_from_results = analysis.metrics.metric_per_person("distance", compare_with=reference.results())
    assert steps == [1, 2]
    assert_frame_equal(comparison_from_results.select(expected.columns).sort(keys), expected,
                       check_exact=False, rel_tol=1e-12, abs_tol=1e-9)
    steps.clear()
    diagnostic = analysis.diagnostics.trip_count_loss()
    assert steps == [1, 2]
    assert diagnostic.history()["step"].to_list() == [1, 2]


def test_direct_final_state_matches_full_cohort(test_data, monkeypatch):
    """A direct final-state request keeps the cohort's random path and result."""
    setup = make_setup(test_data)
    records = []
    first_steps = []
    advance = MultiScenarioRunAsset._advance_cohort_step
    finalize = MultiScenarioRunAsset._finalize_cohort

    def record_step(coordinator, iteration, states, **kwargs):
        assets, next_states, search = advance(coordinator, iteration, states, **kwargs)
        if len(records) < 2:
            first_steps.append({key: state.current_plan_steps.clone() for key, state in next_states.items()})
        records.append((tuple(sorted(search["pool"].candidate_ids)), {
            key: state.rng_state for key, state in next_states.items()
        }))
        return assets, next_states, search

    def check_release(coordinator, *args):
        if not records or len(records) > 2:
            return finalize(coordinator, *args)
        resolver = get_current_asset_resolver()
        for member in coordinator.runs.values():
            previous = member.iteration_state_assets[0]
            assert asset_graph_key(previous) not in resolver.in_memory_values
            assert previous.coordinated_candidate_plan_steps is None
        return finalize(coordinator, *args)

    monkeypatch.setattr(MultiScenarioRunAsset, "_advance_cohort_step", record_step)
    monkeypatch.setattr(MultiScenarioRunAsset, "_finalize_cohort", check_release)
    experiment = setup.experiment("weekday", scenarios=["default", "project"], replication=0)
    run = experiment.run("weekday")
    with asset_resolution_context() as resolver:
        marker = InMemoryAsset({"name": "unrelated-caller-value"})
        marker_value = resolver.get_in_memory_asset(marker, object)
        actual = run.get()
        assert not hasattr(run.coordinated_run_asset, "_cached_result")
        historical = run.iteration_table("plan_steps", 1).collect()
        assert resolver.in_memory_values[asset_graph_key(marker)] is marker_value
    expected_history = first_steps[0][run.coordinated_run_key]
    assert_frame_equal(historical.sort(historical.columns), expected_history.sort(expected_history.columns),
                       check_exact=False, rel_tol=1e-12, abs_tol=1e-9)
    assert len(records) == 4
    assert records[:2] == records[2:]

    diagnostics = run.table("adaptive_search_diagnostics")
    expected_diagnostics = actual.adaptive_search_diagnostics
    assert_frame_equal(diagnostics.sort(diagnostics.columns),
                       expected_diagnostics.sort(expected_diagnostics.columns),
                       check_exact=False, rel_tol=1e-12, abs_tol=1e-9)
    assert len(records) == 6
    assert records[:2] == records[4:]

    # Explicitly requested run tables share one new execution and retain the
    # original, unscoped table shape.
    names = ["iteration_metrics", "plan_steps", "opportunities", "costs", "transitions", "demand_groups"]
    tables = run._resolve_run_tables({name: None for name in names})
    assert len(records) == 8
    assert records[:2] == records[6:]
    assert set(tables) == {
        "iteration_metrics", "plan_steps", "opportunities", "costs", "transitions", "demand_groups"
    }
    plan_steps = tables["plan_steps"]
    assert "scenario" not in plan_steps.columns
    assert "replication" not in plan_steps.columns
    assert plan_steps["n_persons"].sum() == pytest.approx(actual.current_plan_steps["n_persons"].sum())
    assert tables["iteration_metrics"]["step"].unique().sort().to_list() == [1, 2]
    assert tables["iteration_metrics"].schema == pl.Schema(IterationMetricsHistory.schema())
    assert tables["transitions"].schema == pl.Schema(TRANSITION_EVENT_SCHEMA)
    assert not hasattr(run.coordinated_run_asset, "_cached_result")

    other = setup.experiment("weekday", scenarios=["default", "project"], replication=0)
    other_run = other.run("weekday")
    reference = other_run.coordinated_run_asset.get().final_states_by_run_key[other_run.coordinated_run_key]
    assert len(records) == 10
    assert records[:2] == records[8:]
    assert_frame_equal(actual.current_plans.sort(actual.current_plans.columns),
                       reference.current_plans.sort(reference.current_plans.columns),
                       check_exact=False, rel_tol=1e-12, abs_tol=1e-9)
    assert_frame_equal(actual.current_plan_steps.sort(actual.current_plan_steps.columns),
                       reference.current_plan_steps.sort(reference.current_plan_steps.columns),
                       check_exact=False, rel_tol=1e-12, abs_tol=1e-9)


def test_directly_constructed_run_uses_same_cohort_engine(test_data):
    setup = make_setup(test_data, n_steps=1)
    reference = setup.experiment("weekday", scenarios=["default"], replication=0).run("weekday")
    expected = reference.get()
    standalone = Run(
        population=reference.population, transport_costs=reference.transport_costs,
        activities=reference.activities, modes=reference.modes, surveys=reference.surveys,
        survey_plan_assets=reference.survey_plan_assets, parameters=reference.parameters,
        is_weekday=True, scenario="default", replication=0,
        cohort_identity=(("default", None),),
    )
    assert standalone.coordinated_run_asset is None
    analysis = standalone.results()
    assert isinstance(standalone.coordinated_run_asset, MultiScenarioRunAsset)
    assert analysis.run is standalone
    assert standalone.table("plan_steps", iteration=1).height > 0
    actual = standalone.get()
    assert actual.rng_state == expected.rng_state
    assert_frame_equal(
        actual.current_plans.sort(actual.current_plans.columns),
        expected.current_plans.sort(expected.current_plans.columns),
        check_exact=False, rel_tol=1e-12, abs_tol=1e-9,
    )


def test_rejected_checkpoint_is_rewritten_from_recomputed_state(test_data, monkeypatch, tmp_path):
    """An altered state file must not be accepted again after recomputation."""
    class IsolatedIterations(Iterations):
        def __init__(self, *, run_inputs_hash, is_weekday, base_folder):
            super().__init__(run_inputs_hash=run_inputs_hash, is_weekday=is_weekday, base_folder=tmp_path)

    monkeypatch.setattr("mobility.trips.group_day_trips.core.run.Iterations", IsolatedIterations)
    setup = make_setup(test_data, n_steps=1, record_events=True)
    setup.parameters.outputs.checkpoint_each_step = True
    first = setup.experiment("weekday", scenarios=["default", "project"], replication=0)
    run = first.run("weekday")
    run.get()
    asset = run.iterations.iteration(1).checkpoint_assets()["current_plans"]
    original = pl.read_parquet(asset.cache_path)
    original.with_columns(n_persons=pl.col("n_persons") + 1).write_parquet(asset.cache_path)
    event_path = run.iteration_state_assets[0].transition_events_asset.cache_path
    original_events = pl.read_parquet(event_path)
    event_path.write_bytes(event_path.read_bytes() + b"changed")

    # Reusing the same experiment is still a new execution. It must not keep
    # the previous execution's checkpoint-saved flags.
    run.get()
    assert_frame_equal(pl.read_parquet(asset.cache_path).sort(original.columns),
                       original.sort(original.columns), check_exact=False, rel_tol=1e-12, abs_tol=1e-9)
    assert pl.read_parquet(event_path).height == original_events.height
    original.with_columns(n_persons=pl.col("n_persons") + 1).write_parquet(asset.cache_path)
    event_path.write_bytes(event_path.read_bytes() + b"changed")

    second = setup.experiment("weekday", scenarios=["default", "project"], replication=0)
    second_run = second.run("weekday")
    assert all(member.iteration_state_assets[0].resume_checkpoint_loader is None
               for member in second_run.coordinated_run_asset.runs.values())
    second_run.get()
    assert_frame_equal(pl.read_parquet(asset.cache_path).sort(original.columns),
                       original.sort(original.columns), check_exact=False, rel_tol=1e-12, abs_tol=1e-9)
    rewritten_events = pl.read_parquet(event_path)
    assert rewritten_events.schema == original_events.schema
    assert rewritten_events.height == original_events.height
    third = setup.experiment("weekday", scenarios=["default", "project"], replication=0)
    third_run = third.run("weekday")
    assert all(member.iteration_state_assets[0].resume_checkpoint_loader is not None
               for member in third_run.coordinated_run_asset.runs.values())
    # Recheck files when execution starts, not just when the experiment is built.
    original.with_columns(n_persons=pl.col("n_persons") + 1).write_parquet(asset.cache_path)
    third_run.get()
    assert all(member.iteration_state_assets[0].resume_checkpoint_loader is None
               for member in third_run.coordinated_run_asset.runs.values())
    assert_frame_equal(pl.read_parquet(asset.cache_path).sort(original.columns),
                       original.sort(original.columns), check_exact=False, rel_tol=1e-12, abs_tol=1e-9)


def test_cohort_checkpoint_continuation_preserves_random_path(test_data, monkeypatch, tmp_path):
    """Resume both scenarios together without changing candidates or RNG state."""
    record_events = True
    checkpoint_folder = tmp_path / "checkpoints"

    class IsolatedIterations(Iterations):
        def __init__(self, *, run_inputs_hash, is_weekday, base_folder):
            super().__init__(
                run_inputs_hash=run_inputs_hash, is_weekday=is_weekday,
                base_folder=checkpoint_folder,
            )

    monkeypatch.setattr("mobility.trips.group_day_trips.core.run.Iterations", IsolatedIterations)
    setup = make_setup(test_data, record_events=record_events)
    setup.parameters.outputs.checkpoint_each_step = True
    observed = []
    advance = MultiScenarioRunAsset._advance_cohort_step

    def record_step(coordinator, iteration, states, **kwargs):
        assets, next_states, search = advance(coordinator, iteration, states, **kwargs)
        observed.append((
            tuple(sorted(search["pool"].candidate_ids)),
            {key: (state.rng_state, state.current_plans.clone()) for key, state in next_states.items()},
        ))
        return assets, next_states, search

    monkeypatch.setattr(MultiScenarioRunAsset, "_advance_cohort_step", record_step)
    first = setup.experiment("weekday", scenarios=["default", "project"], replication=0)
    first_run = first.run("weekday")
    first_results = first.results()
    first_metric = first_results.metrics.trip_count(by_variable="mode", steps="all")
    first_metric.cache_path = tmp_path / "first-metric.parquet"
    first_metric.hash_path = tmp_path / "first-metric.inputs-hash"
    baseline = first_metric.get()
    coordinator = first_run.coordinated_run_asset
    saved = [
        member.iterations.iteration(step).checkpoint_assets()["current_plans"].cache_path
        for member in coordinator.runs.values() for step in (1, 2)
    ]
    saved_times = [path.stat().st_mtime_ns for path in saved]
    assert coordinator._checkpoint_manifest_path(2).exists()

    resumed = setup.experiment("weekday", scenarios=["default", "project"], replication=0)
    resumed_run = resumed.run("weekday")
    assert all(asset.resume_checkpoint_loader is not None
               for member in resumed_run.coordinated_run_asset.runs.values()
               for asset in member.iteration_state_assets)
    resumed_metric = resumed.results().metrics.trip_count(by_variable="mode", steps="all")
    resumed_metric.cache_path = tmp_path / "resumed-metric.parquet"
    resumed_metric.hash_path = tmp_path / "resumed-metric.inputs-hash"
    actual = resumed_metric.get()
    assert [path.stat().st_mtime_ns for path in saved] == saved_times
    assert all(asset._loaded_checkpoint for member in resumed_run.coordinated_run_asset.runs.values()
               for asset in member.iteration_state_assets)
    assert_frame_equal(actual, baseline, check_exact=False, rel_tol=1e-12, abs_tol=1e-9)
    assert len(observed) == 4
    for (ids, states), (other_ids, other_states) in zip(observed[:2], observed[2:]):
        assert ids == other_ids
        for key, (rng, plans) in states.items():
            other_rng, other_plans = other_states[key]
            assert rng == other_rng
            assert_frame_equal(plans.sort(plans.columns), other_plans.sort(other_plans.columns),
                               check_exact=False, rel_tol=1e-12, abs_tol=1e-9)

    # A marker from this cohort cannot authorize a different scenario set.
    smaller = setup.experiment("weekday", scenarios=["default"], replication=0).run("weekday")
    assert all(asset.resume_checkpoint_loader is None for asset in smaller.iteration_state_assets)

    # A file changed in place must not be trusted just because its path and
    # input-hash sidecar are unchanged.
    with saved[0].open("r+b") as file:
        file.seek(-1, 2)
        original_byte = file.read(1)
        file.seek(-1, 2)
        file.write(bytes([original_byte[0] ^ 1]))
    changed = setup.experiment("weekday", scenarios=["default", "project"], replication=0).run("weekday")
    assert all(asset.resume_checkpoint_loader is None
               for member in changed.coordinated_run_asset.runs.values()
               for asset in member.iteration_state_assets)
    with saved[0].open("r+b") as file:
        file.seek(-1, 2)
        file.write(original_byte)

    if record_events:
        event_path = next(iter(coordinator.runs.values())).iteration_state_assets[0].transition_events_asset.cache_path
        original_events = event_path.read_bytes()
        event_path.write_bytes(original_events + b"changed")
        changed_events = setup.experiment("weekday", scenarios=["default", "project"], replication=0).run("weekday")
        assert all(asset.resume_checkpoint_loader is None
                   for member in changed_events.coordinated_run_asset.runs.values()
                   for asset in member.iteration_state_assets)
        event_path.write_bytes(original_events)

    # A missing file in one member must disable resume for the whole cohort.
    saved[0].unlink()
    incomplete = setup.experiment("weekday", scenarios=["default", "project"], replication=0).run("weekday")
    assert all(asset.resume_checkpoint_loader is None
               for member in incomplete.coordinated_run_asset.runs.values()
               for asset in member.iteration_state_assets)
    assert all(path.exists() for path in saved[1:])

    # A one-step run can be extended only if its saved cohort state matches
    # the first step of the independently executed two-step run.
    checkpoint_folder = tmp_path / "extended-checkpoints"
    short_setup = make_setup(test_data, n_steps=1, record_events=record_events)
    short_setup.parameters.outputs.checkpoint_each_step = True
    short_run = short_setup.experiment(
        "weekday", scenarios=["default", "project"], replication=0
    ).run("weekday")
    short_run.get()
    extended = setup.experiment("weekday", scenarios=["default", "project"], replication=0)
    extended_run = extended.run("weekday")
    assert all(member.iteration_state_assets[0].resume_checkpoint_loader is not None
               and member.iteration_state_assets[1].resume_checkpoint_loader is None
               for member in extended_run.coordinated_run_asset.runs.values())
    extended_metric = extended.results().metrics.trip_count(by_variable="mode", steps="all")
    extended_metric.cache_path = tmp_path / "extended-metric.parquet"
    extended_metric.hash_path = tmp_path / "extended-metric.inputs-hash"
    extended_values = extended_metric.get()
    assert_frame_equal(extended_values, baseline, check_exact=False, rel_tol=1e-12, abs_tol=1e-9)
    assert len(observed) == 7
    for (ids, states), (other_ids, other_states) in zip(observed[:2], observed[5:]):
        assert ids == other_ids
        for key, (rng, plans) in states.items():
            other_rng, other_plans = other_states[key]
            assert rng == other_rng
            assert_frame_equal(plans.sort(plans.columns), other_plans.sort(other_plans.columns),
                               check_exact=False, rel_tol=1e-12, abs_tol=1e-9)


def test_filtered_sensitivity_and_cached_metric_keep_full_cohort(test_data, monkeypatch, tmp_path):
    """A result view and a cache hit must not change shared candidate generation."""
    setup = make_setup(test_data, n_steps=1, sensitivity=True)
    cases = collect_group_day_trips_sensitivity_cases(setup)
    assert len(cases) == 2 and cases[0] is None
    experiment = setup.experiment(
        "weekday", scenarios=["default", "project"], sensitivity_cases=cases, replication=0
    )
    records = []
    advance = MultiScenarioRunAsset._advance_cohort_step

    def record_step(coordinator, iteration, states, **kwargs):
        assert not kwargs["keep_inner_flows"]
        assets, next_states, search = advance(coordinator, iteration, states, **kwargs)
        records.append((
            {(run.scenario, run.sensitivity_case_id) for run in coordinator.runs.values()},
            tuple(sorted(search["pool"].candidate_ids)),
            {key: state.rng_state for key, state in next_states.items()},
        ))
        return assets, next_states, search

    monkeypatch.setattr(MultiScenarioRunAsset, "_advance_cohort_step", record_step)
    full = experiment.results()
    full_metric = full.metrics.trip_count(by_variable="mode")
    full_metric.cache_path = tmp_path / "full-sensitivity.parquet"
    full_metric.hash_path = tmp_path / "full-sensitivity.inputs-hash"
    all_values = full_metric.get()

    selected = experiment.results(scenarios=["project"], sensitivity_cases=[cases[1]])
    selected_metric = selected.metrics.trip_count(by_variable="mode")
    selected_metric.cache_path = tmp_path / "selected-sensitivity.parquet"
    selected_metric.hash_path = tmp_path / "selected-sensitivity.inputs-hash"
    selected_values = selected_metric.get()
    expected = all_values.filter(
        (pl.col("scenario") == "project") & (pl.col("sensitivity_case") == cases[1].case_id)
    )
    assert_frame_equal(selected_values, expected, check_exact=False, rel_tol=1e-12, abs_tol=1e-9)

    # The already cached metric is not a reason to remove cohort members when
    # another output still needs the simulation.
    trace_path = tmp_path / "cached-and-live-trace.jsonl"
    monkeypatch.setenv("MOBILITY_SIMULATION_TRACE", str(trace_path))
    assert_frame_equal(selected.get_all(cached=selected_metric)["cached"], selected_values)
    assert not trace_path.exists()
    outputs = selected.get_all(
        cached=selected_metric,
        steps=selected.tables.plan_steps(columns=["scenario", "sensitivity_case", "mode"]),
    )
    assert_frame_equal(outputs["cached"], selected_values)
    table = outputs["steps"].collect()
    outputs["steps"].close()
    assert set(table["scenario"].unique()) == {"project"}
    assert set(table["sensitivity_case"].unique()) == {cases[1].case_id}
    trace = [json.loads(line) for line in trace_path.read_text().splitlines()]
    assert sum(row["operation"] == "coordinated run" for row in trace) == 1
    assert len(records) == 3
    expected_members = {
        (scenario, case_id)
        for scenario in ("default", "project") for case_id in ("base", cases[1].case_id)
    }
    assert all(members == expected_members for members, _, _ in records)
    assert records[0][1:] == records[1][1:] == records[2][1:]
