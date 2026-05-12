import pytest

import mobility
from mobility.activities import HomeActivity, OtherActivity, WorkActivity
from mobility.trips.group_day_trips import Parameters, PopulationGroupDayTrips
from mobility.surveys.france import EMPMobilitySurvey


@pytest.mark.dependency(
    depends=[
        "tests/back/integration/test_008_group_day_trips_can_be_computed.py::test_008_group_day_trips_can_be_computed"
    ],
    scope="session",
)
def test_009_group_day_trips_results_can_be_computed(test_data):
    transport_zones = mobility.TransportZones(
        local_admin_unit_id=test_data["transport_zones_local_admin_unit_id"],
        radius=test_data["transport_zones_radius"],
    )
    emp = EMPMobilitySurvey()

    pop = mobility.Population(
        transport_zones,
        sample_size=test_data["population_sample_size"],
    )

    pop_trips = PopulationGroupDayTrips(
        population=pop,
        modes=[mobility.CarMode(transport_zones)],
        activities=[HomeActivity(), WorkActivity(), OtherActivity(population=pop)],
        surveys=[emp],
        parameters=Parameters(
            n_iterations=1,
            n_iter_per_cost_update=0,
            alpha=0.01,
            dest_prob_cutoff=0.9,
            k_mode_sequences=3,
            cost_uncertainty_sd=1.0,
            mode_sequence_search_parallel=False,
            seed=0,
            persist_iteration_artifacts=True,
            save_transition_events=True,
        ),
    )

    # Evaluate various metrics
    results = pop_trips.weekday_run.results()
    global_metrics = results.metrics.aggregate()
    weekday_metrics_by_mode = results.metrics.travel_indicators_by(
        variable="mode",
        normalize=True,
        plot=False,
    )
    weekday_metrics_by_activity = results.metrics.travel_indicators_by(
        variable="activity",
        normalize=False,
        plot=False,
    )
    weekday_metrics_by_time_bin = results.metrics.travel_indicators_by(
        variable="time_bin",
        plot=False,
    )
    weekday_metrics_by_distance_bin = results.metrics.travel_indicators_by(
        variable="distance_bin",
        plot=False,
    )
    weekday_immobility = results.metrics.immobility(plot=False)
    weekday_opportunity_occupation = results.metrics.opportunity_occupation()
    weekday_state_waterfall, weekday_state_waterfall_summary = results.transitions.state_waterfall(
        quantity="distance",
        plot=False,
        top_n=3,
    )
    weekday_trip_count_by_demand_group = results.metrics.trip_count_by_demand_group()
    weekday_distance_per_person = results.metrics.distance_per_person()
    weekday_ghg_per_person = results.metrics.ghg_per_person()
    weekday_time_per_person = results.metrics.time_per_person()
    weekday_cost_per_person = results.metrics.cost_per_person()
    grouped_global_metrics = results.metrics.aggregate()
    iteration_metrics = results.diagnostics.iteration_metrics()
    weekday_distance_compare = results.metrics.distance_per_person(compare_with=pop_trips)

    assert global_metrics.height > 0
    assert weekday_metrics_by_mode.height > 0
    assert weekday_metrics_by_activity.height > 0
    assert weekday_metrics_by_time_bin.height > 0
    assert weekday_metrics_by_distance_bin.height > 0
    assert weekday_immobility.height > 0
    assert weekday_opportunity_occupation.height > 0
    assert weekday_state_waterfall.height > 0
    assert weekday_state_waterfall_summary.height > 0
    assert weekday_trip_count_by_demand_group.height > 0
    assert weekday_distance_per_person.height > 0
    assert weekday_ghg_per_person.height > 0
    assert weekday_time_per_person.height > 0
    assert weekday_cost_per_person.height > 0
    assert grouped_global_metrics.height > 0
    assert iteration_metrics.height > 0
    assert weekday_distance_compare.height > 0

    assert {"variable", "mode", "value", "value_ref", "delta", "delta_relative"}.issubset(set(weekday_metrics_by_mode.columns))
    assert {"variable", "activity", "value", "value_ref", "delta", "delta_relative"}.issubset(set(weekday_metrics_by_activity.columns))
    assert {"variable", "time_bin", "value", "value_ref", "delta", "delta_relative"}.issubset(set(weekday_metrics_by_time_bin.columns))
    assert {"variable", "distance_bin", "value", "value_ref", "delta", "delta_relative"}.issubset(set(weekday_metrics_by_distance_bin.columns))
    assert {"country", "csp", "p_immobility", "p_immobility_ref"}.issubset(set(weekday_immobility.columns))
    assert {"iteration", "total_loss", "observed_entropy", "mean_utility"}.issubset(set(iteration_metrics.columns))
    assert "delta" in weekday_distance_compare.columns
