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
            seed=0
        ),
    )

    # Evaluate various metrics
    global_metrics = pop_trips.weekday_run.evaluate("global_metrics")
    weekday_metrics_by_mode = pop_trips.weekday_run.evaluate(
        "metrics_by_variable",
        variable="mode",
        normalize=True,
        plot=False,
    )
    weekday_metrics_by_activity = pop_trips.weekday_run.evaluate(
        "metrics_by_variable",
        variable="activity",
        normalize=False,
        plot=False,
    )
    weekday_metrics_by_time_bin = pop_trips.weekday_run.evaluate(
        "metrics_by_variable",
        variable="time_bin",
        plot=False,
    )
    weekday_metrics_by_distance_bin = pop_trips.weekday_run.evaluate(
        "metrics_by_variable",
        variable="distance_bin",
        plot=False,
    )
    weekday_immobility = pop_trips.weekday_run.evaluate("immobility", plot=False)
    weekday_opportunity_occupation = pop_trips.weekday_run.evaluate("opportunity_occupation")
    weekday_state_waterfall, weekday_state_waterfall_summary = pop_trips.weekday_run.evaluate(
        "state_waterfall",
        quantity="distance",
        plot=False,
        top_n=3,
    )
    weekday_trip_count_by_demand_group = pop_trips.weekday_run.evaluate("trip_count_by_demand_group")
    weekday_distance_per_person = pop_trips.weekday_run.evaluate("distance_per_person")
    weekday_ghg_per_person = pop_trips.weekday_run.evaluate("ghg_per_person")
    weekday_time_per_person = pop_trips.weekday_run.evaluate("time_per_person")
    weekday_cost_per_person = pop_trips.weekday_run.evaluate("cost_per_person")
    weekday_distance_compare = pop_trips.weekday_run.results().distance_per_person(compare_with=pop_trips)

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
    assert weekday_distance_compare.height > 0

    assert {"variable", "mode", "value", "value_ref", "delta", "delta_relative"}.issubset(set(weekday_metrics_by_mode.columns))
    assert {"variable", "activity", "value", "value_ref", "delta", "delta_relative"}.issubset(set(weekday_metrics_by_activity.columns))
    assert {"variable", "time_bin", "value", "value_ref", "delta", "delta_relative"}.issubset(set(weekday_metrics_by_time_bin.columns))
    assert {"variable", "distance_bin", "value", "value_ref", "delta", "delta_relative"}.issubset(set(weekday_metrics_by_distance_bin.columns))
    assert {"country", "csp", "p_immobility", "p_immobility_ref"}.issubset(set(weekday_immobility.columns))
    assert "delta" in weekday_distance_compare.columns
