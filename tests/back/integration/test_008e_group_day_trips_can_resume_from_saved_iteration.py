import pytest

import mobility
from mobility.activities import HomeActivity, OtherActivity, WorkActivity
from mobility.surveys.france import EMPMobilitySurvey
from mobility.trips.group_day_trips import (
    GroupDayTripsDestinationSequenceParameters,
    GroupDayTripsModeSequenceParameters,
    GroupDayTripsOutputParameters,
    GroupDayTripsParameters,
    GroupDayTripsPeriodParameters,
    GroupDayTripsRunParameters,
    PopulationGroupDayTrips,
)


def _build_group_day_trips(test_data):
    transport_zones = mobility.TransportZones(
        local_admin_unit_id=test_data["transport_zones_local_admin_unit_id"],
        radius=test_data["transport_zones_radius"],
    )

    emp = EMPMobilitySurvey()
    pop = mobility.Population(
        transport_zones,
        sample_size=test_data["population_sample_size"],
    )

    car_mode = mobility.CarMode(transport_zones)
    walk_mode = mobility.WalkMode(transport_zones)
    bicycle_mode = mobility.BicycleMode(transport_zones)
    mode_registry = mobility.ModeRegistry([car_mode, walk_mode, bicycle_mode])
    public_transport_mode = mobility.PublicTransportMode(
        transport_zones,
        mode_registry=mode_registry,
        routing_parameters=mobility.PublicTransportRoutingParameters(
            gtfs_reference_date="2026-01-01",
            gtfs_sources_folder="inputs/gtfs_sources",
        ),
    )

    return PopulationGroupDayTrips(
        population=pop,
        modes=[car_mode, walk_mode, bicycle_mode, public_transport_mode],
        activities=[HomeActivity(), WorkActivity(), OtherActivity(population=pop)],
        surveys=[emp],
        parameters=GroupDayTripsParameters(
            run=GroupDayTripsRunParameters(
                n_iterations=2,
                n_iter_per_cost_update=0,
                seed=108,
            ),
            periods=GroupDayTripsPeriodParameters(simulate_weekend=False),
            outputs=GroupDayTripsOutputParameters(
                cache_iteration_events=True,
            ),
            destination_sequences=GroupDayTripsDestinationSequenceParameters(
                dest_prob_cutoff=0.9,
                cost_uncertainty_sd=1.0,
            ),
            mode_sequences=GroupDayTripsModeSequenceParameters(
                k_mode_sequences=6,
                mode_sequence_search_parallel=False,
            ),
        ),
    )


@pytest.mark.dependency(
    depends=[
        "tests/back/integration/test_008_group_day_trips_can_be_computed.py::test_008_group_day_trips_can_be_computed",
    ],
    scope="session",
)
def test_008e_group_day_trips_reuses_cached_iteration_state(test_data):
    pop_trips = _build_group_day_trips(test_data)
    run = pop_trips.run("weekday")
    run.remove()

    first_result = run.get()
    first_plan_steps = first_result["plan_steps"].collect()
    first_transitions = first_result["transitions"].collect()
    first_iteration_metrics = first_result["iteration_metrics"].collect()
    final_state_path = run.final_iteration_state.cache_path["current_plans"]
    final_state_mtime = final_state_path.stat().st_mtime

    assert first_plan_steps.height > 0
    assert first_transitions.height > 0
    assert first_iteration_metrics["iteration"].to_list() == [1, 2]

    # Remove only the final run outputs. The content-addressed iteration state
    # cache stays in place, so the next identical run can skip the iteration loop
    # and rebuild only the final output tables.
    for path in run.cache_path.values():
        path.unlink(missing_ok=True)

    rerun = _build_group_day_trips(test_data).run("weekday")
    assert rerun.final_iteration_state.inputs_hash == run.final_iteration_state.inputs_hash

    result = rerun.get()
    plan_steps = result["plan_steps"].collect()
    transitions = result["transitions"].collect()
    iteration_metrics = result["iteration_metrics"].collect()

    assert rerun.final_iteration_state.cache_path["current_plans"].stat().st_mtime == final_state_mtime
    assert plan_steps.height > 0
    assert transitions.height > 0
    assert iteration_metrics.height == 2
    assert sorted(transitions["iteration"].unique().to_list()) == [1, 2]
    assert iteration_metrics["iteration"].to_list() == [1, 2]
    assert rerun.final_iteration_state.get().destination_saturation.columns == [
        "activity",
        "to",
        "opportunity_capacity",
        "opportunity_occupation",
        "capacity_ratio",
        "destination_soft_capacity_factor",
        "k_saturation_utility",
        "destination_shadow_price",
        "destination_sampling_overload_gamma",
        "destination_sampling_min_attraction_factor",
        "destination_sampling_attraction_factor",
    ]
    assert {
        "total_loss",
        "activity_loss",
        "distance_bin_loss",
        "time_bin_loss",
        "mode_loss",
        "observed_entropy",
        "mean_utility",
        "mean_trip_count",
        "mean_travel_time",
        "mean_travel_distance",
        "excess_occupation_share",
    }.issubset(set(iteration_metrics.columns))
    assert iteration_metrics["mean_utility"].null_count() == 0
    assert iteration_metrics["mean_trip_count"].null_count() == 0
    assert iteration_metrics["mean_travel_time"].null_count() == 0
    assert iteration_metrics["mean_travel_distance"].null_count() == 0
    assert iteration_metrics["excess_occupation_share"].null_count() == 0
