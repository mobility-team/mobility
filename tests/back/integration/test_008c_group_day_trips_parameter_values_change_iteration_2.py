import pytest
import polars as pl

import mobility
from mobility.activities import HomeActivity, OtherActivity, WorkActivity
from mobility.trips.group_day_trips import (
    GroupDayTripsDestinationSequenceParameters,
    GroupDayTripsModeSequenceParameters,
    GroupDayTripsOutputParameters,
    GroupDayTripsParameters,
    GroupDayTripsPeriodParameters,
    GroupDayTripsRunParameters,
    PopulationGroupDayTrips,
)
from mobility.surveys.france import EMPMobilitySurvey


@pytest.mark.dependency(
    depends=[
        "tests/back/integration/test_008_group_day_trips_can_be_computed.py::test_008_group_day_trips_can_be_computed",
    ],
    scope="session",
)
def test_008c_group_day_trips_parameter_values_change_iteration_2(test_data):
    transport_zones = mobility.TransportZones(
        local_admin_unit_id=test_data["transport_zones_local_admin_unit_id"],
        radius=test_data["transport_zones_radius"],
    )

    emp = EMPMobilitySurvey()
    pop = mobility.Population(
        transport_zones,
        sample_size=test_data["population_sample_size"],
    )

    def build_modes():
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
        return [car_mode, walk_mode, bicycle_mode, public_transport_mode]

    static = PopulationGroupDayTrips(
        population=pop,
        modes=build_modes(),
        activities=[
            HomeActivity(),
            WorkActivity(value_of_time=5.0),
            OtherActivity(population=pop),
        ],
        surveys=[emp],
        parameters=GroupDayTripsParameters(
            run=GroupDayTripsRunParameters(
                n_iterations=2,
                n_iter_per_cost_update=0,
                seed=0,
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

    dynamic = PopulationGroupDayTrips(
        population=pop,
        modes=build_modes(),
        activities=[
            HomeActivity(),
            WorkActivity(
                value_of_time=mobility.ParameterValue.by_iteration(
                    {1: 5.0, 2: 50.0},
                ),
            ),
            OtherActivity(population=pop),
        ],
        surveys=[emp],
        parameters=GroupDayTripsParameters(
            run=GroupDayTripsRunParameters(
                n_iterations=2,
                n_iter_per_cost_update=0,
                seed=0,
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

    static_run = static.run("weekday")
    dynamic_run = dynamic.run("weekday")

    assert dynamic_run.initial_iteration_state.inputs_hash == static_run.initial_iteration_state.inputs_hash
    assert dynamic_run.iteration_state_assets[0].inputs_hash == static_run.iteration_state_assets[0].inputs_hash
    assert dynamic_run.iteration_state_assets[1].inputs_hash != static_run.iteration_state_assets[1].inputs_hash

    static_plan_steps = static_run.get()["plan_steps"].collect()
    static_iteration_1_state_path = static_run.iteration_state_assets[0].cache_path["current_plans"]
    static_iteration_1_state_mtime = static_iteration_1_state_path.stat().st_mtime

    dynamic_plan_steps = dynamic_run.get()["plan_steps"].collect()
    static_transitions = static_run.get()["transitions"].collect()
    dynamic_transitions = dynamic_run.get()["transitions"].collect()

    assert static_plan_steps.height > 0
    assert dynamic_plan_steps.height > 0
    assert static_transitions.height > 0
    assert dynamic_transitions.height > 0
    assert dynamic_run.iteration_state_assets[0].cache_path["current_plans"] == static_iteration_1_state_path
    assert dynamic_run.iteration_state_assets[0].cache_path["current_plans"].stat().st_mtime == static_iteration_1_state_mtime

    static_iter_2 = static_transitions.filter(pl.col("iteration") == 2)
    dynamic_iter_2 = dynamic_transitions.filter(pl.col("iteration") == 2)

    assert static_iter_2.height > 0
    assert dynamic_iter_2.height > 0

    static_utility_sum = static_iter_2["utility_to"].sum()
    dynamic_utility_sum = dynamic_iter_2["utility_to"].sum()

    assert dynamic_utility_sum != pytest.approx(static_utility_sum)
