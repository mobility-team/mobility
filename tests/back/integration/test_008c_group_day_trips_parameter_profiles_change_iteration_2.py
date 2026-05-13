import pytest
import polars as pl

import mobility
from mobility.activities import HomeActivity, OtherActivity, WorkActivity
from mobility.trips.group_day_trips import Parameters, PopulationGroupDayTrips
from mobility.surveys.france import EMPMobilitySurvey


@pytest.mark.dependency(
    depends=[
        "tests/back/integration/test_008_group_day_trips_can_be_computed.py::test_008_group_day_trips_can_be_computed",
    ],
    scope="session",
)
def test_008c_group_day_trips_parameter_profiles_change_iteration_2(test_data):
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
        parameters=Parameters(
            n_iterations=2,
            n_iter_per_cost_update=0,
            dest_prob_cutoff=0.9,
            k_mode_sequences=6,
            cost_uncertainty_sd=1.0,
            mode_sequence_search_parallel=False,
            persist_iteration_artifacts=True,
            save_transition_events=True,
            simulate_weekend=False,
            seed=0,
        ),
    )

    dynamic = PopulationGroupDayTrips(
        population=pop,
        modes=build_modes(),
        activities=[
            HomeActivity(),
            WorkActivity(
                value_of_time=mobility.ScalarParameterProfile(
                    mode="step",
                    points={1: 5.0, 2: 50.0},
                )
            ),
            OtherActivity(population=pop),
        ],
        surveys=[emp],
        parameters=Parameters(
            n_iterations=2,
            n_iter_per_cost_update=0,
            dest_prob_cutoff=0.9,
            k_mode_sequences=6,
            cost_uncertainty_sd=1.0,
            mode_sequence_search_parallel=False,
            persist_iteration_artifacts=True,
            save_transition_events=True,
            simulate_weekend=False,
            seed=0,
        ),
    )

    static_plan_steps = static.weekday_run.get()["plan_steps"].collect()
    dynamic_plan_steps = dynamic.weekday_run.get()["plan_steps"].collect()
    static_transitions = static.weekday_run.get()["transitions"].collect()
    dynamic_transitions = dynamic.weekday_run.get()["transitions"].collect()

    assert static_plan_steps.height > 0
    assert dynamic_plan_steps.height > 0
    assert static_transitions.height > 0
    assert dynamic_transitions.height > 0

    static_iter_2 = static_transitions.filter(pl.col("iteration") == 2)
    dynamic_iter_2 = dynamic_transitions.filter(pl.col("iteration") == 2)

    assert static_iter_2.height > 0
    assert dynamic_iter_2.height > 0

    static_utility_sum = static_iter_2["utility_to"].sum()
    dynamic_utility_sum = dynamic_iter_2["utility_to"].sum()

    assert dynamic_utility_sum != pytest.approx(static_utility_sum)
