import pytest

import mobility
from mobility.activities import Home, Other, Work
from mobility.trips.group_day_trips import Parameters, PopulationGroupDayTrips
from mobility.surveys.france import EMPMobilitySurvey


@pytest.mark.dependency(
    depends=[
        "tests/back/integration/test_001_transport_zones_can_be_created.py::test_001_transport_zones_can_be_created",
        "tests/back/integration/test_003_car_costs_can_be_computed.py::test_003_car_costs_can_be_computed",
        "tests/back/integration/test_005_mobility_surveys_can_be_prepared.py::test_005_mobility_surveys_can_be_prepared",
    ],
    scope="session",
)
def test_008_group_day_trips_can_be_computed(test_data):
    transport_zones = mobility.TransportZones(
        local_admin_unit_id=test_data["transport_zones_local_admin_unit_id"],
        radius=test_data["transport_zones_radius"],
    )

    emp = EMPMobilitySurvey()

    pop = mobility.Population(
        transport_zones,
        sample_size=test_data["population_sample_size"],
    )

    car_mode = mobility.Car(transport_zones)
    walk_mode = mobility.Walk(transport_zones)
    bicycle_mode = mobility.Bicycle(transport_zones)
    mode_registry = mobility.ModeRegistry([car_mode, walk_mode, bicycle_mode])
    public_transport_mode = mobility.PublicTransport(
        transport_zones,
        mode_registry=mode_registry,
    )

    group_day_trips = PopulationGroupDayTrips(
        population=pop,
        modes=[car_mode, walk_mode, bicycle_mode, public_transport_mode],
        activities=[Home(), Work(), Other(population=pop)],
        surveys=[emp],
        parameters=Parameters(
            n_iterations=1,
            n_iter_per_cost_update=0,
            alpha=0.01,
            dest_prob_cutoff=0.9,
            k_mode_sequences=6,
            cost_uncertainty_sd=1.0,
            mode_sequence_search_parallel=False,
            simulate_weekend=False
        ),
    )

    plan_steps = group_day_trips.weekday_run.get()["plan_steps"].collect()

    assert plan_steps.height > 0
