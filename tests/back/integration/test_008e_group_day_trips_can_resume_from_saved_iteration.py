import pytest

import mobility
from mobility.activities import HomeActivity, OtherActivity, WorkActivity
from mobility.surveys.france import EMPMobilitySurvey
from mobility.trips.group_day_trips import Parameters, PopulationGroupDayTrips


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
    )

    return PopulationGroupDayTrips(
        population=pop,
        modes=[car_mode, walk_mode, bicycle_mode, public_transport_mode],
        activities=[HomeActivity(), WorkActivity(), OtherActivity(population=pop)],
        surveys=[emp],
        parameters=Parameters(
            n_iterations=2,
            n_iter_per_cost_update=0,
            alpha=0.01,
            dest_prob_cutoff=0.9,
            k_mode_sequences=6,
            cost_uncertainty_sd=1.0,
            mode_sequence_search_parallel=False,
            simulate_weekend=False,
            seed=108,
        ),
    )


@pytest.mark.dependency(
    depends=[
        "tests/back/integration/test_008_group_day_trips_can_be_computed.py::test_008_group_day_trips_can_be_computed",
    ],
    scope="session",
)
def test_008e_group_day_trips_can_resume_from_saved_iteration(test_data):
    pop_trips = _build_group_day_trips(test_data)
    pop_trips.remove()

    run = pop_trips.weekday_run
    iterations, resume_from_iteration = run._prepare_iterations(run.inputs_hash)

    assert resume_from_iteration is None

    state = run._build_state(
        iterations=iterations,
        resume_from_iteration=resume_from_iteration,
    )
    iteration_1 = iterations.iteration(1)
    run._run_model_iteration(
        state=state,
        iteration=iteration_1,
    )
    iteration_1.save_state(state, run.rng.getstate())

    saved_iteration_state = iterations.iteration(1).load_state()
    assert saved_iteration_state.current_plans.height > 0
    assert saved_iteration_state.current_plan_steps.height > 0

    resumed_pop_trips = _build_group_day_trips(test_data)
    result = resumed_pop_trips.get()

    plan_steps = result["weekday_plan_steps"].collect()
    transitions = result["weekday_transitions"].collect()

    assert plan_steps.height > 0
    assert transitions.height > 0
    assert sorted(transitions["iteration"].unique().to_list()) == [1, 2]
