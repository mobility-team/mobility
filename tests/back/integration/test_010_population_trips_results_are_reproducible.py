import pytest
import polars as pl

import mobility
from mobility.choice_models.population_trips import PopulationTrips
from mobility.motives import OtherMotive, HomeMotive, WorkMotive
from mobility.choice_models.population_trips_parameters import PopulationTripsParameters
from mobility.parsers.mobility_survey.france import EMPMobilitySurvey

@pytest.mark.dependency(
    depends=[
        "tests/back/integration/test_008_population_trips_can_be_computed.py::test_008_population_trips_can_be_computed"
    ],
    scope="session",
)
def test_010_population_trips_results_are_reproducible(test_data):
    transport_zones = mobility.TransportZones(
        local_admin_unit_id=test_data["transport_zones_local_admin_unit_id"],
        radius=test_data["transport_zones_radius"],
    )
    emp = EMPMobilitySurvey()

    pop = mobility.Population(
        transport_zones,
        sample_size=test_data["population_sample_size"],
    )
    
    # Reuse the results from test 009
    pop_trips = PopulationTrips(
        population=pop,
        modes=[mobility.CarMode(transport_zones)],
        motives=[HomeMotive(), WorkMotive(), OtherMotive(population=pop)],
        surveys=[emp],
        parameters=PopulationTripsParameters(
            n_iterations=1,
            n_iter_per_cost_update=0,
            alpha=0.01,
            dest_prob_cutoff=0.9,
            k_mode_sequences=3,
            cost_uncertainty_sd=1.0,
            mode_sequence_search_parallel=False,
            seed=0
        )
    )

    metrics_run_1 = pop_trips.evaluate("global_metrics")
    
    # Remove the results then re run the model with the same inputs
    pop_trips.remove()
    
    pop_trips = PopulationTrips(
        population=pop,
        modes=[mobility.CarMode(transport_zones)],
        motives=[HomeMotive(), WorkMotive(), OtherMotive(population=pop)],
        surveys=[emp],
        parameters=PopulationTripsParameters(
            n_iterations=1,
            n_iter_per_cost_update=0,
            alpha=0.01,
            dest_prob_cutoff=0.9,
            k_mode_sequences=3,
            cost_uncertainty_sd=1.0,
            mode_sequence_search_parallel=False,
            seed=0
        )
    )

    metrics_run_2 = pop_trips.evaluate("global_metrics")
    
    # Compare results between runs
    comparison = (
        metrics_run_1 
        .join(metrics_run_2, on=["country", "variable"])
        .with_columns(
            delta=(pl.col("value_right") - pl.col("value")).abs()
        )
        .select("delta").sum().item()
    )
    
    assert comparison < 1e-9

