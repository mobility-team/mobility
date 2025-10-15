import mobility
import pytest

from mobility.choice_models.population_trips import PopulationTrips
from mobility.motives import OtherMotive, HomeMotive, WorkMotive, StudiesMotive
from mobility.choice_models.population_trips_parameters import PopulationTripsParameters
from mobility.parsers.mobility_survey.france import EMPMobilitySurvey

# Uncomment the next lines if you want to test interactively, outside of pytest, 
# but still need the setup phase and input data defined in conftest.py
# Don't forget to recomment or the tests will not pass !

from conftest import get_test_data, do_mobility_setup
do_mobility_setup(True, False, False)
test_data = get_test_data()

@pytest.mark.dependency(
    depends=[
        "tests/test_001_transport_zones_can_be_created.py::test_001_transport_zones_can_be_created",
        "tests/test_003_car_costs_can_be_computed.py::test_003_car_costs_can_be_computed",
        "tests/test_005_mobility_surveys_can_be_prepared.py::test_005_mobility_surveys_can_be_prepared"
    ],
    scope="session"
 )
def test_008_population_trips_can_be_computed(test_data):
    
    transport_zones = mobility.TransportZones(
        local_admin_unit_id=test_data["transport_zones_local_admin_unit_id"],
        radius=test_data["transport_zones_radius"]
    )
    
    emp = EMPMobilitySurvey()
    
    pop = mobility.Population(
        transport_zones,
        sample_size=test_data["population_sample_size"],
    )
    
    pop_trips = PopulationTrips(
        population=pop,
        modes=[
            mobility.CarMode(transport_zones)
        ],
        motives=[
            HomeMotive(),
            WorkMotive(),
            OtherMotive(
                population=pop
            )
        ],
        surveys=[emp],
        parameters=PopulationTripsParameters(
            n_iterations=1,
            n_iter_per_cost_update=0,
            alpha=0.01,
            dest_prob_cutoff=0.9,
            activity_utility_coeff=1.0,
            stay_home_utility_coeff=1.0,
            k_mode_sequences=6,
            cost_uncertainty_sd=1.0,
            mode_sequence_search_parallel=False
        )
    )
    
    flows = pop_trips.get()["weekday_flows"].collect()
    
    assert flows.shape[0] > 0