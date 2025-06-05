import mobility
import pytest

# Uncomment the next lines if you want to test interactively, outside of pytest, 
# but still need the setup phase and input data defined in conftest.py

# from conftest import get_test_data, do_mobility_setup
# do_mobility_setup(True, False, False)
# test_data = get_test_data()

@pytest.mark.dependency(
    depends=["tests/test_002_population_sample_can_be_created.py::test_002_population_sample_can_be_created"],
    scope="session"
 )
def test_006_trips_can_be_sampled(test_data):
    
    transport_zones = mobility.TransportZones(
        local_admin_unit_id=test_data["transport_zones_local_admin_unit_id"],
        radius=test_data["transport_zones_radius"]
    )
    
    population = mobility.Population(
        transport_zones=transport_zones,
        sample_size=test_data["population_sample_size"]
    )
    
    trips = mobility.Trips(population)
    trips = trips.get()
    
    assert trips.shape[0] > 0