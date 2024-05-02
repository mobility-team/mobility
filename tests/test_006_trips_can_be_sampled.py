import mobility
import pytest

@pytest.mark.dependency(
    depends=["tests/test_002_population_sample_can_be_created.py::test_002_population_sample_can_be_created"],
    scope="session"
 )
def test_006_trips_can_be_sampled(test_data):
    
    transport_zones = mobility.TransportZones(
        insee_city_id=test_data["transport_zones_insee_city_id"],
        method="radius",
        radius=test_data["transport_zones_radius"],
    )
    
    population = mobility.Population(
        transport_zones=transport_zones,
        sample_size=test_data["population_sample_size"]
    )
    
    trips = mobility.Trips(population)
    trips = trips.get()
    
    assert trips.shape[0] > 0