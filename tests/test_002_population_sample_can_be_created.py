import mobility
import pytest

@pytest.mark.dependency(
    depends=["tests/test_001_transport_zones_can_be_created.py::test_001_transport_zones_can_be_created"],
    scope="session"
 )
def test_002_population_sample_can_be_created(test_data):
    
    transport_zones = mobility.TransportZones(
        local_admin_unit_id=test_data["transport_zones_local_admin_unit_id"],
        radius=test_data["transport_zones_radius"]
    )
    
    population = mobility.Population(
        transport_zones=transport_zones,
        sample_size=test_data["population_sample_size"]
    )
    
    population = population.get()
    
    assert population.shape[0] > 0