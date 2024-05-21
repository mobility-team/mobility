import mobility
import pytest

@pytest.mark.dependency(
    depends=["tests/test_006_trips_can_be_sampled.py::test_006_trips_can_be_sampled"],
    scope="session"
 )
def test_007_trips_can_be_localized(test_data):
    
    transport_zones = mobility.TransportZones(
        local_admin_unit_id=test_data["transport_zones_local_admin_unit_id"],
        radius=test_data["transport_zones_radius"]
    )
    
    population = mobility.Population(
        transport_zones=transport_zones,
        sample_size=test_data["population_sample_size"]
    )
    
    trips = mobility.Trips(population)
    
    loc_trips = mobility.LocalizedTrips(trips)
    loc_trips = loc_trips.get()
    
    assert loc_trips.shape[0] > 0