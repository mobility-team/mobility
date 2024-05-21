import mobility
import pytest

@pytest.mark.dependency(
    depends=["tests/test_002_population_sample_can_be_created.py::test_002_population_sample_can_be_created"],
    scope="session"
 )
def test_003_car_costs_can_be_computed(test_data):
    
    transport_zones = mobility.TransportZones(
        local_admin_unit_id=test_data["transport_zones_local_admin_unit_id"],
        radius=test_data["transport_zones_radius"]
    )
    
    car_travel_costs = mobility.TravelCosts(transport_zones, "car")
    car_travel_costs = car_travel_costs.get()
    
    assert car_travel_costs.shape[0] > 0