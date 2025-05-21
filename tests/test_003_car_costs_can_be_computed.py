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
    
    walk=mobility.WalkMode(transport_zones)
    
    car = mobility.CarMode(transport_zones)
    bicycle = mobility.BicycleMode(transport_zones)

    work_dest_cm = mobility.WorkDestinationChoiceModel(transport_zones, modes=[walk, car, bicycle])
    
    
    mode_choice_model = mobility.TransportModeChoiceModel(work_dest_cm)

    simple_choices = mode_choice_model.get()
    print(simple_choices)

    #assert car_travel_costs.shape[0] > 0
