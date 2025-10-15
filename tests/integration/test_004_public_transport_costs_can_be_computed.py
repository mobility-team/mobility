import mobility
import pytest

# Uncomment the next lines if you want to test interactively, outside of pytest, 
# but still need the setup phase and input data defined in conftest.py
# Don't forget to recomment or the tests will not pass !

# from conftest import get_test_data, do_mobility_setup
# do_mobility_setup(True, False, False)
# test_data = get_test_data()

@pytest.mark.dependency(
    depends=["tests/test_002_population_sample_can_be_created.py::test_002_population_sample_can_be_created"],
    scope="session"
 )
def test_004_public_transport_costs_can_be_computed(test_data):
    
    transport_zones = mobility.TransportZones(
        local_admin_unit_id=test_data["transport_zones_local_admin_unit_id"],
        radius=test_data["transport_zones_radius"]
    )

    walk = mobility.WalkMode(transport_zones)

    transfer = mobility.IntermodalTransfer(
        max_travel_time=20.0/60.0,
        average_speed=5.0,
        transfer_time=1.0
    )

    gen_cost_parms = mobility.GeneralizedCostParameters(
        cost_constant=0.0,
        cost_of_distance=0.0,
        cost_of_time=mobility.CostOfTimeParameters(
            intercept=7.0,
            breaks=[0.0, 2.0, 10.0, 50.0, 10000.0],
            slopes=[0.0, 1.0, 0.1, 0.067],
            max_value=21.0
        )
    )

    public_transport = mobility.PublicTransportMode(
        transport_zones,
        first_leg_mode=walk,
        first_intermodal_transfer=transfer,
        last_leg_mode=walk,
        last_intermodal_transfer=transfer,
        generalized_cost_parameters=gen_cost_parms,
        routing_parameters=mobility.PublicTransportRoutingParameters(
            max_traveltime=10.0,
            max_perceived_time=10.0
        )
    )

    costs = public_transport.travel_costs.get()
    gen_costs = public_transport.generalized_cost.get(["distance", "time"])
    
    assert costs.shape[0] > 0
    assert gen_costs.shape[0] > 0