import mobility
import pytest

@pytest.mark.dependency()
def test_001_transport_zones_can_be_created(test_data):
    
    transport_zones_radius = mobility.TransportZones(
        local_admin_unit_id=test_data["transport_zones_local_admin_unit_id"],
        radius=test_data["transport_zones_radius"]
    )
    transport_zones_radius = transport_zones_radius.get()
    
    assert transport_zones_radius.shape[0] > 0