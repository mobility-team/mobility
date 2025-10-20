import mobility
import pytest

@pytest.mark.dependency()
def test_001_transport_zones_can_be_created(test_data):
    
    transport_zones_radius = mobility.TransportZones(
        insee_city_id=test_data["transport_zones_insee_city_id"],
        method="radius",
        radius=test_data["transport_zones_radius"],
    )
    transport_zones_radius = transport_zones_radius.get()
    
    transport_zones_rings = mobility.TransportZones(
        insee_city_id=test_data["transport_zones_insee_city_id"],
        method="epci_rings"
    )
    transport_zones_rings = transport_zones_rings.get()
    
    assert transport_zones_radius.shape[0] > 0
    assert transport_zones_rings.shape[0] > 0
