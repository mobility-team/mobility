import geopandas as gpd
import pandas as pd

from mobility.transport_zones import TransportZones

def test_get_cached_asset_returns_existing_value_without_disk(monkeypatch, fake_transport_zones, dependency_fakes):
    transport_zones = TransportZones(local_admin_unit_id="fr-09122")

    # Pre-seed in-memory value
    transport_zones.value = fake_transport_zones

    # If read_file is called, we will fail the test
    def fail_read(*args, **kwargs):
        raise AssertionError("gpd.read_file should not be called when self.value is set")

    monkeypatch.setattr(gpd, "read_file", fail_read, raising=True)

    result = transport_zones.get_cached_asset()
    assert result is fake_transport_zones
