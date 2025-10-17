import pathlib
import geopandas as gpd

from mobility.transport_zones import TransportZones

def test_get_cached_asset_reads_expected_path(monkeypatch, project_dir, fake_inputs_hash, fake_transport_zones, dependency_fakes):
    transport_zones = TransportZones(local_admin_unit_id=["fr-09122", "fr-09121"])

    # Ensure .value is None so code path performs a read
    transport_zones.value = None

    captured = {}

    def fake_read_file(path, *args, **kwargs):
        captured["read_path"] = pathlib.Path(path)
        return fake_transport_zones

    monkeypatch.setattr(gpd, "read_file", fake_read_file, raising=True)

    result = transport_zones.get_cached_asset()
    assert result is fake_transport_zones

    # Assert the cache path used is exactly the hashed path
    expected_file_name = f"{fake_inputs_hash}-transport_zones.gpkg"
    expected_cache_path = pathlib.Path(project_dir) / expected_file_name
    assert captured["read_path"] == expected_cache_path

    # Subsequent call should return the cached value without a second read
    captured["read_path"] = None
    second = transport_zones.get_cached_asset()
    assert second is fake_transport_zones
    assert captured["read_path"] is None  # not called again
