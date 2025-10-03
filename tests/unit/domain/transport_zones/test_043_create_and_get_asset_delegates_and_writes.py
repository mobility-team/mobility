import pathlib
import geopandas as gpd

from mobility.transport_zones import TransportZones

def test_create_and_get_asset_delegates_and_reads(monkeypatch, project_dir, fake_inputs_hash, fake_transport_zones, dependency_fakes):
    transport_zones = TransportZones(local_admin_unit_id="ch-6621", level_of_detail=0, radius=40)

    # Patch geopandas.read_file to verify it reads from the hashed cache path
    seen = {}

    def fake_read_file(path, *args, **kwargs):
        seen["read_path"] = pathlib.Path(path)
        return fake_transport_zones

    monkeypatch.setattr(gpd, "read_file", fake_read_file, raising=True)

    # Run method
    result = transport_zones.create_and_get_asset()
    assert result is fake_transport_zones

    # OSMData.get must have been used by the method
    assert getattr(dependency_fakes, "osm_get_called", False) is True

    # RScript.run must have been called with correct arguments
    assert len(dependency_fakes.rscript_runs) == 1
    args = dependency_fakes.rscript_runs[0]["args"]
    # Expected args: [study_area_fp, osm_buildings_fp, str(level_of_detail), cache_path]
    assert args[0] == transport_zones.study_area.cache_path["polygons"]
    # The fake OSMData.get returns tmp_path / "osm_buildings.gpkg"
    assert pathlib.Path(args[1]).name == "osm_buildings.gpkg"
    # level_of_detail passed as string
    assert args[2] == str(transport_zones.level_of_detail)
    # cache path must be the hashed one
    expected_file_name = f"{fake_inputs_hash}-transport_zones.gpkg"
    expected_cache_path = pathlib.Path(project_dir) / expected_file_name
    assert pathlib.Path(args[3]) == expected_cache_path

    # read_file used the same hashed cache path
    assert seen["read_path"] == expected_cache_path
