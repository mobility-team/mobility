import types
import pathlib

import pytest

from mobility.spatial.transport_zones import TransportZones


@pytest.fixture
def dependency_fakes(monkeypatch, tmp_path):
    state = types.SimpleNamespace(study_area_inits=[], osm_inits=[])

    class _FakeStudyArea:
        def __init__(self, local_admin_unit_id, radius, cutout_geometries=None, parameters=None):
            self.local_admin_unit_id = local_admin_unit_id
            self.radius = radius
            self.cutout_geometries = cutout_geometries
            self.parameters = parameters
            self.inputs = {
                "parameters": parameters,
                "cutout_geometries": cutout_geometries,
            }
            self.cache_path = {
                "polygons": str(tmp_path / "study_area_polygons.gpkg"),
                "boundary": str(tmp_path / "study_area_boundary.geojson"),
            }

    def _StudyArea_spy(local_admin_unit_id=None, radius=None, cutout_geometries=None, **kwargs):
        parameters = kwargs.get("parameters")
        if parameters is not None:
            local_admin_unit_id = parameters.local_admin_unit_id
            radius = parameters.radius
            if cutout_geometries is None:
                cutout_geometries = kwargs.get("cutout_geometries")

        instance = _FakeStudyArea(
            local_admin_unit_id,
            radius,
            cutout_geometries,
            parameters=parameters,
        )
        state.study_area_inits.append(
            {
                "local_admin_unit_id": local_admin_unit_id,
                "radius": radius,
            }
        )
        return instance

    class _FakeOSMData:
        def __init__(self, study_area, object_type, key, **kwargs):
            self.study_area = study_area
            self.object_type = object_type
            self.key = key
            self.exclude_queries = kwargs.get("exclude_queries", [])
            self.geofabrik_extract_date = kwargs.get("geofabrik_extract_date")
            self.split_local_admin_units = kwargs.get("split_local_admin_units")
            self.get_return_path = str(tmp_path / "osm_buildings.gpkg")

        def get(self):
            return self.get_return_path

    def _OSMData_spy(study_area, object_type, key, **kwargs):
        instance = _FakeOSMData(study_area, object_type, key, **kwargs)
        state.osm_inits.append(
            {
                "study_area": study_area,
                "object_type": object_type,
                "key": key,
                "exclude_queries": kwargs.get("exclude_queries", []),
                "geofabrik_extract_date": kwargs.get("geofabrik_extract_date"),
                "split_local_admin_units": kwargs.get("split_local_admin_units"),
            }
        )
        return instance

    import mobility.spatial.transport_zones as tz_module

    monkeypatch.setattr(tz_module, "StudyArea", _StudyArea_spy, raising=True)
    monkeypatch.setattr(tz_module, "OSMData", _OSMData_spy, raising=True)
    return state


def test_init_builds_inputs_and_cache_path(project_dir, dependency_fakes):
    # Construct with explicit arguments
    local_admin_unit_identifier = "fr-09122"
    level_of_detail = 1
    radius_in_km = 30

    transport_zones = TransportZones(
        local_admin_unit_id=local_admin_unit_identifier,
        level_of_detail=level_of_detail,
        radius=radius_in_km,
    )

    # Verify StudyArea and OSMData were constructed with expected args
    assert len(dependency_fakes.study_area_inits) == 1
    assert dependency_fakes.study_area_inits[0] == {
        "local_admin_unit_id": local_admin_unit_identifier,
        "radius": radius_in_km,
    }

    assert len(dependency_fakes.osm_inits) == 1
    osm_init_record = dependency_fakes.osm_inits[0]
    assert osm_init_record["object_type"] == "a"
    assert osm_init_record["key"] == "building"
    assert osm_init_record["exclude_queries"] == [
        "a/building=hut",
        "a/tourism=alpine_hut,wilderness_hut",
    ]
    assert osm_init_record["geofabrik_extract_date"] == "240101"
    assert osm_init_record["split_local_admin_units"] is True

    expected_cache_path = pathlib.Path(project_dir) / "transport_zones.gpkg"
    assert transport_zones.cache_path == expected_cache_path
    assert transport_zones.hash_path == expected_cache_path

    # Inputs surfaced as attributes (via patched Asset.__init__)
    assert transport_zones.inputs["parameters"].level_of_detail == level_of_detail
    assert getattr(transport_zones, "study_area") is not None
    assert getattr(transport_zones, "osm_buildings") is not None

    # New instance has no value cached in memory yet
    assert transport_zones.value is None
