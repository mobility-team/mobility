import types
import pathlib

import geopandas as gpd
import numpy as np
import pandas as pd
import pytest
from shapely.geometry import box

from mobility.spatial.prepare_transport_zones import (
    _create_lau_transport_zones,
    _get_sidecar_paths,
    _resolve_max_workers,
    prepare_transport_zones,
)
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
    assert osm_init_record["geofabrik_extract_date"] == "260101"
    assert osm_init_record["split_local_admin_units"] is True

    expected_cache_path = pathlib.Path(project_dir) / "transport_zones.gpkg"
    assert transport_zones.cache_path == expected_cache_path
    assert transport_zones.hash_path == expected_cache_path

    # Inputs surfaced as attributes (via patched Asset.__init__)
    assert transport_zones.inputs["parameters"].level_of_detail == level_of_detail
    assert transport_zones.inputs["parameters"].backend == "r"
    assert getattr(transport_zones, "study_area") is not None
    assert getattr(transport_zones, "osm_buildings") is not None

    # New instance has no value cached in memory yet
    assert transport_zones.value is None


def test_python_backend_dispatches_to_python_preparation(dependency_fakes, monkeypatch, tmp_path):
    calls = []

    def _prepare_transport_zones_spy(study_area_fp, osm_buildings_fp, level_of_detail, output_fp, max_workers):
        calls.append(
            {
                "study_area_fp": pathlib.Path(study_area_fp),
                "osm_buildings_fp": pathlib.Path(osm_buildings_fp),
                "level_of_detail": level_of_detail,
                "output_fp": pathlib.Path(output_fp),
                "max_workers": max_workers,
            }
        )

    import mobility.spatial.transport_zones as tz_module

    monkeypatch.setattr(
        tz_module,
        "prepare_transport_zones",
        _prepare_transport_zones_spy,
        raising=True,
    )

    transport_zones = TransportZones(
        local_admin_unit_id="fr-09122",
        level_of_detail=1,
        radius=30,
        backend="python",
        backend_workers=8,
    )

    transport_zones.create_transport_zones_with_python(
        study_area_fp=tmp_path / "study_area_polygons.gpkg",
        osm_buildings_fp=tmp_path / "osm_buildings",
    )

    assert calls == [
        {
            "study_area_fp": tmp_path / "study_area_polygons.gpkg",
            "osm_buildings_fp": tmp_path / "osm_buildings",
            "level_of_detail": 1,
            "output_fp": transport_zones.cache_path,
            "max_workers": 8,
        }
    ]


def test_create_and_get_asset_dispatches_to_selected_python_backend(
    dependency_fakes,
    monkeypatch,
    tmp_path,
):
    calls = []
    raw_zones = gpd.GeoDataFrame(
        {
            "transport_zone_id": [1],
            "local_admin_unit_id": ["fr-09122"],
            "x": [0.0],
            "y": [0.0],
        },
        geometry=[box(0, 0, 1, 1)],
        crs="EPSG:3035",
    )

    def fake_create_transport_zones_with_python(self, study_area_fp, osm_buildings_fp):
        calls.append(
            {
                "backend": "python",
                "study_area_fp": pathlib.Path(study_area_fp),
                "osm_buildings_fp": pathlib.Path(osm_buildings_fp),
            }
        )

    def fake_create_transport_zones_with_r(self, study_area_fp, osm_buildings_fp):
        calls.append({"backend": "r"})

    monkeypatch.setattr(
        TransportZones,
        "create_transport_zones_with_python",
        fake_create_transport_zones_with_python,
    )
    monkeypatch.setattr(
        TransportZones,
        "create_transport_zones_with_r",
        fake_create_transport_zones_with_r,
    )
    monkeypatch.setattr(
        "mobility.spatial.transport_zones.gpd.read_file",
        lambda _path: raw_zones.copy(),
    )
    monkeypatch.setattr(
        TransportZones,
        "remove_isolated_zones",
        lambda self, zones: zones,
    )
    monkeypatch.setattr(
        TransportZones,
        "flag_inner_zones",
        lambda self, zones, *_args: zones.assign(is_inner_zone=True),
    )
    monkeypatch.setattr(
        TransportZones,
        "apply_cutout",
        lambda self, zones, _cutout_geometries: zones,
    )
    monkeypatch.setattr(gpd.GeoDataFrame, "to_file", lambda self, *_args, **_kwargs: None)

    transport_zones = TransportZones(
        local_admin_unit_id="fr-09122",
        level_of_detail=1,
        radius=30,
        backend="python",
    )

    result = transport_zones.create_and_get_asset()

    assert calls == [
        {
            "backend": "python",
            "study_area_fp": tmp_path / "study_area_polygons.gpkg",
            "osm_buildings_fp": tmp_path / "osm_buildings.gpkg",
        }
    ]
    assert result["is_inner_zone"].to_list() == [True]


def test_sidecar_paths_use_transport_zone_output_stem(tmp_path):
    clusters_path, cluster_geometries_path = _get_sidecar_paths(
        tmp_path / "transport_zones.gpkg"
    )

    assert clusters_path == tmp_path / "transport_zones_buildings.parquet"
    assert cluster_geometries_path == tmp_path / "transport_zones_buildings_geoms.gpkg"


def test_resolve_max_workers_uses_task_count_as_upper_bound():
    assert _resolve_max_workers(task_count=0, max_workers=None) == 0
    assert _resolve_max_workers(task_count=2, max_workers=8) == 2
    assert _resolve_max_workers(task_count=3, max_workers=None) == 3


def test_python_backend_builds_one_zone_when_lau_has_no_buildings(monkeypatch):
    monkeypatch.setattr(
        "mobility.spatial.prepare_transport_zones._read_lau_buildings",
        lambda _osm_buildings_fp, _lau_id: gpd.GeoDataFrame(geometry=[], crs="EPSG:4326"),
    )

    zones, clusters = _create_lau_transport_zones(
        lau_id="fr-09122",
        lau_geom=box(0, 0, 1000, 1000),
        osm_buildings_fp=pathlib.Path("unused"),
        level_of_detail=1,
        rng=np.random.default_rng(0),
    )

    assert zones["transport_zone_id"].to_list() == [1]
    assert zones["local_admin_unit_id"].to_list() == ["fr-09122"]
    assert clusters["transport_zone_id"].to_list() == [1]
    assert clusters["local_admin_unit_id"].to_list() == ["fr-09122"]


def test_python_backend_writes_transport_zones_and_building_sidecars(
    monkeypatch,
    tmp_path,
):
    study_area_fp = tmp_path / "study_area.gpkg"
    output_fp = tmp_path / "transport_zones.gpkg"
    clusters_fp, cluster_geometries_fp = _get_sidecar_paths(output_fp)

    study_area = gpd.GeoDataFrame(
        {"local_admin_unit_id": ["fr-09122"]},
        geometry=[box(0, 0, 5000, 1000)],
        crs="EPSG:3035",
    )
    study_area.to_file(study_area_fp, driver="GPKG", index=False)

    buildings = gpd.GeoDataFrame(
        geometry=[
            box(100, 100, 350, 350),
            box(360, 100, 610, 350),
            box(3000, 100, 3250, 350),
            box(3260, 100, 3510, 350),
        ],
        crs="EPSG:3035",
    )
    monkeypatch.setattr(
        "mobility.spatial.prepare_transport_zones._read_lau_buildings",
        lambda _osm_buildings_fp, _lau_id: buildings.copy(),
    )

    prepare_transport_zones(
        study_area_fp=study_area_fp,
        osm_buildings_fp=tmp_path / "osm_buildings",
        level_of_detail=1,
        output_fp=output_fp,
        max_workers=1,
    )

    zones = gpd.read_file(output_fp)
    clusters = pd.read_parquet(clusters_fp)

    assert output_fp.exists()
    assert clusters_fp.exists()
    assert cluster_geometries_fp.exists()
    assert len(zones) == 2
    assert zones["local_admin_unit_id"].to_list() == ["fr-09122", "fr-09122"]
    assert zones["transport_zone_id"].to_list() == [1, 2]
    assert pytest.approx(zones["weight"].sum()) == 1.0
    assert set(clusters["transport_zone_id"]) == {1, 2}
