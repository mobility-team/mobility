from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import geopandas as gpd
import pandas as pd
import pytest

from mobility.transport.modes.public_transport.gtfs import gtfs_router as gtfs_router_module
from mobility.transport.modes.public_transport.gtfs.gtfs_router import GTFSRouter


def _set_router_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> tuple[Path, Path]:
    project_data_folder = tmp_path / "project-data"
    package_data_folder = tmp_path / "package-data"
    project_data_folder.mkdir(parents=True, exist_ok=True)
    package_data_folder.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("MOBILITY_GTFS_DOWNLOAD_DATE", "2024-01-01")
    monkeypatch.setenv("MOBILITY_PROJECT_DATA_FOLDER", str(project_data_folder))
    monkeypatch.setenv("MOBILITY_PACKAGE_DATA_FOLDER", str(package_data_folder))
    return project_data_folder, package_data_folder


def test_create_and_get_asset_applies_edits_checks_expected_agencies_and_prepares_router(
    monkeypatch,
    tmp_path,
):
    project_data_folder, _ = _set_router_env(monkeypatch, tmp_path)

    calls = {}

    monkeypatch.setattr(
        GTFSRouter,
        "get_stops",
        lambda self, transport_zones: pd.DataFrame(
            {"resource_url": ["/tmp/base.zip"], "dataset_url": [None]}
        ),
    )
    monkeypatch.setattr(GTFSRouter, "get_gtfs_files", lambda self, stops: ["/tmp/base.zip"])

    def fake_apply_gtfs_edits(gtfs_files, gtfs_edits, edits_folder):
        calls["apply_gtfs_edits"] = (list(gtfs_files), gtfs_edits, Path(edits_folder))
        return ["/tmp/edited.zip"]

    def fake_check_expected_agencies(self, gtfs_files, expected_agencies):
        calls["expected_agencies"] = (list(gtfs_files), list(expected_agencies))
        expected_agencies[:] = []
        return True

    def fake_prepare_gtfs_router(self, transport_zones, gtfs_files):
        calls["prepare_gtfs_router"] = (transport_zones, list(gtfs_files))

    monkeypatch.setattr(gtfs_router_module, "apply_gtfs_edits", fake_apply_gtfs_edits)
    monkeypatch.setattr(GTFSRouter, "check_expected_agencies", fake_check_expected_agencies)
    monkeypatch.setattr(GTFSRouter, "prepare_gtfs_router", fake_prepare_gtfs_router)

    router = GTFSRouter(
        transport_zones="dummy-transport-zones",
        additional_gtfs_files=["/tmp/additional.zip"],
        gtfs_edits=[{"mode": "all", "ops": []}],
        expected_agencies=["SNCF"],
    )

    result = router.create_and_get_asset()

    assert result == router.cache_path
    assert calls["apply_gtfs_edits"][0] == ["/tmp/base.zip", "/tmp/additional.zip"]
    assert calls["apply_gtfs_edits"][2] == project_data_folder / "gtfs_edits"
    assert calls["expected_agencies"][0] == ["/tmp/edited.zip"]
    assert calls["prepare_gtfs_router"][0] == "dummy-transport-zones"
    assert calls["prepare_gtfs_router"][1] == ["/tmp/edited.zip"]


def test_check_expected_agencies_mutates_expected_list_and_raises_when_missing(
    monkeypatch,
    tmp_path,
):
    _set_router_env(monkeypatch, tmp_path)

    class FakeGTFSData:
        def __init__(self, url):
            self.url = url
            self.name = f"fake-{Path(url).stem}"

        def get_agencies_names(self, gtfs_path):
            if "broken" in self.url:
                raise RuntimeError("boom")
            return "SNCF, Keolis"

    monkeypatch.setattr(gtfs_router_module, "GTFSData", FakeGTFSData)

    router = GTFSRouter(transport_zones="dummy")

    expected_agencies = ["sncf", "keolis"]
    assert router.check_expected_agencies(["ok.zip"], expected_agencies) is True
    assert expected_agencies == []

    with pytest.raises(IndexError):
        router.check_expected_agencies(["broken.zip"], ["missing"])


def test_get_stops_and_get_gtfs_urls_collect_expected_sources(monkeypatch, tmp_path):
    _, package_data_folder = _set_router_env(monkeypatch, tmp_path)

    recorded = {}

    class FakeGTFSStops:
        def __init__(self, admin_prefixes, download_date):
            recorded["admin_prefixes"] = list(admin_prefixes)
            recorded["download_date"] = download_date

        def get(self, bbox):
            recorded["bbox"] = bbox
            return pd.DataFrame(
                {
                    "resource_url": ["https://example.com/direct.zip"],
                    "dataset_url": ["https://data.gouv.fr/datasets/dataset-1"],
                }
            )

    monkeypatch.setattr(gtfs_router_module, "GTFSStops", FakeGTFSStops)
    monkeypatch.setattr(gtfs_router_module, "download_file", lambda url, path: None)

    transport_zones = SimpleNamespace(
        cache_path=tmp_path / "transport-zones.rds",
        get=lambda: gpd.GeoDataFrame(
            {"local_admin_unit_id": ["fr-1", "ch-2"]},
            geometry=gpd.points_from_xy([0.0, 1.0], [0.0, 1.0]),
            crs=4326,
        ),
    )

    router = GTFSRouter(transport_zones="dummy")

    stops = router.get_stops(transport_zones)
    assert recorded["admin_prefixes"] == ["fr", "ch"]
    assert recorded["download_date"] == "2024-01-01"
    assert "resource_url" in stops.columns

    metadata_path = package_data_folder / "gtfs" / "2024-01-01_gtfs_metadata.json"
    metadata_path.parent.mkdir(parents=True, exist_ok=True)
    metadata_path.write_text(
        json.dumps(
            [
                {
                    "datagouv_id": "dataset-1",
                    "resources": [
                        {
                            "format": "GTFS",
                            "original_url": "https://example.com/metadata.zip",
                        }
                    ],
                }
            ]
        ),
        encoding="utf-8",
    )

    urls = router.get_gtfs_urls(stops)
    assert "https://example.com/direct.zip" in urls
    assert "https://example.com/metadata.zip" in urls


def test_prepare_gtfs_router_uses_rscript_and_route_types_resource(monkeypatch, tmp_path):
    _set_router_env(monkeypatch, tmp_path)

    calls = {}

    class FakeRScriptRunner:
        def __init__(self, script_path):
            calls["script_path"] = Path(script_path)

        def run(self, args):
            calls["args"] = list(args)

    monkeypatch.setattr(gtfs_router_module, "RScriptRunner", FakeRScriptRunner)

    router = GTFSRouter(transport_zones="dummy")
    transport_zones = SimpleNamespace(cache_path=tmp_path / "transport-zones.rds")

    router.prepare_gtfs_router(transport_zones, ["/tmp/a.zip", "/tmp/b.zip"])

    assert calls["script_path"].name == "prepare_gtfs_router.R"
    assert calls["args"][0] == str(transport_zones.cache_path)
    assert calls["args"][1] == "/tmp/a.zip,/tmp/b.zip"
    assert calls["args"][2].endswith("gtfs/gtfs_route_types.csv")
    assert calls["args"][3] == str(router.cache_path)


def test_audit_gtfs_exports_active_shapes_and_stops(monkeypatch, tmp_path):
    _set_router_env(monkeypatch, tmp_path)

    class FakeFeed:
        def __init__(self):
            self.shapes = pd.DataFrame(
                {
                    "shape_id": ["shape-1", "shape-1"],
                    "shape_pt_lat": [46.0, 46.1],
                    "shape_pt_lon": [6.0, 6.1],
                    "shape_pt_sequence": [1, 2],
                }
            )
            self.trips = pd.DataFrame(
                {
                    "trip_id": ["trip-1"],
                    "route_id": ["route-1"],
                    "shape_id": ["shape-1"],
                }
            )
            self.routes = pd.DataFrame(
                {
                    "route_id": ["route-1"],
                    "route_short_name": ["R1"],
                    "route_long_name": ["Route 1"],
                }
            )
            self.stops = pd.DataFrame(
                {
                    "stop_id": ["A", "B"],
                    "stop_name": ["Stop A", "Stop B"],
                    "stop_lat": [46.0, 46.1],
                    "stop_lon": [6.0, 6.1],
                }
            )
            self.stop_times = pd.DataFrame(
                {
                    "trip_id": ["trip-1", "trip-1"],
                    "stop_id": ["A", "B"],
                    "stop_sequence": [1, 2],
                }
            )

        def get_dates(self):
            return ["20240101"]

        def compute_busiest_date(self, dates):
            return dates[0]

        def get_trips(self, date):
            return self.trips.copy()

    recorded = {"to_file": []}

    def fake_geometrize_shapes(df):
        return gpd.GeoDataFrame(
            df.copy(),
            geometry=gpd.points_from_xy(df["shape_pt_lon"], df["shape_pt_lat"]),
            crs=4326,
        )

    def fake_geometrize_stops(df):
        return gpd.GeoDataFrame(
            df.copy(),
            geometry=gpd.points_from_xy(df["stop_lon"], df["stop_lat"]),
            crs=4326,
        )

    def fake_to_file(self, output_path, driver=None, layer=None):
        recorded["to_file"].append((Path(output_path), driver, layer))

    monkeypatch.setattr(gtfs_router_module.gtfs_kit, "read_feed", lambda gtfs_path, dist_units="m": FakeFeed())
    monkeypatch.setattr(gtfs_router_module.gtfs_kit.shapes, "geometrize_shapes", fake_geometrize_shapes)
    monkeypatch.setattr(gtfs_router_module.gtfs_kit.stops, "geometrize_stops", fake_geometrize_stops)
    monkeypatch.setattr(gpd.GeoDataFrame, "to_file", fake_to_file, raising=False)

    monkeypatch.setattr(GTFSRouter, "get_stops", lambda self, transport_zones: pd.DataFrame({"resource_url": []}))
    monkeypatch.setattr(GTFSRouter, "get_gtfs_files", lambda self, stops: [str(tmp_path / "fake.zip")])

    router = GTFSRouter(transport_zones="dummy")
    router.audit_gtfs()

    assert len(recorded["to_file"]) == 2
    assert recorded["to_file"][0][1:] == ("GPKG", "shapes")
    assert recorded["to_file"][1][1:] == ("GPKG", "stops")
    assert recorded["to_file"][0][0].name == "gtfs_1.gpkg"
    assert recorded["to_file"][1][0].name == "gtfs_1.gpkg"
