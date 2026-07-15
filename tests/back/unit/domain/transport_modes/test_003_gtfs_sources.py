import datetime as dt
import sqlite3
from importlib import import_module

import geopandas as gpd
import pytest
from shapely.geometry import Polygon, box

from mobility.runtime.parameter_values import ParameterValue
from mobility.spatial.transport_zones import TransportZones
from mobility.transport.modes.public_transport.gtfs.gtfs_data import GTFSData
from mobility.transport.modes.public_transport.gtfs.gtfs_area_filter import (
    FrenchGTFSAreaFilter,
)
from mobility.transport.modes.public_transport.gtfs import gtfs_area_filter
from mobility.transport.modes.public_transport.gtfs.gtfs_router import GTFSRouter
from mobility.transport.modes.public_transport.gtfs.gtfs_source_providers import (
    FrenchGTFS,
    SwissGTFS,
)
from mobility.transport.modes.public_transport.gtfs.gtfs_sources import GTFSSources
from mobility.transport.modes.public_transport.public_transport_graph import (
    PublicTransportGraph,
    PublicTransportRoutingParameters,
)


gtfs_source_providers_module = import_module(
    "mobility.transport.modes.public_transport.gtfs.gtfs_source_providers"
)
gtfs_data_module = import_module(
    "mobility.transport.modes.public_transport.gtfs.gtfs_data"
)


class _FakeHTTPResponse:
    def __init__(self, payload, status_code=200):
        self.payload = payload
        self.status_code = status_code
        self.closed = False

    def json(self):
        return self.payload

    @property
    def text(self):
        return self.payload

    def close(self):
        self.closed = True


def test_003_public_transport_routing_parameters_require_reference_date():
    with pytest.raises(ValueError, match="gtfs_reference_date"):
        PublicTransportRoutingParameters()


def test_003_public_transport_routing_parameters_require_sources_folder():
    with pytest.raises(ValueError, match="gtfs_sources_folder"):
        PublicTransportRoutingParameters(gtfs_reference_date="2025-01-01")


def test_003_public_transport_routing_parameters_validate_reference_date_format(tmp_path):
    with pytest.raises(ValueError, match="YYYY-MM-DD"):
        PublicTransportRoutingParameters(
            gtfs_reference_date="20250101",
            gtfs_sources_folder=tmp_path / "gtfs_sources",
        )


def test_003_gtfs_router_tracks_sources_as_input(tmp_path):
    transport_zones = TransportZones(local_admin_unit_id="fr-87085", radius=10.0)
    sources = GTFSSources("2025-01-01", tmp_path / "gtfs_sources", ["fr"])

    router = GTFSRouter(transport_zones=transport_zones, gtfs_sources=sources)

    assert router.inputs["gtfs_sources"] is sources


def test_003_gtfs_router_normalizes_additional_gtfs_paths(tmp_path):
    paths = GTFSRouter.normalize_additional_gtfs_files(
        [tmp_path / "local.zip", "other.zip"]
    )

    assert paths == [str(tmp_path / "local.zip"), "other.zip"]


def test_003_gtfs_router_rejects_unresolved_additional_gtfs_parameter():
    additional_files = ParameterValue.by_iteration({1: None, 5: ["event.zip"]})

    with pytest.raises(ValueError, match="for_iteration"):
        GTFSRouter.normalize_additional_gtfs_files(additional_files)


def test_003_public_transport_graph_gets_countries_from_actual_zones():
    class FakeTransportZones:
        countries = ["ch", "fr"]

        def get(self):
            return gpd.GeoDataFrame(
                {"local_admin_unit_id": ["fr-87085", "ch-261"]},
                geometry=[box(0.0, 0.0, 1.0, 1.0), box(6.0, 46.0, 7.0, 47.0)],
                crs=4326,
            )

    assert PublicTransportGraph.get_countries(None, FakeTransportZones()) == ["ch", "fr"]


def test_003_gtfs_sources_create_missing_file_in_user_folder(tmp_path, monkeypatch):
    class FakeFrenchGTFS:
        def __init__(
            self,
            reference_date,
            sources_created_at_utc,
            use_live_gtfs=False,
            max_gtfs_file_age_days=30,
        ):
            self.reference_date = reference_date

        def insert_data(self, connection, gtfs_sources):
            gtfs_sources.insert_gtfs_file(
                connection,
                country="fr",
                provider="fake",
                dataset_id="dataset",
                resource_id="resource",
                title="Fake",
                download_url="https://example.com/fake.zip",
                gtfs_file_date="2025-01-01",
                gtfs_file_age_days=0,
                status="archived",
                coverage_geometry=box(0.0, 0.0, 1.0, 1.0),
            )

    folder = tmp_path / "inputs" / "gtfs_sources"
    monkeypatch.setattr(
        "mobility.transport.modes.public_transport.gtfs.gtfs_sources.available_gtfs_sources",
        lambda: {"fr": FakeFrenchGTFS},
    )

    sources = GTFSSources("2025-01-01", folder, ["fr"])

    assert sources.get() == sources.cache_path
    assert sources.cache_path.parent == folder
    assert sources.cache_path.exists()
    assert sources.cache_path.name.endswith("gtfs_sources_2025-01-01_fr_archived.sqlite")


def test_003_gtfs_sources_file_identity_includes_max_file_age(tmp_path):
    strict_sources = GTFSSources(
        "2025-01-01",
        tmp_path / "gtfs_sources",
        ["fr"],
        max_gtfs_file_age_days=30,
    )
    tolerant_sources = GTFSSources(
        "2025-01-01",
        tmp_path / "gtfs_sources",
        ["fr"],
        max_gtfs_file_age_days=90,
    )

    assert strict_sources.cache_path != tolerant_sources.cache_path


def test_003_gtfs_sources_use_existing_file_without_provider_calls(tmp_path, monkeypatch):
    class ProviderThatShouldNotRun:
        def __init__(
            self,
            reference_date,
            sources_created_at_utc,
            use_live_gtfs=False,
            max_gtfs_file_age_days=30,
        ):
            pass

        def insert_data(self, connection, gtfs_sources):
            raise AssertionError("Existing GTFS sources should be read-only.")

    sources = GTFSSources("2025-01-01", tmp_path / "gtfs_sources", ["fr"])
    with sqlite3.connect(sources.cache_path) as connection:
        sources.create_schema(connection)
        sources.insert_metadata(connection, "2025-01-01T00:00:00Z")

    monkeypatch.setattr(
        "mobility.transport.modes.public_transport.gtfs.gtfs_sources.available_gtfs_sources",
        lambda: {"fr": ProviderThatShouldNotRun},
    )

    assert sources.get() == sources.cache_path


def test_003_gtfs_sources_rebuild_invalid_existing_file(tmp_path, monkeypatch):
    class FakeFrenchGTFS:
        def __init__(
            self,
            reference_date,
            sources_created_at_utc,
            use_live_gtfs=False,
            max_gtfs_file_age_days=30,
        ):
            pass

        def insert_data(self, connection, gtfs_sources):
            gtfs_sources.insert_gtfs_file(
                connection,
                country="fr",
                provider="fake",
                dataset_id="dataset",
                resource_id="resource",
                title="Fake",
                download_url="https://example.com/fake.zip",
                gtfs_file_date="2025-01-01",
                gtfs_file_age_days=0,
                status="archived",
                coverage_geometry=box(0.0, 0.0, 1.0, 1.0),
            )

    sources = GTFSSources("2025-01-01", tmp_path / "gtfs_sources", ["fr"])
    connection = sqlite3.connect(sources.cache_path)
    try:
        sources.create_schema(connection)
        connection.execute(
            """
            INSERT INTO metadata (
                schema_version,
                gtfs_reference_date,
                countries,
                use_live_gtfs,
                max_gtfs_file_age_days,
                sources_created_at_utc
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            ("1", "2025-01-02", "fr", 0, 30, "2025-01-01T00:00:00Z"),
        )
        connection.commit()
    finally:
        connection.close()

    monkeypatch.setattr(
        "mobility.transport.modes.public_transport.gtfs.gtfs_sources.available_gtfs_sources",
        lambda: {"fr": FakeFrenchGTFS},
    )

    assert sources.get() == sources.cache_path
    assert sources.validate_metadata()["gtfs_reference_date"] == "2025-01-01"


def test_003_gtfs_sources_failed_build_does_not_leave_final_sqlite(tmp_path, monkeypatch):
    class FailingFrenchGTFS:
        def __init__(
            self,
            reference_date,
            sources_created_at_utc,
            use_live_gtfs=False,
            max_gtfs_file_age_days=30,
        ):
            pass

        def insert_data(self, connection, gtfs_sources):
            gtfs_sources.insert_gtfs_file(
                connection,
                country="fr",
                provider="fake",
                dataset_id="dataset",
                resource_id="resource",
                title="Fake",
                download_url="https://example.com/fake.zip",
                gtfs_file_date="2025-01-01",
                gtfs_file_age_days=0,
                status="archived",
                coverage_geometry=box(0.0, 0.0, 1.0, 1.0),
            )
            raise RuntimeError("download blocked")

    monkeypatch.setattr(
        "mobility.transport.modes.public_transport.gtfs.gtfs_sources.available_gtfs_sources",
        lambda: {"fr": FailingFrenchGTFS},
    )
    sources = GTFSSources("2025-01-01", tmp_path / "gtfs_sources", ["fr"])

    with pytest.raises(RuntimeError, match="download blocked"):
        sources.get()

    assert sources.cache_path.exists() is False
    assert sources.cache_path.with_name(f"{sources.cache_path.name}.part").exists() is False


def test_003_gtfs_sources_select_with_bbox_then_exact_intersection(tmp_path):
    sources = GTFSSources("2025-01-01", tmp_path / "gtfs_sources", ["fr"])

    with sqlite3.connect(sources.cache_path) as connection:
        sources.create_schema(connection)
        sources.insert_metadata(connection, "2025-01-01T00:00:00Z")

        sources.insert_gtfs_file(
            connection,
            country="fr",
            provider="transport_data_gouv",
            dataset_id="dataset-b",
            resource_id="resource-b",
            title="B",
            download_url="https://example.com/b.zip",
            gtfs_file_date="2025-01-01",
            gtfs_file_age_days=0,
            status="archived",
            coverage_geometry=box(0.0, 0.0, 2.0, 2.0),
        )
        sources.insert_gtfs_file(
            connection,
            country="fr",
            provider="transport_data_gouv",
            dataset_id="dataset-a",
            resource_id="resource-a",
            title="A",
            download_url="https://example.com/a.zip",
            gtfs_file_date="2025-01-01",
            gtfs_file_age_days=0,
            status="archived",
            coverage_geometry=box(0.0, 0.0, 2.0, 2.0),
        )

        # The bbox overlaps the study area, but the exact geometry does not
        # because the area falls inside the polygon hole.
        polygon_with_hole = Polygon(
            shell=[(-1.0, -1.0), (3.0, -1.0), (3.0, 3.0), (-1.0, 3.0), (-1.0, -1.0)],
            holes=[[(0.5, 0.5), (1.5, 0.5), (1.5, 1.5), (0.5, 1.5), (0.5, 0.5)]],
        )
        sources.insert_gtfs_file(
            connection,
            country="fr",
            provider="transport_data_gouv",
            dataset_id="dataset-hole",
            resource_id="resource-hole",
            title="Hole",
            download_url="https://example.com/hole.zip",
            gtfs_file_date="2025-01-01",
            gtfs_file_age_days=0,
            status="archived",
            coverage_geometry=polygon_with_hole,
        )

    transport_zones = gpd.GeoDataFrame(
        {"local_admin_unit_id": ["fr-test"]},
        geometry=[box(0.75, 0.75, 1.25, 1.25)],
        crs=4326,
    )

    selected = sources.get_gtfs_resources_for_area(transport_zones)

    assert selected["resource_id"].tolist() == ["resource-a", "resource-b"]
    assert selected["sources_created_at_utc"].tolist() == [
        "2025-01-01T00:00:00Z",
        "2025-01-01T00:00:00Z",
    ]


def test_003_gtfs_sources_fail_when_only_missing_archive_intersects_area(tmp_path):
    sources = GTFSSources("2025-01-01", tmp_path / "gtfs_sources", ["fr"])

    with sqlite3.connect(sources.cache_path) as connection:
        sources.create_schema(connection)
        sources.insert_metadata(connection, "2025-01-01T00:00:00Z")
        sources.insert_gtfs_file(
            connection,
            country="fr",
            provider="transport_data_gouv",
            dataset_id="dataset-live",
            resource_id="resource-live",
            title="Live",
            download_url=None,
            gtfs_file_date=None,
            gtfs_file_age_days=None,
            status="missing_archive",
            coverage_geometry=box(0.0, 0.0, 2.0, 2.0),
        )

    transport_zones = gpd.GeoDataFrame(
        {"local_admin_unit_id": ["fr-test"]},
        geometry=[box(0.75, 0.75, 1.25, 1.25)],
        crs=4326,
    )

    with pytest.raises(ValueError, match="none can be used after"):
        sources.get_gtfs_resources_for_area(transport_zones)


def test_003_gtfs_sources_skip_missing_archive_when_archived_source_exists(tmp_path, caplog):
    sources = GTFSSources("2025-01-01", tmp_path / "gtfs_sources", ["fr"])

    with sqlite3.connect(sources.cache_path) as connection:
        sources.create_schema(connection)
        sources.insert_metadata(connection, "2025-01-01T00:00:00Z")
        sources.insert_gtfs_file(
            connection,
            country="fr",
            provider="transport_data_gouv",
            dataset_id="dataset-live",
            resource_id="resource-live",
            title="Live",
            download_url=None,
            gtfs_file_date=None,
            gtfs_file_age_days=None,
            status="missing_archive",
            coverage_geometry=box(0.0, 0.0, 2.0, 2.0),
        )
        sources.insert_gtfs_file(
            connection,
            country="fr",
            provider="transport_data_gouv",
            dataset_id="dataset-archived",
            resource_id="resource-archived",
            title="Archived",
            download_url="https://example.com/archived.zip",
            gtfs_file_date="2025-01-01",
            gtfs_file_age_days=0,
            status="archived",
            coverage_geometry=box(0.0, 0.0, 2.0, 2.0),
        )

    transport_zones = gpd.GeoDataFrame(
        {"local_admin_unit_id": ["fr-test"]},
        geometry=[box(0.75, 0.75, 1.25, 1.25)],
        crs=4326,
    )

    with caplog.at_level("WARNING"):
        selected = sources.get_gtfs_resources_for_area(transport_zones)

    assert selected["resource_id"].tolist() == ["resource-archived"]
    assert "Skipped GTFS sources" in caplog.text
    assert "resource-live" in caplog.text


def test_003_gtfs_sources_fail_when_no_source_intersects_area(tmp_path):
    sources = GTFSSources("2025-01-01", tmp_path / "gtfs_sources", ["fr"])

    with sqlite3.connect(sources.cache_path) as connection:
        sources.create_schema(connection)
        sources.insert_metadata(connection, "2025-01-01T00:00:00Z")
        sources.insert_gtfs_file(
            connection,
            country="fr",
            provider="transport_data_gouv",
            dataset_id="dataset",
            resource_id="resource",
            title="Far away",
            download_url="https://example.com/far-away.zip",
            gtfs_file_date="2025-01-01",
            gtfs_file_age_days=0,
            status="archived",
            coverage_geometry=box(10.0, 10.0, 11.0, 11.0),
        )

    transport_zones = gpd.GeoDataFrame(
        {"local_admin_unit_id": ["fr-test"]},
        geometry=[box(0.75, 0.75, 1.25, 1.25)],
        crs=4326,
    )

    with pytest.raises(ValueError, match="No GTFS source intersects"):
        sources.get_gtfs_resources_for_area(transport_zones)


def test_003_gtfs_sources_fail_when_only_stale_archive_intersects_area(tmp_path):
    sources = GTFSSources(
        "2025-02-15",
        tmp_path / "gtfs_sources",
        ["fr"],
        max_gtfs_file_age_days=30,
    )

    with sqlite3.connect(sources.cache_path) as connection:
        sources.create_schema(connection)
        sources.insert_metadata(connection, "2025-02-15T00:00:00Z")
        sources.insert_gtfs_file(
            connection,
            country="fr",
            provider="transport_data_gouv",
            dataset_id="dataset-stale",
            resource_id="resource-stale",
            title="Stale",
            download_url="https://example.com/stale.zip",
            gtfs_file_date="2025-01-01",
            gtfs_file_age_days=45,
            status="stale_archive",
            coverage_geometry=box(0.0, 0.0, 2.0, 2.0),
        )

    transport_zones = gpd.GeoDataFrame(
        {"local_admin_unit_id": ["fr-test"]},
        geometry=[box(0.75, 0.75, 1.25, 1.25)],
        crs=4326,
    )

    with pytest.raises(ValueError, match="none can be used after"):
        sources.get_gtfs_resources_for_area(transport_zones)


def test_003_gtfs_sources_skip_stale_archive_when_archived_source_exists(tmp_path, caplog):
    sources = GTFSSources(
        "2025-02-15",
        tmp_path / "gtfs_sources",
        ["fr"],
        max_gtfs_file_age_days=30,
    )

    with sqlite3.connect(sources.cache_path) as connection:
        sources.create_schema(connection)
        sources.insert_metadata(connection, "2025-02-15T00:00:00Z")
        sources.insert_gtfs_file(
            connection,
            country="fr",
            provider="transport_data_gouv",
            dataset_id="dataset-stale",
            resource_id="resource-stale",
            title="Stale",
            download_url="https://example.com/stale.zip",
            gtfs_file_date="2025-01-01",
            gtfs_file_age_days=45,
            status="stale_archive",
            coverage_geometry=box(0.0, 0.0, 2.0, 2.0),
        )
        sources.insert_gtfs_file(
            connection,
            country="fr",
            provider="transport_data_gouv",
            dataset_id="dataset-archived",
            resource_id="resource-archived",
            title="Archived",
            download_url="https://example.com/archived.zip",
            gtfs_file_date="2025-02-10",
            gtfs_file_age_days=5,
            status="archived",
            coverage_geometry=box(0.0, 0.0, 2.0, 2.0),
        )

    transport_zones = gpd.GeoDataFrame(
        {"local_admin_unit_id": ["fr-test"]},
        geometry=[box(0.75, 0.75, 1.25, 1.25)],
        crs=4326,
    )

    with caplog.at_level("WARNING"):
        selected = sources.get_gtfs_resources_for_area(transport_zones)

    assert selected["resource_id"].tolist() == ["resource-archived"]
    assert "older than max_gtfs_file_age_days=30" in caplog.text
    assert "resource-stale" in caplog.text


def test_003_gtfs_sources_error_links_to_provider_dataset_pages():
    french_url = GTFSSources.dataset_url(
        {"provider": "transport_data_gouv", "dataset_id": "dataset-fr"}
    )
    swiss_url = GTFSSources.dataset_url(
        {"provider": "opentransportdata_swiss", "dataset_id": "timetable-2026-gtfs2020"}
    )
    swiss_archive_url = GTFSSources.dataset_url(
        {"provider": "opentransportdata_swiss", "dataset_id": "timetable-2025-gtfs2020"}
    )

    assert french_url == "https://transport.data.gouv.fr/datasets/dataset-fr"
    assert swiss_url == (
        "https://data.opentransportdata.swiss/en/dataset/timetable-2026-gtfs2020"
    )
    assert swiss_archive_url == "https://archive.opentransportdata.swiss/timetable_gtfs_archive.htm"


def test_003_gtfs_data_cache_identity_uses_source_metadata():
    first = GTFSData(
        provider="provider",
        dataset_id="dataset",
        resource_id="resource",
        download_url="https://example.com/gtfs.zip",
        gtfs_file_date="2025-01-01",
        source_status="archived",
        sources_created_at_utc="2025-01-01T00:00:00Z",
    )
    second = GTFSData(
        provider="provider",
        dataset_id="dataset",
        resource_id="resource",
        download_url="https://example.com/gtfs.zip",
        gtfs_file_date="2025-01-02",
        source_status="archived",
        sources_created_at_utc="2025-01-01T00:00:00Z",
    )

    assert first.cache_path != second.cache_path


def test_003_gtfs_data_downloads_selected_sources_in_parallel(monkeypatch):
    source = {
        "provider": "provider",
        "dataset_id": "dataset",
        "resource_id": "resource",
        "download_url": "https://example.com/gtfs.zip",
        "gtfs_file_date": "2025-01-01",
        "status": "archived",
        "sources_created_at_utc": "2025-01-01T00:00:00Z",
    }
    downloaded_pairs = []

    def fake_download_files(url_path_pairs, **kwargs):
        downloaded_pairs.extend(url_path_pairs)
        return [path for _url, path in url_path_pairs]

    monkeypatch.setattr(gtfs_data_module, "download_files", fake_download_files)
    monkeypatch.setattr(GTFSData, "is_update_needed", lambda self: True)
    monkeypatch.setattr(GTFSData, "is_gtfs_file_ok", lambda self, path: True)

    gtfs_files = GTFSData.download_gtfs_files([source])

    assert downloaded_pairs == [(source["download_url"], gtfs_files[0][0])]
    assert gtfs_files[0][1] is True


def test_003_archived_gtfs_data_cache_identity_ignores_sources_timestamp():
    first = GTFSData(
        provider="provider",
        dataset_id="dataset",
        resource_id="resource",
        download_url="https://example.com/gtfs.zip",
        gtfs_file_date="2025-01-01",
        source_status="archived",
        sources_created_at_utc="2025-01-01T00:00:00Z",
    )
    second = GTFSData(
        provider="provider",
        dataset_id="dataset",
        resource_id="resource",
        download_url="https://example.com/gtfs.zip",
        gtfs_file_date="2025-01-01",
        source_status="archived",
        sources_created_at_utc="2025-01-02T00:00:00Z",
    )
    live = GTFSData(
        provider="provider",
        dataset_id="dataset",
        resource_id="resource",
        download_url="https://example.com/gtfs.zip",
        gtfs_file_date="2025-01-01",
        source_status="live",
        sources_created_at_utc="2025-01-02T00:00:00Z",
    )

    assert first.cache_path == second.cache_path
    assert first.cache_path != live.cache_path


def test_003_swiss_gtfs_parser_keeps_dated_gtfs_zip_links():
    html = """
    <a href="/dataset/x/resource/1/download/GTFS_FP2026_20260603.zip">old</a>
    <a href="/dataset/x/resource/2/download/GTFS_FP2026_20260606.zip">new</a>
    <a href="/dataset/x/resource/3/download/GTFS_FP2026_2025-09-22.zip">older</a>
    <a href="/dataset/x/resource/3/download/readme.zip">ignore</a>
    """

    provider = SwissGTFS(dt.date(2026, 6, 9), "2026-06-10T00:00:00Z")
    resources = provider.parse_gtfs_links(
        html,
        "https://data.opentransportdata.swiss/en/dataset/timetable-2026-gtfs2020",
    )

    assert [resource["gtfs_file_date"] for resource in resources] == [
        "2026-06-03",
        "2026-06-06",
        "2025-09-22",
    ]
    assert resources[0]["download_url"].endswith("GTFS_FP2026_20260603.zip")


def test_003_swiss_gtfs_parser_reads_archive_links():
    html = """
    <a href="timetable_gtfs/timetable-2017-gtfs/GTFS_FP2017_2016-11-30.zip">old</a>
    <a href="timetable_gtfs/timetable-2025-gtfs2020/GTFS_FP2025_20241215.zip">new</a>
    """

    provider = SwissGTFS(dt.date(2025, 1, 1), "2025-01-01T00:00:00Z")
    resources = provider.parse_gtfs_links(
        html,
        "https://archive.opentransportdata.swiss/timetable_gtfs.php",
    )

    assert resources[0]["dataset_id"] == "timetable-2017-gtfs"
    assert resources[0]["download_url"].endswith(
        "timetable_gtfs/timetable-2017-gtfs/GTFS_FP2017_2016-11-30.zip"
    )
    assert resources[0]["gtfs_file_date"] == "2016-11-30"


def test_003_swiss_gtfs_insert_data_computes_file_age(monkeypatch):
    provider = SwissGTFS(dt.date(2026, 6, 10), "2026-06-10T00:00:00Z")

    class FakeSources:
        inserted = None

        def insert_gtfs_file(self, connection, **kwargs):
            self.inserted = kwargs

    monkeypatch.setattr(
        provider,
        "select_gtfs_resource",
        lambda: {
            "dataset_id": "timetable-2026-gtfs2020",
            "resource_id": "GTFS_FP2026_20260603",
            "title": "GTFS_FP2026_20260603.zip",
            "download_url": "https://example.com/GTFS_FP2026_20260603.zip",
            "gtfs_file_date": "2026-06-03",
        },
    )
    monkeypatch.setattr(
        provider,
        "derive_coverage_from_gtfs_url",
        lambda **kwargs: box(5.0, 45.0, 11.0, 48.0),
    )

    sources = FakeSources()
    provider.insert_data(None, sources)

    assert sources.inserted["gtfs_file_age_days"] == 7
    assert sources.inserted["status"] == "archived"
    assert sources.inserted["dataset_id"] == "timetable-2026-gtfs2020"


def test_003_swiss_gtfs_uses_archive_listing_for_old_reference_dates(monkeypatch):
    requested_urls = []

    def fake_request_url(url):
        requested_urls.append(url)
        return _FakeHTTPResponse(
            """
            <a href="timetable_gtfs/timetable-2025-gtfs2020/GTFS_FP2025_20250101.zip">old</a>
            <a href="timetable_gtfs/timetable-2025-gtfs2020/GTFS_FP2025_20250115.zip">new</a>
            """
        )

    monkeypatch.setattr(gtfs_source_providers_module, "request_url", fake_request_url)

    provider = SwissGTFS(dt.date(2025, 1, 10), "2025-01-10T00:00:00Z")
    resource = provider.select_gtfs_resource()

    assert requested_urls == ["https://archive.opentransportdata.swiss/timetable_gtfs.php"]
    assert resource["gtfs_file_date"] == "2025-01-01"
    assert resource["dataset_id"] == "timetable-2025-gtfs2020"


def test_003_transport_data_gouv_resource_snapshot_uses_latest_history_before_reference_date():
    resource = {
        "id": "current",
        "format": "GTFS",
        "url": "https://example.com/current.zip",
        "history": [
            {
                "payload": {
                    "format": "GTFS",
                    "url": "https://example.com/old.zip",
                    "download_datetime": "2025-01-01T12:00:00Z",
                }
            },
            {
                "payload": {
                    "format": "GTFS",
                    "url": "https://example.com/new.zip",
                    "download_datetime": "2025-02-01T12:00:00Z",
                }
            },
            {
                "payload": {
                    "format": "GTFS",
                    "url": "https://example.com/future.zip",
                    "download_datetime": "2025-03-01T12:00:00Z",
                }
            },
        ],
    }

    provider = FrenchGTFS(dt.date(2025, 2, 15), "2025-02-15T00:00:00Z")
    selected = provider.select_resource_snapshot(resource)

    assert selected["url"] == "https://example.com/new.zip"
    assert selected["gtfs_file_date"] == "2025-02-01"


def test_003_transport_data_gouv_resource_snapshot_uses_dataset_history():
    dataset = {
        "resources": [
            {
                "id": 83281,
                "format": "GTFS",
                "url": "https://example.com/current.zip",
            }
        ],
        "history": [
            {
                "resource_id": 83281,
                "payload": {
                    "format": "GTFS",
                    "permanent_url": "https://example.com/archive-2025-01-01.zip",
                    "download_datetime": "2025-01-01T12:00:00Z",
                },
            },
            {
                "resource_id": 83281,
                "payload": {
                    "format": "GTFS",
                    "permanent_url": "https://example.com/archive-2025-02-01.zip",
                    "download_datetime": "2025-02-01T12:00:00Z",
                },
            },
            {
                "resource_id": 99999,
                "payload": {
                    "format": "GTFS",
                    "permanent_url": "https://example.com/other-resource.zip",
                    "download_datetime": "2025-02-10T12:00:00Z",
                },
            },
        ],
    }

    provider = FrenchGTFS(dt.date(2025, 2, 15), "2025-02-15T00:00:00Z")
    selected = provider.select_gtfs_resources(dataset, [])[0]

    assert selected["permanent_url"] == "https://example.com/archive-2025-02-01.zip"
    assert selected["gtfs_file_date"] == "2025-02-01"


def test_003_transport_data_gouv_insert_data_fetches_metadata_in_catalog_order(monkeypatch):
    catalog_payload = [
        {"id": "dataset-b", "resources": [{"id": "resource-b", "format": "GTFS"}]},
        {"id": "dataset-a", "resources": [{"id": "resource-a", "format": "GTFS"}]},
    ]
    detailed_payloads = {
        "dataset-b": {
            "id": "dataset-b",
            "resources": [
                {"id": "resource-b", "format": "GTFS", "url": "https://example.com/live-b.zip"}
            ],
            "history": [
                {
                    "resource_id": "resource-b",
                    "payload": {
                        "format": "GTFS",
                        "permanent_url": "https://example.com/archive-b.zip",
                        "download_datetime": "2025-01-01T00:00:00Z",
                    },
                }
            ],
        },
        "dataset-a": {
            "id": "dataset-a",
            "resources": [
                {"id": "resource-a", "format": "GTFS", "url": "https://example.com/live-a.zip"}
            ],
            "history": [
                {
                    "resource_id": "resource-a",
                    "payload": {
                        "format": "GTFS",
                        "permanent_url": "https://example.com/archive-a.zip",
                        "download_datetime": "2025-01-01T00:00:00Z",
                    },
                }
            ],
        },
    }
    coverage_payload = {
        "features": [
            {
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [
                        [
                            [0.0, 0.0],
                            [1.0, 0.0],
                            [1.0, 1.0],
                            [0.0, 1.0],
                            [0.0, 0.0],
                        ]
                    ],
                }
            }
        ]
    }

    requested_url_batches = []

    def fake_request_url(url, **kwargs):
        return _FakeHTTPResponse(catalog_payload)

    def fake_request_urls(urls, **kwargs):
        requested_url_batches.append(list(urls))
        responses = []
        for url in urls:
            url_parts = url.rstrip("/").split("/")
            if url_parts[-1] == "geojson":
                responses.append(_FakeHTTPResponse(coverage_payload))
            else:
                dataset_id = url_parts[-1]
                responses.append(_FakeHTTPResponse(detailed_payloads[dataset_id]))
        return responses

    class FakeSources:
        def __init__(self):
            self.inserted_rows = []

        def insert_gtfs_file(self, connection, **kwargs):
            self.inserted_rows.append(kwargs)

    monkeypatch.setattr(gtfs_source_providers_module, "request_url", fake_request_url)
    monkeypatch.setattr(gtfs_source_providers_module, "request_urls", fake_request_urls)

    sources = FakeSources()
    provider = FrenchGTFS(dt.date(2025, 1, 15), "2025-01-15T00:00:00Z")
    provider.insert_data(None, sources)

    assert requested_url_batches == [
        [
            "https://transport.data.gouv.fr/api/datasets/dataset-b",
            "https://transport.data.gouv.fr/api/datasets/dataset-a",
        ],
        [
            "https://transport.data.gouv.fr/api/datasets/dataset-b/geojson",
            "https://transport.data.gouv.fr/api/datasets/dataset-a/geojson",
        ],
    ]
    assert [row["dataset_id"] for row in sources.inserted_rows] == ["dataset-b", "dataset-a"]
    assert [row["download_url"] for row in sources.inserted_rows] == [
        "https://example.com/archive-b.zip",
        "https://example.com/archive-a.zip",
    ]


def test_003_french_gtfs_area_filter_matches_admin_codes():
    area_filter = FrenchGTFSAreaFilter(
        countries={"FR"},
        regions={"11"},
        departements={"75"},
        epcis={"200054781"},
        communes={"75056", "75101"},
    )

    assert area_filter.matches_dataset(
        {"covered_area": [{"type": "region", "nom": "Île-de-France", "insee": "11"}]}
    )
    assert area_filter.matches_dataset(
        {"covered_area": [{"type": "commune", "nom": "Paris", "insee": "75056"}]}
    )
    assert not area_filter.matches_dataset(
        {"covered_area": [{"type": "region", "nom": "Bretagne", "insee": "53"}]}
    )
    assert not area_filter.matches_dataset(
        {"covered_area": [{"type": "pays", "nom": "Monaco", "insee": "MC"}]}
    )


def test_003_french_gtfs_area_filter_keeps_uncertain_datasets():
    area_filter = FrenchGTFSAreaFilter(
        countries={"FR"},
        regions=set(),
        departements=set(),
        epcis=set(),
        communes=set(),
    )

    assert area_filter.matches_dataset({})
    assert area_filter.matches_dataset({"covered_area": []})
    assert area_filter.matches_dataset({"covered_area": [{"type": "unknown", "insee": "x"}]})
    assert area_filter.matches_dataset({"covered_area": [{"type": "region"}]})


def test_003_french_gtfs_area_filter_adds_parent_commune_codes(monkeypatch):
    class FakeFrenchAdminUnits:
        def __init__(self, level):
            assert level == "commune"

        def get(self):
            return gpd.GeoDataFrame(
                {
                    "admin_id": ["fr-75101"],
                    "parent_commune_id": ["fr-75056"],
                    "epci_id": ["fr-200054781"],
                    "departement_id": ["fr-75"],
                    "region_id": ["fr-11"],
                },
                geometry=[box(2.0, 48.0, 3.0, 49.0)],
                crs=4326,
            )

    monkeypatch.setattr(gtfs_area_filter, "FrenchAdminUnits", FakeFrenchAdminUnits)
    transport_zones = gpd.GeoDataFrame(
        {"local_admin_unit_id": ["fr-75101"]},
        geometry=[box(2.0, 48.0, 3.0, 49.0)],
        crs=4326,
    )

    area_filter = FrenchGTFSAreaFilter.from_transport_zones(transport_zones)

    assert area_filter.communes == {"75101", "75056"}
    assert area_filter.epcis == {"200054781"}
    assert area_filter.departements == {"75"}
    assert area_filter.regions == {"11"}


def test_003_transport_data_gouv_insert_data_prefilters_catalog_with_covered_area(monkeypatch):
    catalog_payload = [
        {
            "id": "dataset-kept",
            "title": "Kept",
            "covered_area": [{"type": "region", "nom": "Île-de-France", "insee": "11"}],
            "resources": [{"id": "resource-kept", "format": "GTFS"}],
        },
        {
            "id": "dataset-dropped",
            "title": "Dropped",
            "covered_area": [{"type": "region", "nom": "Bretagne", "insee": "53"}],
            "resources": [{"id": "resource-dropped", "format": "GTFS"}],
        },
    ]
    detailed_payload = {
        "id": "dataset-kept",
        "resources": [
            {"id": "resource-kept", "format": "GTFS", "url": "https://example.com/live.zip"}
        ],
        "history": [
            {
                "resource_id": "resource-kept",
                "payload": {
                    "format": "GTFS",
                    "permanent_url": "https://example.com/archive.zip",
                    "download_datetime": "2025-01-01T00:00:00Z",
                },
            }
        ],
    }
    coverage_payload = {
        "features": [
            {
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [
                        [
                            [0.0, 0.0],
                            [1.0, 0.0],
                            [1.0, 1.0],
                            [0.0, 1.0],
                            [0.0, 0.0],
                        ]
                    ],
                }
            }
        ]
    }
    requested_url_batches = []

    def fake_request_url(url, **kwargs):
        return _FakeHTTPResponse(catalog_payload)

    def fake_request_urls(urls, **kwargs):
        requested_url_batches.append(list(urls))
        responses = []
        for url in urls:
            if url.endswith("/geojson"):
                responses.append(_FakeHTTPResponse(coverage_payload))
            else:
                responses.append(_FakeHTTPResponse(detailed_payload))
        return responses

    class FakeSources:
        def __init__(self):
            self.inserted_rows = []

        def insert_gtfs_file(self, connection, **kwargs):
            self.inserted_rows.append(kwargs)

    monkeypatch.setattr(gtfs_source_providers_module, "request_url", fake_request_url)
    monkeypatch.setattr(gtfs_source_providers_module, "request_urls", fake_request_urls)

    provider = FrenchGTFS(
        dt.date(2025, 1, 15),
        "2025-01-15T00:00:00Z",
        area_filter=FrenchGTFSAreaFilter(
            countries={"FR"},
            regions={"11"},
            departements=set(),
            epcis=set(),
            communes=set(),
        ),
    )
    sources = FakeSources()
    provider.insert_data(None, sources)

    assert requested_url_batches == [
        ["https://transport.data.gouv.fr/api/datasets/dataset-kept"],
        ["https://transport.data.gouv.fr/api/datasets/dataset-kept/geojson"],
    ]
    assert [row["dataset_id"] for row in sources.inserted_rows] == ["dataset-kept"]


def test_003_transport_data_gouv_resource_marks_stale_archive():
    resource = {
        "id": "current",
        "format": "GTFS",
        "url": "https://example.com/current.zip",
        "gtfs_file_date": "2025-01-01",
    }
    provider = FrenchGTFS(
        dt.date(2025, 2, 15),
        "2025-02-15T00:00:00Z",
        max_gtfs_file_age_days=30,
    )
    coverage = box(0.0, 0.0, 1.0, 1.0)

    class FakeSources:
        inserted = None

        def insert_gtfs_file(self, connection, **kwargs):
            self.inserted = kwargs

    sources = FakeSources()
    provider.insert_resource(None, sources, "dataset", resource, coverage)

    assert sources.inserted["status"] == "stale_archive"
    assert sources.inserted["gtfs_file_age_days"] == 45


def test_003_transport_data_gouv_resource_snapshot_marks_live_url_by_default():
    resource = {
        "id": "current",
        "format": "GTFS",
        "url": "https://example.com/current.zip",
    }

    provider = FrenchGTFS(dt.date(2025, 2, 15), "2025-02-15T00:00:00Z")
    selected = provider.select_resource_snapshot(resource)

    assert selected["is_reproducible"] is False


def test_003_transport_data_gouv_resource_snapshot_accepts_live_url_when_requested():
    resource = {
        "id": "current",
        "format": "GTFS",
        "url": "https://example.com/current.zip",
    }

    provider = FrenchGTFS(
        dt.date(2025, 2, 15),
        "2025-02-15T00:00:00Z",
        use_live_gtfs=True,
    )
    selected = provider.select_resource_snapshot(resource)

    assert selected["url"] == resource["url"]
    assert selected["is_reproducible"] is False


def test_003_gtfs_sources_can_use_a_new_country_source_class(tmp_path, monkeypatch):
    class FakeGermanGTFS:
        def __init__(
            self,
            reference_date,
            sources_created_at_utc,
            use_live_gtfs=False,
            max_gtfs_file_age_days=30,
            area_filter=None,
        ):
            self.reference_date = reference_date
            self.sources_created_at_utc = sources_created_at_utc
            self.use_live_gtfs = use_live_gtfs
            self.max_gtfs_file_age_days = max_gtfs_file_age_days
            self.area_filter = area_filter

        def insert_data(self, connection, gtfs_sources):
            gtfs_sources.insert_gtfs_file(
                connection,
                country="de",
                provider="fake",
                dataset_id="dataset-de",
                resource_id="resource-de",
                title="Fake Germany",
                download_url="https://example.com/de.zip",
                gtfs_file_date="2025-01-01",
                gtfs_file_age_days=0,
                status="archived",
                coverage_geometry=box(0.0, 0.0, 1.0, 1.0),
            )

    monkeypatch.setattr(
        "mobility.transport.modes.public_transport.gtfs.gtfs_sources.available_gtfs_sources",
        lambda: {"de": FakeGermanGTFS},
    )

    sources = GTFSSources("2025-01-01", tmp_path / "gtfs_sources", ["DE"])

    assert sources.inputs["countries"] == ["de"]
    assert isinstance(sources.build_country_source("de", dt.date(2025, 1, 1), "2025-01-01T00:00:00Z"), FakeGermanGTFS)
