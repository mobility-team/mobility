"""Frozen GTFS inputs must survive provider updates and empty local caches."""

import csv
import datetime as dt
import io
import json
import shutil
import sqlite3
import zipfile
from importlib import import_module

import geopandas as gpd
import pytest
import requests
from shapely.geometry import box

from mobility.runtime.assets.in_memory_asset import InMemoryAsset
from mobility.runtime.assets.file_asset import FileAsset
from mobility.transport.modes.public_transport.gtfs.gtfs_data import GTFSData
from mobility.transport.modes.public_transport.gtfs.gtfs_feed import GTFSFeed
from mobility.transport.modes.public_transport.gtfs.gtfs_router import GTFSRouter
from mobility.transport.modes.public_transport.gtfs.gtfs_source_providers import FrenchGTFS, SwissGTFS
from mobility.transport.modes.public_transport.gtfs.gtfs_sources import GTFSSources


@pytest.mark.parametrize("recent_history", [True, False])
def test_french_complete_csv_includes_archives_missing_from_api(monkeypatch, recent_history):
    """An old reference date works even when the API's recent sample is empty."""
    provider = FrenchGTFS(dt.date(2026, 9, 1), "2026-09-14T00:00:00Z")
    dataset = {"id": "star", "history": []}
    if recent_history:
        dataset["history"] = [{"payload": {"dataset_id": 507}}]
    stream = io.StringIO()
    writer = csv.DictWriter(stream, fieldnames=["resource_id", "permanent_url", "payload"])
    writer.writeheader()
    for timestamp in ["2026-09-14T12:00:00Z", "2026-09-01T12:36:41Z", "2026-09-01T00:05:00Z"]:
        writer.writerow({
            "resource_id": "83281", "permanent_url": f"https://archive.example/{timestamp}.zip",
            "payload": json.dumps({"format": "GTFS", "download_datetime": timestamp}),
        })
    requested = []

    def request(url):
        requested.append(url)
        response = requests.Response()
        response._content_consumed = True
        response.status_code = 200
        response._content = (
            stream.getvalue() if url.endswith("resources_history_csv")
            else '<a href="/datasets/507/resources_history_csv">history</a>'
        ).encode()
        return response

    module = import_module(FrenchGTFS.__module__)
    monkeypatch.setattr(module, "request_url", request)
    history = provider.fetch_history(dataset)
    selected = provider.select_resource_snapshot(
        {"id": "83281", "url": "https://live.example/feed.zip"}, history,
    )
    assert selected["permanent_url"].endswith("2026-09-01T12:36:41Z.zip")
    assert requested[-1] == "https://transport.data.gouv.fr/datasets/507/resources_history_csv"
    assert len(requested) == (1 if recent_history else 2)


def test_french_history_accepts_large_metadata_fields(monkeypatch):
    """Large ZIP metadata must not hit csv's 128 KiB field limit or change IDs."""
    payload = {
        "format": "GTFS", "download_datetime": "2026-09-01T12:00:00Z",
        "zip_metadata": [{"file_name": 'stops, "quoted"\n.txt', "details": "x" * 200_000}],
    }
    stream = io.StringIO()
    writer = csv.DictWriter(stream, fieldnames=["resource_id", "permanent_url", "payload"])
    writer.writeheader()
    for resource_id in ["", "00042", "NA"]:
        writer.writerow({
            "resource_id": resource_id, "permanent_url": "https://example.com/789/789.20260901.120000.000000.zip",
            "payload": json.dumps(payload),
        })
    response = requests.Response()
    response._content_consumed = True
    response.status_code = 200
    response._content = stream.getvalue().encode("utf-8-sig")
    monkeypatch.setattr(import_module(FrenchGTFS.__module__), "request_url", lambda url: response)

    provider = FrenchGTFS(dt.date(2026, 9, 1), "unused")
    history = provider.fetch_history({"history": [{"payload": {"dataset_id": 787}}]})

    assert [row["resource_id"] for row in history] == ["789", "00042", "NA"]
    assert history[0]["payload"]["zip_metadata"] == payload["zip_metadata"]
    assert provider.select_resource_snapshot({"id": "00042"}, history)["gtfs_file_date"] == "2026-09-01"


@pytest.mark.parametrize("extension", ["zip", "gtfs", "GTFS"])
def test_french_history_recovers_resource_id_from_archive_name(monkeypatch, extension):
    """Historical GTFS archives can have a .gtfs extension instead of .zip."""
    url = (
        "https://transport-data-gouv-fr-resource-history-prod.cellar-c2.services.clever-cloud.com/"
        f"82418/82418.20241019.060739.466836.{extension}"
    )
    stream = io.StringIO()
    writer = csv.DictWriter(stream, fieldnames=["resource_id", "permanent_url", "payload"])
    writer.writeheader()
    writer.writerow({
        "resource_id": "", "permanent_url": url,
        "payload": json.dumps({"format": "GTFS", "download_datetime": "2024-10-19T06:07:39Z"}),
    })
    response = requests.Response()
    response.status_code = 200
    response._content_consumed = True
    response._content = stream.getvalue().encode()
    monkeypatch.setattr(import_module(FrenchGTFS.__module__), "request_url", lambda url: response)

    provider = FrenchGTFS(dt.date(2024, 10, 20), "unused")
    history = provider.fetch_history({"history": [{"payload": {"dataset_id": 507}}]})

    assert history[0]["resource_id"] == "82418"
    selected = provider.select_resource_snapshot({"id": "82418"}, history)
    assert selected["permanent_url"] == url
    assert selected["gtfs_file_date"] == "2024-10-19"


def test_archive_timestamp_ties_are_independent_of_record_order():
    provider = FrenchGTFS(dt.date(2026, 9, 1), "unused")
    history = [
        {"format": "GTFS", "download_datetime": timestamp, "permanent_url": url}
        for timestamp, url in [
            ("2026-09-01T10:00:00Z", "https://example/b.zip"),
            ("2026-09-01T12:00:00+02:00", "https://example/a.zip"),
            ("2026-09-01T08:00:00Z", "https://example/c.zip"),
        ]
    ]
    for records in [history, list(reversed(history))]:
        assert provider.select_resource_snapshot({"id": "feed"}, records)["permanent_url"] == "https://example/b.zip"


def test_removed_resources_are_selected_from_history():
    provider = FrenchGTFS(dt.date(2025, 9, 1), "unused")
    dataset = {"resources": [{"id": "new", "format": "GTFS", "url": "https://example/new.zip"}],
               "history": [{"resource_id": "old", "payload": {
                   "format": "GTFS", "download_datetime": "2025-08-31T12:00:00Z",
                   "permanent_url": "https://example/old.zip",
               }}]}
    selected = {row["id"]: row for row in provider.select_gtfs_resources(dataset, [])}
    assert selected["old"]["gtfs_file_date"] == "2025-08-31"
    assert selected["old"]["permanent_url"] == "https://example/old.zip"
    assert selected["new"]["is_reproducible"] is False


def test_archive_age_is_optional_and_independent_of_calendar_dates():
    provider = FrenchGTFS(dt.date(2026, 9, 1), "unused")
    assert provider.select_source_status(True, 316) == "archived"
    strict = FrenchGTFS(dt.date(2026, 9, 1), "unused", max_gtfs_file_age_days=30)
    assert strict.select_source_status(True, 316) == "stale_archive"


@pytest.mark.parametrize("start,end", [
    ("2026-09-01", None), ("2026-09-10", "2026-09-01"), ("2026-09-02", "2026-09-03"),
])
def test_invalid_service_windows_fail_before_discovery(tmp_path, start, end):
    with pytest.raises(ValueError):
        GTFSSources("2026-09-01", tmp_path, ["fr"], gtfs_service_start_date=start, gtfs_service_end_date=end)


def test_service_window_defaults_to_four_weeks_and_changes_identity(tmp_path):
    sources = GTFSSources("2025-09-01", tmp_path, ["fr"])
    assert sources.gtfs_service_start_date == "2025-09-01"
    assert sources.gtfs_service_end_date == "2025-09-28"
    other = GTFSSources("2025-09-01", tmp_path, ["fr"],
                        gtfs_service_start_date="2025-10-01", gtfs_service_end_date="2025-10-28")
    assert other.cache_path != sources.cache_path


def test_swiss_service_window_can_span_two_timetable_years(monkeypatch):
    provider = SwissGTFS(dt.date(2024, 12, 1), "unused")
    requested = []

    def select(year):
        requested.append(year)
        return {"dataset_id": f"timetable-{year}-gtfs2020", "title": "Swiss timetable",
                "download_url": f"https://example/{year}.zip", "gtfs_file_date": "2024-11-30"}

    class Sources:
        inputs = {"gtfs_service_start_date": "2024-12-08", "gtfs_service_end_date": "2024-12-21"}

        def __init__(self):
            self.rows = []

        def insert_gtfs_file(self, connection, **row):
            self.rows.append(row)

    monkeypatch.setattr(provider, "select_gtfs_resource", select)
    monkeypatch.setattr(provider, "derive_coverage_from_gtfs_url", lambda **kwargs: box(5, 45, 11, 48))
    sources = Sources()
    provider.insert_data(None, sources)
    assert requested == [2024, 2025]
    assert [row["resource_id"] for row in sources.rows] == ["timetable-2024", "timetable-2025"]


def test_failed_history_request_is_not_an_empty_history(monkeypatch):
    module = import_module(FrenchGTFS.__module__)

    def unavailable(url):
        raise requests.HTTPError("history unavailable")

    monkeypatch.setattr(module, "request_url", unavailable)
    provider = FrenchGTFS(dt.date(2026, 9, 1), "unused")
    with pytest.raises(requests.HTTPError, match="history unavailable"):
        provider.fetch_history({"id": "star", "history": [{"payload": {"dataset_id": 507}}]})


def test_complete_history_takes_priority_over_embedded_recent_history():
    provider = FrenchGTFS(dt.date(2026, 9, 1), "unused")
    resource = {
        "id": "bus", "url": "https://live.example/feed.zip",
        "history": [{"format": "GTFS", "download_datetime": "2026-09-01T12:00:00Z",
                     "permanent_url": "https://example.com/embedded.zip"}],
    }
    selected = provider.select_resource_snapshot(resource, [])
    assert selected["is_reproducible"] is False


@pytest.mark.parametrize("reference,year", [
    ("2024-12-08", 2024), ("2024-12-15", 2025),
    ("2025-01-01", 2025), ("2025-12-14", 2026), ("2026-09-01", 2026),
])
def test_swiss_archive_fallback_and_timetable_year(monkeypatch, reference, year):
    """A removed yearly page falls back to archives for the correct service year."""
    module = import_module(SwissGTFS.__module__)
    requested = []

    def request(url, **kwargs):
        requested.append(url)
        response = requests.Response()
        response._content_consumed = True
        response.status_code = 200 if url == SwissGTFS.archive_listing_url else 404
        response._content = "".join(
            f'<a href="timetable_gtfs/timetable-{candidate}-gtfs2020/GTFS_FP{candidate}_20241201.zip">feed</a>'
            for candidate in [2024, 2025, 2026, 2027]
        ).encode()
        return response

    monkeypatch.setattr(module, "request_url", request)
    selected = SwissGTFS(dt.date.fromisoformat(reference), "unused").select_gtfs_resource()
    assert selected["dataset_id"] == f"timetable-{year}-gtfs2020"
    assert selected["resource_id"] == f"timetable-{year}"
    assert requested == [SwissGTFS.dataset_url_template.format(year=year), SwissGTFS.archive_listing_url]


@pytest.fixture
def source_run(tmp_path, monkeypatch):
    """Use real assets and a tiny feed, replacing only provider discovery and HTTP."""
    monkeypatch.setenv("MOBILITY_PACKAGE_DATA_FOLDER", str(tmp_path / "downloads"))
    monkeypatch.setenv("MOBILITY_PROJECT_DATA_FOLDER", str(tmp_path / "results"))
    upstream = tmp_path / "provider.zip"
    tables = {
        "agency": "agency_id,agency_name,agency_url,agency_timezone\na,Fixture,https://example.com,Europe/Paris\n",
        "routes": "route_id,agency_id,route_long_name,route_type\nr,a,Bus,3\n",
        "stops": "stop_id,stop_name,stop_lat,stop_lon\na,A,48.11,-1.68\nb,B,48.11,-1.6799\n",
        "trips": "route_id,service_id,trip_id\nr,s,t\n",
        "stop_times": "trip_id,arrival_time,departure_time,stop_id,stop_sequence\nt,08:00:00,08:00:00,a,1\nt,08:10:00,08:10:00,b,2\n",
        "calendar_dates": "service_id,date,exception_type\ns,20260908,1\n",
    }
    with zipfile.ZipFile(upstream, "w") as archive:
        for name, content in tables.items():
            archive.writestr(name + ".txt", content)
    state = {"discoveries": 0, "downloads": [], "status": "archived"}
    coverage = box(-1.69, 48.10, -1.67, 48.12)

    class Provider:
        def __init__(self, *args, **kwargs):
            pass

        def insert_data(self, connection, sources):
            state["discoveries"] += 1
            for resource, status in [("bus", "archived"), ("tram", state["status"])]:
                sources.insert_gtfs_file(
                    connection, country="fr", provider="fixture", dataset_id="city", resource_id=resource,
                    title=resource, download_url=f"https://example.com/{resource}.zip",
                    gtfs_file_date="2026-09-01", gtfs_file_age_days=0, status=status,
                    coverage_geometry=coverage if status != "missing_coverage" else None,
                )

    def download(url, path, **kwargs):
        if not path.exists():
            state["downloads"].append(url)
            path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(upstream, path)
        return path

    data_module = import_module(GTFSData.__module__)
    sources_module = import_module(GTFSSources.__module__)
    monkeypatch.setattr(data_module, "download_file", download)
    monkeypatch.setattr(sources_module, "available_gtfs_sources", lambda: {"fr": Provider})
    monkeypatch.setattr(GTFSSources, "build_french_area_filter", lambda self: None)
    zones = InMemoryAsset({"zones": "Rennes"})
    monkeypatch.setattr(zones, "get", lambda: gpd.GeoDataFrame(geometry=[coverage], crs=4326))
    state.update({"zones": zones, "upstream": upstream})
    return state


def test_repeated_run_reuses_sqlite_zip_and_router(tmp_path, source_run):
    sources = GTFSSources("2026-09-01", tmp_path / "sources", ["fr"], transport_zones=source_run["zones"])
    router = GTFSRouter(source_run["zones"], sources)
    first_paths = router.get()
    before = sources.cache_path.read_bytes()
    assert len(source_run["downloads"]) == 2
    repeated_sources = GTFSSources("2026-09-01", tmp_path / "sources", ["fr"], transport_zones=source_run["zones"])
    repeated = GTFSRouter(source_run["zones"], repeated_sources)
    assert repeated.cache_path == first_paths
    assert not repeated.is_update_needed()
    assert repeated.get() == first_paths
    assert source_run["discoveries"] == 1
    assert len(source_run["downloads"]) == 2
    assert sources.cache_path.read_bytes() == before


def test_fresh_clone_downloads_pinned_urls_without_discovery(tmp_path, source_run, monkeypatch):
    sources = GTFSSources("2026-09-01", tmp_path / "sources", ["fr"])
    sources.get()
    clone = GTFSSources("2026-09-01", tmp_path / "clone", ["fr"])
    shutil.copyfile(sources.cache_path, clone.cache_path)
    monkeypatch.setenv("MOBILITY_PACKAGE_DATA_FOLDER", str(tmp_path / "fresh-downloads"))
    router = GTFSRouter(source_run["zones"], clone)
    paths = router.get()
    assert paths["stops_and_lines"].exists()
    assert source_run["discoveries"] == 1
    assert len(source_run["downloads"]) == 4


@pytest.mark.parametrize("status", ["missing_archive", "stale_archive", "missing_coverage"])
def test_missing_feeds_fail_until_explicitly_excluded(tmp_path, source_run, status):
    source_run["status"] = status
    sources = GTFSSources("2026-09-01", tmp_path / "sources", ["fr"])
    with pytest.raises(ValueError, match=r"excluded_gtfs_sources=\['fixture:tram'\]"):
        sources.get()
    assert not sources.cache_path.exists()
    excluded = GTFSSources(
        "2026-09-01", tmp_path / "sources", ["fr"], excluded_gtfs_sources=["fixture:tram"],
    )
    excluded.get()
    assert excluded.cache_path != sources.cache_path
    assert excluded.validate_metadata()["excluded_gtfs_sources"] == ["fixture:tram"]
    with sqlite3.connect(excluded.cache_path) as connection:
        assert connection.execute("SELECT status, sha256 FROM gtfs_files WHERE resource_id='tram'").fetchone() == ("excluded", None)


def test_zip_sidecar_is_reused_and_rebuilt_without_download(tmp_path, source_run, monkeypatch):
    data = GTFSData("fixture", "city", "bus", "https://example.com/bus.zip", "2026-09-01", "archived", "first")
    outputs = data.get()
    metadata = json.loads(outputs["metadata"].read_text())
    assert metadata["service_start_date"] == metadata["service_end_date"] == "2026-09-08"
    assert metadata["sha256"] == GTFSData.file_sha256(outputs["zip"])
    before = outputs["zip"].stat().st_mtime_ns
    outputs["metadata"].unlink()
    assert data.get() == outputs
    assert outputs["zip"].stat().st_mtime_ns == before
    assert source_run["downloads"] == ["https://example.com/bus.zip"]

    def unexpected_read(reader):
        raise AssertionError("The existing sidecar must be reused")

    monkeypatch.setattr(GTFSFeed, "read_calendars", unexpected_read)
    repeated = GTFSData("fixture", "city", "bus", "https://example.com/bus.zip", "2026-09-01", "archived", "later")
    assert repeated.get() == outputs
    pinned = GTFSData("fixture", "city", "bus", "https://example.com/bus.zip", "2026-09-01", "archived", "later", metadata["sha256"])
    assert json.loads(pinned.get()["metadata"].read_text()) == metadata
    # A new reference date starts with unpinned assets; reuse the pinned sidecar too.
    outputs["metadata"].unlink()
    assert json.loads(repeated.get()["metadata"].read_text()) == metadata
    assert source_run["downloads"] == ["https://example.com/bus.zip"]


def test_snapshot_filters_expired_feeds_before_attaching_dependencies(tmp_path, source_run):
    sources = GTFSSources("2026-09-01", tmp_path / "sources", ["fr"])
    sources.get()
    with sqlite3.connect(sources.cache_path) as connection:
        connection.execute("UPDATE gtfs_files SET service_start_date='2025-01-01', service_end_date='2025-12-31' WHERE resource_id='tram'")
    router = GTFSRouter(source_run["zones"], sources)
    assert [feed.resource_id for feed in router.gtfs_data] == ["bus"]
    with sqlite3.connect(sources.cache_path) as connection:
        connection.execute("UPDATE gtfs_files SET service_start_date=NULL, service_end_date=NULL WHERE resource_id='tram'")
    router = GTFSRouter(source_run["zones"], sources)
    assert {feed.resource_id for feed in router.gtfs_data} == {"bus", "tram"}


@pytest.mark.parametrize("exception_date", ["20260908", ""])
def test_sidecar_keeps_added_service_and_unknown_dates(tmp_path, source_run, exception_date):
    with zipfile.ZipFile(source_run["upstream"]) as archive:
        tables = {name: archive.read(name) for name in archive.namelist()}
    tables["calendar.txt"] = (
        "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date\n"
        "s,0,1,0,0,0,0,0,20250101,20251231\n"
    ).encode()
    tables["calendar_dates.txt"] = f"service_id,date,exception_type\ns,{exception_date},1\n".encode()
    with zipfile.ZipFile(source_run["upstream"], "w") as archive:
        for name, data in tables.items():
            archive.writestr(name, data)
    sources = GTFSSources("2026-09-01", tmp_path / "sources", ["fr"])
    router = GTFSRouter(source_run["zones"], sources)
    assert len(router.gtfs_data) == 2
    with sqlite3.connect(sources.cache_path) as connection:
        assert connection.execute("SELECT DISTINCT service_end_date FROM gtfs_files").fetchall() == [
            ("2026-09-08" if exception_date else None,)
        ]


def test_previous_snapshot_is_enriched_without_rediscovery(tmp_path, source_run):
    sources = GTFSSources("2026-09-01", tmp_path / "sources", ["fr"])
    sources.get()
    sources.cache_path.replace(sources.previous_path)
    with sqlite3.connect(sources.previous_path) as connection:
        connection.execute("UPDATE metadata SET schema_version='3'")
        connection.execute("ALTER TABLE gtfs_files DROP COLUMN service_start_date")
        connection.execute("ALTER TABLE gtfs_files DROP COLUMN service_end_date")
    before = sources.previous_path.read_bytes()
    sources.get()
    assert sources.previous_path.read_bytes() == before
    assert source_run["discoveries"] == 1
    assert len(source_run["downloads"]) == 2
    with sqlite3.connect(sources.cache_path) as connection:
        assert connection.execute("SELECT DISTINCT service_start_date, service_end_date FROM gtfs_files").fetchall() == [("2026-09-08", "2026-09-08")]


def test_changed_archive_fails_without_replacing_snapshot(tmp_path, source_run, monkeypatch):
    sources = GTFSSources("2026-09-01", tmp_path / "sources", ["fr"])
    sources.get()
    before = sources.cache_path.read_bytes()
    with zipfile.ZipFile(source_run["upstream"], "a") as archive:
        archive.writestr("changed.txt", "provider changed the frozen URL")
    monkeypatch.setenv("MOBILITY_PACKAGE_DATA_FOLDER", str(tmp_path / "fresh-downloads"))
    router = GTFSRouter(source_run["zones"], sources)
    with pytest.raises(ValueError, match="SHA-256 differs"):
        router.get()
    assert source_run["discoveries"] == 1
    assert sources.cache_path.read_bytes() == before


def test_sqlite_content_change_changes_downstream_hash(tmp_path, source_run):
    sources = GTFSSources("2026-09-01", tmp_path / "sources", ["fr"])
    first = GTFSRouter(source_run["zones"], sources)
    with sqlite3.connect(sources.cache_path) as connection:
        connection.execute("UPDATE gtfs_files SET title='Reviewed operator name' WHERE resource_id='bus'")
    second = GTFSRouter(source_run["zones"], sources)
    assert first.cache_path != second.cache_path
    assert InMemoryAsset({"router": first}).inputs_hash != InMemoryAsset({"router": second}).inputs_hash
    assert source_run["discoveries"] == 1


def test_scenario_routers_share_selection_but_observe_snapshot_edits(tmp_path, source_run, monkeypatch):
    sources = GTFSSources("2026-09-01", tmp_path / "sources", ["fr"])
    select = GTFSSources.get_gtfs_resources_for_area
    selections = []

    def count_selection(asset, zones):
        selections.append(asset.cache_path)
        return select(asset, zones)

    monkeypatch.setattr(GTFSSources, "get_gtfs_resources_for_area", count_selection)
    first = GTFSRouter(source_run["zones"], sources)
    second_sources = GTFSSources("2026-09-01", tmp_path / "sources", ["fr"])
    variant = GTFSRouter(source_run["zones"], second_sources, expected_agencies=["Fixture"])
    repeated = GTFSRouter(source_run["zones"], second_sources)
    assert len(selections) == 1
    assert [feed.cache_path for feed in first.gtfs_data] == [feed.cache_path for feed in variant.gtfs_data]
    assert first.cache_path == repeated.cache_path

    with sqlite3.connect(sources.cache_path) as connection:
        connection.execute("UPDATE gtfs_files SET title='Changed' WHERE resource_id='bus'")
    changed = GTFSRouter(source_run["zones"], sources)
    assert len(selections) == 2
    assert changed.cache_path != first.cache_path


def test_new_date_creates_separate_snapshot_and_legacy_is_preserved(tmp_path, source_run):
    sources = GTFSSources("2026-09-01", tmp_path / "sources", ["fr"])
    sources.get()
    before = sources.cache_path.read_bytes()
    next_date = GTFSSources("2026-09-02", tmp_path / "sources", ["fr"])
    next_date.get()
    assert next_date.cache_path != sources.cache_path
    assert source_run["discoveries"] == 2
    later = GTFSSources("2027-03-01", tmp_path / "sources", ["fr"])
    assert later.cache_path != sources.cache_path
    later.legacy_path.write_bytes(b"old snapshot")
    with pytest.raises(ValueError, match="Legacy GTFS snapshot"):
        later.get()
    assert later.legacy_path.read_bytes() == b"old snapshot"
    assert not later.cache_path.exists()
    assert sources.cache_path.read_bytes() == before


def test_missing_zone_cache_does_not_reselect_frozen_sources(tmp_path, source_run):
    class Zones(FileAsset):
        def __init__(self):
            super().__init__({"zone": "Rennes"}, tmp_path / "zones.txt")

        def create_and_get_asset(self):
            self.cache_path.write_text("zones")
            return self.get_cached_asset()

        def get_cached_asset(self):
            return source_run["zones"].get()

    zones = Zones()
    sources = GTFSSources("2026-09-01", tmp_path / "sources", ["fr"], transport_zones=zones)
    sources.get()
    before = sources.cache_path.read_bytes()
    zones.cache_path.unlink()
    sources.get()
    sources.create_and_get_asset()
    assert not zones.cache_path.exists()
    assert source_run["discoveries"] == 1
    assert sources.cache_path.read_bytes() == before


def test_unknown_exclusion_does_not_publish_a_snapshot(tmp_path, source_run):
    sources = GTFSSources(
        "2026-09-01", tmp_path / "sources", ["fr"], excluded_gtfs_sources=["fixture:typo"],
    )
    with pytest.raises(ValueError, match="Unknown excluded_gtfs_sources"):
        sources.get()
    assert not sources.cache_path.exists()


def test_invalid_zip_does_not_publish_a_snapshot(tmp_path, source_run):
    source_run["upstream"].write_bytes(b"provider returned an HTML error page")
    sources = GTFSSources("2026-09-01", tmp_path / "sources", ["fr"])
    with pytest.raises(ValueError, match="Invalid GTFS archive"):
        sources.get()
    assert not sources.cache_path.exists()
    assert not sources.cache_path.with_name(sources.cache_path.name + ".part").exists()
