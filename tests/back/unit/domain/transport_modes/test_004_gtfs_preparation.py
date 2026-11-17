"""Small GTFS feeds that exercise calendar, timetable and transfer behaviour."""

import json
import zipfile
import shutil
import subprocess
from importlib import resources
from pathlib import Path

import geopandas as gpd
import pandas as pd
import pytest
from shapely.geometry import box

import mobility.transport.modes.public_transport.public_transport_graph as graph_module
from mobility.runtime.assets.file_asset import FileAsset
from mobility.transport.modes.public_transport.gtfs.gtfs_timetable import GTFSTimetable
from mobility.transport.modes.public_transport.gtfs.gtfs_router import GTFSRouter
from mobility.transport.modes.public_transport.gtfs.gtfs_feed import GTFSFeed
from mobility.transport.modes.public_transport.gtfs.gtfs_data import GTFSData
from mobility.runtime.assets.in_memory_asset import InMemoryAsset


@pytest.fixture
def feed_files():
    """A Tuesday trip with two stops in Rennes."""
    return {
        "agency": "agency_id,agency_name,agency_url,agency_timezone\na,Test,https://example.com,Europe/Paris\n",
        "routes": "route_id,agency_id,route_long_name,route_type\nr,a,Test route,3\n",
        "stops": "stop_id,stop_name,stop_lat,stop_lon\na,A,48.11,-1.68\nb,B,48.11,-1.6799\n",
        "trips": "route_id,service_id,trip_id\nr,s,t\n",
        "stop_times": "trip_id,arrival_time,departure_time,stop_id,stop_sequence\nt,08:00:00,08:00:00,a,1\nt,08:10:00,08:10:00,b,2\n",
        "calendar": "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date\ns,0,1,0,0,0,0,0,20260908,20260915\n",
    }


def write_feed(path, files):
    """Write a fixture feed without external data or downloads."""
    with zipfile.ZipFile(path, "w") as archive:
        for name, content in files.items():
            archive.writestr(name + ".txt", content)
    return path


def write_feed_pair(folder, first, second):
    """Write two feeds for a comparison."""
    return [write_feed(folder / "first.zip", first), write_feed(folder / "second.zip", second)]


def frequency(start="08:00:00", end="09:00:00", headway=600, exact="0"):
    """A single frequency interval for trip t."""
    return f"trip_id,start_time,end_time,headway_secs,exact_times\nt,{start},{end},{headway},{exact}\n"


def test_spatiotemporal_overlap_requires_priority_and_reports_feeds(tmp_path, feed_files, zones, route_types):
    other = dict(feed_files)
    other["agency"] = other["agency"].replace("Test,", "Another operator,")
    other["agency"] += "b,Outside study area,https://example.com,Europe/Paris\n"
    other["trips"] = other["trips"].replace("r,s,t", "r,s,other")
    other["stop_times"] = other["stop_times"].replace("\nt,", "\nother,")
    paths = write_feed_pair(tmp_path, feed_files, other)
    ids = {str(paths[0]): "test:first", str(paths[1]): "test:second"}
    with pytest.raises(ValueError, match="test:first.*test:second") as error:
        GTFSTimetable(paths, zones, route_types, source_ids=ids).prepare()
    assert "All agencies listed in each affected feed" in str(error.value)
    assert "matched operators:" not in str(error.value)
    assert "Another operator [a]" in str(error.value)
    assert "Outside study area [b]" in str(error.value)
    tables, metadata = GTFSTimetable(paths, zones, route_types, source_ids=ids,
                                     feed_priority=["test:first", "test:second"]).prepare()
    assert tables["trips"].trip_id.tolist() == ["1-t"]
    assert metadata["spatiotemporal_overlap_pairs"] == 1
    assert metadata["trip_overlaps"][["left_trip_id", "right_trip_id"]].values.tolist() == [["1-t", "2-other"]]
    assert metadata["trip_overlaps"].removed_trip_id.tolist() == ["2-other"]
    router = object.__new__(GTFSRouter)
    router.cache_path = {"stops_and_lines": tmp_path / "report.gpkg"}
    router.write_stops_and_lines(tables, metadata)
    import pyogrio
    report = pyogrio.read_dataframe(router.cache_path["stops_and_lines"], layer="trip_overlaps")
    assert report[["left_trip_id", "right_trip_id"]].values.tolist() == [["1-t", "2-other"]]


@pytest.mark.parametrize("exact,expected_pairs", [("0", 0), ("1", 1), ("", 0)])
def test_frequency_departures_are_only_matched_when_exact(tmp_path, feed_files, zones, route_types, exact, expected_pairs):
    other = dict(feed_files)
    other["agency"] = other["agency"].replace("Test,", "Another operator,")
    other["frequencies"] = frequency(end="08:01:00", exact=exact)
    paths = write_feed_pair(tmp_path, feed_files, other)
    tables, metadata = GTFSTimetable(paths, zones, route_types, feed_priority=[str(p) for p in paths]).prepare()
    assert len(tables["trips"]) == 2 - expected_pairs
    assert "_frequency_kind" not in tables["trips"]
    assert metadata["spatiotemporal_overlap_pairs"] == expected_pairs
    assert metadata["headway_generated_trips"] == (0 if exact == "1" else 1)


@pytest.mark.parametrize("headway", [False, True])
def test_overlap_with_different_arrivals_keeps_preferred_timetable(tmp_path, feed_files, zones, route_types, headway):
    first, second = dict(feed_files), dict(feed_files)
    first["stop_times"] = first["stop_times"].replace("08:10:00,08:10:00,b", "08:09:00,08:10:00,b")
    if headway:
        first["frequencies"] = second["frequencies"] = frequency()
    paths = write_feed_pair(tmp_path, first, second)
    with pytest.raises(ValueError, match="maximum arrival difference 60 s"):
        GTFSTimetable(paths, zones, route_types).prepare()
    tables, metadata = GTFSTimetable(paths, zones, route_types, feed_priority=[str(p) for p in paths]).prepare()
    overlaps = metadata["headway_overlaps" if headway else "trip_overlaps"]
    assert overlaps.max_arrival_difference_s.tolist() == [60.0]
    assert all(trip.startswith("1-") for trip in tables["trips"].trip_id)
    assert (tables["stop_times"].departure_time - tables["stop_times"].arrival_time).max() == 60


def test_different_arrivals_within_feed_are_not_removed(tmp_path, feed_files, zones, route_types):
    feed_files["trips"] += "r,s,u\n"
    feed_files["stop_times"] += "u,08:00:00,08:00:00,a,1\nu,08:09:00,08:10:00,b,2\n"
    tables, metadata = GTFSTimetable([write_feed(tmp_path / "feed.zip", feed_files)], zones, route_types).prepare()
    assert len(tables["trips"]) == 2
    assert metadata["within_feed_deduplicated_trips"] == 0


def test_overlap_compares_only_selected_day(tmp_path, feed_files, zones, route_types):
    other = dict(feed_files)
    other["agency"] = other["agency"].replace("Test,", "Another operator,")
    other["calendar_dates"] = "service_id,date,exception_type\ns,20260908,2\n"
    paths = write_feed_pair(tmp_path, feed_files, other)
    tables, metadata = GTFSTimetable(paths, zones, route_types, "2026-09-08", "2026-09-08").prepare()
    assert metadata["selected_date"] == "2026-09-08"
    assert len(tables["trips"]) == 1
    assert metadata["spatiotemporal_overlap_pairs"] == 0


def test_trips_diverging_outside_study_area_match_locally(tmp_path, feed_files, zones, route_types):
    first, second = dict(feed_files), dict(feed_files)
    first["stops"] += "outside1,Far north,49.0,-1.68\n"
    second["stops"] += "outside2,Far east,48.11,-0.5\n"
    first["stop_times"] += "t,09:00:00,09:00:00,outside1,3\n"
    second["stop_times"] += "t,09:00:00,09:00:00,outside2,3\n"
    paths = write_feed_pair(tmp_path, first, second)
    tables, metadata = GTFSTimetable(paths, zones, route_types, feed_priority=[str(p) for p in paths]).prepare()
    assert metadata["spatiotemporal_overlap_pairs"] == 1
    assert tables["trips"].trip_id.tolist() == ["1-t"]
    assert len(tables["stop_times"]) == 2


@pytest.mark.parametrize("column", ["pickup_type", "drop_off_type"])
@pytest.mark.parametrize("headway", [False, True])
def test_different_boarding_rules_preserve_both_feeds(tmp_path, feed_files, zones, route_types, column, headway):
    other = dict(feed_files)
    if headway:
        feed_files["frequencies"] = other["frequencies"] = frequency()
    other["agency"] = other["agency"].replace("Test,", "Another operator,")
    other["stop_times"] = other["stop_times"].replace("stop_sequence\n", f"stop_sequence,{column}\n")
    other["stop_times"] = other["stop_times"].replace(",a,1\n", ",a,1,0\n").replace(",b,2\n", ",b,2,1\n")
    paths = write_feed_pair(tmp_path, feed_files, other)
    tables, metadata = GTFSTimetable(paths, zones, route_types).prepare()
    overlap_count = "headway_interval_pairs" if headway else "spatiotemporal_overlap_pairs"
    assert metadata[overlap_count] == 0
    assert len(tables["trips"]) == (12 if headway else 2)


def test_duplicate_trips_within_feed_are_removed_with_warning(tmp_path, feed_files, zones, route_types, caplog):
    feed_files["trips"] += "r,s,u\n"
    feed_files["stop_times"] += "u,08:00:00,08:00:00,a,1\nu,08:10:00,08:10:00,b,2\n"
    path = write_feed(tmp_path / "feed.zip", feed_files)
    tables, metadata = GTFSTimetable([path], zones, route_types,
                                     source_ids={str(path): "test:feed"}).prepare()
    assert tables["trips"].trip_id.tolist() == ["1-t"]
    assert metadata["within_feed_deduplicated_trips"] == 1
    assert metadata["within_feed_overlaps"].removed_trip_id.tolist() == ["1-u"]
    assert "within GTFS feed test:feed" in caplog.text


def test_identical_headway_intervals_require_priority_and_remove_one_interval(tmp_path, feed_files, zones, route_types):
    first, second = dict(feed_files), dict(feed_files)
    first["frequencies"] = frequency()
    second["frequencies"] = first["frequencies"]
    second["agency"] = second["agency"].replace("Test,", "Other,")
    paths = write_feed_pair(tmp_path, first, second)
    with pytest.raises(ValueError, match="Potential overlapping GTFS headway intervals"):
        GTFSTimetable(paths, zones, route_types).prepare()
    tables, metadata = GTFSTimetable(paths, zones, route_types, feed_priority=[str(p) for p in paths]).prepare()
    assert len(tables["trips"]) == 6
    assert metadata["headway_interval_pairs"] == 1
    assert metadata["deduplicated_headway_intervals"] == 1
    assert metadata["deduplicated_trips"] == 6
    assert all(trip.startswith("1-") for trip in tables["trips"].trip_id)


def test_partially_overlapping_headway_intervals_stop_for_review(tmp_path, feed_files, zones, route_types):
    first, second = dict(feed_files), dict(feed_files)
    first["frequencies"] = frequency()
    second["frequencies"] = frequency(start="08:30:00", end="09:30:00")
    second["agency"] = second["agency"].replace("Test,", "Other,")
    paths = write_feed_pair(tmp_path, first, second)
    with pytest.raises(ValueError, match="different service windows or headways") as error:
        GTFSTimetable(paths, zones, route_types, feed_priority=[str(p) for p in paths]).prepare()
    assert str(paths[0]) in str(error.value)
    assert str(paths[1]) in str(error.value)
    assert "All agencies listed in each affected feed" in str(error.value)


def test_identical_headway_intervals_within_feed_are_removed_with_warning(tmp_path, feed_files, zones, route_types, caplog):
    feed_files["trips"] += "r,s,second\n"
    feed_files["stop_times"] += "second,08:00:00,08:00:00,a,1\nsecond,08:10:00,08:10:00,b,2\n"
    feed_files["frequencies"] = ("trip_id,start_time,end_time,headway_secs,exact_times\n"
                                 "t,08:00:00,09:00:00,600,0\nsecond,08:00:00,09:00:00,600,0\n")
    path = write_feed(tmp_path / "feed.zip", feed_files)
    tables, metadata = GTFSTimetable([path], zones, route_types).prepare()
    assert len(tables["trips"]) == 6
    assert metadata["within_feed_deduplicated_headway_intervals"] == 1
    assert metadata["within_feed_deduplicated_trips"] == 6
    assert "identical headway intervals" in caplog.text


@pytest.mark.parametrize("other_window,other_headway,expected", [
    ("23:30:00,25:30:00", "600", "matched"),
    ("25:30:00,26:30:00", "600", "separate"),
    ("23:30:00,25:30:00", "900", "partial"),
])
def test_headway_windows_across_midnight(tmp_path, feed_files, zones, route_types, other_window, other_headway, expected):
    first, second = dict(feed_files), dict(feed_files)
    first["frequencies"] = frequency(start="23:30:00", end="25:30:00")
    start, end = other_window.split(",")
    second["frequencies"] = frequency(start=start, end=end, headway=other_headway)
    second["agency"] = second["agency"].replace("Test,", "Other,")
    paths = write_feed_pair(tmp_path, first, second)
    model = GTFSTimetable(paths, zones, route_types, feed_priority=[str(p) for p in paths])
    if expected == "partial":
        with pytest.raises(ValueError, match="different service windows or headways"):
            model.prepare()
    else:
        tables, metadata = model.prepare()
        assert metadata["headway_interval_pairs"] == (1 if expected == "matched" else 0)
        assert len(tables["trips"]) == (12 if expected == "matched" else 18)


def test_headway_windows_are_aligned_at_first_retained_stop(tmp_path, feed_files, zones, route_types):
    first, second = dict(feed_files), dict(feed_files)
    for feed in (first, second):
        feed["stops"] += "outside,Outside area,49.0,-1.68\n"
    first["stop_times"] += "t,07:50:00,07:50:00,outside,0\n"
    second["stop_times"] += "t,07:55:00,07:55:00,outside,0\n"
    first["frequencies"] = frequency()
    second["frequencies"] = frequency(start="08:05:00", end="09:05:00")
    paths = write_feed_pair(tmp_path, first, second)
    tables, metadata = GTFSTimetable(paths, zones, route_types, feed_priority=[str(p) for p in paths]).prepare()
    assert len(tables["trips"]) == 6
    assert metadata["headway_interval_pairs"] == 1
    assert metadata["headway_overlaps"].identical_interval.tolist() == [True]


@pytest.mark.parametrize("reverse", [False, True])
def test_priority_keeps_unmatched_trips_and_preferred_feed(tmp_path, feed_files, zones, route_types, reverse):
    other = dict(feed_files)
    other["agency"] = other["agency"].replace("Test,", "Another operator,")
    other["trips"] += "r,s,unique\n"
    other["stop_times"] += "unique,10:00:00,10:00:00,a,1\nunique,10:10:00,10:10:00,b,2\n"
    paths = write_feed_pair(tmp_path, feed_files, other)
    priority = [str(paths[1]), str(paths[0])] if reverse else [str(paths[0]), str(paths[1])]
    tables, metadata = GTFSTimetable(paths, zones, route_types, feed_priority=priority).prepare()
    assert set(tables["trips"].trip_id) == ({"2-t", "2-unique"} if reverse else {"1-t", "2-unique"})
    assert metadata["deduplicated_trips"] == 1


@pytest.fixture
def zones():
    return gpd.GeoDataFrame(geometry=[box(-1.69, 48.10, -1.67, 48.12)], crs=4326)


@pytest.fixture
def route_types():
    return resources.files("mobility.runtime.resources").joinpath("gtfs/gtfs_route_types.csv")


def test_quoted_csv_header_with_bom_keeps_stop_ids(tmp_path, feed_files, zones, route_types):
    expected, _ = GTFSTimetable([write_feed(tmp_path / "plain.zip", feed_files)], zones, route_types).prepare()
    feed_files["stops"] = '\ufeff' + feed_files["stops"].replace("stop_id,", '"stop_id",', 1)
    actual, _ = GTFSTimetable([write_feed(tmp_path / "quoted.zip", feed_files)], zones, route_types).prepare()
    for name in expected:
        pd.testing.assert_frame_equal(actual[name], expected[name])


def test_feed_in_subfolder_matches_root_feed(tmp_path, feed_files, zones, route_types):
    """A provider's enclosing folder must not change the prepared timetable."""
    root = write_feed(tmp_path / "root.zip", feed_files)
    nested = write_feed(tmp_path / "nested.zip", {f"gtfs st malo hiver/{name}": text for name, text in feed_files.items()})
    data = GTFSData("fixture", "dataset", "feed", "https://example/feed.zip", "2023-08-23", "archived", "unused")
    data.cache_path["zip"] = nested
    data.validate_file()
    expected, _ = GTFSTimetable([root], zones, route_types).prepare()
    actual, _ = GTFSTimetable([nested], zones, route_types).prepare()
    for table in expected:
        pd.testing.assert_frame_equal(actual[table], expected[table])


@pytest.mark.parametrize("duplicate", [True, False])
def test_tables_in_multiple_folders_are_rejected(tmp_path, feed_files, duplicate):
    files = {f"first/{name}": text for name, text in feed_files.items()}
    files["second/stops"] = feed_files["stops"]
    if not duplicate:
        del files["first/stops"]
    path = write_feed(tmp_path / "ambiguous.zip", files)
    with zipfile.ZipFile(path) as archive, pytest.raises(ValueError, match="multiple"):
        GTFSFeed.table_paths(archive)


def test_padded_csv_lines_match_unpadded_feed(tmp_path, feed_files, zones, route_types):
    """Trailing spaces in headers and values do not change service or identifiers."""
    padded = {name: "\n".join(line + " " * 100 for line in text.splitlines()) + "\n"
              for name, text in feed_files.items()}
    padded["calendar_dates"] = "service_id,date,exception_type    \n"
    expected, _ = GTFSTimetable([write_feed(tmp_path / "plain.zip", feed_files)], zones, route_types).prepare()
    actual, _ = GTFSTimetable([write_feed(tmp_path / "padded.zip", padded)], zones, route_types).prepare()
    for table in expected:
        pd.testing.assert_frame_equal(actual[table], expected[table])


def test_empty_optional_calendar_dates_uses_weekly_calendar(tmp_path, feed_files, zones, route_types):
    expected, _ = GTFSTimetable([write_feed(tmp_path / "plain.zip", feed_files)], zones, route_types).prepare()
    feed_files["calendar_dates"] = ""
    actual, _ = GTFSTimetable([write_feed(tmp_path / "empty.zip", feed_files)], zones, route_types).prepare()
    for table in expected:
        pd.testing.assert_frame_equal(actual[table], expected[table])


@pytest.mark.parametrize("conflict_date", ["20260216", "20260908"])
def test_calendar_conflicts_only_fail_inside_study_window(tmp_path, feed_files, zones, route_types, conflict_date):
    feed_files["calendar_dates"] = (
        "service_id,date,exception_type\n"
        f"s,{conflict_date},1\ns,{conflict_date},2\n"
        "s,20260915,1\ns,20260915,1\n"
    )
    timetable = GTFSTimetable([write_feed(tmp_path / "feed.zip", feed_files)], zones, route_types,
                             "2026-09-01", "2026-09-28")
    if conflict_date == "20260908":
        with pytest.raises(ValueError, match="both added and cancelled on 2026-09-08"):
            timetable.prepare()
    else:
        tables, metadata = timetable.prepare()
        assert metadata["selected_date"] == "2026-09-08"
        assert len(tables["trips"]) == 1


def test_dates_only_and_long_route_name(tmp_path, feed_files, zones, route_types):
    del feed_files["calendar"]
    feed_files["calendar_dates"] = "service_id,date,exception_type\ns,20260908,1\n"
    tables, metadata = GTFSTimetable(
        [write_feed(tmp_path / "feed.zip", feed_files)], zones, route_types
    ).prepare()
    assert metadata["selected_date"] == "2026-09-08"
    assert tables["routes"].route_short_name.tolist() == ["Test route"]


def test_service_window_excludes_expired_feeds_downstream(tmp_path, feed_files, zones, route_types):
    old = {**feed_files, "calendar": feed_files["calendar"].replace("20260908,20260915", "20250701,20260703")}
    tables, metadata = GTFSTimetable(
        [write_feed(tmp_path / "old.zip", old), write_feed(tmp_path / "current.zip", feed_files)],
        zones, route_types, "2026-09-01", "2026-09-28",
    ).prepare()
    assert metadata["selected_date"] == "2026-09-08"
    assert metadata["sources"][0]["status"] == "no_service_in_window"
    assert metadata["sources"][0]["selected_trips"] == 0
    assert len(tables["trips"]) == 1
    assert all("2026-09-01" <= row["date"] <= "2026-09-28" for row in metadata["tuesdays"])


@pytest.mark.parametrize("expired", [True, False])
def test_expired_feed_is_skipped_before_invalid_stops_are_read(
    tmp_path, feed_files, zones, route_types, monkeypatch, expired,
):
    """Broken stops in an expired feed cannot block a current study."""
    other = dict(feed_files)
    other["stops"] += "a,Duplicate A,48.11,-1.68\n"
    if expired:
        other["calendar"] = other["calendar"].replace("20260908,20260915", "20250701,20260703")
    paths = [write_feed(tmp_path / "other.zip", other), write_feed(tmp_path / "current.zip", feed_files)]
    read_paths = []
    opened_tables = []
    original_read = GTFSFeed.read
    original_open = zipfile.ZipFile.open

    def open_table(archive, name, *args, **kwargs):
        if Path(archive.filename) == paths[0]:
            opened_tables.append(name)
        return original_open(archive, name, *args, **kwargs)

    def read(reader, *args):
        read_paths.append(reader.path)
        return original_read(reader, *args)

    monkeypatch.setattr(GTFSFeed, "read", read)
    monkeypatch.setattr(zipfile.ZipFile, "open", open_table)
    timetable = GTFSTimetable(paths, zones, route_types, "2026-09-01", "2026-09-28")
    if expired:
        _, metadata = timetable.prepare()
        assert read_paths == [paths[1]]
        assert opened_tables == ["calendar.txt"]
        assert metadata["sources"][0]["status"] == "no_service_in_window"
    else:
        with pytest.raises(ValueError, match="duplicate stop_id"):
            timetable.prepare()


def test_weekend_service_and_cancelled_tuesdays_are_reported(tmp_path, feed_files, zones, route_types):
    other = {**feed_files,
             "calendar": feed_files["calendar"].replace("s,0,1,0,0,0,0,0", "s,0,1,0,0,0,0,1"),
             "calendar_dates": "service_id,date,exception_type\ns,20260908,2\ns,20260915,2\n"}
    _, metadata = GTFSTimetable(
        [write_feed(tmp_path / "sunday.zip", other), write_feed(tmp_path / "current.zip", feed_files)],
        zones, route_types, "2026-09-01", "2026-09-28",
    ).prepare()
    assert metadata["sources"][0]["status"] == "no_tuesday_service"
    assert metadata["sources"][0]["active_service_dates"] == "2026-09-13"


def test_disjoint_tuesdays_produce_a_coverage_report(tmp_path, feed_files, zones, route_types):
    first = {**feed_files, "calendar": feed_files["calendar"].replace("20260908,20260915", "20260908,20260908")}
    second = {**feed_files, "calendar": feed_files["calendar"].replace("20260908,20260915", "20260915,20260915")}
    _, metadata = GTFSTimetable(
        [write_feed(tmp_path / "first.zip", first), write_feed(tmp_path / "second.zip", second)],
        zones, route_types, "2026-09-01", "2026-09-28",
    ).prepare()
    assert metadata["selected_date"] == "2026-09-08"
    assert metadata["common_tuesday_available"] is False
    assert metadata["sources"][1]["status"] == "no_service_on_selected_date"


def test_possible_overlapping_replacements_are_reported(tmp_path, feed_files, zones, route_types):
    second = {**feed_files, "calendar": feed_files["calendar"].replace("20260915", "20260922")}
    paths = [write_feed(tmp_path / "first.zip", feed_files), write_feed(tmp_path / "second.zip", second)]
    _, metadata = GTFSTimetable(
        paths, zones, route_types, "2026-09-01", "2026-09-28", feed_priority=[str(p) for p in paths],
    ).prepare()
    assert metadata["overlapping_trip_ids"] == 2
    assert [source["overlapping_trip_ids"] for source in metadata["sources"]] == [1, 1]


def test_historical_window_uses_historical_service_dates(tmp_path, feed_files, zones, route_types):
    historical = {**feed_files, "calendar": feed_files["calendar"].replace("20260908,20260915", "20250901,20250930")}
    tables, metadata = GTFSTimetable(
        [write_feed(tmp_path / "past.zip", historical)], zones, route_types, "2025-09-01", "2025-09-28",
    ).prepare()
    assert metadata["selected_date"] == "2025-09-02"
    assert len(tables["stop_times"]) == 2


def test_trips_ignore_unused_columns_with_unquoted_commas(
    tmp_path, feed_files, zones, route_types
):
    """Trip preparation skips an unused shape value with an unquoted comma."""
    feed_files["trips"] = (
        "route_id,service_id,trip_id,trip_headsign,trip_short_name,direction_id,"
        "block_id,wheelchair_accessible,bikes_allowed,shape_id\n"
        "r,s,t,Test trip,0,0,,0,0,TRANSURBAIN:2720$T3 - A12:Clos Nord,.$23\n"
    )

    tables, _ = GTFSTimetable(
        [write_feed(tmp_path / "feed.zip", feed_files)], zones, route_types
    ).prepare()

    assert tables["trips"]["trip_id"].tolist() == ["1-t"]
    assert tables["trips"]["route_id"].tolist() == ["1-r"]
    assert "shape_id" not in tables["trips"]


@pytest.mark.parametrize("cancelled", [True, False])
def test_no_tuesday_does_not_restore_trips(tmp_path, feed_files, zones, route_types, cancelled):
    if cancelled:
        feed_files["calendar_dates"] = (
            "service_id,date,exception_type\ns,20260908,2\ns,20260915,2\n"
        )
    else:
        feed_files["calendar"] = feed_files["calendar"].replace(
            "s,0,1,0,0,0,0,0", "s,0,0,0,0,0,0,1"
        )
    with pytest.raises(ValueError, match="No active Tuesday"):
        GTFSTimetable([write_feed(tmp_path / "feed.zip", feed_files)], zones, route_types).prepare()


def test_frequency_and_restrictions_survive(tmp_path, feed_files, zones, route_types):
    feed_files["frequencies"] = (
        "trip_id,start_time,end_time,headway_secs,exact_times\nt,08:00:00,10:00:00,600,1\n"
    )
    feed_files["stop_times"] = (
        "trip_id,arrival_time,departure_time,stop_id,stop_sequence,pickup_type,drop_off_type\nt,08:00:00,08:00:00,a,1,1,0\nt,08:10:00,08:10:00,b,2,0,1\n"
    )
    tables, _ = GTFSTimetable(
        [write_feed(tmp_path / "feed.zip", feed_files)], zones, route_types
    ).prepare()
    assert len(tables["trips"]) == 12
    assert len(tables["stop_times"]) == 24
    assert tables["stop_times"].departure_time.max() == 36000
    assert tables["stop_times"].pickup_type.sum() == 12
    assert tables["stop_times"].drop_off_type.sum() == 12


def test_intermediate_time_is_interpolated(tmp_path, feed_files, zones, route_types):
    feed_files["stops"] += "c,C,48.11,-1.67995\n"
    feed_files["stop_times"] = (
        "trip_id,arrival_time,departure_time,stop_id,stop_sequence,timepoint\nt,08:00:00,08:00:00,a,1,1\nt,,,c,2,0\nt,08:10:00,08:10:00,b,3,1\n"
    )
    tables, _ = GTFSTimetable(
        [write_feed(tmp_path / "feed.zip", feed_files)], zones, route_types
    ).prepare()
    assert tables["stop_times"].set_index("stop_id").loc["1-c", "arrival_time"] == 29100


def test_duplicate_feed_and_original_dates(tmp_path, feed_files, zones, route_types):
    first = write_feed(tmp_path / "one.zip", feed_files)
    duplicate = write_feed(tmp_path / "duplicate.zip", feed_files)
    feed_files["calendar"] = feed_files["calendar"].replace(
        "20260908,20260915", "20261006,20261006"
    )
    future = write_feed(tmp_path / "future.zip", feed_files)
    tables, metadata = GTFSTimetable([first, duplicate, future], zones, route_types).prepare()
    assert metadata["selected_date"] == "2026-09-08"
    assert metadata["sources"][1]["status"] == "duplicate"
    assert metadata["sources"][2]["selected_trips"] == 0
    assert len(tables["trips"]) == 1
    assert max(day["stop_visits"] for day in metadata["tuesdays"]) == 2


def test_stop_visits_outweigh_calendar_ids(tmp_path, feed_files, zones, route_types):
    feed_files["trips"] += "r,s,t2\nr,q1,q1\nr,q2,q2\n"
    feed_files["stops"] += "c,C,48.11,-1.67995\n"
    feed_files[
        "stop_times"
    ] += "t2,08:00:00,08:00:00,a,1\nt2,08:05:00,08:05:00,c,2\nt2,08:10:00,08:10:00,b,3\nq1,08:00:00,08:00:00,a,1\nq1,08:10:00,08:10:00,b,2\nq2,08:00:00,08:00:00,a,1\nq2,08:10:00,08:10:00,b,2\n"
    feed_files["calendar"] = feed_files["calendar"].replace(
        "20260908,20260915", "20260908,20260908"
    )
    feed_files[
        "calendar"
    ] += "q1,0,1,0,0,0,0,0,20260915,20260915\nq2,0,1,0,0,0,0,0,20260915,20260915\n"
    tables, metadata = GTFSTimetable(
        [write_feed(tmp_path / "feed.zip", feed_files)], zones, route_types
    ).prepare()
    assert metadata["selected_date"] == "2026-09-08"
    assert len(tables["stop_times"]) == 5


def test_forbidden_and_route_specific_transfers_preserved(tmp_path, feed_files, zones, route_types):
    feed_files["transfers"] = (
        "from_stop_id,to_stop_id,transfer_type,min_transfer_time,from_route_id\na,b,3,,\na,b,2,600,r\n"
    )
    tables, _ = GTFSTimetable(
        [write_feed(tmp_path / "feed.zip", feed_files)], zones, route_types
    ).prepare()
    explicit = tables["transfers"].query("specificity >= 0")
    assert explicit.transfer_type.tolist() == [3, 2]
    assert explicit.from_route_id.tolist() == ["", "1-r"]
    assert explicit.min_transfer_time.iloc[1] == 600


def test_one_stop_feed_is_discarded(tmp_path, feed_files, zones, route_types):
    good = write_feed(tmp_path / "good.zip", feed_files)
    feed_files["stop_times"] = (
        "trip_id,arrival_time,departure_time,stop_id,stop_sequence\nt,08:00:00,08:00:00,a,1\n"
    )
    bad = write_feed(tmp_path / "one-stop.zip", feed_files)
    tables, metadata = GTFSTimetable([good, bad], zones, route_types).prepare()
    assert metadata["sources"][1]["status"] == "no usable trips in area"
    assert len(tables["trips"]) == 1


def test_asset_writes_preparation_tables_and_invalidates_changed_manual_feed(
    tmp_path, feed_files, zones, monkeypatch
):
    monkeypatch.setenv("MOBILITY_PROJECT_DATA_FOLDER", str(tmp_path))
    path = write_feed(tmp_path / "feed.zip", feed_files)
    zone_asset = InMemoryAsset({"zones": "fixture"})
    monkeypatch.setattr(zone_asset, "get", lambda: zones)
    asset = GTFSRouter(zone_asset, None, additional_gtfs_files=[path])
    asset.prepare_gtfs_router(zone_asset, [path])
    paths = asset.get_cached_asset()
    assert paths == asset.cache_path
    assert set(paths) == {"agency", "routes", "stops", "trips", "stop_times", "transfers", "stops_and_lines"}
    summary = gpd.read_file(paths["stops_and_lines"], layer="summary")
    assert summary.selected_date.tolist() == ["2026-09-08"]
    assert summary.version.tolist() == [asset.inputs["version"]]
    sources = gpd.read_file(paths["stops_and_lines"], layer="sources")
    assert sources.path.tolist() == [str(path)]
    assert sources.selected_trips.tolist() == [1]
    tuesdays = gpd.read_file(paths["stops_and_lines"], layer="tuesdays")
    assert tuesdays.stop_visits.tolist() == [2, 2]
    assert not asset.assets_missing()
    assert len(pd.read_parquet(asset.cache_path["stop_times"])) == 2
    gpkg_path = paths["stops_and_lines"]
    stops = gpd.read_file(gpkg_path, layer="stops")
    lines = gpd.read_file(gpkg_path, layer="lines")
    assert stops.stop_visits.sum() == 2
    assert stops.selected_date.unique().tolist() == ["2026-09-08"]
    assert lines[["from_stop_id", "to_stop_id"]].values.tolist() == [["1-a", "1-b"]]
    assert lines.trip_count.tolist() == [1]
    assert lines.agency_name.tolist() == ["Test"]
    assert list(lines.geometry.iloc[0].coords) == [(-1.68, 48.11), (-1.6799, 48.11)]
    assert stops.crs.to_epsg() == lines.crs.to_epsg() == 4326
    gpkg_path.unlink()
    assert asset.assets_missing()
    asset.cache_path["trips"].unlink()
    assert asset.assets_missing()
    feed_files["calendar"] = feed_files["calendar"].replace(
        "20260908,20260915", "20261006,20261006"
    )
    write_feed(path, feed_files)
    changed = GTFSRouter(zone_asset, None, additional_gtfs_files=[path])
    assert changed.inputs_hash != asset.inputs_hash


def test_stops_and_lines_counts_selected_journeys_and_keeps_both_directions(
    tmp_path: Path,
    feed_files: dict[str, str],
    zones: gpd.GeoDataFrame,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Map each line segment once, counting only journeys on the selected day."""
    monkeypatch.setenv("MOBILITY_PROJECT_DATA_FOLDER", str(tmp_path))
    feed_files["trips"] += "r,s,t2\nr,s,reverse\nr,later,late\n"
    feed_files["calendar"] += "later,0,1,0,0,0,0,0,20260922,20260922\n"
    feed_files["stop_times"] += (
        "t2,09:00:00,09:00:00,a,1\nt2,09:10:00,09:10:00,b,2\n"
        "reverse,10:00:00,10:00:00,b,1\nreverse,10:10:00,10:10:00,a,2\n"
        "late,08:00:00,08:00:00,a,1\nlate,08:10:00,08:10:00,b,2\n"
    )
    path = write_feed(tmp_path / "feed.zip", feed_files)
    zone_asset = InMemoryAsset({"zones": "fixture"})
    monkeypatch.setattr(zone_asset, "get", lambda: zones)
    asset = GTFSRouter(zone_asset, None, additional_gtfs_files=[path])

    # Writing again replaces the map rather than appending duplicate features.
    for _ in range(2):
        asset.prepare_gtfs_router(zone_asset, [path])
        stops = gpd.read_file(asset.cache_path["stops_and_lines"], layer="stops")
        lines = gpd.read_file(asset.cache_path["stops_and_lines"], layer="lines")
        assert stops.stop_visits.tolist() == [3, 3]
        assert stops.line_count.tolist() == [1, 1]
        assert lines.set_index(["from_stop_id", "to_stop_id"]).trip_count.to_dict() == {
            ("1-a", "1-b"): 2,
            ("1-b", "1-a"): 1,
        }
        assert lines.selected_date.unique().tolist() == ["2026-09-08"]


def test_version_changes_invalidate_timetable_graph_and_downstream_assets(
    tmp_path: Path, feed_files: dict[str, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Older saved results must not hide timetable or graph corrections."""
    monkeypatch.setenv("MOBILITY_PROJECT_DATA_FOLDER", str(tmp_path))
    zones = InMemoryAsset({"zones": "fixture"})
    zones.countries = ["fr"]
    parameters = graph_module.PublicTransportRoutingParameters(
        gtfs_reference_date="2026-09-01",
        gtfs_sources_folder=str(tmp_path),
        additional_gtfs_files=[str(write_feed(tmp_path / "feed.zip", feed_files))],
    )
    file_asset_init = FileAsset.__init__
    monkeypatch.setattr(graph_module, "GTFSSources", lambda **kwargs: None)

    def use_previous_version(self, inputs, cache_path):
        previous_inputs = dict(inputs)
        previous_inputs.pop("version")
        previous_inputs["preparation_version"] = "python-2" if isinstance(self, GTFSRouter) else "2"
        file_asset_init(self, previous_inputs, cache_path)

    with monkeypatch.context() as previous:
        previous.setattr(FileAsset, "__init__", use_previous_version)
        old_graph = graph_module.PublicTransportGraph(zones, parameters)

    graph = graph_module.PublicTransportGraph(zones, parameters)
    changed_priority = graph_module.PublicTransportGraph(
        zones, parameters.model_copy(update={"gtfs_feed_priority": [str(tmp_path / "feed.zip")]})
    )
    assert graph.inputs["version"] == "3"
    assert graph.gtfs_router.inputs["version"] == "13"
    assert graph.gtfs_router.cache_path != changed_priority.gtfs_router.cache_path
    assert graph.cache_path != old_graph.cache_path
    assert graph.gtfs_router.cache_path != old_graph.gtfs_router.cache_path
    assert (
        InMemoryAsset({"graph": graph}).inputs_hash
        != InMemoryAsset({"graph": old_graph}).inputs_hash
    )


@pytest.mark.parametrize(
    "case",
    [
        "restricted",
        "restriction_outside_period",
        "forbidden",
        "route_override",
        "transfer_wait",
        "transfer_exact",
        "transfer_over_limit",
        "transfer_same_stop",
    ],
)
def test_r_graph_reads_parquet_and_respects_direction(
    tmp_path, feed_files, zones, route_types, case
):
    """Exercise the remaining R consumer through graph serialization."""
    rscript = shutil.which("Rscript")
    if rscript is None:
        pytest.skip("R is only needed to check the existing graph consumer")
    feed_files["transfers"] = "from_stop_id,to_stop_id,transfer_type,min_transfer_time\na,b,3,\n"
    feed_files["stop_times"] = (
        "trip_id,arrival_time,departure_time,stop_id,stop_sequence,pickup_type,drop_off_type\nt,08:00:00,08:00:00,a,1,0,1\nt,08:10:00,08:10:00,b,2,1,0\n"
    )
    if case not in ("restricted", "restriction_outside_period"):
        # Both stops have arrival and departure nodes, so the prohibition must
        # actually remove a possible transfer rather than an unused endpoint.
        feed_files["stop_times"] = (
            "trip_id,arrival_time,departure_time,stop_id,stop_sequence\nt,08:00:00,08:00:00,a,1\nt,08:10:00,08:10:00,b,2\nt,08:20:00,08:20:00,a,3\n"
        )
    if case == "route_override":
        feed_files["transfers"] = (
            "from_stop_id,to_stop_id,transfer_type,min_transfer_time,from_route_id\na,b,3,,\na,b,2,600,r\n"
        )
    if case.startswith("transfer_"):
        # Arrive on line r at stop a at 08:00. Line q departs at 08:03
        # and again at the time below, taking ten minutes to reach stop c.
        boarding_stop = "a" if case == "transfer_same_stop" else "b"
        minimum_connection_seconds = 5 * 60
        if case == "transfer_wait":
            # Ready at 08:05: miss 08:03 and wait until 08:10.
            second_departure, second_arrival = "08:10:00", "08:20:00"
            expected_transfer_seconds = 10 * 60
        elif case == "transfer_exact":
            # The second departure leaves exactly when the passenger is ready.
            second_departure, second_arrival = "08:05:00", "08:15:00"
            expected_transfer_seconds = 5 * 60
        elif case == "transfer_over_limit":
            # Ready at 08:17, but the next departure makes the total 21 minutes.
            second_departure, second_arrival = "08:21:00", "08:31:00"
            minimum_connection_seconds = 17 * 60
        else:
            # At the same stop, the default 31 seconds lets us catch 08:03.
            second_departure, second_arrival = "08:10:00", "08:20:00"
            expected_transfer_seconds = 3 * 60
        feed_files["stops"] += "c,C,48.11,-1.677\n"
        feed_files["routes"] += "q,a,Connecting line,3\n"
        feed_files["trips"] = "route_id,service_id,trip_id\nr,s,t\nq,s,q1\nq,s,q2\n"
        feed_files["stop_times"] = (
            "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n"
            "t,07:50:00,07:50:00,b,1\nt,08:00:00,08:00:00,a,2\n"
            f"q1,08:03:00,08:03:00,{boarding_stop},1\n"
            "q1,08:13:00,08:13:00,c,2\n"
            f"q2,{second_departure},{second_departure},{boarding_stop},1\n"
            f"q2,{second_arrival},{second_arrival},c,2\n"
        )
        feed_files["transfers"] = (
            "from_stop_id,to_stop_id,transfer_type,min_transfer_time\n"
            f"a,b,2,{minimum_connection_seconds}\n"
        )
        if case == "transfer_same_stop":
            del feed_files["transfers"]
    if case == "restriction_outside_period":
        feed_files["trips"] += "r,s,later\n"
        feed_files[
            "stop_times"
        ] += "later,11:00:00,11:00:00,a,1,0,0\nlater,11:10:00,11:10:00,b,2,0,0\n"
    tables, metadata = GTFSTimetable(
        [write_feed(tmp_path / "feed.zip", feed_files)], zones, route_types
    ).prepare()
    for name, table in tables.items():
        table.to_parquet(tmp_path / f"{name}.parquet", index=False)
    table_paths = {name: str(tmp_path / f"{name}.parquet") for name in tables}
    script_path = resources.files("mobility.transport.modes.public_transport").joinpath(
        "prepare_public_transport_graph.R"
    )
    script = script_path.read_text()
    if case in ("restricted", "restriction_outside_period"):
        script += """
stopifnot(nrow(travel_times) == 1)
origin <- stops_routes[stop_index == travel_times$from, gtfs_stop_id]
destination <- stops_routes[stop_index == travel_times$to, gtfs_stop_id]
stopifnot(origin == "1-a", destination == "1-b", travel_times$time == 600)
stopifnot(all(stops_routes[stop_index %in% access_times$to, gtfs_stop_id] == "1-a"))
stopifnot(all(stops_routes[stop_index %in% exit_times$from, gtfs_stop_id] == "1-b"))
stopifnot(nrow(transfers) == 1)
"""
    elif case.startswith("transfer_"):
        script += f"""
origin <- stops_routes[gtfs_stop_id == "1-a" & route_id == "1-r" & stop_type == "arrival", stop_index]
destination <- stops_routes[gtfs_stop_id == "1-{boarding_stop}" & route_id == "1-q" & stop_type == "departure", stop_index]
connection <- transfer_times[from %in% origin & to %in% destination]
"""
        if case == "transfer_over_limit":
            script += "stopifnot(nrow(connection) == 0)\n"
        else:
            script += (
                f"stopifnot(nrow(connection) == 1, "
                f"connection$time == {expected_transfer_seconds}, "
                f"connection$perceived_time == {expected_transfer_seconds * 2})\n"
            )
    else:
        script += """
stopifnot(nrow(travel_times) == 2, all(travel_times$time == 600))
origin <- stops_routes[gtfs_stop_id == "1-a" & stop_type == "arrival", stop_index]
destination <- stops_routes[gtfs_stop_id == "1-b" & stop_type == "departure", stop_index]
connection <- transfers[from %in% origin & to %in% destination]
"""
        script += (
            "stopifnot(nrow(connection) == 0)\n"
            if case == "forbidden"
            else "stopifnot(nrow(connection) == 1, connection$min_transfer_time == 600)\n"
        )
    test_script = tmp_path / "check_graph.R"
    test_script.write_text(script)
    output = tmp_path / "graph" / "simplified" / "fixture-graph"
    output.parent.mkdir(parents=True)
    result = subprocess.run(
        [
            rscript,
            str(test_script),
            str(resources.files("mobility")),
            "unused",
            json.dumps(table_paths),
            json.dumps(
                {
                    "start_time_min": 7,
                    "start_time_max": 10,
                    "wait_time_coeff": 2,
                    "transfer_time_coeff": 2,
                    "no_show_perceived_prob": 0.2,
                    "target_time": 9,
                }
            ),
            str(output),
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert output.exists()


def test_bad_trip_is_dropped_without_losing_good_service(tmp_path, feed_files, zones, route_types):
    feed_files["trips"] += "r,s,bad\n"
    feed_files["stop_times"] += "bad,08:00:00,08:00:00,a,1\nbad,07:00:00,07:00:00,b,2\n"
    tables, metadata = GTFSTimetable(
        [write_feed(tmp_path / "feed.zip", feed_files)], zones, route_types
    ).prepare()
    assert tables["trips"].trip_id.tolist() == ["1-t"]
    assert metadata["sources"][0]["dropped_invalid_trips"] == 1


def test_calendar_exception_adds_service_on_a_nonweekly_tuesday(
    tmp_path, feed_files, zones, route_types
):
    feed_files["calendar"] = feed_files["calendar"].replace("s,0,1,0,0,0,0,0", "s,0,0,0,0,0,0,1")
    feed_files["calendar_dates"] = "service_id,date,exception_type\ns,20260915,1\n"
    _, metadata = GTFSTimetable(
        [write_feed(tmp_path / "feed.zip", feed_files)], zones, route_types
    ).prepare()
    assert metadata["selected_date"] == "2026-09-15"


def test_frequency_offsets_use_original_first_stop(tmp_path, feed_files, zones, route_types):
    feed_files["stops"] += "outside,Outside,47.0,-1.0\n"
    feed_files["stop_times"] += "t,07:00:00,07:00:00,outside,0\n"
    feed_files["frequencies"] = (
        "trip_id,start_time,end_time,headway_secs,exact_times\nt,08:00:00,08:10:00,600,1\n"
    )
    tables, _ = GTFSTimetable(
        [write_feed(tmp_path / "feed.zip", feed_files)], zones, route_types
    ).prepare()
    assert tables["stop_times"].departure_time.min() == 9 * 3600


@pytest.mark.parametrize(
    "rule", ["2,60,t", "4,,", "5,,", "9,,", "2,,", "2,-1,", "2,bad,", "2,nan,"]
)
def test_unusable_transfer_omits_connection_and_keeps_timetable(
    tmp_path: Path,
    feed_files: dict[str, str],
    zones: gpd.GeoDataFrame,
    route_types: Path,
    caplog: pytest.LogCaptureFixture,
    rule: str,
) -> None:
    """An unsupported rule must not abort preparation or restore a walking link."""
    feed_files["transfers"] = (
        "from_stop_id,to_stop_id,transfer_type,min_transfer_time,from_trip_id\n"
        f"a,b,2,60,\na,b,{rule}\n"
    )
    tables, _ = GTFSTimetable(
        [write_feed(tmp_path / "feed.zip", feed_files)], zones, route_types
    ).prepare()

    assert len(tables["trips"]) == 1
    assert len(tables["stop_times"]) == 2
    assert tables["transfers"][["from_stop_id", "to_stop_id"]].to_dict("records") == [
        {"from_stop_id": "1-b", "to_stop_id": "1-a"}
    ]
    assert "Omitting all transfers from 1-a to 1-b" in caplog.text


def test_unusable_transfer_on_unused_line_does_not_remove_connections(
    tmp_path: Path,
    feed_files: dict[str, str],
    zones: gpd.GeoDataFrame,
    route_types: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A restriction on a line outside the selected timetable has no effect."""
    feed_files["transfers"] = "from_stop_id,to_stop_id,transfer_type,from_route_id\na,b,4,unused\n"
    tables, _ = GTFSTimetable(
        [write_feed(tmp_path / "feed.zip", feed_files)], zones, route_types
    ).prepare()

    assert len(tables["transfers"]) == 2
    assert "Omitting all transfers" not in caplog.text


def test_frequency_periods_keep_unique_ids_and_scheduled_trips(
    tmp_path, feed_files, zones, route_types
):
    """Multiple periods share one ID sequence and leave scheduled trips intact."""
    feed_files["trips"] += "r,s,scheduled\n"
    feed_files["stop_times"] += "scheduled,08:15:00,08:15:00,a,1\nscheduled,08:25:00,08:25:00,b,2\n"
    feed_files["frequencies"] = (
        "trip_id,start_time,end_time,headway_secs,exact_times\n"
        "t,25:00:00,25:20:00,600,1\nt,08:00:00,08:10:00,600,1\n"
    )
    tables, _ = GTFSTimetable(
        [write_feed(tmp_path / "feed.zip", feed_files)], zones, route_types
    ).prepare()

    assert set(tables["trips"].trip_id) == {
        "1-scheduled",
        "1-t:frequency:0",
        "1-t:frequency:1",
        "1-t:frequency:2",
    }
    first_stops = tables["stop_times"].query("stop_sequence == 1").set_index("trip_id")
    assert first_stops.departure_time.to_dict() == {
        "1-scheduled": 29700,
        "1-t:frequency:0": 90000,
        "1-t:frequency:1": 90600,
        "1-t:frequency:2": 28800,
    }


def test_frequency_periods_outside_area_leave_local_schedule(
    tmp_path, feed_files, zones, route_types
):
    """Pruning every frequency template must still retain scheduled service."""
    feed_files["trips"] += "r,s,outside\n"
    feed_files["frequencies"] = (
        "trip_id,start_time,end_time,headway_secs\noutside,08:00:00,10:00:00,600\n"
    )
    tables, _ = GTFSTimetable(
        [write_feed(tmp_path / "feed.zip", feed_files)], zones, route_types
    ).prepare()

    assert tables["trips"].trip_id.tolist() == ["1-t"]
    assert len(tables["stop_times"]) == 2


def test_station_transfer_rules_apply_to_active_platforms(
    tmp_path: Path, feed_files: dict[str, str], zones: gpd.GeoDataFrame, route_types: Path
) -> None:
    """Station rules survive filtering while unrelated transfer rules are ignored."""
    feed_files["stops"] = (
        "stop_id,stop_name,stop_lat,stop_lon,parent_station\n"
        "a,A,48.11,-1.68,station\nb,B,48.11,-1.6799,\n"
        "station,Station,48.11,-1.68,\noutside,Outside,47.0,-1.0,\n"
    )
    feed_files["transfers"] = (
        "from_stop_id,to_stop_id,transfer_type,from_trip_id\n" "station,b,3,\noutside,b,4,unused\n"
    )
    tables, _ = GTFSTimetable(
        [write_feed(tmp_path / "feed.zip", feed_files)], zones, route_types
    ).prepare()

    explicit = tables["transfers"].query("specificity >= 0")
    assert explicit[["from_stop_id", "to_stop_id", "transfer_type"]].to_dict("records") == [
        {"from_stop_id": "1-a", "to_stop_id": "1-b", "transfer_type": 3}
    ]
    assert set(tables["stops"].stop_id) == {"1-a", "1-b"}


@pytest.mark.parametrize(
    "column,value", [("stop_sequence", "bad"), ("pickup_type", "4"), ("drop_off_type", "bad")]
)
def test_invalid_numeric_stop_fields_are_rejected(
    tmp_path: Path,
    feed_files: dict[str, str],
    zones: gpd.GeoDataFrame,
    route_types: Path,
    column: str,
    value: str,
) -> None:
    """Faster numeric parsing must retain timetable validation."""
    times = pd.DataFrame(
        {
            "trip_id": ["t", "t"],
            "arrival_time": ["08:00:00", "08:10:00"],
            "departure_time": ["08:00:00", "08:10:00"],
            "stop_id": ["a", "b"],
            "stop_sequence": ["1", "2"],
            "pickup_type": ["", "0"],
            "drop_off_type": ["0", ""],
        }
    )
    times.loc[0, column] = value
    feed_files["stop_times"] = times.to_csv(index=False)

    with pytest.raises(ValueError, match=column):
        GTFSTimetable([write_feed(tmp_path / "feed.zip", feed_files)], zones, route_types).prepare()
