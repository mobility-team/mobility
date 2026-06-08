import csv
import zipfile

import pytest

import mobility


def _read_table(gtfs_zip, table_name):
    with zipfile.ZipFile(gtfs_zip) as archive:
        with archive.open(table_name) as file:
            lines = (line.decode("utf-8") for line in file)
            return list(csv.DictReader(lines))


def _base_builder():
    return mobility.GTFSBuilder(
        agency_id="test_agency",
        agency_name="Test Agency",
        route_id="test_route",
        route_short_name="T1",
        route_type="bus",
        service_id="test_service",
    )


def test_002_gtfs_builder_writes_bidirectional_feed_with_deterministic_trip_ids(tmp_path):
    builder = _base_builder()
    builder.add_stops(
        {
            "A": [2.0, 48.0],
            "B": [2.1, 48.1],
        }
    )
    builder.add_line(
        [("A", "B", 300.0)],
        start_time=6.0 * 3600.0,
        end_time=6.0 * 3600.0 + 600.0,
        period=300.0,
    )

    gtfs_zip = builder.write_zip(tmp_path / "test_gtfs.zip")

    agency = _read_table(gtfs_zip, "agency.txt")
    trips = _read_table(gtfs_zip, "trips.txt")
    stop_times = _read_table(gtfs_zip, "stop_times.txt")

    assert agency[0]["agency_timezone"] == "Europe/Paris"
    assert [trip["trip_id"] for trip in trips] == [
        "trip_0_0_0",
        "trip_0_0_1",
        "trip_0_0_2",
        "trip_0_1_0",
        "trip_0_1_1",
        "trip_0_1_2",
    ]
    assert len(stop_times) == 12
    assert stop_times[0] == {
        "trip_id": "trip_0_0_0",
        "stop_id": "A",
        "arrival_time": "06:00:00",
        "departure_time": "06:00:00",
        "stop_sequence": "1",
    }
    assert stop_times[6] == {
        "trip_id": "trip_0_1_0",
        "stop_id": "B",
        "arrival_time": "06:00:00",
        "departure_time": "06:00:00",
        "stop_sequence": "1",
    }


def test_002_gtfs_builder_represents_asymmetric_directions(tmp_path):
    builder = _base_builder()
    builder.add_stops({"A": [2.0, 48.0], "B": [2.1, 48.1]})
    builder.add_line(
        [("A", "B", 60.0)],
        start_time=0.0,
        end_time=0.0,
        period=600.0,
        bidirectional=False,
    )
    builder.add_line(
        [("B", "A", 60.0)],
        start_time=100.0,
        end_time=100.0,
        period=600.0,
        bidirectional=False,
    )

    gtfs_zip = builder.write_zip(tmp_path / "test_gtfs.zip")

    stop_times = _read_table(gtfs_zip, "stop_times.txt")

    assert [row["trip_id"] for row in stop_times] == [
        "trip_0_0_0",
        "trip_0_0_0",
        "trip_1_0_0",
        "trip_1_0_0",
    ]
    assert [row["stop_id"] for row in stop_times] == ["A", "B", "B", "A"]
    assert stop_times[2]["departure_time"] == "00:01:40"


def test_002_gtfs_builder_keeps_repeated_stops_and_after_midnight_times(tmp_path):
    builder = _base_builder()
    builder.add_stops({"A": [2.0, 48.0], "B": [2.1, 48.1]})
    builder.add_line_from_stop_ids(
        ["A", "B", "A"],
        {
            ("A", "B"): 300.0,
            ("B", "A"): 300.0,
        },
        start_time=25.0 * 3600.0,
        end_time=25.0 * 3600.0,
        period=600.0,
    )

    gtfs_zip = builder.write_zip(tmp_path / "test_gtfs.zip")

    stop_times = _read_table(gtfs_zip, "stop_times.txt")

    assert [row["stop_id"] for row in stop_times] == ["A", "B", "A"]
    assert [row["stop_sequence"] for row in stop_times] == ["1", "2", "3"]
    assert [row["departure_time"] for row in stop_times] == [
        "25:00:00",
        "25:05:00",
        "25:10:00",
    ]


def test_002_gtfs_builder_writes_project_zip(tmp_path, monkeypatch):
    monkeypatch.setenv("MOBILITY_PROJECT_DATA_FOLDER", str(tmp_path))
    builder = _base_builder()
    builder.add_stops({"A": [2.0, 48.0], "B": [2.1, 48.1]})
    builder.add_line(
        [("A", "B", 60.0)],
        start_time=0.0,
        end_time=0.0,
        period=600.0,
    )

    gtfs_zip = builder.write_project_zip("project_gtfs.zip")

    assert gtfs_zip == tmp_path / "project_gtfs.zip"
    assert gtfs_zip.exists()


def test_002_build_project_gtfs_zip_writes_under_project_data_folder(tmp_path, monkeypatch):
    monkeypatch.setenv("MOBILITY_PROJECT_DATA_FOLDER", str(tmp_path))
    feed = _base_builder()
    feed.add_stops({"A": [2.0, 48.0], "B": [2.1, 48.1]})
    feed.add_line(
        [("A", "B", 60.0)],
        start_time=0.0,
        end_time=0.0,
        period=600.0,
    )

    gtfs_zip = mobility.build_project_gtfs_zip(feed.build_feed(), "direct_project_gtfs.zip")

    assert gtfs_zip == tmp_path / "direct_project_gtfs.zip"
    assert gtfs_zip.exists()


def test_002_build_project_gtfs_zip_reports_missing_project_data_folder(monkeypatch):
    monkeypatch.delenv("MOBILITY_PROJECT_DATA_FOLDER", raising=False)
    feed = _base_builder()
    feed.add_stops({"A": [2.0, 48.0], "B": [2.1, 48.1]})
    feed.add_line(
        [("A", "B", 60.0)],
        start_time=0.0,
        end_time=0.0,
        period=600.0,
    )

    with pytest.raises(ValueError, match="mobility.set_params"):
        mobility.build_project_gtfs_zip(feed.build_feed(), "missing_folder.zip")


def test_002_gtfs_feed_spec_can_still_be_written_directly(tmp_path):
    feed = mobility.GTFSFeedSpec(
        agency_id="test_agency",
        agency_name="Test Agency",
        route_id="test_route",
        route_short_name="T1",
        route_type="train",
        service_id="test_service",
        stops={
            "A": mobility.GTFSStopSpec(lon=2.0, lat=48.0, name="First stop"),
            "B": mobility.GTFSStopSpec(lon=2.1, lat=48.1, name="Second stop"),
        },
        lines=[
            mobility.GTFSLineSpec(
                stop_ids=["A", "B"],
                segment_travel_times=[60.0],
                start_time=0.0,
                end_time=0.0,
                period=600.0,
                bidirectional=False,
            )
        ],
    )

    gtfs_zip = mobility.build_gtfs_zip(feed, tmp_path / "test_gtfs.zip")

    routes = _read_table(gtfs_zip, "routes.txt")
    stops = _read_table(gtfs_zip, "stops.txt")

    assert routes[0]["route_type"] == "2"
    assert [stop["stop_name"] for stop in stops] == ["First stop", "Second stop"]


def test_002_gtfs_builder_rejects_unknown_route_type(tmp_path):
    builder = mobility.GTFSBuilder(
        agency_id="test_agency",
        agency_name="Test Agency",
        route_id="test_route",
        route_short_name="T1",
        route_type="submarine",
        service_id="test_service",
    )
    builder.add_stops({"A": [2.0, 48.0], "B": [2.1, 48.1]})
    builder.add_line(
        [("A", "B", 60.0)],
        start_time=0.0,
        end_time=0.0,
        period=600.0,
    )

    with pytest.raises(ValueError, match="route_type"):
        builder.write_zip(tmp_path / "test_gtfs.zip")


def test_002_gtfs_builder_rejects_unknown_stop_id(tmp_path):
    builder = _base_builder()
    builder.add_stops({"A": [2.0, 48.0]})
    builder.add_line(
        [("A", "B", 60.0)],
        start_time=0.0,
        end_time=0.0,
        period=600.0,
    )

    with pytest.raises(ValueError, match="unknown stop ids: B"):
        builder.write_zip(tmp_path / "test_gtfs.zip")


def test_002_gtfs_line_from_stop_ids_reports_missing_segment_time():
    with pytest.raises(ValueError, match="'A' -> 'B'"):
        mobility.GTFSLineSpec.from_stop_ids(
            ["A", "B"],
            {},
            start_time=0.0,
            end_time=0.0,
            period=600.0,
        )


def test_002_gtfs_line_from_segments_reports_malformed_first_segment():
    with pytest.raises(ValueError, match="from_stop_id, to_stop_id and travel_time"):
        mobility.GTFSLineSpec.from_segments(
            [[]],
            start_time=0.0,
            end_time=0.0,
            period=600.0,
        )
