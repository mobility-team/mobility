from pathlib import Path

import pytest

from mobility.transport.modes.public_transport.gtfs.gtfs_edit import (
    GTFSFeed,
    NewStop,
    apply_gtfs_edits,
    insert_stop_between,
)
from mobility.transport.modes.public_transport.gtfs_builder import (
    GTFSFeedSpec,
    GTFSLineSpec,
    GTFSStopSpec,
    build_gtfs_zip,
)


def _build_simple_gtfs_zip(
    tmp_path: Path,
    *,
    bidirectional: bool,
    start_time: float = 0.0,
    end_time: float = 0.0,
) -> Path:
    feed = GTFSFeedSpec(
        agency_id="agency_1",
        agency_name="Test agency",
        route_id="route_1",
        route_short_name="T1",
        route_type="bus",
        service_id="service_1",
        stops={
            "A": GTFSStopSpec(lon=6.0, lat=46.0, name="Stop A"),
            "B": GTFSStopSpec(lon=6.01, lat=46.01, name="Stop B"),
            "C": GTFSStopSpec(lon=6.02, lat=46.02, name="Stop C"),
        },
        lines=[
            GTFSLineSpec(
                stop_ids=["A", "B", "C"],
                segment_travel_times=[600.0, 600.0],
                start_time=start_time,
                end_time=end_time,
                period=1.0,
                bidirectional=bidirectional,
            )
        ],
    )

    output_path = tmp_path / "simple_feed.zip"
    return build_gtfs_zip(feed, output_path)


def test_gtfs_feed_roundtrip_and_insert_stop_between(tmp_path):
    gtfs_path = _build_simple_gtfs_zip(tmp_path, bidirectional=False)

    feed = GTFSFeed(gtfs_path).load()
    roundtrip_path = tmp_path / "roundtrip.zip"
    feed.save(roundtrip_path)

    roundtrip = GTFSFeed(roundtrip_path).load()
    assert set(roundtrip.tables) == {
        "agency.txt",
        "routes.txt",
        "trips.txt",
        "calendar.txt",
        "stops.txt",
        "stop_times.txt",
    }

    insert_stop_between(
        feed=feed,
        from_stop_id="A",
        to_stop_id="B",
        new_stop=NewStop(
            stop_id="X",
            stop_name="Inserted stop",
            stop_lat=46.005,
            stop_lon=6.005,
        ),
        dwell_time_s=45,
        extra_run_time_s=30,
        split_ratio=0.5,
        propagate="after",
    )

    stops = feed.tables["stops.txt"]
    stop_times = feed.tables["stop_times.txt"].sort_values(["trip_id", "stop_sequence"]).reset_index(drop=True)

    assert "X" in stops["stop_id"].tolist()
    assert stop_times["stop_id"].tolist() == ["A", "X", "B", "C"]
    assert stop_times["stop_sequence"].tolist() == [1, 2, 3, 4]
    assert stop_times.loc[1, "arrival_time"] == "00:05:00"
    assert stop_times.loc[1, "departure_time"] == "00:05:45"
    assert stop_times.loc[2, "arrival_time"] == "00:11:15"
    assert stop_times.loc[3, "arrival_time"] == "00:21:15"


def test_apply_gtfs_edits_creates_and_reuses_edited_feed(tmp_path):
    gtfs_path = _build_simple_gtfs_zip(tmp_path, bidirectional=True)
    edits_folder = tmp_path / "edits"

    edits = [
        {
            "path": str(gtfs_path),
            "mode": "explicit",
            "ops": [
                {
                    "op": "insert_stop_between",
                    "from_stop_id": "A",
                    "to_stop_id": "B",
                    "new_stop": {
                        "stop_id": "X",
                        "stop_name": "Inserted stop",
                        "stop_lat": 46.005,
                        "stop_lon": 6.005,
                    },
                    "dwell_time_s": 45,
                    "extra_run_time_s": 30,
                    "split_ratio": 0.5,
                    "propagate": "after",
                    "bidirectional": True,
                }
            ],
        }
    ]

    edited_files = apply_gtfs_edits([str(gtfs_path)], edits, edits_folder)
    assert len(edited_files) == 1
    edited_path = Path(edited_files[0])
    assert edited_path.exists()

    edited_feed = GTFSFeed(edited_path).load()
    stop_times = edited_feed.tables["stop_times.txt"]

    assert stop_times.shape[0] == 8
    assert stop_times["stop_id"].tolist().count("X") == 2

    reused_files = apply_gtfs_edits([str(gtfs_path)], edits, edits_folder)
    assert reused_files == edited_files


def test_apply_gtfs_edits_mode_all_uses_chain_matches(tmp_path):
    gtfs_path = _build_simple_gtfs_zip(tmp_path, bidirectional=True)
    edits_folder = tmp_path / "all_edits"

    edits = [
        {
            "mode": "all",
            "ops": [
                {
                    "op": "insert_stop_between",
                    "from_stop_id": "A",
                    "to_stop_id": "B",
                    "new_stop": {
                        "stop_id": "X",
                        "stop_name": "Inserted stop",
                        "stop_lat": 46.005,
                        "stop_lon": 6.005,
                    },
                    "dwell_time_s": 45,
                    "extra_run_time_s": 30,
                    "split_ratio": 0.5,
                    "propagate": "after",
                    "bidirectional": True,
                }
            ],
        }
    ]

    edited_files = apply_gtfs_edits([str(gtfs_path)], edits, edits_folder)
    assert len(edited_files) == 1

    edited_feed = GTFSFeed(edited_files[0]).load()
    stop_times = edited_feed.tables["stop_times.txt"]

    assert stop_times.shape[0] == 8
    assert stop_times["stop_id"].tolist().count("X") == 2


@pytest.mark.parametrize(
    ("propagate", "expected_a_time", "expected_b_time", "expected_c_time"),
    [
        ("before", "00:58:45", "01:10:00", "01:20:00"),
        ("symmetric", "00:59:22", "01:10:37", "01:20:37"),
    ],
)
def test_insert_stop_between_supports_before_and_symmetric(
    tmp_path,
    propagate,
    expected_a_time,
    expected_b_time,
    expected_c_time,
):
    gtfs_path = _build_simple_gtfs_zip(
        tmp_path,
        bidirectional=False,
        start_time=3600.0,
        end_time=3600.0,
    )

    feed = GTFSFeed(gtfs_path).load()
    insert_stop_between(
        feed=feed,
        from_stop_id="A",
        to_stop_id="B",
        new_stop=NewStop(
            stop_id="X",
            stop_name="Inserted stop",
            stop_lat=46.005,
            stop_lon=6.005,
        ),
        dwell_time_s=45,
        extra_run_time_s=30,
        split_ratio=0.5,
        propagate=propagate,
    )

    stop_times = feed.tables["stop_times.txt"].sort_values(["trip_id", "stop_sequence"]).reset_index(drop=True)

    assert stop_times["stop_id"].tolist() == ["A", "X", "B", "C"]
    assert stop_times.loc[0, "arrival_time"] == expected_a_time
    assert stop_times.loc[0, "departure_time"] == expected_a_time
    assert stop_times.loc[1, "arrival_time"] == "01:05:00"
    assert stop_times.loc[1, "departure_time"] == "01:05:45"
    assert stop_times.loc[2, "arrival_time"] == expected_b_time
    assert stop_times.loc[2, "departure_time"] == expected_b_time
    assert stop_times.loc[3, "arrival_time"] == expected_c_time
    assert stop_times.loc[3, "departure_time"] == expected_c_time
