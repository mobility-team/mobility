"""Small GTFS feeds that exercise calendar, timetable and transfer behaviour."""

import json
import zipfile
import shutil
import subprocess
from importlib import resources

import geopandas as gpd
import pandas as pd
import pytest
from shapely.geometry import box

from mobility.transport.modes.public_transport.gtfs.gtfs_timetable import GTFSTimetable
from mobility.transport.modes.public_transport.gtfs.gtfs_router import GTFSRouter
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


@pytest.fixture
def zones():
    return gpd.GeoDataFrame(geometry=[box(-1.69, 48.10, -1.67, 48.12)], crs=4326)


@pytest.fixture
def route_types():
    return resources.files("mobility.runtime.resources").joinpath("gtfs/gtfs_route_types.csv")


def test_dates_only_and_long_route_name(tmp_path, feed_files, zones, route_types):
    del feed_files["calendar"]
    feed_files["calendar_dates"] = "service_id,date,exception_type\ns,20260908,1\n"
    tables, metadata = GTFSTimetable(
        [write_feed(tmp_path / "feed.zip", feed_files)], zones, route_types
    ).prepare()
    assert metadata["selected_date"] == "2026-09-08"
    assert tables["routes"].route_short_name.tolist() == ["Test route"]
    assert len(tables["stop_times"]) == 2


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


def test_asset_writes_manifest_and_invalidates_changed_manual_feed(
    tmp_path, feed_files, zones, monkeypatch
):
    monkeypatch.setenv("MOBILITY_PROJECT_DATA_FOLDER", str(tmp_path))
    path = write_feed(tmp_path / "feed.zip", feed_files)
    zone_asset = InMemoryAsset({"zones": "fixture"})
    monkeypatch.setattr(zone_asset, "get", lambda: zones)
    asset = GTFSRouter(zone_asset, None, additional_gtfs_files=[path])
    asset.prepare_gtfs_router(zone_asset, [path])
    manifest = json.loads(asset.get_cached_asset().read_text())
    assert manifest["selected_date"] == "2026-09-08"
    assert not asset.assets_missing()
    assert len(pd.read_parquet(asset.cache_path["stop_times"])) == 2
    asset.cache_path["trips"].unlink()
    assert asset.assets_missing()
    feed_files["calendar"] = feed_files["calendar"].replace(
        "20260908,20260915", "20261006,20261006"
    )
    write_feed(path, feed_files)
    changed = GTFSRouter(zone_asset, None, additional_gtfs_files=[path])
    assert changed.inputs_hash != asset.inputs_hash


@pytest.mark.parametrize("case", ["restricted", "forbidden", "route_override"])
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
    if case != "restricted":
        # Both stops have arrival and departure nodes, so the prohibition must
        # actually remove a possible transfer rather than an unused endpoint.
        feed_files["stop_times"] = (
            "trip_id,arrival_time,departure_time,stop_id,stop_sequence\nt,08:00:00,08:00:00,a,1\nt,08:10:00,08:10:00,b,2\nt,08:20:00,08:20:00,a,3\n"
        )
    if case == "route_override":
        feed_files["transfers"] = (
            "from_stop_id,to_stop_id,transfer_type,min_transfer_time,from_route_id\na,b,3,,\na,b,2,600,r\n"
        )
    tables, metadata = GTFSTimetable(
        [write_feed(tmp_path / "feed.zip", feed_files)], zones, route_types
    ).prepare()
    for name, table in tables.items():
        table.to_parquet(tmp_path / f"{name}.parquet", index=False)
    metadata["tables"] = {name: f"{name}.parquet" for name in tables}
    manifest = tmp_path / "router.json"
    manifest.write_text(json.dumps(metadata))
    script_path = resources.files("mobility.transport.modes.public_transport").joinpath(
        "prepare_public_transport_graph.R"
    )
    script = script_path.read_text()
    if case == "restricted":
        script += """
stopifnot(nrow(travel_times) == 1)
origin <- stops_routes[stop_index == travel_times$from, gtfs_stop_id]
destination <- stops_routes[stop_index == travel_times$to, gtfs_stop_id]
stopifnot(origin == "1-a", destination == "1-b", travel_times$time == 600)
stopifnot(all(stops_routes[stop_index %in% access_times$to, gtfs_stop_id] == "1-a"))
stopifnot(all(stops_routes[stop_index %in% exit_times$from, gtfs_stop_id] == "1-b"))
stopifnot(nrow(transfers) == 1)
"""
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
            else "stopifnot(nrow(connection) == 1, connection$transfer_time == 600)\n"
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
            str(manifest),
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


def test_trip_specific_transfer_fails_explicitly(tmp_path, feed_files, zones, route_types):
    feed_files["transfers"] = (
        "from_stop_id,to_stop_id,transfer_type,min_transfer_time,from_trip_id\na,b,2,60,t\n"
    )
    with pytest.raises(ValueError, match="Trip-specific transfer"):
        GTFSTimetable([write_feed(tmp_path / "feed.zip", feed_files)], zones, route_types).prepare()


def test_frequency_periods_keep_unique_ids_and_scheduled_trips(
    tmp_path, feed_files, zones, route_types
):
    """Multiple periods share one ID sequence and leave scheduled trips intact."""
    feed_files["trips"] += "r,s,scheduled\n"
    feed_files["stop_times"] += "scheduled,08:00:00,08:00:00,a,1\nscheduled,08:10:00,08:10:00,b,2\n"
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
        "1-scheduled": 28800,
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
