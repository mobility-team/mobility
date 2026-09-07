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
    assert graph.inputs["version"] == "3"
    assert graph.gtfs_router.inputs["version"] == "5"
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
