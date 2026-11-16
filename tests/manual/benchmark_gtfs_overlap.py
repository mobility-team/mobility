"""Benchmark timetable preparation and overlap detection with local GTFS ZIPs.

Example: pixi run python tests/manual/benchmark_gtfs_overlap.py --date 2026-10-06
    --lon 4.03 --lat 49.25 --radius 40 feed1.zip feed2.zip
"""

import argparse
import logging
import time
from importlib import resources

import geopandas as gpd
import pandas as pd
from shapely.geometry import Point

import mobility.transport.modes.public_transport.gtfs.gtfs_timetable as timetable_module
from mobility.transport.modes.public_transport.gtfs.gtfs_timetable import GTFSTimetable


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("paths", nargs="+")
parser.add_argument("--date", required=True)
parser.add_argument("--lat", type=float, required=True)
parser.add_argument("--lon", type=float, required=True)
parser.add_argument("--radius", type=float, default=40)
parser.add_argument("--use-input-priority", action="store_true", help="Prefer the first feed, then the following feeds")
parser.add_argument("--source-id", action="append", help="Source ID for each input path, in input order")
parser.add_argument("--audit-output", help="Write a CSV of candidate pairs with route and terminal details")
args = parser.parse_args()

point = gpd.GeoSeries([Point(args.lon, args.lat)], crs=4326).to_crs(3035)
zones = gpd.GeoDataFrame(geometry=point.buffer(args.radius * 1000), crs=3035).to_crs(4326)
route_types = resources.files("mobility.runtime.resources").joinpath("gtfs/gtfs_route_types.csv")
started = time.perf_counter()
logging.getLogger().setLevel(logging.ERROR)
overlap_elapsed = []
headway_elapsed = []
find_overlaps = timetable_module.find_trip_overlaps
find_headways = timetable_module.find_headway_overlaps


def timed_overlap(*args, **kwargs):
    start = time.perf_counter()
    result = find_overlaps(*args, **kwargs)
    overlap_elapsed.append(time.perf_counter() - start)
    return result


timetable_module.find_trip_overlaps = timed_overlap


def timed_headway(*args, **kwargs):
    start = time.perf_counter()
    result = find_headways(*args, **kwargs)
    headway_elapsed.append(time.perf_counter() - start)
    return result


timetable_module.find_headway_overlaps = timed_headway
if args.audit_output:
    captured = []
    captured_within = []
    resolve = GTFSTimetable.resolve_trip_overlaps
    find_within = timetable_module.find_within_feed_duplicates

    def capture_within(trips, stop_times):
        result = find_within(trips, stop_times)
        if result.height:
            details = trips[["trip_id", "route_id"]].copy()
            visits = stop_times.sort_values(["trip_id", "stop_sequence"])
            terminals = visits.groupby("trip_id", sort=False).agg(
                first_stop=("stop_id", "first"), last_stop=("stop_id", "last"),
                first_departure=("departure_time", "first"), stop_count=("stop_id", "size"),
            ).reset_index()
            details = details.merge(terminals, on="trip_id", how="left")
            report = result.to_pandas().merge(details, left_on="kept_trip_id", right_on="trip_id", how="left")
            captured_within.append(report.drop(columns="trip_id"))
        return result

    timetable_module.find_within_feed_duplicates = capture_within

    def capture_matches(timetable, pairs, identities, sources, date, item_kind="trips"):
        removed = resolve(timetable, pairs, identities, sources, date, item_kind)
        if pairs.empty:
            return removed
        details = identities.reindex(columns=["trip_id", "agency_name", "route_short_name", "route_long_name", "route_type"])
        visits = timetable.tables["stop_times"].sort_values(["trip_id", "stop_sequence"])
        stops = timetable.tables["stops"][["stop_id", "stop_name"]]
        visits = visits.merge(stops, on="stop_id", how="left")
        terminals = visits.groupby("trip_id", sort=False).agg(
            first_stop=("stop_name", "first"), last_stop=("stop_name", "last"),
            first_departure=("departure_time", "first"), last_arrival=("arrival_time", "last"),
        ).reset_index()
        details = details.merge(terminals, on="trip_id", how="left")
        report = pairs.copy()
        for side in ("left", "right"):
            renamed = details.rename(columns={column: f"{side}_{column}" for column in details.columns})
            report = report.merge(renamed, on=f"{side}_trip_id", how="left")
        report["match_type"] = item_kind
        captured.append(report)
        return removed

    GTFSTimetable.resolve_trip_overlaps = capture_matches
if args.source_id and len(args.source_id) != len(args.paths):
    parser.error("Provide one --source-id per input path")
source_ids = dict(zip(args.paths, args.source_id)) if args.source_id else {}
priority = (args.source_id or args.paths) if args.use_input_priority else []
tables, metadata = GTFSTimetable(args.paths, zones, route_types, args.date, args.date,
                                 source_ids=source_ids, feed_priority=priority).prepare()
print(f"Elapsed seconds: {time.perf_counter() - started:.2f}")
print(f"Overlap detection seconds: {sum(overlap_elapsed):.2f}")
print(f"Headway detection seconds: {sum(headway_elapsed):.2f}")
print(f"Selected trips: {len(tables['trips'])}")
print(f"Candidate pairs: {metadata['spatiotemporal_overlap_pairs']}")
print(f"Removed within-feed trips: {metadata['within_feed_deduplicated_trips']}")
print(f"Removed cross-feed trips: {metadata['deduplicated_trips']}")
print(f"Generated headway trips: {metadata['headway_generated_trips']}")
print(f"Headway interval pairs: {metadata['headway_interval_pairs']}")
print(f"Deduplicated headway intervals: {metadata['deduplicated_headway_intervals']}")
if args.audit_output and captured:
    pd.concat(captured, ignore_index=True).to_csv(args.audit_output, index=False)
if args.audit_output and captured_within:
    path = args.audit_output.rsplit(".", 1)[0] + "_within.csv"
    pd.concat(captured_within, ignore_index=True).to_csv(path, index=False)
