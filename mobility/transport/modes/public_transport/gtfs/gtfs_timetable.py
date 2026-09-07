"""Merge local GTFS feeds into a dated timetable for the router asset."""

import logging
import tempfile
import zipfile
from pathlib import Path
from typing import Any

import geopandas as gpd
import numpy as np
import pandas as pd
from scipy.spatial import cKDTree

from .gtfs_feed import GTFSFeed


class GTFSTimetable:
    """Prepare local tables, select a Tuesday and resolve walking connections.

    This class owns the in-memory preparation. GTFSRouter owns source selection
    and persistent cache files; GTFSFeed owns reading and validating one archive.
    """

    def __init__(
        self,
        paths: list[str | Path],
        transport_zones: gpd.GeoDataFrame,
        route_types_path: str | Path,
    ) -> None:
        self.paths = paths
        self.transport_zones = transport_zones
        self.route_types_path = route_types_path
        self.tables: dict[str, pd.DataFrame] = {}
        self.sources: list[dict[str, Any]] = []

    def prepare(self) -> tuple[dict[str, pd.DataFrame], dict[str, Any]]:
        """Return the selected tables and metadata describing their coverage."""
        self.tables, self.sources = self.read_feeds()
        tables, sources = self.tables, self.sources
        date, services, scores = self.select_tuesday()
        trips = tables["trips"]
        tables["trips"] = trips.loc[trips.service_id.isin(services)].copy()

        # Prune related tables in dependency order, keeping only selected trips.
        for name, key, parent in (
            ("stop_times", "trip_id", "trips"),
            ("routes", "route_id", "trips"),
            ("agency", "agency_id", "routes"),
        ):
            table = tables[name]
            tables[name] = table.loc[table[key].isin(tables[parent][key].unique())].copy()

        # Keep parent station records while resolving station-level transfer rules.
        tables["transfers"] = self.prepare_transfers()
        stops = tables["stops"]
        tables["stops"] = stops.loc[
            stops.stop_id.isin(tables["stop_times"].stop_id.unique())
        ].copy()

        route_types = pd.read_csv(self.route_types_path, sep=";")
        tables["routes"] = tables["routes"].merge(
            route_types[["route_type", "route_type_label", "vehicle_capacity"]],
            on="route_type",
            how="left",
        )
        tables["routes"] = tables["routes"].fillna(
            {"route_type_label": "bus", "vehicle_capacity": 50}
        )

        for source in sources:
            if "feed_id" in source:
                prefix = source["feed_id"] + "-"
                source["selected_trips"] = int(tables["trips"].trip_id.str.startswith(prefix).sum())
                if not source["selected_trips"]:
                    logging.warning(
                        "GTFS %s has no service on selected Tuesday %s",
                        source["path"],
                        date.date(),
                    )
        metadata = {
            "selected_date": date.strftime("%Y-%m-%d"),
            "selection": "maximum stop visits; earliest tie",
            "date_alignment": False,
            "sources": sources,
            "tuesdays": scores,
            "frequency_assumption": "regular departures also approximate exact_times=0",
            "interpolation": "linear by stop_sequence between timed endpoints",
        }
        return {
            key: tables[key]
            for key in ("agency", "routes", "stops", "trips", "stop_times", "transfers")
        }, metadata

    def read_feeds(self) -> tuple[dict[str, pd.DataFrame], list[dict[str, Any]]]:
        """Read each distinct timetable once, retaining its original dates."""
        boundary = self.transport_zones.to_crs(3035).geometry.union_all().buffer(10_000)
        boundary = gpd.GeoSeries([boundary], crs=3035).to_crs(4326).iloc[0]
        feeds, sources, seen = [], [], set()

        for path in self.paths:
            logging.info("Preparing GTFS %s", path)
            try:
                # Hash while extracting. Temporary files allow Polars to scan
                # local trips without decompressing the ZIP on each pass.
                with tempfile.TemporaryDirectory(prefix="mobility-gtfs-") as directory:
                    reader = GTFSFeed(path, Path(directory))
                    fingerprint = reader.extract()
                    source = {"path": str(path), "sha256": fingerprint}
                    sources.append(source)
                    if fingerprint in seen:
                        source["status"] = "duplicate"
                        continue
                    seen.add(fingerprint)

                    feed = reader.read(boundary)
                    if feed is None:
                        source["status"] = "no usable trips in area"
                        continue
                    source.update(reader.diagnostics)

                source["status"] = "prepared"
                source["feed_id"] = str(len(feeds) + 1)
                prefix = source["feed_id"] + "-"
                for table in feed.values():
                    for column in table.columns:
                        if column in (
                            "agency_id",
                            "route_id",
                            "stop_id",
                            "trip_id",
                            "service_id",
                            "parent_station",
                            "from_stop_id",
                            "to_stop_id",
                            "from_route_id",
                            "to_route_id",
                            "from_trip_id",
                            "to_trip_id",
                        ):
                            table[column] = table[column].where(
                                table[column].eq(""), prefix + table[column]
                            )
                feeds.append(feed)
            except (ValueError, KeyError, zipfile.BadZipFile) as error:
                raise ValueError(f"Cannot prepare GTFS {path}: {error}") from error

        if not feeds:
            raise ValueError("No GTFS trips with two visits in the study area")

        tables = {
            name: pd.concat([feed[name] for feed in feeds], ignore_index=True).fillna("")
            for name in feeds[0]
        }
        return tables, sources

    def select_tuesday(self) -> tuple[pd.Timestamp, set[str], list[dict[str, Any]]]:
        """Choose the earliest Tuesday with the most stop visits on its real date."""
        tables = self.tables
        calendar, exceptions = (
            tables["calendar"].copy(),
            tables["calendar_dates"].copy(),
        )
        candidates = set()
        if not calendar.empty:
            for col in ("start_date", "end_date"):
                calendar[col] = pd.to_datetime(calendar[col], format="%Y%m%d", errors="raise")
            calendar["tuesday"] = pd.to_numeric(calendar.tuesday, errors="raise")
            if not calendar.tuesday.isin([0, 1]).all():
                raise ValueError("Invalid calendar tuesday flag")
            if (calendar.end_date < calendar.start_date).any():
                raise ValueError("Calendar end date precedes start date")
            calendar = calendar.loc[calendar.tuesday.eq(1)]
            if not calendar.empty:
                candidates.update(
                    pd.date_range(calendar.start_date.min(), calendar.end_date.max(), freq="W-TUE")
                )
        if not exceptions.empty:
            exceptions["date"] = pd.to_datetime(exceptions.date, format="%Y%m%d", errors="raise")
            exceptions["exception_type"] = pd.to_numeric(exceptions.exception_type, errors="raise")
            if exceptions.duplicated(["service_id", "date"]).any():
                raise ValueError("Duplicate calendar exception for the same service and date")
            if not exceptions.exception_type.isin([1, 2]).all():
                raise ValueError("Invalid calendar exception_type")
            candidates.update(exceptions.loc[exceptions.date.dt.dayofweek.eq(1), "date"])
        supply = tables["stop_times"].merge(
            tables["trips"][["trip_id", "service_id"]], on="trip_id"
        )
        weights = supply.groupby("service_id").size()
        best, best_services, best_count = None, set(), 0
        scores = []
        for date in sorted(candidates):
            active = set()
            if not calendar.empty:
                active.update(
                    calendar.loc[
                        calendar.start_date.le(date) & calendar.end_date.ge(date),
                        "service_id",
                    ]
                )
            if not exceptions.empty:
                day = exceptions.loc[exceptions.date.eq(date)]
                active.update(day.loc[day.exception_type.eq(1), "service_id"])
                active.difference_update(day.loc[day.exception_type.eq(2), "service_id"])
            count = int(weights.reindex(list(active), fill_value=0).sum())
            scores.append({"date": date.strftime("%Y-%m-%d"), "stop_visits": count})
            if count > best_count:
                best, best_services, best_count = date, active, count
        if best is None:
            raise ValueError("No active Tuesday service in the study area")
        return best, best_services, scores

    def prepare_transfers(self) -> pd.DataFrame:
        """Keep explicit prohibitions and route rules alongside walking links."""
        tables = self.tables
        stops = tables["stops"]
        active = stops.loc[stops.stop_id.isin(tables["stop_times"].stop_id.unique())].copy()
        points = gpd.GeoSeries(gpd.points_from_xy(active.stop_lon, active.stop_lat), crs=4326)
        points = points.to_crs(points.estimate_utm_crs())
        coordinates = np.column_stack([points.x, points.y])
        pairs = cKDTree(coordinates).query_pairs(200, output_type="ndarray")
        pairs = np.concatenate([pairs, pairs[:, ::-1]], axis=0)
        distance = np.linalg.norm(coordinates[pairs[:, 0]] - coordinates[pairs[:, 1]], axis=1)
        ids = active.stop_id.to_numpy()
        positions = dict(zip(ids, coordinates))
        transfers = pd.DataFrame(
            {
                "from_stop_id": ids[pairs[:, 0]],
                "to_stop_id": ids[pairs[:, 1]],
                "min_transfer_time": 31 + 1.125 * distance,
                "transfer_type": 2,
                "from_route_id": "",
                "to_route_id": "",
                "specificity": -1,
            }
        )

        # Explicit rules override generated walking links, including prohibitions.
        # A station rule applies to its platforms rather than to an unused station node.
        children = {stop: [stop] for stop in active.stop_id}
        if "parent_station" in active:
            platforms = active.loc[active.parent_station.ne("")]
            for parent, stop_ids in platforms.groupby("parent_station").stop_id:
                children[parent] = stop_ids.tolist()

        # Discard rules outside the selected timetable before creating Python
        # records. National feeds can contain many unrelated transfer rules.
        rules = (
            tables["transfers"]
            .reindex(
                columns=[
                    "from_stop_id",
                    "to_stop_id",
                    "transfer_type",
                    "min_transfer_time",
                    "from_route_id",
                    "to_route_id",
                    "from_trip_id",
                    "to_trip_id",
                ]
            )
            .fillna("")
        )
        rules = rules.loc[rules.from_stop_id.isin(children) & rules.to_stop_id.isin(children)]
        explicit = []
        for row in rules.to_dict("records"):
            origins = children[row["from_stop_id"]]
            destinations = children[row["to_stop_id"]]
            if row.get("from_trip_id", "") or row.get("to_trip_id", ""):
                raise ValueError(
                    "Trip-specific transfer rules are not yet supported by the public transport graph"
                )
            kind = int(row.get("transfer_type", "") or 0)
            if kind not in (0, 1, 2, 3):
                raise ValueError(
                    f"Transfer type {kind} is not supported by the public transport graph"
                )
            minimum = row.get("min_transfer_time", "")
            if kind == 2 and minimum == "":
                raise ValueError("Transfer type 2 requires min_transfer_time")
            if minimum != "" and float(minimum) < 0:
                raise ValueError("Negative min_transfer_time")
            from_route, to_route = row.get("from_route_id", ""), row.get("to_route_id", "")
            for origin in origins:
                for destination in destinations:
                    # Recommended connections with no stated minimum still need walking time.
                    left, right = positions[origin], positions[destination]
                    seconds = (
                        float(minimum)
                        if minimum != ""
                        else 31 + 1.125 * np.linalg.norm(left - right)
                    )
                    explicit.append(
                        {
                            "from_stop_id": origin,
                            "to_stop_id": destination,
                            "min_transfer_time": seconds,
                            "transfer_type": kind,
                            "from_route_id": from_route,
                            "to_route_id": to_route,
                            "specificity": int(bool(from_route)) + int(bool(to_route)),
                        }
                    )
        return pd.concat([transfers, pd.DataFrame(explicit)], ignore_index=True).drop_duplicates()
