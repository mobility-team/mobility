"""Combine GTFS feeds into one dated timetable for the study area."""

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
    """Combine local vehicle trips, select a Tuesday and prepare transfers.

    GTFSFeed reads each source timetable. This class combines their service
    calendars and stop visits; GTFSRouter selects sources and saves the result.
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

    def prepare(self) -> tuple[dict[str, pd.DataFrame], dict[str, Any]]:
        """Return the selected timetable and a summary of dates and source coverage."""
        self.tables, sources = self.read_feeds()
        tables = self.tables
        date, services, tuesday_counts = self.select_tuesday()
        trips = tables["trips"]
        tables["trips"] = trips.loc[trips.service_id.isin(services)].copy()

        # Keep stop visits, routes and agencies used by the selected vehicle trips.
        for name, key, parent in (
            ("stop_times", "trip_id", "trips"),
            ("routes", "route_id", "trips"),
            ("agency", "agency_id", "routes"),
        ):
            table = tables[name]
            tables[name] = table.loc[table[key].isin(tables[parent][key].unique())].copy()

        # Keep station records until their transfer rules have been applied to platforms.
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
            "tuesdays": tuesday_counts,
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
            except (ValueError, zipfile.BadZipFile) as error:
                raise ValueError(f"Cannot prepare GTFS {path}: {error}") from error

        if not feeds:
            raise ValueError("No GTFS trips with two visits in the study area")

        tables = {
            name: pd.concat([feed[name] for feed in feeds], ignore_index=True).fillna("")
            for name in feeds[0]
        }
        return tables, sources

    def select_tuesday(self) -> tuple[pd.Timestamp, set[str], list[dict[str, Any]]]:
        """Choose the earliest Tuesday with the most scheduled stop visits.

        Count the whole service day, including visits with boarding restrictions.
        Apply calendar additions and cancellations before comparing dates.
        """
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
        stop_visits = tables["stop_times"].merge(
            tables["trips"][["trip_id", "service_id"]], on="trip_id"
        )
        visits_by_service = stop_visits.groupby("service_id").size()
        selected_date, selected_services, most_visits = None, set(), 0
        tuesday_counts = []
        for date in sorted(candidates):
            active_services = set()
            if not calendar.empty:
                active_services.update(
                    calendar.loc[
                        calendar.start_date.le(date) & calendar.end_date.ge(date),
                        "service_id",
                    ]
                )
            if not exceptions.empty:
                day = exceptions.loc[exceptions.date.eq(date)]
                active_services.update(day.loc[day.exception_type.eq(1), "service_id"])
                active_services.difference_update(day.loc[day.exception_type.eq(2), "service_id"])
            count = int(visits_by_service.reindex(list(active_services), fill_value=0).sum())
            tuesday_counts.append({"date": date.strftime("%Y-%m-%d"), "stop_visits": count})
            if count > most_visits:
                selected_date, selected_services, most_visits = date, active_services, count
        if selected_date is None:
            raise ValueError("No active Tuesday service in the study area")
        return selected_date, selected_services, tuesday_counts

    def prepare_transfers(self) -> pd.DataFrame:
        """Prepare walking times and declared transfer rules between retained stops."""
        tables = self.tables
        stops = tables["stops"]
        active = stops.loc[stops.stop_id.isin(tables["stop_times"].stop_id.unique())]
        points = gpd.GeoSeries(gpd.points_from_xy(active.stop_lon, active.stop_lat), crs=4326)
        points = points.to_crs(points.estimate_utm_crs())
        coordinates = np.column_stack([points.x, points.y])
        pairs = cKDTree(coordinates).query_pairs(200, output_type="ndarray")
        pairs = np.concatenate([pairs, pairs[:, ::-1]], axis=0)
        # Different lines can share one stop. Include these connections before
        # applying declared rules, so same-stop prohibitions still take priority.
        visits = tables["stop_times"][["trip_id", "stop_id"]].merge(
            tables["trips"][["trip_id", "route_id"]], on="trip_id"
        )
        lines_per_stop = visits.groupby("stop_id").route_id.nunique()
        shared_stops = np.flatnonzero(active.stop_id.isin(lines_per_stop[lines_per_stop > 1].index))
        pairs = np.concatenate([pairs, np.column_stack([shared_stops, shared_stops])], axis=0)
        distance = np.linalg.norm(coordinates[pairs[:, 0]] - coordinates[pairs[:, 1]], axis=1)
        ids = active.stop_id.to_numpy()
        positions = dict(zip(ids, coordinates))

        # Rule priority: added walks (-1), declared stop rules (0),
        # rules naming one route (1), or both connecting routes (2).
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

        # Declared transfer rules take priority over added walking connections.
        # A station rule applies to its platforms, where vehicles actually stop.
        transfer_stops = {stop: [stop] for stop in active.stop_id}
        if "parent_station" in active:
            platforms = active.loc[active.parent_station.ne("")]
            for parent, stop_ids in platforms.groupby("parent_station").stop_id:
                transfer_stops[parent] = stop_ids.tolist()

        # Discard rules outside the selected timetable before creating Python
        # records. National feeds can contain many unrelated transfer rules.
        rules = tables["transfers"].reindex(
            columns=[
                "from_stop_id",
                "to_stop_id",
                "transfer_type",
                "min_transfer_time",
                "from_route_id",
                "to_route_id",
                "from_trip_id",
                "to_trip_id",
            ],
            fill_value="",
        )
        rules = rules.loc[
            rules.from_stop_id.isin(transfer_stops) & rules.to_stop_id.isin(transfer_stops)
        ]
        # Rules for lines absent from the selected day cannot affect its connections.
        for column in ("from_route_id", "to_route_id"):
            rules = rules.loc[rules[column].eq("") | rules[column].isin(tables["routes"].route_id)]
        declared_transfers = []
        excluded_connections = set()
        for row in rules.to_dict("records"):
            origins = transfer_stops[row["from_stop_id"]]
            destinations = transfer_stops[row["to_stop_id"]]
            try:
                if row["from_trip_id"] or row["to_trip_id"]:
                    raise ValueError("a rule for individual vehicle journeys cannot be represented")
                transfer_type = int(row["transfer_type"] or 0)
                if transfer_type not in (0, 1, 2, 3):
                    raise ValueError(f"transfer_type {transfer_type} cannot be represented")
                minimum = (
                    float(row["min_transfer_time"]) if row["min_transfer_time"] != "" else None
                )
                if transfer_type == 2 and minimum is None:
                    raise ValueError("the required min_transfer_time is missing")
                if minimum is not None and (not np.isfinite(minimum) or minimum < 0):
                    raise ValueError("min_transfer_time must be a finite, nonnegative number")
            except ValueError as error:
                # Omit the connection, including added walks and other rules,
                # instead of replacing an unreadable restriction with permission.
                logging.warning(
                    "Omitting all transfers from %s to %s (including station platforms): %s",
                    row["from_stop_id"],
                    row["to_stop_id"],
                    error,
                )
                excluded_connections.update(
                    (origin, destination) for origin in origins for destination in destinations
                )
                continue
            from_route, to_route = row["from_route_id"], row["to_route_id"]
            for origin in origins:
                for destination in destinations:
                    # Recommended connections with no stated minimum still need walking time.
                    left, right = positions[origin], positions[destination]
                    seconds = (
                        minimum
                        if minimum is not None
                        else 31 + 1.125 * np.linalg.norm(left - right)
                    )
                    declared_transfers.append(
                        {
                            "from_stop_id": origin,
                            "to_stop_id": destination,
                            "min_transfer_time": seconds,
                            "transfer_type": transfer_type,
                            "from_route_id": from_route,
                            "to_route_id": to_route,
                            "specificity": int(bool(from_route)) + int(bool(to_route)),
                        }
                    )
        transfers = pd.concat(
            [transfers, pd.DataFrame(declared_transfers)], ignore_index=True
        ).drop_duplicates()
        if excluded_connections:
            stop_pairs = pd.MultiIndex.from_frame(transfers[["from_stop_id", "to_stop_id"]])
            transfers = transfers.loc[~stop_pairs.isin(excluded_connections)]
        return transfers
