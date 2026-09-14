"""Combine GTFS feeds into one dated timetable for the study area."""

import logging
import datetime as dt
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
        service_start_date: str | None = None,
        service_end_date: str | None = None,
    ) -> None:
        self.paths = paths
        self.transport_zones = transport_zones
        self.route_types_path = route_types_path
        self.service_start = dt.date.fromisoformat(service_start_date) if service_start_date else None
        self.service_end = dt.date.fromisoformat(service_end_date) if service_end_date else None
        self.tables: dict[str, pd.DataFrame] = {}

    def prepare(self) -> tuple[dict[str, pd.DataFrame], dict[str, Any]]:
        """Return the selected timetable and a summary of dates and source coverage."""
        self.tables, sources = self.read_feeds()
        tables = self.tables
        date, services, tuesday_counts = self.select_tuesday()
        trips = tables["trips"]
        tables["trips"] = trips.loc[trips.service_id.isin(services)].copy()
        # Shared IDs can indicate overlapping replacements. Do not guess which
        # operator feed to remove: it may also contain complementary lines.
        identities = tables["trips"].merge(tables["routes"], on="route_id").merge(tables["agency"], on="agency_id")
        identities["source"] = identities.trip_id.str.split("-", n=1).str[0]
        identities["original_trip_id"] = identities.trip_id.str.split("-", n=1).str[1]
        identities["original_route_id"] = identities.route_id.str.split("-", n=1).str[1]
        keys = ["agency_name", "original_route_id", "original_trip_id"]
        shared = identities.groupby(keys)["source"].transform("nunique").gt(1)
        overlapping = identities.loc[shared]
        if not overlapping.empty:
            logging.warning(
                "%s selected GTFS trips share operator, route and trip IDs across feeds. "
                "Review overlapping_trip_ids in the source report before using the result.", len(overlapping),
            )

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
                source["overlapping_trip_ids"] = int(overlapping.trip_id.str.startswith(prefix).sum())
                active_dates = self.service_dates_by_source.get(source["feed_id"], [])
                source["active_service_dates"] = ",".join(day.strftime("%Y-%m-%d") for day in active_dates)
                if not source["selected_trips"]:
                    source["status"] = (
                        "no_service_in_window" if not active_dates else
                        "no_tuesday_service" if not any(day.weekday() == 1 for day in active_dates) else
                        "no_service_on_selected_date"
                    )
                    logging.warning(
                        "GTFS %s has no service on selected Tuesday %s",
                        source["path"],
                        date.date(),
                    )
        feed_count = sum(any(day.weekday() == 1 for day in dates) for dates in self.service_dates_by_source.values())
        common_tuesday = any(row["feeds_with_service"] == feed_count for row in tuesday_counts)
        if not common_tuesday:
            logging.warning("No Tuesday in the study window has service from every retained GTFS feed; inspect the source report.")
        metadata = {
            "selected_date": date.strftime("%Y-%m-%d"),
            "service_start_date": self.service_start.isoformat() if self.service_start else "unbounded",
            "service_end_date": self.service_end.isoformat() if self.service_end else "unbounded",
            "selection": "maximum stop visits; earliest tie",
            "date_alignment": False,
            "common_tuesday_available": common_tuesday,
            "overlapping_trip_ids": len(overlapping),
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
        self.services_by_date = {}
        self.service_dates_by_source = {}

        for path in self.paths:
            logging.info("Checking GTFS %s", path)
            try:
                # Check the small calendars before extracting the larger trip tables.
                with tempfile.TemporaryDirectory(prefix="mobility-gtfs-") as directory:
                    reader = GTFSFeed(path, Path(directory))
                    reader.extract(("calendar", "calendar_dates"))
                    source = {"path": str(path), "sha256": None}
                    sources.append(source)

                    calendar, exceptions = reader.read_calendars()
                    services_by_date = self.get_service_dates(calendar, exceptions)
                    active_dates = [date for date, services in services_by_date.items() if services]
                    if not any(date.weekday() == 1 for date in active_dates):
                        source.update(
                            status="no_service_in_window" if not active_dates else "no_tuesday_service",
                            selected_trips=0,
                            active_service_dates=",".join(date.strftime("%Y-%m-%d") for date in active_dates),
                        )
                        logging.info(
                            "Skipping GTFS %s: %s", path,
                            "no service in the study window" if not active_dates else
                            "no Tuesday service in the study window",
                        )
                        continue

                    logging.info("Preparing GTFS %s", path)
                    fingerprint = reader.extract()
                    source["sha256"] = fingerprint
                    if fingerprint in seen:
                        source["status"] = "duplicate"
                        continue
                    seen.add(fingerprint)
                    feed = reader.read(boundary, calendar, exceptions)
                    if feed is None:
                        source["status"] = "no usable trips in area"
                        continue
                    source.update(reader.diagnostics)

                source["status"] = "prepared"
                source["feed_id"] = str(len(feeds) + 1)
                prefix = source["feed_id"] + "-"
                retained_services = set(feed["trips"].service_id)
                for date, services in services_by_date.items():
                    self.services_by_date.setdefault(date, set()).update(
                        prefix + service for service in services & retained_services
                    )
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
            raise ValueError("No active Tuesday service with two visits in the study area in the requested window")

        tables = {
            name: pd.concat([feed[name] for feed in feeds], ignore_index=True).fillna("")
            for name in feeds[0]
        }
        return tables, sources

    def get_service_dates(self, calendar: pd.DataFrame, exceptions: pd.DataFrame) -> dict:
        """Apply weekly service and exceptions within the requested window."""
        calendar, exceptions = calendar.copy(), exceptions.copy()
        if not exceptions.empty:
            if self.service_start:
                exceptions = exceptions.loc[exceptions.date.ge(pd.Timestamp(self.service_start))]
            if self.service_end:
                exceptions = exceptions.loc[exceptions.date.le(pd.Timestamp(self.service_end))]
            conflicts = exceptions.loc[exceptions.duplicated(["service_id", "date"], keep=False)]
            if not conflicts.empty:
                row = conflicts.iloc[0]
                raise ValueError(
                    f"Service {row.service_id!r} is both added and cancelled on {row.date:%Y-%m-%d}. "
                    "The feed contains conflicting calendar exceptions in the study window."
                )
        weekdays = ("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday")
        bounds = []
        if not calendar.empty:
            bounds.extend([calendar.start_date.min(), calendar.end_date.max()])
        if not exceptions.empty:
            bounds.extend([exceptions.date.min(), exceptions.date.max()])
        if not bounds:
            return {}
        if (
            self.service_start and max(bounds).date() < self.service_start
            or self.service_end and min(bounds).date() > self.service_end
        ):
            return {}
        candidates = pd.date_range(self.service_start or min(bounds), self.service_end or max(bounds))
        services_by_date = {}
        for date in candidates:
            active_services = set()
            if not calendar.empty:
                active_services.update(calendar.loc[
                    calendar.start_date.le(date) & calendar.end_date.ge(date)
                    & calendar[weekdays[date.weekday()]].eq(1), "service_id",
                ])
            if not exceptions.empty:
                day = exceptions.loc[exceptions.date.eq(date)]
                active_services.update(day.loc[day.exception_type.eq(1), "service_id"])
                active_services.difference_update(day.loc[day.exception_type.eq(2), "service_id"])
            services_by_date[date] = active_services
        return services_by_date

    def select_tuesday(self) -> tuple[pd.Timestamp, set[str], list[dict[str, Any]]]:
        """Choose the earliest Tuesday with the most scheduled stop visits.

        Count the whole service day, including visits with boarding restrictions.
        Apply calendar additions and cancellations before comparing dates.
        """
        tables = self.tables
        stop_visits = tables["stop_times"].merge(
            tables["trips"][["trip_id", "service_id"]], on="trip_id"
        )
        visits_by_service = stop_visits.groupby("service_id").size()
        selected_date, selected_services, most_visits = None, set(), 0
        tuesday_counts = []
        for date, active_services in sorted(self.services_by_date.items()):
            for source_id in {service.split("-", 1)[0] for service in active_services}:
                self.service_dates_by_source.setdefault(source_id, []).append(date)
            if date.weekday() != 1:
                continue
            count = int(visits_by_service.reindex(list(active_services), fill_value=0).sum())
            tuesday_counts.append({"date": date.strftime("%Y-%m-%d"), "stop_visits": count,
                                   "feeds_with_service": len({service.split("-", 1)[0] for service in active_services})})
            if count > most_visits:
                selected_date, selected_services, most_visits = pd.Timestamp(date), active_services, count
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
