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
from scipy.sparse import csr_matrix
from scipy.sparse.csgraph import min_weight_full_bipartite_matching
from scipy.spatial import cKDTree

from mobility.runtime.temporary import temporary_folder

from .gtfs_feed import GTFSFeed
from .gtfs_overlap import find_headway_overlaps, find_trip_overlaps, find_within_feed_duplicates


def format_agency_inventories(sources, feed_ids, involved):
    """List the full agency inventory for the affected feed IDs."""
    lines = []
    for source in sources:
        feed = feed_ids.get(source.get("feed_id"))
        if feed in involved:
            inventory = source.get("agency_inventory") or "unavailable"
            lines.append(f"- {feed}:\n    " + inventory.replace("\n", "\n    "))
    return "\n".join(lines)


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
        source_ids: dict[str, str] | None = None,
        feed_priority: list[str] | None = None,
        exclusions: dict[str, dict[str, list[str]]] | None = None,
        max_calendar_distance_years: int | None = 1,
    ) -> None:
        self.paths = paths
        self.transport_zones = transport_zones
        self.route_types_path = route_types_path
        self.service_start = dt.date.fromisoformat(service_start_date) if service_start_date else None
        self.service_end = dt.date.fromisoformat(service_end_date) if service_end_date else None
        self.source_ids = source_ids or {}
        self.feed_priority = feed_priority or []
        self.exclusions = exclusions or {}
        self.max_calendar_distance_years = max_calendar_distance_years
        if len(self.feed_priority) != len(set(self.feed_priority)):
            raise ValueError("gtfs_feed_priority must not contain duplicate feed IDs.")
        known = {self.source_ids.get(str(path), str(path)) for path in paths}
        unknown_exclusions = set(self.exclusions) - known
        if unknown_exclusions:
            raise ValueError(f"Unknown gtfs_exclusions sources: {sorted(unknown_exclusions)}")
        unknown = set(self.feed_priority) - known
        if unknown:
            raise ValueError(f"Unknown gtfs_feed_priority IDs: {sorted(unknown)}. Use IDs from the overlap error.")
        self.tables: dict[str, pd.DataFrame] = {}

    def prepare(self) -> tuple[dict[str, pd.DataFrame], dict[str, Any]]:
        """Return the selected timetable and a summary of dates and source coverage."""
        self.tables, sources = self.read_feeds()
        tables = self.tables
        feed_ids = {source["feed_id"]: self.source_ids.get(str(source["path"]), str(source["path"]))
                    for source in sources if "feed_id" in source}
        date, services, tuesday_counts = self.select_tuesday()
        trips = tables["trips"]
        tables["trips"] = trips.loc[trips.service_id.isin(services)].copy()
        # The overlap checks only need stop visits for the selected day.
        stop_times = tables["stop_times"]
        tables["stop_times"] = stop_times.loc[stop_times.trip_id.isin(tables["trips"].trip_id)].copy()
        within_feed = find_within_feed_duplicates(tables["trips"], tables["stop_times"]).to_pandas()
        within_removed = set(within_feed.removed_trip_id)
        if within_removed:
            within_feed["feed_id"] = within_feed.source.map(feed_ids)
            counts = within_feed.groupby("feed_id").size()
            for feed, count in counts.items():
                logging.warning("Removed %s identical scheduled trips within GTFS feed %s; "
                                "distinct simultaneous vehicles can have identical schedules.", count, feed)
            tables["trips"] = tables["trips"].loc[~tables["trips"].trip_id.isin(within_removed)].copy()
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

        trip_overlaps = find_trip_overlaps(tables["trips"], tables["stop_times"], tables["stops"])
        headway_trips = int((tables["trips"]["_frequency_kind"] == "headway").sum())
        if headway_trips:
            logging.warning("Headway-to-scheduled overlaps and partially overlapping headway "
                            "intervals within one feed are not checked.")
        trip_overlaps = trip_overlaps.to_pandas()
        removed_trips = self.resolve_trip_overlaps(trip_overlaps, identities, sources, date)
        headway_overlaps, headway_members, within_headway = find_headway_overlaps(
            tables["trips"], tables["stop_times"], tables["stops"]
        )
        if not within_headway.empty:
            within_headway["feed_id"] = within_headway.source.map(feed_ids)
            removed_headway_members = headway_members.loc[
                headway_members.representative_trip_id.isin(within_headway.removed_trip_id), "trip_id"
            ]
            within_removed.update(removed_headway_members)
            for feed, count in within_headway.groupby("feed_id").size().items():
                logging.warning("Removed %s identical headway intervals within GTFS feed %s; "
                                "distinct parallel services can have identical intervals.", count, feed)
        removed_intervals = set()
        if not headway_overlaps.empty:
            partial = headway_overlaps.loc[~headway_overlaps.identical_interval]
            if not partial.empty:
                lines = []
                for row in partial.head(10).itertuples(index=False):
                    lines.append(
                        f"- {feed_ids[row.left_source]} ({row.left_trip_id}, "
                        f"{row.left_interval_start:g}–{row.left_interval_end:g}s, headway {row.left_headway:g}s) "
                        f"<-> {feed_ids[row.right_source]} ({row.right_trip_id}, "
                        f"{row.right_interval_start:g}–{row.right_interval_end:g}s, headway {row.right_headway:g}s)"
                    )
                involved = {feed_ids[source] for source in set(partial.left_source) | set(partial.right_source)}
                raise ValueError(
                    f"Found {len(partial)} overlapping headway intervals with different service windows or "
                    "headways. Mobility cannot safely split or deduplicate these intervals automatically. "
                    "Times are seconds on the selected service day at the first retained stop. "
                    "Review the affected feeds and use excluded_gtfs_sources if appropriate.\n"
                    + "\n".join(lines)
                    + ("\nOnly the first 10 candidate pairs are shown." if len(partial) > 10 else "")
                    + "\n\nAll agencies listed in each affected feed (including outside the study area):\n"
                    + format_agency_inventories(sources, feed_ids, involved)
                )
            removed_intervals = self.resolve_trip_overlaps(headway_overlaps, identities, sources, date, item_kind="headway intervals")
            removed_trips.update(headway_members.loc[
                headway_members.representative_trip_id.isin(removed_intervals), "trip_id"
            ])
        if removed_trips or within_removed:
            tables["trips"] = tables["trips"].loc[~tables["trips"].trip_id.isin(removed_trips | within_removed)].copy()

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
                source["deduplicated_trips"] = sum(trip.startswith(prefix) for trip in removed_trips)
                source["within_feed_deduplicated_trips"] = sum(trip.startswith(prefix) for trip in within_removed)
                source["overlapping_trip_ids"] = int(overlapping.trip_id.str.startswith(prefix).sum())
                active_dates = self.service_dates_by_source.get(source["feed_id"], [])
                source["active_service_dates"] = ",".join(day.strftime("%Y-%m-%d") for day in active_dates)
                if not source["selected_trips"]:
                    source["status"] = ("deduplicated" if source["deduplicated_trips"] else
                        "no_service_in_window" if not active_dates else
                        "no_tuesday_service" if not any(day.weekday() == 1 for day in active_dates) else
                        "no_service_on_selected_date"
                    )
                    if source["status"] != "deduplicated":
                        logging.warning("GTFS %s has no service on selected Tuesday %s", source["path"], date.date())
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
            "spatiotemporal_overlap_pairs": len(trip_overlaps),
            "deduplicated_trips": len(removed_trips),
            "within_feed_deduplicated_trips": len(within_removed),
            "headway_generated_trips": headway_trips,
            "headway_interval_pairs": len(headway_overlaps),
            "deduplicated_headway_intervals": len(removed_intervals),
            "within_feed_deduplicated_headway_intervals": len(within_headway),
            "trip_overlaps": trip_overlaps,
            "headway_overlaps": headway_overlaps,
            "within_feed_overlaps": within_feed,
            "within_feed_headway_overlaps": within_headway,
            "sources": sources,
            "tuesdays": tuesday_counts,
            "frequency_assumption": "regular departures also approximate exact_times=0",
            "interpolation": "linear by stop_sequence between timed endpoints",
        }
        tables["trips"] = tables["trips"].drop(columns=[column for column in tables["trips"] if column.startswith("_frequency_")])
        return {
            key: tables[key]
            for key in ("agency", "routes", "stops", "trips", "stop_times", "transfers")
        }, metadata

    def resolve_trip_overlaps(self, pairs, identities, sources, date, item_kind="trips"):
        """Use feed priority to decide which matching trips to remove.

        First show an error if a modeller has not ranked every affected feed.
        For each pair of ranked feeds, match trips one-to-one, preferring trips
        with closer times and stop locations. This prevents a single preferred
        trip from removing several trips. Unmatched trips stay in the model.
        """
        if pairs.empty:
            return set()
        source_ids = {source["feed_id"]: self.source_ids.get(str(source["path"]), str(source["path"]))
                      for source in sources if "feed_id" in source}
        pairs["left_feed"] = pairs.left_source.map(source_ids)
        pairs["right_feed"] = pairs.right_source.map(source_ids)
        operators = identities.set_index("trip_id")["agency_name"]
        pairs["left_operator"] = pairs.left_trip_id.map(operators).fillna("unknown")
        pairs["right_operator"] = pairs.right_trip_id.map(operators).fillna("unknown")
        pairs["selected_for_removal"] = False
        pairs["removed_trip_id"] = ""
        involved = set(pairs.left_feed) | set(pairs.right_feed)
        if not involved.issubset(self.feed_priority):
            report = []
            grouped = pairs.groupby(["left_feed", "right_feed"], sort=True)
            for (left, right), group in grouped:
                ambiguous_left = int((group.groupby("left_trip_id").size() > 1).sum())
                ambiguous_right = int((group.groupby("right_trip_id").size() > 1).sum())
                report.append(
                    f"- {left}  <->  {right} : "
                    f"{len(group)} candidate pairs, {group.left_trip_id.nunique()} / "
                    f"{group.right_trip_id.nunique()} distinct trips; "
                    f"{ambiguous_left} / {ambiguous_right} trips have multiple candidates; "
                    f"maximum arrival difference {group.max_arrival_difference_s.max():g} s"
                )
            raise ValueError(
                f"Potential overlapping GTFS {item_kind} on {date.date()}:\n"
                + "\n".join(report)
                + "\n\nAll agencies listed in each affected feed (including outside the study area):\n"
                + format_agency_inventories(sources, source_ids, involved)
                + "\n\nReview the feed pairs, then set gtfs_feed_priority in "
                "PublicTransportRoutingParameters, listing every overlapping feed ID "
                "in preference order, highest first. Example:\n"
                "    gtfs_feed_priority=['<preferred feed ID>', '<other feed ID>']\n"
                f"Matched {item_kind} from lower-priority feeds will be removed one-to-one; "
                f"unmatched {item_kind} are retained."
            )
        ambiguous_left = int((pairs.groupby(["left_trip_id", "right_feed"]).size() > 1).sum())
        ambiguous_right = int((pairs.groupby(["right_trip_id", "left_feed"]).size() > 1).sum())
        if ambiguous_left or ambiguous_right:
            logging.warning(
                "%s / %s GTFS %s have multiple candidate matches in another feed; "
                "review the chosen matches in the overlap report.", ambiguous_left, ambiguous_right, item_kind,
            )
        removed = set()
        # Process feeds in priority order. For each feed pair, choose as many
        # one-to-one matches as possible, then prefer the closest matches.
        for lower_rank in range(1, len(self.feed_priority)):
            lower = self.feed_priority[lower_rank]
            for higher in self.feed_priority[:lower_rank]:
                direct = pairs.loc[(pairs.left_feed == higher) & (pairs.right_feed == lower)].copy()
                reversed_pairs = pairs.loc[(pairs.left_feed == lower) & (pairs.right_feed == higher)].copy()
                if not reversed_pairs.empty:
                    reversed_pairs = reversed_pairs.rename(columns={"left_trip_id": "right_trip_id", "right_trip_id": "left_trip_id"})
                edges = pd.concat([direct, reversed_pairs])
                if edges.empty:
                    continue
                edges = edges.loc[~edges.left_trip_id.isin(removed) & ~edges.right_trip_id.isin(removed)]
                if edges.empty:
                    continue
                high_ids = pd.Index(sorted(edges.left_trip_id.unique()))
                low_ids = pd.Index(sorted(edges.right_trip_id.unique()))
                # Each preferred trip gets its own "unmatched" option. Its
                # cost is larger than all real match costs combined, so the
                # solver first maximizes the number of matches. Among those,
                # it prefers smaller time and distance differences.
                cost = (edges.max_time_difference_s.to_numpy() +
                        edges.max_distance_m.to_numpy() / 101.0 + 1.0)
                unmatched_cost = (cost.max() + 1) * (min(len(high_ids), len(low_ids)) + 1)
                rows = np.concatenate([high_ids.get_indexer(edges.left_trip_id), np.arange(len(high_ids))])
                columns = np.concatenate([low_ids.get_indexer(edges.right_trip_id),
                                          len(low_ids) + np.arange(len(high_ids))])
                graph = csr_matrix((np.concatenate([cost, np.full(len(high_ids), unmatched_cost)]),
                                    (rows, columns)), shape=(len(high_ids), len(low_ids) + len(high_ids)))
                row_indices, column_indices = min_weight_full_bipartite_matching(graph)
                matched = column_indices < len(low_ids)
                high_matches = high_ids.take(row_indices[matched])
                low_matches = low_ids.take(column_indices[matched])
                selected_pairs = pd.MultiIndex.from_arrays([high_matches, low_matches])
                direct_mask = pd.MultiIndex.from_frame(pairs[["left_trip_id", "right_trip_id"]]).isin(selected_pairs)
                reverse_mask = pd.MultiIndex.from_frame(pairs[["right_trip_id", "left_trip_id"]]).isin(selected_pairs)
                pairs.loc[direct_mask | reverse_mask, "selected_for_removal"] = True
                pairs.loc[direct_mask, "removed_trip_id"] = pairs.loc[direct_mask, "right_trip_id"]
                pairs.loc[reverse_mask, "removed_trip_id"] = pairs.loc[reverse_mask, "left_trip_id"]
                removed.update(low_matches)
        logging.warning("Removed %s matched lower-priority GTFS %s; unmatched %s are retained.",
                        len(removed), item_kind, item_kind)
        return removed

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
                with tempfile.TemporaryDirectory(prefix="mobility-gtfs-", dir=temporary_folder()) as directory:
                    source_id = self.source_ids.get(str(path), str(path))
                    reader = GTFSFeed(path, Path(directory), self.exclusions.get(source_id), source_id)
                    tables_to_read = ("calendar", "calendar_dates")
                    if reader.exclusions:
                        tables_to_read += ("trips", "routes")
                    reader.extract(tables_to_read)
                    reader.prepare_exclusions()
                    source = {"path": str(path), "sha256": None}
                    source.update(reader.exclusion_diagnostics)
                    sources.append(source)

                    calendar, exceptions = reader.read_calendars()
                    services_by_date = self.get_service_dates(calendar, exceptions, reader)
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

                    if self.service_start and self.service_end and self.max_calendar_distance_years is not None:
                        earliest = pd.Timestamp(self.service_start) - pd.DateOffset(years=self.max_calendar_distance_years)
                        latest = pd.Timestamp(self.service_end) + pd.DateOffset(years=self.max_calendar_distance_years)
                        for frame, columns in ((calendar, ("start_date", "end_date")), (exceptions, ("date",))):
                            for column in columns:
                                if column in frame:
                                    outside = frame[column].lt(earliest) | frame[column].gt(latest)
                                    if outside.any():
                                        values = frame.loc[outside, column].dt.strftime("%Y-%m-%d").tolist()[:10]
                                        reader.problem(
                                            f"Calendar {column} outside {earliest:%Y-%m-%d} to {latest:%Y-%m-%d}: {values}. "
                                            "Adjust gtfs_max_calendar_distance_years or set it to None if the dates are intentional",
                                            "services", frame.loc[outside, "service_id"],
                                        )

                    logging.info("Preparing GTFS %s", path)
                    fingerprint = reader.extract()
                    source["sha256"] = fingerprint
                    identity = (fingerprint, repr(reader.exclusions))
                    if identity in seen:
                        source["status"] = "duplicate"
                        continue
                    seen.add(identity)
                    feed = reader.read(boundary, calendar, exceptions)
                    if feed is None:
                        source["status"] = "no usable trips in area"
                        continue
                    source.update(reader.diagnostics)
                    source["agency_inventory"] = reader.agency_inventory

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
                source_id = self.source_ids.get(str(path))
                if source_id is not None:
                    raise ValueError(
                        f"Cannot prepare GTFS source {source_id} ({path}): {error}\n"
                        "To omit this feed, create a new snapshot with "
                        f"{source_id!r} added to excluded_gtfs_sources."
                    ) from error
                raise ValueError(f"Cannot prepare GTFS {path}: {error}") from error

        if not feeds:
            raise ValueError("No active Tuesday service with two visits in the study area in the requested window")

        tables = {
            name: pd.concat([feed[name] for feed in feeds], ignore_index=True).fillna("")
            for name in feeds[0]
        }
        return tables, sources

    def get_service_dates(self, calendar: pd.DataFrame, exceptions: pd.DataFrame, reader=None) -> dict:
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
                if reader is not None:
                    reader.problem(f"Service {row.service_id!r} is both added and cancelled on {row.date:%Y-%m-%d}", "services", conflicts.service_id)
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
        # Trips of the same route can follow different stop patterns. Include
        # same-stop connections before applying declared transfer restrictions.
        visit_counts = tables["stop_times"].groupby("stop_id").size()
        shared_stops = np.flatnonzero(active.stop_id.isin(visit_counts[visit_counts > 1].index))
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
