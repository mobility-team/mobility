"""Prepare one dated public transport timetable without running R."""

import hashlib
import logging
import zipfile

import geopandas as gpd
import numpy as np
import pandas as pd

from .gtfs_transfers import prepare_transfers


def read_table(archive, name, required=()):
    """Read identifiers as strings and report missing GTFS columns clearly."""
    filename = name + ".txt"
    if filename not in archive.namelist():
        if required:
            raise ValueError(f"Missing {filename}")
        return pd.DataFrame()
    with archive.open(filename) as stream:
        table = pd.read_csv(stream, dtype=str, keep_default_na=False)
    missing = set(required) - set(table.columns)
    if missing:
        raise ValueError(f"{filename} is missing columns: {sorted(missing)}")
    return table


def time_seconds(values):
    """Parse GTFS times, including hours after midnight, preserving blanks."""
    present = values.ne("")
    if not values[present].str.fullmatch(r"\d+:[0-5]\d:[0-5]\d").all():
        raise ValueError("Invalid GTFS time; expected HH:MM:SS")
    parts = values.where(present).str.split(":", expand=True).reindex(columns=range(3))
    parts = parts.apply(pd.to_numeric, errors="coerce")
    return parts[0] * 3600 + parts[1] * 60 + parts[2]


def read_feed(path, boundary):
    """Read a feed, keeping trips with at least two visits in the study area."""
    with zipfile.ZipFile(path) as archive:
        stops = read_table(archive, "stops", ("stop_id", "stop_lat", "stop_lon"))
        stops["stop_lon"] = pd.to_numeric(stops.stop_lon, errors="coerce")
        stops["stop_lat"] = pd.to_numeric(stops.stop_lat, errors="coerce")
        stops = stops.dropna(subset=["stop_lon", "stop_lat"])
        points = gpd.GeoSeries(gpd.points_from_xy(stops.stop_lon, stops.stop_lat), crs=4326)
        stops = stops.loc[points.intersects(boundary).to_numpy()].copy()
        if stops.empty:
            return None
        trips = read_table(archive, "trips", ("trip_id", "route_id", "service_id"))
        if trips.trip_id.duplicated().any():
            raise ValueError("Duplicate trip_id in trips.txt")

        # First locate trips that visit the area. Then read their full sequences,
        # so interpolation and frequency offsets use the original endpoints.
        selected = set()
        with archive.open("stop_times.txt") as stream:
            for chunk in pd.read_csv(stream, dtype=str, keep_default_na=False,
                                     usecols=["trip_id", "stop_id"], chunksize=200_000):
                selected.update(chunk.loc[chunk.stop_id.isin(stops.stop_id), "trip_id"])
        if not selected:
            return None
        chunks = []
        columns = {"trip_id", "stop_id", "stop_sequence", "arrival_time", "departure_time",
                   "pickup_type", "drop_off_type", "timepoint", "shape_dist_traveled"}
        with archive.open("stop_times.txt") as stream:
            for chunk in pd.read_csv(stream, dtype=str, keep_default_na=False,
                                     usecols=lambda col: col in columns, chunksize=200_000):
                chunks.append(chunk.loc[chunk.trip_id.isin(selected)])
        times = pd.concat(chunks, ignore_index=True)
        del chunks
        if not times.trip_id.isin(trips.trip_id).all():
            raise ValueError("stop_times.txt references unknown trips")
        counts = times.loc[times.stop_id.isin(stops.stop_id)].groupby("trip_id").size()
        times = times.loc[times.trip_id.isin(counts[counts >= 2].index)].copy()
        if times.empty:
            return None
        times["stop_sequence"] = pd.to_numeric(times.stop_sequence, errors="raise")
        times = times.sort_values(["trip_id", "stop_sequence"])
        if times.duplicated(["trip_id", "stop_sequence"]).any():
            raise ValueError("Duplicate stop_sequence within a trip")
        for col in ("arrival_time", "departure_time"):
            times[col] = time_seconds(times[col])
        for col in ("pickup_type", "drop_off_type"):
            if col not in times:
                times[col] = 0
            times[col] = pd.to_numeric(times[col].replace("", "0"), errors="raise")
            if not times[col].isin([0, 1, 2, 3]).all():
                raise ValueError(f"Invalid {col}")

        # Missing intermediate times are estimated by sequence position. Never
        # convert blanks to midnight or extrapolate beyond timed endpoints.
        missing_trips = times.loc[times[["arrival_time", "departure_time"]].isna().any(axis=1), "trip_id"].unique()
        invalid_trips = set()
        for trip_id in missing_trips:
            group = times.loc[times.trip_id.eq(trip_id)]
            for col in ("arrival_time", "departure_time"):
                values = group[col].copy()
                if pd.isna(values.iloc[0]) or pd.isna(values.iloc[-1]):
                    invalid_trips.add(trip_id)
                    continue
                known = values.notna()
                times.loc[group.index, col] = np.interp(
                    group.stop_sequence, group.loc[known, "stop_sequence"], values[known]
                )
        previous = times.groupby("trip_id").departure_time.shift()
        invalid_trips.update(times.loc[(times.departure_time < times.arrival_time) | (times.arrival_time < previous), "trip_id"])
        if invalid_trips:
            logging.warning("Dropping %s trips with missing endpoint times or backwards times from %s",
                            len(invalid_trips), path)
            times = times.loc[~times.trip_id.isin(invalid_trips)].copy()
        starts = times.groupby("trip_id").departure_time.first()
        times = times.loc[times.stop_id.isin(stops.stop_id)].copy()
        counts = times.groupby("trip_id").size()
        trips = trips.loc[trips.trip_id.isin(counts[counts >= 2].index)].copy()
        if trips.empty:
            return None
        times = times.loc[times.trip_id.isin(trips.trip_id)].copy()

        # Expand repeated departures only after clipping, to limit memory use.
        frequencies = read_table(archive, "frequencies")
        if not frequencies.empty:
            required = {"trip_id", "start_time", "end_time", "headway_secs"}
            if not required.issubset(frequencies.columns):
                raise ValueError("Invalid frequencies.txt columns")
            frequencies = frequencies.loc[frequencies.trip_id.isin(trips.trip_id)].copy()
            if "exact_times" in frequencies and not frequencies.exact_times.isin(["", "0", "1"]).all():
                raise ValueError("Invalid frequencies.txt exact_times")
            if "exact_times" not in frequencies or frequencies.exact_times.isin(["", "0"]).any():
                logging.warning("Approximating frequency-based service by regular departures in %s", path)
            expanded_times, expanded_trips = [], []
            for trip_id, periods in frequencies.groupby("trip_id", sort=False):
                template = times.loc[times.trip_id.eq(trip_id)]
                trip = trips.loc[trips.trip_id.eq(trip_id)]
                departures = []
                intervals = []
                for period in periods.itertuples():
                    start, end = time_seconds(pd.Series([period.start_time, period.end_time]))
                    headway = int(period.headway_secs)
                    if headway <= 0 or end <= start:
                        raise ValueError(f"Invalid frequency interval for {trip_id}")
                    intervals.append((start, end))
                    departures.extend(range(int(start), int(end), headway))
                intervals.sort()
                if any(end > next_start for (_, end), (next_start, _) in zip(intervals, intervals[1:])):
                    raise ValueError(f"Overlapping frequency intervals for {trip_id}")
                for index, departure in enumerate(departures):
                    new_id = f"{trip_id}:frequency:{index}"
                    if new_id in selected:
                        raise ValueError(f"Frequency trip ID collision: {new_id}")
                    copy = template.copy()
                    copy["trip_id"] = new_id
                    copy[["arrival_time", "departure_time"]] += departure - starts[trip_id]
                    expanded_times.append(copy)
                    expanded_trips.append(trip.assign(trip_id=new_id))
            repeated = set(frequencies.trip_id)
            times = pd.concat([times.loc[~times.trip_id.isin(repeated)], *expanded_times], ignore_index=True)
            trips = pd.concat([trips.loc[~trips.trip_id.isin(repeated)], *expanded_trips], ignore_index=True)

        # Keep related tables consistent before merging different feed IDs.
        routes = read_table(archive, "routes", ("route_id", "route_type"))
        routes = routes.loc[routes.route_id.isin(trips.route_id)].copy()
        if not trips.route_id.isin(routes.route_id).all():
            raise ValueError("trips.txt references unknown routes")
        agency = read_table(archive, "agency", ("agency_name",))
        if "agency_id" not in agency:
            if len(agency) != 1:
                raise ValueError("Multiple agencies require agency_id")
            agency["agency_id"] = "agency"
        for name, frame, key in (("stops", stops, "stop_id"), ("routes", routes, "route_id"),
                                 ("agency", agency, "agency_id")):
            if frame[key].eq("").any() or frame[key].duplicated().any():
                raise ValueError(f"Empty or duplicate {key} in {name}.txt")
        if "agency_id" not in routes or routes.agency_id.eq("").any():
            if len(agency) != 1:
                raise ValueError("Routes with multiple agencies require agency_id")
            routes["agency_id"] = agency.agency_id.iloc[0]
        if not routes.agency_id.isin(agency.agency_id).all():
            raise ValueError("routes.txt references unknown agencies")
        agency = agency.loc[agency.agency_id.isin(routes.agency_id)].copy()
        if "route_short_name" not in routes:
            routes["route_short_name"] = ""
        routes["route_short_name"] = routes.route_short_name.replace("", np.nan).fillna(
            routes.get("route_long_name", routes.route_id)
        )
        routes["route_type"] = pd.to_numeric(routes.route_type, errors="raise")
        calendar = read_table(archive, "calendar")
        exceptions = read_table(archive, "calendar_dates")
        for name, frame, required in (
            ("calendar", calendar, {"service_id", "tuesday", "start_date", "end_date"}),
            ("calendar_dates", exceptions, {"service_id", "date", "exception_type"}),
        ):
            if not frame.empty and not required.issubset(frame.columns):
                raise ValueError(f"Invalid {name}.txt columns")
        if calendar.empty and exceptions.empty:
            raise ValueError("No service calendar")
        calendar = calendar.loc[calendar.service_id.isin(trips.service_id)].copy() if not calendar.empty else calendar
        exceptions = exceptions.loc[exceptions.service_id.isin(trips.service_id)].copy() if not exceptions.empty else exceptions
        declared = set(calendar.get("service_id", [])) | set(exceptions.get("service_id", []))
        if not set(trips.service_id).issubset(declared):
            raise ValueError("Trips reference services without a calendar")
        transfers = read_table(archive, "transfers")
        times.attrs["dropped_invalid_trips"] = len(invalid_trips)
        times.attrs["interpolated_trips"] = len(set(missing_trips) - invalid_trips)
        return dict(agency=agency, routes=routes, stops=stops, trips=trips,
                    stop_times=times, calendar=calendar, calendar_dates=exceptions,
                    transfers=transfers)


def select_tuesday(tables):
    """Choose the earliest Tuesday with the most stop visits on its real date."""
    calendar, exceptions = tables["calendar"].copy(), tables["calendar_dates"].copy()
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
            candidates.update(pd.date_range(calendar.start_date.min(), calendar.end_date.max(), freq="W-TUE"))
    if not exceptions.empty:
        exceptions["date"] = pd.to_datetime(exceptions.date, format="%Y%m%d", errors="raise")
        exceptions["exception_type"] = pd.to_numeric(exceptions.exception_type, errors="raise")
        if exceptions.duplicated(["service_id", "date"]).any():
            raise ValueError("Duplicate calendar exception for the same service and date")
        if not exceptions.exception_type.isin([1, 2]).all():
            raise ValueError("Invalid calendar exception_type")
        candidates.update(exceptions.loc[exceptions.date.dt.dayofweek.eq(1), "date"])
    supply = tables["stop_times"].merge(tables["trips"][["trip_id", "service_id"]], on="trip_id")
    weights = supply.groupby("service_id").size()
    best, best_services, best_count = None, set(), 0
    scores = []
    for date in sorted(candidates):
        active = set()
        if not calendar.empty:
            active.update(calendar.loc[calendar.start_date.le(date) & calendar.end_date.ge(date), "service_id"])
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


def prepare_gtfs(paths, transport_zones, route_types_path):
    """Return selected GTFS tables and an audit of source coverage and dates."""
    boundary = gpd.GeoSeries([transport_zones.to_crs(3035).geometry.union_all().buffer(10_000)], crs=3035).to_crs(4326).iloc[0]
    feeds, sources, seen = [], [], set()
    for path in paths:
        logging.info("Preparing GTFS %s", path)
        try:
            # Hash timetable content, not ZIP metadata or URLs. Different archive
            # records can contain exactly the same timetable.
            digest = hashlib.sha256()
            with zipfile.ZipFile(path) as archive:
                for name in sorted(archive.namelist()):
                    if name in {"agency.txt", "routes.txt", "stops.txt", "trips.txt", "stop_times.txt",
                                "calendar.txt", "calendar_dates.txt", "transfers.txt", "frequencies.txt"}:
                        digest.update(name.encode())
                        with archive.open(name) as stream:
                            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                                digest.update(chunk)
            fingerprint = digest.hexdigest()
            source = {"path": str(path), "sha256": fingerprint}
            sources.append(source)
            if fingerprint in seen:
                source["status"] = "duplicate"
                continue
            seen.add(fingerprint)
            feed = read_feed(path, boundary)
            if feed is None:
                source["status"] = "no usable trips in area"
                continue
            source["status"] = "prepared"
            source.update(feed["stop_times"].attrs)
            source["feed_id"] = str(len(feeds) + 1)
            prefix = source["feed_id"] + "-"
            for table in feed.values():
                for col in ("agency_id", "route_id", "stop_id", "trip_id", "service_id", "parent_station",
                            "from_stop_id", "to_stop_id", "from_route_id", "to_route_id", "from_trip_id", "to_trip_id"):
                    if col in table:
                        table[col] = table[col].where(table[col].eq(""), prefix + table[col])
            feeds.append(feed)
        except (ValueError, KeyError, zipfile.BadZipFile) as error:
            raise ValueError(f"Cannot prepare GTFS {path}: {error}") from error
    if not feeds:
        raise ValueError("No GTFS trips with two visits in the study area")
    tables = {name: pd.concat([feed[name] for feed in feeds], ignore_index=True).fillna("") for name in feeds[0]}
    date, services, scores = select_tuesday(tables)
    tables["trips"] = tables["trips"].loc[tables["trips"].service_id.isin(services)].copy()
    tables["stop_times"] = tables["stop_times"].loc[tables["stop_times"].trip_id.isin(tables["trips"].trip_id)].copy()
    tables["routes"] = tables["routes"].loc[tables["routes"].route_id.isin(tables["trips"].route_id)].copy()
    tables["agency"] = tables["agency"].loc[tables["agency"].agency_id.isin(tables["routes"].agency_id)].copy()
    # Keep parent station records while resolving station-level transfer rules.
    tables["transfers"] = prepare_transfers(tables)
    tables["stops"] = tables["stops"].loc[tables["stops"].stop_id.isin(tables["stop_times"].stop_id)].copy()
    route_types = pd.read_csv(route_types_path, sep=";")
    tables["routes"] = tables["routes"].merge(route_types[["route_type", "route_type_label", "vehicle_capacity"]], on="route_type", how="left")
    tables["routes"]["route_type_label"] = tables["routes"].route_type_label.fillna("bus")
    tables["routes"]["vehicle_capacity"] = tables["routes"].vehicle_capacity.fillna(50)
    for source in sources:
        if "feed_id" in source:
            prefix = source["feed_id"] + "-"
            source["selected_trips"] = int(tables["trips"].trip_id.str.startswith(prefix).sum())
            if not source["selected_trips"]:
                logging.warning("GTFS %s has no service on selected Tuesday %s", source["path"], date.date())
    metadata = {"selected_date": date.strftime("%Y-%m-%d"), "selection": "maximum stop visits; earliest tie",
                "date_alignment": False, "sources": sources, "tuesdays": scores,
                "frequency_assumption": "regular departures also approximate exact_times=0",
                "interpolation": "linear by stop_sequence between timed endpoints"}
    return {key: tables[key] for key in ("agency", "routes", "stops", "trips", "stop_times", "transfers")}, metadata
