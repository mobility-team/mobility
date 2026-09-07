"""Read and validate one GTFS feed before merging it with other feeds."""

import hashlib
import logging
import zipfile
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import polars as pl
from shapely.geometry.base import BaseGeometry


class GTFSFeed:
    """A source archive and its temporary extracted timetable tables.

    Extraction and hashing share a single ZIP pass. Polars filters the large
    stop-time file before it is converted to pandas for timetable validation.
    The caller owns the temporary directory and removes it after read().
    """

    table_names = (
        "agency",
        "routes",
        "stops",
        "trips",
        "stop_times",
        "calendar",
        "calendar_dates",
        "transfers",
        "frequencies",
    )

    def __init__(self, path: str | Path, folder: Path) -> None:
        self.path = Path(path)
        self.folder = folder
        self.diagnostics: dict[str, int] = {}

    def read(self, boundary: BaseGeometry) -> dict[str, pd.DataFrame] | None:
        """Return local tables, or None when there are no usable trips."""
        stops = self.read_table("stops", ("stop_id", "stop_lat", "stop_lon"))
        stops["stop_lon"] = pd.to_numeric(stops.stop_lon, errors="coerce")
        stops["stop_lat"] = pd.to_numeric(stops.stop_lat, errors="coerce")
        stops = stops.dropna(subset=["stop_lon", "stop_lat"])
        points = gpd.GeoSeries(gpd.points_from_xy(stops.stop_lon, stops.stop_lat), crs=4326)
        stops = stops.loc[points.intersects(boundary).to_numpy()].copy()
        if stops.empty:
            return None

        trips = self.read_table("trips", ("trip_id", "route_id", "service_id"))
        if trips.trip_id.duplicated().any():
            raise ValueError("Duplicate trip_id in trips.txt")

        times = self.read_stop_times(stops.stop_id.tolist())
        if times.empty:
            return None
        if not set(times.trip_id.unique()).issubset(trips.trip_id.to_numpy()):
            raise ValueError("stop_times.txt references unknown trips")

        times, diagnostics = self.clean_stop_times(times)
        starts = times.groupby("trip_id").departure_time.first()
        times = times.loc[times.stop_id.isin(stops.stop_id)].copy()
        # The scan already required two local visits. Validation only removes
        # whole trips, so their remaining sequences do not need another count.
        trips = trips.loc[trips.trip_id.isin(times.trip_id.unique())].copy()
        if trips.empty:
            return None

        trips, times = self.expand_frequencies(trips, times, starts)

        agency, routes, calendar, exceptions = self.read_related_tables(stops, trips)

        transfers = self.read_table("transfers")
        self.diagnostics = diagnostics
        return dict(
            agency=agency,
            routes=routes,
            stops=stops,
            trips=trips,
            stop_times=times,
            calendar=calendar,
            calendar_dates=exceptions,
            transfers=transfers,
        )

    def extract(self) -> str:
        """Extract known tables safely and return their content fingerprint."""
        digest = hashlib.sha256()

        with zipfile.ZipFile(self.path) as archive:
            for name in sorted(self.table_names):
                filename = name + ".txt"
                if filename not in archive.namelist():
                    continue

                digest.update(filename.encode())
                with (
                    archive.open(filename) as source,
                    (self.folder / filename).open("wb") as target,
                ):
                    for chunk in iter(lambda: source.read(1024 * 1024), b""):
                        digest.update(chunk)
                        target.write(chunk)

        return digest.hexdigest()

    def read_table(self, name: str, required: tuple[str, ...] = ()) -> pd.DataFrame:
        """Read a small table without changing identifiers such as '001'."""
        path = self.folder / (name + ".txt")
        if not path.exists():
            if required:
                raise ValueError(f"Missing {path.name}")
            return pd.DataFrame()

        table = pl.read_csv(
            path,
            infer_schema=False,
            missing_utf8_is_empty_string=True,
        ).to_pandas()
        missing = set(required) - set(table.columns)
        if missing:
            raise ValueError(f"{path.name} is missing columns: {sorted(missing)}")

        return table

    def read_stop_times(self, stop_ids: list[str]) -> pd.DataFrame:
        """Read complete sequences only for trips with two local stop visits."""
        scan = pl.scan_csv(
            self.folder / "stop_times.txt",
            infer_schema=False,
            missing_utf8_is_empty_string=True,
        )
        columns = scan.collect_schema().names()
        required = {
            "trip_id",
            "stop_id",
            "stop_sequence",
            "arrival_time",
            "departure_time",
        }
        if not required.issubset(columns):
            raise ValueError("Invalid stop_times.txt columns")

        # Keep the original endpoints for interpolation and frequency offsets.
        local_trips = (
            scan.filter(pl.col("stop_id").is_in(stop_ids))
            .group_by("trip_id")
            .len()
            .filter(pl.col("len") >= 2)
            .select("trip_id")
        )
        needed = required | {"pickup_type", "drop_off_type", "timepoint", "shape_dist_traveled"}
        return (
            scan.select([column for column in columns if column in needed])
            .join(local_trips, on="trip_id", how="semi")
            .collect()
            .to_pandas()
        )

    def clean_stop_times(self, times: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, int]]:
        """Parse times, interpolate internal blanks and discard invalid trips."""
        times["stop_sequence"] = pd.to_numeric(times.stop_sequence, errors="raise")
        times = times.sort_values(["trip_id", "stop_sequence"])
        if times.duplicated(["trip_id", "stop_sequence"]).any():
            raise ValueError("Duplicate stop_sequence within a trip")
        for col in ("arrival_time", "departure_time"):
            times[col] = self.time_seconds(times[col])
        for col in ("pickup_type", "drop_off_type"):
            if col not in times:
                times[col] = 0
            times[col] = pd.to_numeric(times[col].replace("", "0"), errors="raise")
            if not times[col].isin([0, 1, 2, 3]).all():
                raise ValueError(f"Invalid {col}")

        # Missing intermediate times are estimated by sequence position. Never
        # convert blanks to midnight or extrapolate beyond timed endpoints.
        missing_trips = times.loc[
            times[["arrival_time", "departure_time"]].isna().any(axis=1), "trip_id"
        ].unique()
        invalid_trips = set()
        groups = times.groupby("trip_id", sort=False)
        for trip_id in missing_trips:
            group = groups.get_group(trip_id)
            for col in ("arrival_time", "departure_time"):
                values = group[col].copy()
                if pd.isna(values.iloc[0]) or pd.isna(values.iloc[-1]):
                    invalid_trips.add(trip_id)
                    continue
                known = values.notna()
                times.loc[group.index, col] = np.interp(
                    group.stop_sequence,
                    group.loc[known, "stop_sequence"],
                    values[known],
                )
        previous = times.groupby("trip_id").departure_time.shift()
        invalid_trips.update(
            times.loc[
                (times.departure_time < times.arrival_time) | (times.arrival_time < previous),
                "trip_id",
            ]
        )
        if invalid_trips:
            logging.warning(
                "Dropping %s trips with missing endpoint times or backwards times from %s",
                len(invalid_trips),
                self.path,
            )
            times = times.loc[~times.trip_id.isin(invalid_trips)].copy()

        return times, {
            "dropped_invalid_trips": len(invalid_trips),
            "interpolated_trips": len(set(missing_trips) - invalid_trips),
        }

    @staticmethod
    def time_seconds(values: pd.Series) -> pd.Series:
        """Parse GTFS times, including hours after midnight, preserving blanks."""
        times = pl.from_pandas(values).replace("", None)
        if not times.str.contains(r"^\d+:[0-5]\d:[0-5]\d$").all():
            raise ValueError("Invalid GTFS time; expected HH:MM:SS")

        parts = times.str.split_exact(":", 2).struct.unnest()
        seconds = parts.select(
            pl.col("field_0").cast(pl.Float64) * 3600
            + pl.col("field_1").cast(pl.Float64) * 60
            + pl.col("field_2").cast(pl.Float64)
        ).to_series()
        return pd.Series(seconds.to_numpy(), index=values.index)

    def expand_frequencies(
        self,
        trips: pd.DataFrame,
        times: pd.DataFrame,
        starts: pd.Series,
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Expand frequency templates with one departure list and two joins."""
        frequencies = self.read_table("frequencies")
        if frequencies.empty:
            return trips, times

        required = {"trip_id", "start_time", "end_time", "headway_secs"}
        if not required.issubset(frequencies.columns):
            raise ValueError("Invalid frequencies.txt columns")
        frequencies = frequencies.loc[frequencies.trip_id.isin(trips.trip_id.unique())].copy()
        if frequencies.empty:
            return trips, times

        if "exact_times" in frequencies and not frequencies.exact_times.isin(["", "0", "1"]).all():
            raise ValueError("Invalid frequencies.txt exact_times")
        if "exact_times" not in frequencies or frequencies.exact_times.isin(["", "0"]).any():
            logging.warning(
                "Approximating frequency-based service by regular departures in %s", self.path
            )

        # Validate intervals before generating departures. Sorting is only used
        # for validation; original period order keeps generated IDs stable.
        for column in ("start_time", "end_time"):
            frequencies[column] = self.time_seconds(frequencies[column]).astype("int64")
        frequencies["headway_secs"] = frequencies.headway_secs.astype("int64")
        if (frequencies.headway_secs <= 0).any() or (
            frequencies.end_time <= frequencies.start_time
        ).any():
            raise ValueError("Invalid frequency interval")
        ordered = frequencies.sort_values(["trip_id", "start_time"])
        if (ordered.groupby("trip_id").end_time.shift() > ordered.start_time).any():
            raise ValueError("Overlapping frequency intervals")

        departures = (
            pl.from_pandas(frequencies)
            .select(
                "trip_id",
                pl.int_ranges("start_time", "end_time", "headway_secs").alias("departure"),
            )
            .explode("departure", empty_as_null=False)
            .to_pandas()
        )
        sequence = departures.groupby("trip_id", sort=False).cumcount().astype(str)
        departures["new_trip_id"] = departures.trip_id + ":frequency:" + sequence
        if departures.new_trip_id.isin(trips.trip_id.unique()).any():
            raise ValueError("Frequency trip ID collision")
        departures["offset"] = departures.pop("departure") - departures.trip_id.map(starts)

        # Reuse the departure list for both tables. Only stop times need a time
        # offset; every generated trip keeps its original route and calendar.
        expanded_tables = []
        for table, shift_times in ((trips, False), (times, True)):
            expanded = table.merge(departures, on="trip_id")
            if shift_times:
                expanded[["arrival_time", "departure_time"]] += expanded.offset.to_numpy()[:, None]
            expanded["trip_id"] = expanded.pop("new_trip_id")
            expanded = expanded.drop(columns="offset")
            scheduled = table.loc[~table.trip_id.isin(frequencies.trip_id.unique())]
            expanded_tables.append(pd.concat([scheduled, expanded], ignore_index=True))

        return expanded_tables[0], expanded_tables[1]

    def read_related_tables(
        self,
        stops: pd.DataFrame,
        trips: pd.DataFrame,
    ) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """Validate and prune agencies, routes and service calendars."""
        # Keep related tables consistent before merging different feed IDs.
        routes = self.read_table("routes", ("route_id", "route_type"))
        routes = routes.loc[routes.route_id.isin(trips.route_id.unique())].copy()
        if not trips.route_id.isin(routes.route_id).all():
            raise ValueError("trips.txt references unknown routes")
        agency = self.read_table("agency", ("agency_name",))
        if "agency_id" not in agency:
            if len(agency) != 1:
                raise ValueError("Multiple agencies require agency_id")
            agency["agency_id"] = "agency"
        for name, frame, key in (
            ("stops", stops, "stop_id"),
            ("routes", routes, "route_id"),
            ("agency", agency, "agency_id"),
        ):
            if frame[key].eq("").any() or frame[key].duplicated().any():
                raise ValueError(f"Empty or duplicate {key} in {name}.txt")
        if "agency_id" not in routes or routes.agency_id.eq("").any():
            if len(agency) != 1:
                raise ValueError("Routes with multiple agencies require agency_id")
            routes["agency_id"] = agency.agency_id.iloc[0]
        if not routes.agency_id.isin(agency.agency_id).all():
            raise ValueError("routes.txt references unknown agencies")
        agency = agency.loc[agency.agency_id.isin(routes.agency_id.unique())].copy()
        if "route_short_name" not in routes:
            routes["route_short_name"] = ""
        routes["route_short_name"] = routes.route_short_name.replace("", np.nan).fillna(
            routes.get("route_long_name", routes.route_id)
        )
        routes["route_type"] = pd.to_numeric(routes.route_type, errors="raise")

        # Services may be declared by a weekly calendar, dated exceptions, or both.
        calendar = self.read_table("calendar")
        exceptions = self.read_table("calendar_dates")
        for name, frame, required in (
            ("calendar", calendar, {"service_id", "tuesday", "start_date", "end_date"}),
            ("calendar_dates", exceptions, {"service_id", "date", "exception_type"}),
        ):
            if not frame.empty and not required.issubset(frame.columns):
                raise ValueError(f"Invalid {name}.txt columns")
        if calendar.empty and exceptions.empty:
            raise ValueError("No service calendar")
        calendar = (
            calendar.loc[calendar.service_id.isin(trips.service_id.unique())].copy()
            if not calendar.empty
            else calendar
        )
        exceptions = (
            exceptions.loc[exceptions.service_id.isin(trips.service_id.unique())].copy()
            if not exceptions.empty
            else exceptions
        )
        declared = set(calendar.get("service_id", [])) | set(exceptions.get("service_id", []))
        if not set(trips.service_id).issubset(declared):
            raise ValueError("Trips reference services without a calendar")

        return agency, routes, calendar, exceptions
