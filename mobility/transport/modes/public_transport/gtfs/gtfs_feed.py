"""Read and validate one GTFS feed before merging it with other feeds."""

import csv
import hashlib
import logging
import zipfile
from pathlib import Path, PurePosixPath

import geopandas as gpd
import numpy as np
import pandas as pd
import polars as pl
from shapely.geometry.base import BaseGeometry


class GTFSFeed:
    """One GTFS ZIP file containing routes, vehicle trips and service calendars.

    Extraction and hashing share a single ZIP pass. Polars filters the large
    stop-time file before it is converted to pandas for timetable validation.
    GTFSTimetable supplies the temporary folder and removes it after reading.
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

    def __init__(self, path: str | Path, folder: Path, exclusions=None, source_id=None) -> None:
        self.path = Path(path)
        self.folder = folder
        self.diagnostics: dict[str, int] = {}
        self.agency_inventory = ""
        self.exclusions = exclusions or {}
        self.source_id = source_id or str(path)
        self.removed_trips = set()
        self.exclusion_diagnostics = {}

    def prepare_exclusions(self):
        """Resolve original IDs before validating calendars or filtering trips."""
        if not self.exclusions:
            return
        unknown = set(self.exclusions) - {"trips", "services", "routes"}
        if unknown:
            raise ValueError(f"Unknown gtfs_exclusions categories: {sorted(unknown)}")
        for name, values in self.exclusions.items():
            if not isinstance(values, list) or any(not isinstance(v, str) or not v for v in values):
                raise ValueError(f"gtfs_exclusions {name} must be a list of nonempty strings")
        trips = self.read_table("trips", ("trip_id", "route_id", "service_id"),
                                columns=("trip_id", "route_id", "service_id"))
        routes = self.read_table("routes", ("route_id",), columns=("route_id",))
        calendars = [self.read_table(name, columns=("service_id",)) for name in ("calendar", "calendar_dates")]
        known = {"trips": set(trips.trip_id), "routes": set(routes.route_id),
                 "services": set().union(*(set(table.get("service_id", [])) for table in calendars))}
        for name, values in self.exclusions.items():
            missing = set(values) - known[name]
            if missing:
                raise ValueError(f"Unknown {name} in gtfs_exclusions: {sorted(missing)}")
        removed = trips.loc[
            trips.trip_id.isin(self.exclusions.get("trips", []))
            | trips.route_id.isin(self.exclusions.get("routes", []))
            | trips.service_id.isin(self.exclusions.get("services", []))
        ]
        self.removed_trips = set(removed.trip_id)
        self.exclusion_diagnostics = {
            "excluded_trip_ids": ",".join(sorted(self.removed_trips)),
            "excluded_trip_count": len(self.removed_trips),
            "excluded_services": ",".join(sorted(self.exclusions.get("services", []))),
            "excluded_routes": ",".join(sorted(self.exclusions.get("routes", []))),
        }

    def problem(self, message, category, ids):
        """Identify records that can be explicitly removed, without guessing a repair."""
        ids = sorted(set(str(value) for value in ids if str(value)))
        if ids:
            example = {self.source_id: {category: ids[:10]}}
            message += (f"; affected {category}: {ids[:10]}"
                        f". To remove these records and their dependent trips, add {example!r} to gtfs_exclusions."
                        " Review the lost service first.")
            if len(ids) > 10:
                message += f" Showing 10 of {len(ids)} IDs."
        raise ValueError(message)

    @staticmethod
    def calendar_dates(values):
        """Reject dates outside pandas' nanosecond range before building service dates."""
        in_range = values.str.match(r"^\d{8}$") & values.ge("16770922") & values.le("22620411")
        return pd.to_datetime(values.where(in_range), format="%Y%m%d", errors="coerce")

    def read(self, boundary: BaseGeometry, calendar: pd.DataFrame, exceptions: pd.DataFrame) -> dict[str, pd.DataFrame] | None:
        """Return trips with two local stop visits, or None if none can be retained."""
        stops = self.read_table(
            "stops",
            ("stop_id", "stop_lat", "stop_lon"),
            columns=("stop_id", "stop_lat", "stop_lon", "stop_name", "parent_station"),
        )
        stops["stop_lon"] = pd.to_numeric(stops.stop_lon, errors="coerce")
        stops["stop_lat"] = pd.to_numeric(stops.stop_lat, errors="coerce")
        stops = stops.dropna(subset=["stop_lon", "stop_lat"])
        points = gpd.GeoSeries(gpd.points_from_xy(stops.stop_lon, stops.stop_lat), crs=4326)
        stops = stops.loc[points.intersects(boundary).to_numpy()].copy()
        if stops.empty:
            return None

        trips = self.read_table(
            "trips",
            ("trip_id", "route_id", "service_id"),
            columns=("trip_id", "route_id", "service_id"),
        )
        trips = trips.loc[~trips.trip_id.isin(self.removed_trips)].copy()
        if trips.trip_id.duplicated().any():
            self.problem("Duplicate trip_id in trips.txt", "trips", trips.loc[trips.trip_id.duplicated(False), "trip_id"])

        times = self.read_stop_times(stops.stop_id.tolist())
        if times.empty:
            return None
        if not set(times.trip_id.unique()).issubset(trips.trip_id.to_numpy()):
            raise ValueError("stop_times.txt references unknown trips; repair the feed before excluding records")

        times, diagnostics = self.clean_stop_times(times)
        starts = times.groupby("trip_id").departure_time.first()
        times = times.loc[times.stop_id.isin(stops.stop_id)].copy()
        # The scan already required two local visits. Validation only removes
        # whole trips, so their remaining sequences do not need another count.
        trips = trips.loc[trips.trip_id.isin(times.trip_id.unique())].copy()
        if trips.empty:
            return None

        trips, times = self.expand_frequencies(trips, times, starts)

        agency, routes, calendar, exceptions = self.read_related_tables(stops, trips, calendar, exceptions)

        transfers = self.read_table(
            "transfers",
            columns=(
                "from_stop_id",
                "to_stop_id",
                "transfer_type",
                "min_transfer_time",
                "from_route_id",
                "to_route_id",
                "from_trip_id",
                "to_trip_id",
            ),
        )
        for column, excluded in (("from_trip_id", self.removed_trips), ("to_trip_id", self.removed_trips),
                                 ("from_route_id", self.exclusions.get("routes", [])),
                                 ("to_route_id", self.exclusions.get("routes", []))):
            if column in transfers:
                transfers = transfers.loc[~transfers[column].isin(excluded)].copy()
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

    def extract(self, table_names: tuple[str, ...] | None = None) -> str:
        """Extract requested tables and return a checksum of their contents."""
        digest = hashlib.sha256()

        with zipfile.ZipFile(self.path) as archive:
            paths = self.table_paths(archive)
            for name in sorted(self.table_names if table_names is None else table_names):
                filename = name + ".txt"
                if filename not in paths:
                    continue

                digest.update(filename.encode())
                with (
                    archive.open(paths[filename]) as source,
                    (self.folder / filename).open("wb") as target,
                ):
                    for chunk in iter(lambda: source.read(1024 * 1024), b""):
                        digest.update(chunk)
                        target.write(chunk)

        return digest.hexdigest()

    @classmethod
    def table_paths(cls, archive: zipfile.ZipFile) -> dict[str, str]:
        """Locate one feed's tables, at the ZIP root or inside a shared folder."""
        paths = {}
        folders = set()
        filenames = {name + ".txt" for name in cls.table_names}
        for member in archive.infolist():
            path = PurePosixPath(member.filename)
            if member.is_dir() or path.name not in filenames:
                continue
            if path.name in paths:
                raise ValueError(f"multiple copies of {path.name} in the ZIP")
            paths[path.name] = member.filename
            folders.add(path.parent)
        if len(folders) > 1:
            raise ValueError("GTFS tables are spread across multiple folders in the ZIP")
        return paths

    def read_table(
        self,
        name: str,
        required: tuple[str, ...] = (),
        *,
        columns: tuple[str, ...],
    ) -> pd.DataFrame:
        """Read selected table columns while retaining identifiers such as '001'."""
        path = self.folder / (name + ".txt")
        if not path.exists():
            if required:
                raise ValueError(f"Missing {path.name}")
            return pd.DataFrame()

        # Read only fields used by the preparation code. Optional fields are
        # selected when present, so valid GTFS variants remain supported.
        with path.open("r", encoding="utf-8-sig") as source:
            available = next(csv.reader(source), [])
        if not available and not required:
            logging.warning("Ignoring empty optional GTFS table %s", path)
            return pd.DataFrame()
        selected = [column for column in available if column.strip() in columns] or None
        table = pl.read_csv(
            path,
            columns=selected,
            infer_schema=False,
            empty_string_is_null=False,
        )
        # Some publishers pad CSV lines, including the final header and value.
        table = table.rename({name: name.strip() for name in table.columns})
        table = table.with_columns(pl.all().str.strip_chars_end()).to_pandas()
        missing = set(required) - set(table.columns)
        if missing:
            raise ValueError(f"{path.name} is missing columns: {sorted(missing)}")

        return table

    def read_stop_times(self, stop_ids: list[str]) -> pd.DataFrame:
        """Read complete sequences only for trips with two local stop visits."""
        scan = pl.scan_csv(
            self.folder / "stop_times.txt",
            infer_schema=False,
            empty_string_is_null=False,
        )
        columns = scan.collect_schema().names()
        scan = scan.rename({name: name.strip() for name in columns})
        columns = [name.strip() for name in columns]
        required = {
            "trip_id",
            "stop_id",
            "stop_sequence",
            "arrival_time",
            "departure_time",
        }
        if not required.issubset(columns):
            raise ValueError("Invalid stop_times.txt columns")
        needed = required | {"pickup_type", "drop_off_type", "timepoint", "shape_dist_traveled"}
        scan = scan.select([column for column in columns if column in needed]).with_columns(
            pl.all().str.strip_chars_end()
        )
        if self.removed_trips:
            scan = scan.filter(~pl.col("trip_id").is_in(sorted(self.removed_trips)))

        # Keep the original endpoints for interpolation and frequency offsets.
        local_trips = (
            scan.filter(pl.col("stop_id").is_in(stop_ids))
            .group_by("trip_id")
            .len()
            .filter(pl.col("len") >= 2)
            .select("trip_id")
        )
        return (
            scan.join(local_trips, on="trip_id", how="semi")
            .collect()
            .to_pandas()
        )

    def clean_stop_times(self, times: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, int]]:
        """Estimate missing intermediate times and remove trips with unusable times."""
        # Parse numeric columns in Polars before pandas sorts and groups trips.
        # Invalid values become NaN and are rejected by the checks below.
        times["stop_sequence"] = (
            pl.from_pandas(times.stop_sequence).cast(pl.Float64, strict=False).to_numpy()
        )
        if times.stop_sequence.isna().any():
            self.problem("Invalid stop_sequence", "trips", times.loc[times.stop_sequence.isna(), "trip_id"])
        times = times.sort_values(["trip_id", "stop_sequence"])
        if times.duplicated(["trip_id", "stop_sequence"]).any():
            self.problem("Duplicate stop_sequence within a trip", "trips", times.loc[times.duplicated(["trip_id", "stop_sequence"], keep=False), "trip_id"])
        for col in ("arrival_time", "departure_time"):
            try:
                times[col] = self.time_seconds(times[col])
            except ValueError:
                bad = ~times[col].str.match(r"^(\d+:[0-5]\d:[0-5]\d)?$")
                self.problem(f"Invalid {col}; expected HH:MM:SS", "trips", times.loc[bad, "trip_id"])
        for col in ("pickup_type", "drop_off_type"):
            if col not in times:
                times[col] = 0
                continue

            times[col] = (
                pl.from_pandas(times[col])
                .replace("", "0")
                .cast(pl.Float64, strict=False)
                .to_numpy()
            )
            if not times[col].isin([0, 1, 2, 3]).all():
                self.problem(f"Invalid {col}", "trips", times.loc[~times[col].isin([0, 1, 2, 3]), "trip_id"])

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
                values = group[col]
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
                "Removing %s vehicle trips with missing first/last times or times out of order from %s",
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
        """Create vehicle departures at the intervals specified in frequencies.txt.

        Each departure retains the original trip's travel and stopping times.
        """
        frequencies = self.read_table(
            "frequencies",
            columns=("trip_id", "start_time", "end_time", "headway_secs", "exact_times"),
        )
        # Keep the original interval on generated trips. For headway-based
        # service, the generated departures are only a routing approximation;
        # overlap checks must compare the original windows and headways.
        trips = trips.copy()
        trips["_frequency_kind"] = "scheduled"
        trips["_frequency_template"] = ""
        for column in ("_frequency_start", "_frequency_end", "_frequency_headway", "_frequency_departure"):
            trips[column] = 0
        if frequencies.empty:
            return trips, times

        required = {"trip_id", "start_time", "end_time", "headway_secs"}
        if not required.issubset(frequencies.columns):
            raise ValueError("Invalid frequencies.txt columns")
        frequencies = frequencies.loc[frequencies.trip_id.isin(trips.trip_id.unique())].copy()
        if frequencies.empty:
            return trips, times

        if "exact_times" in frequencies and not frequencies.exact_times.isin(["", "0", "1"]).all():
            self.problem("Invalid frequencies.txt exact_times", "trips", frequencies.loc[~frequencies.exact_times.isin(["", "0", "1"]), "trip_id"])
        if "exact_times" not in frequencies or frequencies.exact_times.isin(["", "0"]).any():
            logging.warning(
                "Approximating frequency-based service by regular departures in %s", self.path
            )

        # Validate intervals before generating departures. Sorting is only used
        # for validation; original period order keeps generated IDs stable.
        for column in ("start_time", "end_time"):
            valid = frequencies[column].str.match(r"^\d+:[0-5]\d:[0-5]\d$")
            if not valid.all():
                self.problem(f"Invalid frequency {column}", "trips", frequencies.loc[~valid, "trip_id"])
            frequencies[column] = self.time_seconds(frequencies[column]).astype("int64")
        headways = pd.to_numeric(frequencies.headway_secs, errors="coerce")
        valid = headways.notna() & (headways % 1 == 0)
        if not valid.all():
            self.problem("Invalid frequency headway_secs", "trips", frequencies.loc[~valid, "trip_id"])
        frequencies["headway_secs"] = headways.astype("int64")
        if "exact_times" not in frequencies:
            frequencies["exact_times"] = "0"
        frequencies["exact_times"] = frequencies["exact_times"].replace("", "0")
        if (frequencies.headway_secs <= 0).any() or (
            frequencies.end_time <= frequencies.start_time
        ).any():
            bad = (frequencies.headway_secs <= 0) | (frequencies.end_time <= frequencies.start_time)
            self.problem("Invalid frequency interval", "trips", frequencies.loc[bad, "trip_id"])
        ordered = frequencies.sort_values(["trip_id", "start_time"])
        if (ordered.groupby("trip_id").end_time.shift() > ordered.start_time).any():
            bad = ordered.groupby("trip_id").end_time.shift() > ordered.start_time
            self.problem("Overlapping frequency intervals", "trips", ordered.loc[bad, "trip_id"])

        departures = (
            pl.from_pandas(frequencies)
            .select(
                "trip_id",
                "exact_times",
                "start_time",
                "end_time",
                "headway_secs",
                pl.int_ranges("start_time", "end_time", "headway_secs").alias("departure"),
            )
            .explode("departure", empty_as_null=False)
            .to_pandas()
        )
        sequence = departures.groupby("trip_id", sort=False).cumcount().astype(str)
        departures["new_trip_id"] = departures.trip_id + ":frequency:" + sequence
        if departures.new_trip_id.isin(trips.trip_id.unique()).any():
            raise ValueError("Generated frequency trip_id already exists in trips.txt")
        departures["offset"] = departures.departure - departures.trip_id.map(starts)

        # Reuse the departure list for both tables. Only stop times need a time
        # offset; every generated trip keeps its original route and calendar.
        expanded_tables = []
        for table, shift_times in ((trips, False), (times, True)):
            if shift_times:
                # Stop times are much larger than trips. They only need the
                # generated ID and offset, not the interval metadata.
                expanded = table.merge(departures[["trip_id", "new_trip_id", "offset"]], on="trip_id")
                expanded[["arrival_time", "departure_time"]] += expanded.offset.to_numpy()[:, None]
                expanded = expanded.drop(columns="offset")
            else:
                expanded = table.merge(departures, on="trip_id")
                expanded["_frequency_kind"] = expanded["exact_times"].map({"0": "headway", "1": "exact"})
                expanded["_frequency_template"] = expanded["trip_id"]
                expanded["_frequency_start"] = expanded["start_time"]
                expanded["_frequency_end"] = expanded["end_time"]
                expanded["_frequency_headway"] = expanded["headway_secs"]
                expanded["_frequency_departure"] = expanded["departure"]
                expanded = expanded.drop(columns=["offset", "exact_times", "start_time", "end_time", "headway_secs", "departure"])
            expanded["trip_id"] = expanded.pop("new_trip_id")
            scheduled = table.loc[~table.trip_id.isin(frequencies.trip_id.unique())]
            expanded_tables.append(pd.concat([scheduled, expanded], ignore_index=True))

        return expanded_tables[0], expanded_tables[1]

    def read_calendars(self) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Read service calendars once, before preparing stops and trips."""
        # Services may be declared by a weekly calendar, dated exceptions, or both.
        calendar_columns = ("service_id", "monday", "tuesday", "wednesday", "thursday", "friday",
                            "saturday", "sunday", "start_date", "end_date")
        calendar = self.read_table("calendar", columns=calendar_columns)
        exceptions = self.read_table(
            "calendar_dates",
            columns=("service_id", "date", "exception_type"),
        )
        had_calendar = not calendar.empty or not exceptions.empty
        for frame in (calendar, exceptions):
            if "service_id" in frame:
                frame.drop(frame.index[frame.service_id.isin(self.exclusions.get("services", []))], inplace=True)
        for name, frame, required in (
            ("calendar", calendar, set(calendar_columns)),
            ("calendar_dates", exceptions, {"service_id", "date", "exception_type"}),
        ):
            if not frame.empty and not required.issubset(frame.columns):
                raise ValueError(f"Invalid {name}.txt columns")
        if calendar.empty and exceptions.empty:
            if had_calendar and self.exclusions.get("services"):
                return calendar, exceptions
            raise ValueError("No service calendar")
        weekdays = ("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday")
        if not calendar.empty:
            for column in ("start_date", "end_date"):
                dates = self.calendar_dates(calendar[column])
                if dates.isna().any():
                    bad = calendar.loc[dates.isna()]
                    self.problem(f"Invalid or out-of-range calendar {column}: {bad[column].tolist()[:10]}", "services", bad.service_id)
                calendar[column] = dates
                if calendar[column].isna().any():
                    raise ValueError(f"Missing calendar {column}")
            if (calendar.end_date < calendar.start_date).any():
                self.problem("Calendar end date precedes start date", "services", calendar.loc[calendar.end_date < calendar.start_date, "service_id"])
            if calendar.service_id.duplicated().any():
                self.problem("Duplicate service_id in calendar.txt", "services", calendar.loc[calendar.service_id.duplicated(False), "service_id"])
            for weekday in weekdays:
                calendar[weekday] = pd.to_numeric(calendar[weekday], errors="coerce")
                if not calendar[weekday].isin([0, 1]).all():
                    self.problem(f"Invalid calendar {weekday} flag", "services", calendar.loc[~calendar[weekday].isin([0, 1]), "service_id"])
        if not exceptions.empty:
            dates = self.calendar_dates(exceptions.date)
            if dates.isna().any():
                bad = exceptions.loc[dates.isna()]
                self.problem(f"Invalid or out-of-range calendar exception date: {bad.date.tolist()[:10]}", "services", bad.service_id)
            exceptions["date"] = dates
            if exceptions.date.isna().any():
                raise ValueError("Missing calendar exception date")
            exceptions["exception_type"] = pd.to_numeric(exceptions.exception_type, errors="coerce")
            if not exceptions.exception_type.isin([1, 2]).all():
                self.problem("Invalid calendar exception_type", "services", exceptions.loc[~exceptions.exception_type.isin([1, 2]), "service_id"])
            exceptions = exceptions.drop_duplicates()
        return calendar, exceptions

    def read_related_tables(
        self,
        stops: pd.DataFrame,
        trips: pd.DataFrame,
        calendar: pd.DataFrame,
        exceptions: pd.DataFrame,
    ) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """Check agencies, routes and calendars, keeping those used by retained trips."""
        # Each retained trip needs a known route, operator and operating calendar.
        routes = self.read_table(
            "routes",
            ("route_id", "route_type"),
            columns=(
                "route_id",
                "route_type",
                "agency_id",
                "route_short_name",
                "route_long_name",
            ),
        )
        routes = routes.loc[routes.route_id.isin(trips.route_id.unique())].copy()
        if not trips.route_id.isin(routes.route_id).all():
            self.problem("trips.txt references unknown routes", "trips", trips.loc[~trips.route_id.isin(routes.route_id), "trip_id"])
        agency = self.read_table(
            "agency",
            ("agency_name",),
            columns=("agency_name", "agency_id"),
        )
        has_agency_id = "agency_id" in agency
        if not has_agency_id:
            if len(agency) != 1:
                raise ValueError("Multiple agencies require agency_id")
            agency["agency_id"] = "agency"
        # Record the complete feed inventory before filtering to local routes.
        # A regional aggregate can contain many operators outside this study.
        self.agency_inventory = "\n".join(sorted(
            f"{row.agency_name} [{row.agency_id}]" if has_agency_id else row.agency_name
            for row in agency.itertuples(index=False)
        ))
        for name, frame, key in (
            ("stops", stops, "stop_id"),
            ("routes", routes, "route_id"),
            ("agency", agency, "agency_id"),
        ):
            if frame[key].eq("").any() or frame[key].duplicated().any():
                if name == "routes" and not frame[key].eq("").any():
                    self.problem("Empty or duplicate route_id in routes.txt", "routes", frame.loc[frame[key].duplicated(False), key])
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
        if not set(trips.service_id.unique()).issubset(declared):
            self.problem("Trips reference services without a calendar", "trips", trips.loc[~trips.service_id.isin(declared), "trip_id"])

        return agency, routes, calendar, exceptions
