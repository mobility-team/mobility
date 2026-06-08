from __future__ import annotations

import io
import os
import shutil
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Sequence

import pandas as pd


GTFS_ROUTE_TYPES = {
    "train": 2,
    "bus": 3,
    "boat": 4,
}


@dataclass(frozen=True)
class GTFSStopSpec:
    """One stop used by a generated GTFS feed."""

    lon: float
    lat: float
    name: str | None = None

    def validate(self, stop_id: str) -> None:
        """Check that the stop has valid WGS84 coordinates."""

        try:
            lon = float(self.lon)
            lat = float(self.lat)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"GTFS stop {stop_id!r} must use numeric lon and lat.") from exc

        if not -180.0 <= lon <= 180.0:
            raise ValueError(f"GTFS stop {stop_id!r} lon must be between -180 and 180.")

        if not -90.0 <= lat <= 90.0:
            raise ValueError(f"GTFS stop {stop_id!r} lat must be between -90 and 90.")


@dataclass(frozen=True)
class GTFSLineSpec:
    """One line with a fixed stop order and a regular timetable."""

    stop_ids: list[str]
    segment_travel_times: list[float]
    start_time: float
    end_time: float
    period: float
    bidirectional: bool = True

    @classmethod
    def from_segments(
        cls,
        segments: Sequence[Sequence[float | str]],
        *,
        start_time: float,
        end_time: float,
        period: float,
        bidirectional: bool = True,
    ) -> GTFSLineSpec:
        """Build a line from ``(from_stop_id, to_stop_id, travel_time)`` segments."""

        if not segments:
            raise ValueError("GTFSLineSpec.from_segments segments must not be empty.")

        stop_ids = []
        segment_travel_times = []
        previous_to_stop_id = None
        for segment in segments:
            if len(segment) != 3:
                raise ValueError("Each segment must contain from_stop_id, to_stop_id and travel_time.")

            from_stop_id = str(segment[0])
            to_stop_id = str(segment[1])
            if previous_to_stop_id is None:
                stop_ids.append(from_stop_id)

            if previous_to_stop_id is not None and from_stop_id != previous_to_stop_id:
                raise ValueError(
                    "GTFSLineSpec.from_segments segments must be continuous: "
                    f"expected {previous_to_stop_id!r}, got {from_stop_id!r}."
                )

            stop_ids.append(to_stop_id)
            segment_travel_times.append(float(segment[2]))
            previous_to_stop_id = to_stop_id

        return cls(
            stop_ids=stop_ids,
            segment_travel_times=segment_travel_times,
            start_time=start_time,
            end_time=end_time,
            period=period,
            bidirectional=bidirectional,
        )

    @classmethod
    def from_stop_ids(
        cls,
        stop_ids: Sequence[str],
        segment_times: Mapping[tuple[str, str], float],
        *,
        start_time: float,
        end_time: float,
        period: float,
        bidirectional: bool = False,
    ) -> GTFSLineSpec:
        """Build a line from an ordered stop list and a segment-time lookup."""

        if len(stop_ids) < 2:
            raise ValueError("GTFSLineSpec.from_stop_ids stop_ids must contain at least two stops.")

        line_stop_ids = [str(stop_id) for stop_id in stop_ids]
        segment_travel_times = []
        for index in range(len(line_stop_ids) - 1):
            from_stop_id = line_stop_ids[index]
            to_stop_id = line_stop_ids[index + 1]
            try:
                segment_travel_times.append(float(segment_times[(from_stop_id, to_stop_id)]))
            except KeyError as exc:
                raise ValueError(
                    "GTFSLineSpec.from_stop_ids is missing a segment time for "
                    f"{from_stop_id!r} -> {to_stop_id!r}."
                ) from exc

        return cls(
            stop_ids=line_stop_ids,
            segment_travel_times=segment_travel_times,
            start_time=start_time,
            end_time=end_time,
            period=period,
            bidirectional=bidirectional,
        )

    def validate(self) -> None:
        """Check the line geometry and timetable."""

        if len(self.stop_ids) < 2:
            raise ValueError("GTFSLineSpec.stop_ids must contain at least two stops.")

        if len(self.segment_travel_times) != len(self.stop_ids) - 1:
            raise ValueError(
                "GTFSLineSpec.segment_travel_times must contain exactly len(stop_ids) - 1 values."
            )

        if any(travel_time <= 0 for travel_time in self.segment_travel_times):
            raise ValueError("GTFSLineSpec.segment_travel_times must be strictly positive.")

        if self.period <= 0:
            raise ValueError("GTFSLineSpec.period must be strictly positive.")

        if self.end_time < self.start_time:
            raise ValueError("GTFSLineSpec.end_time must be greater than or equal to start_time.")

    def reverse(self) -> GTFSLineSpec:
        """Return the same line in the opposite direction."""

        self.validate()
        return GTFSLineSpec(
            stop_ids=list(reversed(self.stop_ids)),
            segment_travel_times=list(reversed(self.segment_travel_times)),
            start_time=self.start_time,
            end_time=self.end_time,
            period=self.period,
            bidirectional=False,
        )


@dataclass(frozen=True)
class GTFSFeedSpec:
    """Declarative input for one generated GTFS zip feed."""

    agency_id: str
    agency_name: str
    route_id: str
    route_short_name: str
    route_type: str
    service_id: str
    stops: dict[str, GTFSStopSpec]
    lines: list[GTFSLineSpec]
    agency_timezone: str = "Europe/Paris"
    agency_url: str = ""

    def validate(self) -> None:
        """Check that the feed can be written as GTFS tables."""

        if not self.stops:
            raise ValueError("GTFSFeedSpec.stops must not be empty.")

        if not self.lines:
            raise ValueError("GTFSFeedSpec.lines must not be empty.")

        if self.route_type not in GTFS_ROUTE_TYPES:
            known_route_types = ", ".join(sorted(GTFS_ROUTE_TYPES))
            raise ValueError(f"GTFSFeedSpec.route_type must be one of: {known_route_types}.")

        if not self.agency_timezone:
            raise ValueError("GTFSFeedSpec.agency_timezone must not be empty.")

        for stop_id, stop in self.stops.items():
            stop.validate(stop_id)

        for line in self.lines:
            line.validate()

        unknown_stop_ids = {
            stop_id
            for line in self.lines
            for stop_id in line.stop_ids
            if stop_id not in self.stops
        }
        if unknown_stop_ids:
            unknown = ", ".join(sorted(unknown_stop_ids))
            raise ValueError(f"GTFSFeedSpec.lines reference unknown stop ids: {unknown}")


class GTFSBuilder:
    """Build one small GTFS feed with plain stops, lines and schedules."""

    def __init__(
        self,
        agency_id: str,
        agency_name: str,
        route_id: str,
        route_short_name: str,
        route_type: str,
        service_id: str,
        *,
        agency_timezone: str = "Europe/Paris",
        agency_url: str = "",
    ) -> None:
        self.agency_id = agency_id
        self.agency_name = agency_name
        self.route_id = route_id
        self.route_short_name = route_short_name
        self.route_type = route_type
        self.service_id = service_id
        self.agency_timezone = agency_timezone
        self.agency_url = agency_url
        self.stops: dict[str, GTFSStopSpec] = {}
        self.lines: list[GTFSLineSpec] = []

    def add_stop(self, stop_id: str, lon: float, lat: float, name: str | None = None) -> GTFSBuilder:
        """Add one stop to the feed."""

        self.stops[str(stop_id)] = GTFSStopSpec(lon=lon, lat=lat, name=name)
        return self

    def add_stops(self, stops: Mapping[str, GTFSStopSpec | Sequence[float]]) -> GTFSBuilder:
        """Add stops from ``GTFSStopSpec`` values or ``[lon, lat]`` lists."""

        for stop_id, stop in stops.items():
            if isinstance(stop, GTFSStopSpec):
                self.stops[str(stop_id)] = stop
                continue

            if len(stop) not in (2, 3):
                raise ValueError(f"Stop {stop_id!r} must be [lon, lat] or [lon, lat, name].")

            name = str(stop[2]) if len(stop) == 3 else None
            self.add_stop(str(stop_id), lon=float(stop[0]), lat=float(stop[1]), name=name)

        return self

    def add_line(
        self,
        segments: Sequence[Sequence[float | str]],
        *,
        start_time: float,
        end_time: float,
        period: float,
        bidirectional: bool = True,
    ) -> GTFSBuilder:
        """Add a line from ``(from_stop_id, to_stop_id, travel_time)`` segments."""

        self.lines.append(
            GTFSLineSpec.from_segments(
                segments,
                start_time=start_time,
                end_time=end_time,
                period=period,
                bidirectional=bidirectional,
            )
        )
        return self

    def add_line_from_stop_ids(
        self,
        stop_ids: Sequence[str],
        segment_times: Mapping[tuple[str, str], float],
        *,
        start_time: float,
        end_time: float,
        period: float,
        bidirectional: bool = False,
    ) -> GTFSBuilder:
        """Add a line from an ordered stop list and a segment-time lookup."""

        self.lines.append(
            GTFSLineSpec.from_stop_ids(
                stop_ids,
                segment_times,
                start_time=start_time,
                end_time=end_time,
                period=period,
                bidirectional=bidirectional,
            )
        )
        return self

    def build_feed(self) -> GTFSFeedSpec:
        """Return this builder as a feed specification."""

        return GTFSFeedSpec(
            agency_id=self.agency_id,
            agency_name=self.agency_name,
            route_id=self.route_id,
            route_short_name=self.route_short_name,
            route_type=self.route_type,
            service_id=self.service_id,
            stops=self.stops,
            lines=self.lines,
            agency_timezone=self.agency_timezone,
            agency_url=self.agency_url,
        )

    def write_zip(self, path: str | Path) -> Path:
        """Write this feed as a GTFS zip file."""

        return build_gtfs_zip(self.build_feed(), path)

    def write_project_zip(self, file_name: str) -> Path:
        """Write this feed under ``MOBILITY_PROJECT_DATA_FOLDER``."""

        return build_project_gtfs_zip(self.build_feed(), file_name)


def build_project_gtfs_zip(feed: GTFSFeedSpec, file_name: str) -> Path:
    """Build one GTFS zip under ``MOBILITY_PROJECT_DATA_FOLDER``."""

    project_data_folder = os.environ.get("MOBILITY_PROJECT_DATA_FOLDER")
    if project_data_folder is None:
        raise ValueError(
            "MOBILITY_PROJECT_DATA_FOLDER is not set. "
            "Use mobility.set_params(...) before write_project_zip()."
        )

    output_path = Path(project_data_folder) / file_name
    return build_gtfs_zip(feed, output_path)


def build_gtfs_zip(feed: GTFSFeedSpec, output_path: str | Path) -> Path:
    """Build one GTFS zip from a feed specification."""

    feed.validate()
    tables = _create_gtfs_tables(feed)
    output_path = Path(output_path)
    _write_gtfs_zip(tables, output_path)
    return output_path


def _create_gtfs_tables(feed: GTFSFeedSpec) -> dict[str, pd.DataFrame]:
    """Create the GTFS tables needed by Mobility routing."""

    agency = pd.DataFrame(
        {
            "agency_id": [feed.agency_id],
            "agency_name": [feed.agency_name],
            "agency_url": [feed.agency_url],
            "agency_timezone": [feed.agency_timezone],
        }
    )

    routes = pd.DataFrame(
        {
            "route_id": [feed.route_id],
            "agency_id": [feed.agency_id],
            "route_short_name": [feed.route_short_name],
            "route_type": [GTFS_ROUTE_TYPES[feed.route_type]],
        }
    )

    calendar = pd.DataFrame(
        {
            "service_id": [feed.service_id],
            "monday": [1],
            "tuesday": [1],
            "wednesday": [1],
            "thursday": [1],
            "friday": [1],
            "saturday": [1],
            "sunday": [1],
            "start_date": [20000101],
            "end_date": [21001231],
        }
    )

    stops = pd.DataFrame(
        [
            {
                "stop_id": stop_id,
                "stop_name": stop.name or stop_id,
                "stop_lon": stop.lon,
                "stop_lat": stop.lat,
            }
            for stop_id, stop in feed.stops.items()
        ]
    )

    stop_times = pd.concat(_build_stop_times(feed.lines), ignore_index=True)
    trips = pd.DataFrame(
        {
            "route_id": feed.route_id,
            "service_id": feed.service_id,
            "trip_id": stop_times["trip_id"].unique(),
        }
    )

    return {
        "agency.txt": agency,
        "trips.txt": trips,
        "routes.txt": routes,
        "calendar.txt": calendar,
        "stops.txt": stops,
        "stop_times.txt": stop_times,
    }


def _build_stop_times(lines: list[GTFSLineSpec]) -> list[pd.DataFrame]:
    """Create stop times for all requested directions."""

    stop_times = []
    for line_index, line in enumerate(lines):
        stop_times.append(_create_stop_times(line, trip_id_prefix=f"trip_{line_index}_0"))

        if line.bidirectional:
            stop_times.append(_create_stop_times(line.reverse(), trip_id_prefix=f"trip_{line_index}_1"))

    return stop_times


def _create_stop_times(line: GTFSLineSpec, trip_id_prefix: str) -> pd.DataFrame:
    """Create ordered GTFS stop times for one line direction."""

    rows = []
    for departure_index, departure_time in enumerate(_departure_times(line.start_time, line.end_time, line.period)):
        trip_id = f"{trip_id_prefix}_{departure_index}"
        elapsed_time = 0.0

        for stop_index, stop_id in enumerate(line.stop_ids):
            stop_time = departure_time + elapsed_time
            rows.append(
                {
                    "trip_id": trip_id,
                    "stop_id": stop_id,
                    "arrival_time": _format_gtfs_time(stop_time),
                    "departure_time": _format_gtfs_time(stop_time),
                    "stop_sequence": stop_index + 1,
                }
            )

            if stop_index < len(line.segment_travel_times):
                elapsed_time += line.segment_travel_times[stop_index]

    return pd.DataFrame(
        rows,
        columns=["trip_id", "stop_id", "arrival_time", "departure_time", "stop_sequence"],
    )


def _departure_times(start_time: float, end_time: float, period: float) -> list[float]:
    """Return regular departures between start and end, both in seconds."""

    departures = []
    departure_time = float(start_time)
    while departure_time <= float(end_time) + 1e-9:
        departures.append(departure_time)
        departure_time += float(period)

    return departures


def _format_gtfs_time(seconds: float) -> str:
    """Format seconds after midnight as a GTFS time string."""

    if seconds < 0:
        raise ValueError("GTFS times must not be negative.")

    total_seconds = int(round(seconds))
    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    remaining_seconds = total_seconds % 60
    return f"{hours:02d}:{minutes:02d}:{remaining_seconds:02d}"


def _write_gtfs_zip(tables: Mapping[str, pd.DataFrame], path: Path) -> None:
    """Write GTFS tables into one zip archive."""

    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
        for name, df in tables.items():
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False)
            zip_file.writestr(name, csv_buffer.getvalue())

    zip_buffer.seek(0)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "wb") as file:
        shutil.copyfileobj(zip_buffer, file)
