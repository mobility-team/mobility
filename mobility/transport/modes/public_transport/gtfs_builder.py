from __future__ import annotations

import io
import shutil
import zipfile
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd
import shortuuid


@dataclass(frozen=True)
class GTFSStopSpec:
    """One stop definition used by the simplified GTFS builder."""

    lon: float
    lat: float
    name: str | None = None


@dataclass(frozen=True)
class GTFSLineSpec:
    """One simplified line with fixed stop order and timetable."""

    stop_ids: list[str]
    segment_travel_times: list[float]
    start_time: float
    end_time: float
    period: float
    bidirectional: bool = True

    def validate(self) -> None:
        """Validate the simplified line specification."""

        if len(self.stop_ids) < 2:
            raise ValueError("GTFSLineSpec.stop_ids must contain at least two stops.")

        if len(self.segment_travel_times) != len(self.stop_ids) - 1:
            raise ValueError(
                "GTFSLineSpec.segment_travel_times must contain exactly len(stop_ids) - 1 values."
            )

        if self.period <= 0:
            raise ValueError("GTFSLineSpec.period must be strictly positive.")

    def to_travel_times(self) -> list[list[float | str]]:
        """Return ``[from_stop_id, to_stop_id, travel_time]`` rows for one direction."""

        self.validate()
        return [
            [self.stop_ids[index], self.stop_ids[index + 1], self.segment_travel_times[index]]
            for index in range(len(self.segment_travel_times))
        ]


@dataclass(frozen=True)
class GTFSFeedSpec:
    """Declarative input for generating one simple GTFS zip feed."""

    agency_id: str
    agency_name: str
    route_id: str
    route_short_name: str
    route_type: str
    service_id: str
    stops: dict[str, GTFSStopSpec]
    lines: list[GTFSLineSpec]

    def validate(self) -> None:
        """Validate the feed-level specification."""

        if not self.stops:
            raise ValueError("GTFSFeedSpec.stops must not be empty.")

        if not self.lines:
            raise ValueError("GTFSFeedSpec.lines must not be empty.")

        unknown_stop_ids = {
            stop_id
            for line in self.lines
            for stop_id in line.stop_ids
            if stop_id not in self.stops
        }
        if unknown_stop_ids:
            unknown = ", ".join(sorted(unknown_stop_ids))
            raise ValueError(f"GTFSFeedSpec.lines reference unknown stop ids: {unknown}")


def create_gtfs_tables(
    agency_id: str,
    agency_name: str,
    route_id: str,
    route_short_name: str,
    route_type: str,
    service_id: str,
    stops: dict[str, tuple[str, list[float]]],
    stop_times: list[pd.DataFrame],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Create the standard GTFS tables for one simplified feed."""

    agency = pd.DataFrame(
        {
            "agency_id": agency_id,
            "agency_name": agency_name,
        },
        index=[0],
    )

    gtfs_route_types = {
        "train": 2,
        "bus": 3,
        "boat": 4,
    }

    routes = pd.DataFrame(
        {
            "route_id": route_id,
            "agency_id": agency_id,
            "route_short_name": route_short_name,
            "route_type": gtfs_route_types[route_type],
        },
        index=[0],
    )

    calendar = pd.DataFrame(
        {
            "service_id": service_id,
            "monday": 1,
            "tuesday": 1,
            "wednesday": 1,
            "thursday": 1,
            "friday": 1,
            "saturday": 1,
            "sunday": 1,
            "start_date": 20000101,
            "end_date": 21001231,
        },
        index=[0],
    )

    stops_table = pd.DataFrame.from_dict(
        [
            {
                "stop_id": stop_id,
                "stop_name": stop_name,
                "stop_lon": coords[0],
                "stop_lat": coords[1],
            }
            for stop_id, (stop_name, coords) in stops.items()
        ]
    )

    stop_times_table = pd.concat(stop_times)
    trips = pd.DataFrame(
        {
            "route_id": route_id,
            "service_id": service_id,
            "trip_id": stop_times_table["trip_id"].unique(),
        }
    )

    return agency, routes, trips, calendar, stops_table, stop_times_table


def create_stop_times(
    travel_times: list[list[float | str]],
    start_time: float,
    end_time: float,
    period: float,
) -> pd.DataFrame:
    """Create one GTFS ``stop_times`` table from a simple line description."""

    travel_times_df = pd.DataFrame(
        travel_times,
        columns=["from_stop_id", "to_stop_id", "travel_time"],
    )

    travel_times_df["cum_travel_time"] = travel_times_df["travel_time"].cumsum()
    travel_times_df["prev_cum_travel_time"] = travel_times_df["cum_travel_time"].shift(1, fill_value=0.0)

    departure_times = np.arange(start_time, end_time + period, period)
    trip_ids = [shortuuid.uuid() for _ in range(len(departure_times))]

    stop_times = pd.concat([travel_times_df] * len(departure_times))
    stop_times["trip_id"] = np.repeat(trip_ids, travel_times_df.shape[0])
    stop_times["trip_departure_time"] = np.repeat(departure_times, travel_times_df.shape[0])

    stop_times["departure_time"] = stop_times["trip_departure_time"] + stop_times["prev_cum_travel_time"]
    stop_times["arrival_time"] = stop_times["trip_departure_time"] + stop_times["cum_travel_time"]

    stop_times["departure_time"] = pd.to_datetime(stop_times["departure_time"], unit="s").dt.strftime("%H:%M:%S")
    stop_times["arrival_time"] = pd.to_datetime(stop_times["arrival_time"], unit="s").dt.strftime("%H:%M:%S")

    stop_times = pd.concat(
        [
            stop_times[["trip_id", "from_stop_id", "departure_time"]]
            .rename({"from_stop_id": "stop_id", "departure_time": "time"}, axis=1)
            .assign(var="departure"),
            stop_times[["trip_id", "to_stop_id", "arrival_time"]]
            .rename({"to_stop_id": "stop_id", "arrival_time": "time"}, axis=1)
            .assign(var="arrival"),
        ]
    )

    stop_times = stop_times.pivot(columns="var", index=["trip_id", "stop_id"], values="time")
    stop_times["arrival"] = stop_times["arrival"].fillna(stop_times["departure"])
    stop_times["departure"] = stop_times["departure"].fillna(stop_times["arrival"])
    stop_times = stop_times.sort_values(["trip_id", "departure"])
    stop_times["stop_sequence"] = stop_times.groupby("trip_id").cumcount() + 1
    stop_times = stop_times.rename({"arrival": "arrival_time", "departure": "departure_time"}, axis=1)

    return stop_times.reset_index()


def zip_gtfs_tables(
    agency: pd.DataFrame,
    routes: pd.DataFrame,
    trips: pd.DataFrame,
    calendar: pd.DataFrame,
    stops: pd.DataFrame,
    stop_times: pd.DataFrame,
    path: str | Path,
) -> None:
    """Write GTFS tables into one zip archive."""

    tables = {
        "agency.txt": agency,
        "trips.txt": trips,
        "routes.txt": routes,
        "calendar.txt": calendar,
        "stops.txt": stops,
        "stop_times.txt": stop_times,
    }

    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
        for name, df in tables.items():
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False)
            zip_file.writestr(name, csv_buffer.getvalue())

    zip_buffer.seek(0)
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "wb") as file:
        shutil.copyfileobj(zip_buffer, file)


def build_gtfs_zip(feed: GTFSFeedSpec, output_path: str | Path) -> Path:
    """Build one GTFS zip from a declarative feed specification."""

    feed.validate()

    stop_times_tables: list[pd.DataFrame] = []
    for line in feed.lines:
        travel_times = line.to_travel_times()
        stop_times_tables.append(
            create_stop_times(
                travel_times=travel_times,
                start_time=line.start_time,
                end_time=line.end_time,
                period=line.period,
            )
        )

        if line.bidirectional:
            reverse_travel_times = [[to_stop, from_stop, travel_time] for from_stop, to_stop, travel_time in travel_times]
            reverse_travel_times.reverse()
            stop_times_tables.append(
                create_stop_times(
                    travel_times=reverse_travel_times,
                    start_time=line.start_time,
                    end_time=line.end_time,
                    period=line.period,
                )
            )

    stops = {
        stop_id: (spec.name or stop_id, [spec.lon, spec.lat])
        for stop_id, spec in feed.stops.items()
    }

    agency, routes, trips, calendar, stops_table, stop_times_table = create_gtfs_tables(
        agency_id=feed.agency_id,
        agency_name=feed.agency_name,
        route_id=feed.route_id,
        route_short_name=feed.route_short_name,
        route_type=feed.route_type,
        service_id=feed.service_id,
        stops=stops,
        stop_times=stop_times_tables,
    )

    output_path = Path(output_path)
    zip_gtfs_tables(
        agency=agency,
        routes=routes,
        trips=trips,
        calendar=calendar,
        stops=stops_table,
        stop_times=stop_times_table,
        path=output_path,
    )
    return output_path
