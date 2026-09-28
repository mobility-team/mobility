"""Build a service specification with stops, lines and departure intervals."""

from __future__ import annotations

from pathlib import Path
from typing import Mapping, Sequence

from .custom_gtfs import CustomGTFS

# Preserve the existing imports used by callers of this module.
from .gtfs_feed_spec import (
    GTFS_ROUTE_TYPES,
    GTFSFeedSpec,
    GTFSLineSpec,
    GTFSStopSpec,
    build_gtfs_zip,
    build_project_gtfs_zip,
)


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

    def build_asset(self) -> CustomGTFS:
        """Snapshot this service as an asset without writing its ZIP."""
        return CustomGTFS(self.build_feed())

    def write_zip(self, path: str | Path) -> Path:
        """Write this feed as a GTFS zip file."""

        return build_gtfs_zip(self.build_feed(), path)

    def write_project_zip(self, file_name: str) -> Path:
        """Write this feed under ``MOBILITY_PROJECT_DATA_FOLDER``."""

        return build_project_gtfs_zip(self.build_feed(), file_name)
