"""
GTFS router asset.

Responsibilities:
- Identify GTFS sources covering the transport zones.
- Optionally apply small "surgical" GTFS edits (e.g. insert a stop between two others).
- Run the R pipeline (prepare_gtfs_router.R) to build a merged Tuesday-only GTFS router (.rds).

This class is a FileAsset to benefit from caching.
"""

from __future__ import annotations

import json
import logging
import os
import pathlib
from importlib import resources
from typing import Any

import gtfs_kit
import pandas as pd

from mobility.file_asset import FileAsset
from mobility.parsers.download_file import download_file
from mobility.parsers.gtfs_stops import GTFSStops
from mobility.r_utils.r_script import RScript
from mobility.transport_zones import TransportZones

from .gtfs_data import GTFSData
from .gtfs_edit import apply_gtfs_edits

LOGGER = logging.getLogger(__name__)


class GTFSRouter(FileAsset):
    """
    Create a GTFS router (.rds) for given transport zones.

    It:
    - Retrieves GTFS URLs covering the zone bounding box.
    - Downloads GTFS files.
    - Optionally applies edits (gtfs_edits).
    - Optionally checks that expected agencies exist in at least one GTFS.
    - Runs prepare_gtfs_router.R to build the router.
    """

    def __init__(
        self,
        transport_zones: TransportZones,
        additional_gtfs_files: list[str] | None = None,
        gtfs_edits: list[dict[str, Any]] | None = None,
        expected_agencies: list[str] | None = None,
    ):
        inputs = {
            "transport_zones": transport_zones,
            "additional_gtfs_files": additional_gtfs_files,
            "download_date": os.environ["MOBILITY_GTFS_DOWNLOAD_DATE"],
            "expected_agencies": expected_agencies,
            "gtfs_edits": gtfs_edits,
        }

        cache_path = pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"]) / "gtfs_router.rds"
        super().__init__(inputs, cache_path)

    def get_cached_asset(self):
        return self.cache_path

    def create_and_get_asset(self):
        LOGGER.info("Downloading GTFS files for stops within the transport zones...")

        transport_zones = self.inputs["transport_zones"]
        expected_agencies = self.inputs["expected_agencies"]

        stops = self.get_stops(transport_zones)

        gtfs_files = self.get_gtfs_files(stops)
        if self.inputs["additional_gtfs_files"]:
            gtfs_files.extend(self.inputs["additional_gtfs_files"])

        if self.inputs.get("gtfs_edits"):
            edits_folder = pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"]) / "gtfs_edits"
            gtfs_files = apply_gtfs_edits(gtfs_files, self.inputs["gtfs_edits"], edits_folder)

        if expected_agencies:
            self.check_expected_agencies(gtfs_files, expected_agencies)

        self.prepare_gtfs_router(transport_zones, gtfs_files)
        return self.cache_path

    def check_expected_agencies(self, gtfs_files: list[str], expected_agencies: list[str]) -> bool:
        """
        Ensure each agency in expected_agencies is found in at least one GTFS.

        Note: mutates the expected_agencies list in-place (removes found agencies),
        matching the previous behavior.
        """
        missing = list(expected_agencies)

        for gtfs_path in gtfs_files:
            try:
                gtfs = GTFSData(gtfs_path)
                agencies = gtfs.get_agencies_names(gtfs_path)
            except Exception:
                LOGGER.exception("Failed reading agencies for GTFS: %s", gtfs_path)
                continue

            for agency in list(missing):
                if agency.lower() in str(agencies).lower():
                    LOGGER.info("%s found in %s", agency, gtfs.name)
                    missing.remove(agency)

            if not missing:
                LOGGER.info("All expected agencies were found.")
                expected_agencies[:] = []
                return True

        LOGGER.error("Some agencies were not found in GTFS files: %s", missing)
        raise IndexError("Missing agencies")

    def get_stops(self, transport_zones: TransportZones):
        tz = transport_zones.get()

        admin_prefixes = ["fr", "ch"]
        admin_prefixes = [
            prefix for prefix in admin_prefixes if tz["local_admin_unit_id"].str.contains(prefix).any()
        ]

        stops = GTFSStops(admin_prefixes, self.inputs["download_date"])
        return stops.get(bbox=tuple(tz.total_bounds))

    def prepare_gtfs_router(self, transport_zones: TransportZones, gtfs_files: list[str]) -> None:
        gtfs_files_arg = ",".join(gtfs_files)

        script = RScript(
            resources.files("mobility.transport_modes.public_transport.gtfs").joinpath("prepare_gtfs_router.R")
        )
        script.run(
            args=[
                str(transport_zones.cache_path),
                gtfs_files_arg,
                str(resources.files("mobility.data").joinpath("gtfs/gtfs_route_types.csv")),
                str(self.cache_path),
            ]
        )

    def get_gtfs_files(self, stops) -> list[str]:
        gtfs_urls = self.get_gtfs_urls(stops)
        gtfs_files = [GTFSData(url).get() for url in gtfs_urls]
        return [str(f[0]) for f in gtfs_files if f[1] is True]

    def get_gtfs_urls(self, stops) -> list[str]:
        gtfs_urls: list[str] = []

        gtfs_urls.extend(stops["resource_url"].dropna().unique().tolist())

        datagouv_dataset_urls = stops["dataset_url"].dropna().unique()
        datagouv_dataset_ids = [pathlib.Path(url).name for url in datagouv_dataset_urls]

        url = "https://transport.data.gouv.fr/api/datasets"
        path = (
            pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"])
            / "gtfs"
            / (self.inputs["download_date"] + "_gtfs_metadata.json")
        )
        download_file(url, path)

        with open(path, "r", encoding="utf-8") as f:
            metadata = json.load(f)

        for dataset_metadata in metadata:
            if dataset_metadata.get("datagouv_id") not in datagouv_dataset_ids:
                continue

            resources_ = dataset_metadata.get("resources", [])
            gtfs_resources = [r for r in resources_ if r.get("format") == "GTFS"]
            for r in gtfs_resources:
                if r.get("original_url"):
                    gtfs_urls.append(r["original_url"])

        return gtfs_urls

    def audit_gtfs(self):
        """
        Audit GTFS files for the current transport zones.

        Exports (per GTFS source) a GeoPackage with:
        - active shapes (busiest date) enriched with trip counts and route names
        - active stops (busiest date)
        """
        transport_zones = self.inputs["transport_zones"]
        stops = self.get_stops(transport_zones)
        gtfs_files = self.get_gtfs_files(stops)

        for i, gtfs_path in enumerate(gtfs_files, start=1):
            LOGGER.info("Auditing GTFS: %s", gtfs_path)

            try:
                feed = gtfs_kit.read_feed(gtfs_path, dist_units="m")
            except Exception as e:
                LOGGER.info("Error loading GTFS: %s", e)
                continue

            shapes_df, trips_df = self._load_shapes_and_trips(feed)
            routes_df = feed.routes[["route_id", "route_short_name", "route_long_name"]].copy()
            stops_df = feed.stops[["stop_id", "stop_name", "stop_lat", "stop_lon"]].copy()
            stop_times_df = feed.stop_times.copy()

            dates = feed.get_dates()
            max_services_date = feed.compute_busiest_date(dates)
            LOGGER.info("Max services date is %s", max_services_date)

            active_trips = feed.get_trips(date=max_services_date)
            if active_trips.empty:
                LOGGER.info("No active trips found for %s.", max_services_date)
                continue

            LOGGER.info("%s trips found for %s", len(active_trips), max_services_date)

            active_shapes_df, trips_counts = self._build_active_shapes(
                active_trips=active_trips,
                shapes_df=shapes_df,
                stop_times_df=stop_times_df,
                stops_df=stops_df,
            )

            trips_counts = trips_counts.merge(routes_df, on="route_id", how="left")

            active_stop_times = stop_times_df[stop_times_df["trip_id"].isin(active_trips["trip_id"])]
            active_stop_ids = active_stop_times["stop_id"].unique()
            active_stops_df = stops_df[stops_df["stop_id"].isin(active_stop_ids)]

            active_shapes_gdf = gtfs_kit.shapes.geometrize_shapes(active_shapes_df)
            active_stops_gdf = gtfs_kit.stops.geometrize_stops(active_stops_df)

            LOGGER.info("Enriching shapes with trip counts and route names...")
            active_shapes_gdf = active_shapes_gdf.merge(trips_counts, on="shape_id", how="left")

            nb_shapes = active_shapes_gdf["shape_id"].nunique()
            trips_total = active_shapes_gdf["trip_count"].sum()
            LOGGER.info(
                "Network has %s shapes with a total of %s trips on %s",
                nb_shapes,
                trips_total,
                max_services_date,
            )

            active_shapes_gdf["trip_count"] = active_shapes_gdf["trip_count"].fillna(0)

            output_path = (
                pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / "gtfs" / "gpkg" / f"gtfs_{i}.gpkg"
            )
            output_path.parent.mkdir(parents=True, exist_ok=True)

            active_shapes_gdf.to_file(output_path, driver="GPKG", layer="shapes")
            active_stops_gdf.to_file(output_path, driver="GPKG", layer="stops")

            LOGGER.info("GTFS stops and shapes exported as GeoPackage in %s", output_path)

    @staticmethod
    def _load_shapes_and_trips(feed):
        try:
            shapes_df = feed.shapes.copy()
            trips_df = feed.trips[["trip_id", "route_id", "shape_id"]].copy()
        except Exception:
            shapes_df = None
            trips_df = feed.trips[["trip_id", "route_id"]].copy()
        return shapes_df, trips_df

    @staticmethod
    def _build_active_shapes(active_trips, shapes_df, stop_times_df, stops_df):
        if shapes_df is not None and not shapes_df.empty:
            LOGGER.info("shapes.txt present: counting trips by shape_id...")
            trips_counts = active_trips.groupby(["shape_id"]).size().reset_index(name="trip_count")
            shapes_routes = active_trips.groupby(["shape_id"])["route_id"].first().reset_index()
            trips_counts = trips_counts.merge(shapes_routes, on="shape_id", how="left")
            trips_counts = trips_counts[trips_counts["shape_id"].notna()]
            active_shapes_df = shapes_df[shapes_df["shape_id"].isin(active_trips["shape_id"])]
            return active_shapes_df, trips_counts

        LOGGER.info("shapes.txt missing: reconstructing shapes from stop sequences...")
        trips_stop_sequences = pd.merge(
            active_trips[["trip_id", "route_id"]],
            stop_times_df[["trip_id", "stop_id", "stop_sequence"]],
            on="trip_id",
            how="left",
        ).sort_values(by=["trip_id", "stop_sequence"])

        trips_with_pseudo_shape_id = trips_stop_sequences.groupby(["trip_id", "route_id"]).agg(
            pseudo_shape_id=("stop_id", lambda x: "-".join(x.astype(str)))
        ).reset_index()

        trips_stop_sequences = pd.merge(
            trips_stop_sequences,
            trips_with_pseudo_shape_id[["trip_id", "pseudo_shape_id"]],
            on="trip_id",
        )

        pseudo_shapes = trips_stop_sequences[
            ["pseudo_shape_id", "stop_id", "stop_sequence", "route_id"]
        ].drop_duplicates(subset=["pseudo_shape_id", "stop_sequence"])

        stops_coords = stops_df[["stop_id", "stop_lat", "stop_lon"]].copy().rename(
            columns={"stop_lat": "shape_pt_lat", "stop_lon": "shape_pt_lon"}
        )
        pseudo_shapes = pd.merge(pseudo_shapes, stops_coords, on="stop_id")

        active_shapes_df = pseudo_shapes.rename(
            columns={"stop_sequence": "shape_pt_sequence", "pseudo_shape_id": "shape_id"}
        ).sort_values(["shape_id", "shape_pt_sequence"])

        LOGGER.info("Counting trips by reconstructed shape_id...")
        trips_counts = trips_with_pseudo_shape_id.groupby(["pseudo_shape_id"]).size().reset_index(name="trip_count")
        shapes_routes = trips_with_pseudo_shape_id.groupby(["pseudo_shape_id"])["route_id"].first().reset_index()
        trips_counts = trips_counts.merge(shapes_routes, on="pseudo_shape_id", how="left").rename(
            columns={"pseudo_shape_id": "shape_id"}
        )

        return active_shapes_df, trips_counts
