import datetime as dt
import json
import logging
import os
import pathlib
import re
import sqlite3
import sys
import zipfile
from urllib.parse import urljoin, urlparse

import pandas as pd
import polars as pl
from shapely.geometry import box, shape
from shapely.ops import unary_union

from mobility.runtime.io.http import request_url, request_urls
from mobility.transport.modes.public_transport.gtfs.gtfs_data import GTFSData
from mobility.transport.modes.public_transport.gtfs.gtfs_feed import GTFSFeed


class GTFSDataSource:
    """Shared helpers for GTFS source providers."""

    def __init__(
        self,
        reference_date: dt.date,
        sources_created_at_utc: str,
        use_live_gtfs: bool = False,
        max_gtfs_file_age_days: int | None = None,
        area_filter=None,
    ):
        self.reference_date = reference_date
        self.sources_created_at_utc = sources_created_at_utc
        self.use_live_gtfs = use_live_gtfs
        self.max_gtfs_file_age_days = max_gtfs_file_age_days
        self.area_filter = area_filter

    @staticmethod
    def use_rich_progress() -> bool:
        """Return True when Mobility feedback is configured for Rich progress."""
        feedback = os.environ.get("MOBILITY_FEEDBACK")
        if feedback is not None:
            return feedback.lower() == "progress"

        progress = os.environ.get("MOBILITY_PROGRESS")
        if progress is not None:
            return progress.lower() in {"auto", "rich"}

        return not os.environ.get("CI") and sys.stderr.isatty()

    def gtfs_file_age_days(self, gtfs_file_date: str | None) -> int | None:
        """Return the age of a GTFS file relative to the GTFS reference date."""
        if gtfs_file_date is None:
            return None
        try:
            parsed_gtfs_file_date = dt.date.fromisoformat(str(gtfs_file_date)[:10])
        except ValueError:
            return None
        return (self.reference_date - parsed_gtfs_file_date).days

    def select_source_status(self, is_archived: bool, gtfs_file_age_days: int | None) -> str:
        """Return whether a GTFS source can be used by a reproducible run."""
        if not is_archived:
            if self.use_live_gtfs:
                return "live"
            return "missing_archive"

        if (
            gtfs_file_age_days is not None
            and self.max_gtfs_file_age_days is not None
            and gtfs_file_age_days > self.max_gtfs_file_age_days
        ):
            return "stale_archive"

        return "archived"

    def derive_coverage_from_gtfs_url(
        self,
        *,
        dataset_id: str,
        resource_id: str,
        download_url: str,
        gtfs_file_date: str | None,
        status: str,
    ):
        """Download one GTFS and build a rough coverage box from stops.txt."""
        gtfs_path = GTFSData(
            provider=self.provider,
            dataset_id=dataset_id,
            resource_id=resource_id,
            download_url=download_url,
            gtfs_file_date=gtfs_file_date,
            source_status=status,
            sources_created_at_utc=self.sources_created_at_utc,
        ).get()["zip"]
        try:
            with zipfile.ZipFile(gtfs_path, "r") as gtfs_zip:
                with gtfs_zip.open(GTFSFeed.table_paths(gtfs_zip)["stops.txt"]) as stops_file:
                    stops = pd.read_csv(stops_file, usecols=["stop_lon", "stop_lat"])
        except (KeyError, ValueError, zipfile.BadZipFile):
            return None

        stops = stops.dropna(subset=["stop_lon", "stop_lat"])
        if stops.empty:
            return None

        return box(
            stops["stop_lon"].min(),
            stops["stop_lat"].min(),
            stops["stop_lon"].max(),
            stops["stop_lat"].max(),
        )


class FrenchGTFS(GTFSDataSource):
    """French GTFS metadata from transport.data.gouv.fr."""

    country = "fr"
    provider = "transport_data_gouv"
    datasets_url = "https://transport.data.gouv.fr/api/datasets"
    metadata_max_workers = 8

    def insert_data(self, connection: sqlite3.Connection, gtfs_sources) -> None:
        """Insert French GTFS files and coverage into the sources file."""
        logging.info("Fetching French GTFS metadata.")
        datasets_response = request_url(self.datasets_url)
        try:
            datasets = datasets_response.json()
        finally:
            datasets_response.close()

        catalog_datasets = []
        for catalog_dataset in datasets:
            catalog_resources = [
                resource
                for resource in catalog_dataset.get("resources", [])
                if self.resource_format(resource) == "GTFS"
            ]
            if len(catalog_resources) == 0:
                continue

            dataset_id = self.dataset_id(catalog_dataset)
            if dataset_id is None:
                continue

            if self.area_filter is not None and not self.area_filter.matches_dataset(catalog_dataset):
                continue

            catalog_datasets.append((dataset_id, catalog_dataset, catalog_resources))

        dataset_responses = []
        coverage_responses = []
        try:
            dataset_responses = request_urls(
                [
                    f"{self.datasets_url}/{dataset_id}"
                    for dataset_id, _catalog_dataset, _catalog_resources in catalog_datasets
                ],
                max_workers=self.metadata_max_workers,
                progress_description="Fetching French GTFS dataset metadata"
                if self.use_rich_progress()
                else None,
            )
            coverage_responses = request_urls(
                [
                    f"{self.datasets_url}/{dataset_id}/geojson"
                    for dataset_id, _catalog_dataset, _catalog_resources in catalog_datasets
                ],
                max_workers=self.metadata_max_workers,
                allowed_status_codes={404},
                progress_description="Fetching French GTFS source coverage"
                if self.use_rich_progress()
                else None,
            )

            dataset_payloads = []
            coverage_geometries = []
            for dataset_response, coverage_response in zip(dataset_responses, coverage_responses):
                dataset_payloads.append(dataset_response.json())
                coverage_geometries.append(self.coverage_from_response(coverage_response))
        finally:
            for response in dataset_responses + coverage_responses:
                response.close()

        for (
            (dataset_id, _catalog_dataset, catalog_resources),
            dataset,
            coverage_geometry,
        ) in zip(catalog_datasets, dataset_payloads, coverage_geometries):
            self.insert_dataset_data(
                connection,
                gtfs_sources,
                dataset_id,
                catalog_resources,
                dataset,
                coverage_geometry,
            )

    def insert_dataset_data(
        self,
        connection: sqlite3.Connection,
        gtfs_sources,
        dataset_id: str,
        catalog_resources: list[dict],
        dataset: dict,
        coverage_geometry,
    ) -> None:
        """Insert all GTFS files for one French dataset."""
        if coverage_geometry is not None and gtfs_sources.area_geometry is not None:
            if not coverage_geometry.intersects(gtfs_sources.area_geometry):
                return
        dataset = {**dataset, "history": self.fetch_history(dataset)}
        gtfs_resources = self.select_gtfs_resources(dataset, catalog_resources)
        if coverage_geometry is None:
            logging.warning(
                "No transport.data.gouv coverage for dataset %s. "
                "Downloading GTFS resources to derive coverage from stops.txt.",
                dataset_id,
            )

        for resource in gtfs_resources:
            self.insert_resource(connection, gtfs_sources, dataset_id, resource, coverage_geometry)

    def insert_resource(
        self,
        connection: sqlite3.Connection,
        gtfs_sources,
        dataset_id: str,
        resource: dict,
        coverage_geometry,
    ) -> None:
        """Insert one French GTFS source and derive coverage when needed."""
        resource_id = self.resource_id(resource)
        download_url = self.resource_download_url(resource)
        if resource_id is None:
            raise ValueError(f"French GTFS resource without a stable id in dataset {dataset_id}.")

        gtfs_file_date = resource.get("gtfs_file_date") or resource.get("updated_at") or resource.get("created_at")
        gtfs_file_age_days = self.gtfs_file_age_days(gtfs_file_date)
        status = self.select_source_status(
            resource.get("is_reproducible") is not False,
            gtfs_file_age_days,
        )
        if download_url is None:
            status = "missing_archive"
        if f"{self.provider}:{resource_id}" in gtfs_sources.inputs.get("excluded_gtfs_sources", []):
            status = "excluded"

        resource_coverage = coverage_geometry
        if resource_coverage is None and status in ("archived", "live"):
            resource_coverage = self.derive_coverage_from_gtfs_url(
                dataset_id=dataset_id,
                resource_id=resource_id,
                download_url=download_url,
                gtfs_file_date=gtfs_file_date,
                status=status,
            )

        if resource_coverage is None and status in ("archived", "live"):
            status = "missing_coverage"

        if status == "missing_archive":
            download_url = None

        gtfs_sources.insert_gtfs_file(
            connection,
            country=self.country,
            provider=self.provider,
            dataset_id=dataset_id,
            resource_id=resource_id,
            title=resource.get("title") or resource.get("name"),
            download_url=download_url,
            gtfs_file_date=gtfs_file_date,
            gtfs_file_age_days=gtfs_file_age_days,
            status=status,
            coverage_geometry=resource_coverage,
        )

    def select_gtfs_resources(self, dataset: dict, fallback_resources: list[dict]) -> list[dict]:
        """Select archived GTFS files, or live files when explicitly allowed."""
        resources = [
            resource
            for resource in dataset.get("resources", [])
            if self.resource_format(resource) == "GTFS"
        ]
        if len(resources) == 0:
            resources = list(fallback_resources)

        history_by_resource_id = self.history_by_resource_id(dataset)
        known = {self.resource_id(resource) for resource in resources}
        # Removed resource IDs remain useful for historical studies.
        for resource_id, entries in sorted(history_by_resource_id.items()):
            if resource_id not in known and any(
                self.resource_format(entry.get("payload", entry)) == "GTFS" for entry in entries
            ):
                resources.append({"id": resource_id, "format": "GTFS", "title": f"Archived resource {resource_id}"})

        selected_resources = []
        for resource in resources:
            resource_history = history_by_resource_id.get(self.resource_id(resource), [])
            selected_resource = self.select_resource_snapshot(resource, resource_history)
            if selected_resource is not None:
                selected_resources.append(selected_resource)

        return selected_resources

    def select_resource_snapshot(self, resource: dict, resource_history: list[dict] | None = None) -> dict | None:
        """Return the latest archived GTFS resource before the GTFS reference date."""
        history_payloads = []
        if resource_history is None:
            for history_key in ("history", "resource_history", "backups", "archived_resources"):
                for entry in resource.get(history_key, []) or []:
                    payload = entry.get("payload", entry)
                    if self.resource_format(payload) == "GTFS":
                        history_payloads.append(payload)
        for entry in resource_history or []:
            payload = entry.get("payload", entry)
            if self.resource_format(payload) == "GTFS":
                history_payloads.append(payload)

        eligible_payloads = []
        for payload in history_payloads:
            payload_date = self.payload_date(payload)
            if payload_date is not None and payload_date <= self.reference_date:
                eligible_payloads.append((self.payload_timestamp(payload), payload))

        if len(eligible_payloads) > 0:
            eligible_payloads.sort(key=lambda item: (item[0], self.resource_download_url(item[1]) or ""))
            selected = dict(resource)
            selected.update(eligible_payloads[-1][1])
            selected["gtfs_file_date"] = eligible_payloads[-1][0].date().isoformat()
            selected["id"] = self.resource_id(resource)
            selected["is_reproducible"] = True
            return selected

        if self.resource_download_url(resource) is not None:
            selected = dict(resource)
            selected["is_reproducible"] = False
            if self.use_live_gtfs:
                logging.warning(
                    "Using a live French GTFS URL because use_live_gtfs=True. "
                    "Results may change if the provider updates this file."
                )
            return selected

        return {**resource, "is_reproducible": False}

    def fetch_history(self, dataset: dict) -> list[dict]:
        """Read the complete archive CSV; the API history is only a recent sample."""
        internal_ids = {
            str(entry.get("payload", {}).get("dataset_id"))
            for entry in dataset.get("history", [])
            if entry.get("payload", {}).get("dataset_id") is not None
        }
        if len(internal_ids) == 1 and next(iter(internal_ids)).isdigit():
            history_url = (
                "https://transport.data.gouv.fr/datasets/"
                f"{next(iter(internal_ids))}/resources_history_csv"
            )
        else:
            page_url = f"https://transport.data.gouv.fr/datasets/{self.dataset_id(dataset)}"
            response = request_url(page_url)
            try:
                match = re.search(r'/datasets/(\d+)/resources_history_csv', response.text)
            finally:
                response.close()
            if match is None:
                raise ValueError(f"Cannot locate the complete GTFS history for {page_url}.")
            history_url = urljoin(page_url, match.group(0))

        response = request_url(history_url)
        try:
            # Archive metadata can exceed the standard csv reader's field size limit.
            rows = pl.read_csv(response.content, infer_schema=False)
            if not {"resource_id", "payload", "permanent_url"}.issubset(rows.columns):
                raise ValueError(f"Invalid GTFS history CSV at {history_url}.")
            history = []
            for row in rows.iter_rows(named=True):
                payload = json.loads(row["payload"])
                if row["permanent_url"]:
                    payload["permanent_url"] = row["permanent_url"]
                resource_id = row["resource_id"]
                if not resource_id and row["permanent_url"]:
                    parts = pathlib.PurePosixPath(urlparse(row["permanent_url"]).path).parts
                    if len(parts) >= 2 and re.fullmatch(
                        re.escape(parts[-2]) + r"\.\d{8}\.\d{6}\.\d+\.(?:zip|gtfs)",
                        parts[-1], re.IGNORECASE,
                    ):
                        resource_id = parts[-2]
                if resource_id:
                    history.append({"resource_id": resource_id, "payload": payload})
                elif self.resource_format(payload) == "GTFS":
                    raise ValueError(
                        f"Cannot identify historical GTFS resource in {history_url}: {row['permanent_url']}. "
                        "Its coverage cannot safely be omitted from a historical study."
                    )
            return history
        finally:
            response.close()

    @classmethod
    def history_by_resource_id(cls, dataset: dict) -> dict[str, list[dict]]:
        """Group transport.data.gouv archive records by resource id."""
        history = {}
        for entry in dataset.get("history", []) or []:
            resource_id = entry.get("resource_id")
            if resource_id is None:
                continue
            history.setdefault(str(resource_id), []).append(entry)
        return history

    @staticmethod
    def coverage_from_response(response):
        """Return a coverage polygon from a transport.data.gouv GeoJSON response."""
        if response.status_code == 404:
            return None
        payload = response.json()
        features = payload.get("features", [])
        geometries = [
            shape(feature["geometry"])
            for feature in features
            if feature.get("geometry") is not None
        ]
        if len(geometries) == 0:
            return None
        return unary_union(geometries)

    @staticmethod
    def dataset_id(dataset: dict) -> str | None:
        """Return the dataset id from transport.data.gouv metadata."""
        dataset_id = dataset.get("id") or dataset.get("datagouv_id")
        if dataset_id is None and dataset.get("url") is not None:
            dataset_id = pathlib.PurePosixPath(str(dataset["url"]).rstrip("/")).name
        if dataset_id is None:
            return None
        return str(dataset_id)

    @staticmethod
    def resource_id(resource: dict) -> str | None:
        """Return the resource id from transport.data.gouv metadata."""
        resource_id = (
            resource.get("id")
            or resource.get("resource_id")
            or resource.get("datagouv_id")
            or resource.get("resource_datagouv_id")
        )
        if resource_id is None:
            return None
        return str(resource_id)

    @staticmethod
    def resource_format(resource: dict) -> str | None:
        """Return the upper-case resource format."""
        resource_format = resource.get("format")
        if resource_format is None:
            return None
        return str(resource_format).upper()

    @staticmethod
    def resource_download_url(resource: dict) -> str | None:
        """Return the download URL from provider metadata."""
        for key in ("permanent_url", "download_url", "url", "original_url"):
            if resource.get(key):
                return str(resource[key])
        return None

    @staticmethod
    def payload_date(payload: dict) -> dt.date | None:
        """Return the date attached to a transport.data.gouv history payload."""
        timestamp = FrenchGTFS.payload_timestamp(payload)
        return timestamp.date() if timestamp is not None else None

    @staticmethod
    def payload_timestamp(payload: dict) -> dt.datetime | None:
        """Keep the archive's full UTC timestamp for same-day selection."""
        for key in ("download_datetime", "created_at", "updated_at", "published_at", "publication_date"):
            if not payload.get(key):
                continue
            try:
                timestamp = dt.datetime.fromisoformat(str(payload[key]).replace("Z", "+00:00"))
                if timestamp.tzinfo is None:
                    timestamp = timestamp.replace(tzinfo=dt.timezone.utc)
                return timestamp.astimezone(dt.timezone.utc)
            except ValueError:
                try:
                    return dt.datetime.combine(dt.date.fromisoformat(str(payload[key])[:10]), dt.time(), dt.timezone.utc)
                except ValueError:
                    continue
        return None

class SwissGTFS(GTFSDataSource):
    """Swiss GTFS metadata from official opentransportdata sources."""

    country = "ch"
    provider = "opentransportdata_swiss"
    archive_page_url = "https://archive.opentransportdata.swiss/timetable_gtfs_archive.htm"
    archive_listing_url = "https://archive.opentransportdata.swiss/timetable_gtfs.php"
    dataset_url_template = (
        "https://data.opentransportdata.swiss/en/dataset/timetable-{year}-gtfs2020"
    )

    def insert_data(self, connection: sqlite3.Connection, gtfs_sources) -> None:
        """Insert the Swiss national GTFS selected for the GTFS reference date."""
        logging.info("Fetching Swiss GTFS metadata.")
        start = dt.date.fromisoformat(gtfs_sources.inputs.get("gtfs_service_start_date", self.reference_date.isoformat()))
        end = dt.date.fromisoformat(gtfs_sources.inputs.get("gtfs_service_end_date", self.reference_date.isoformat()))
        exclusions = gtfs_sources.inputs.get("excluded_gtfs_sources", [])
        for year in range(self.timetable_year(start), self.timetable_year(end) + 1):
            resource_id = f"timetable-{year}"
            excluded = f"{self.provider}:{resource_id}" in exclusions or f"{self.provider}:timetable" in exclusions
            resource = None if excluded else self.select_gtfs_resource(year)
            resource = resource or {
                "dataset_id": f"timetable-{year}-gtfs2020", "title": f"Swiss timetable {year}",
                "download_url": None, "gtfs_file_date": None,
            }
            age = self.gtfs_file_age_days(resource["gtfs_file_date"])
            status = self.select_source_status(resource["download_url"] is not None, age)
            if resource["download_url"] is None:
                status = "missing_archive"
            if excluded:
                status = "excluded"
            coverage = None
            if status in ("archived", "live", "stale_archive"):
                coverage = self.derive_coverage_from_gtfs_url(
                    dataset_id=resource["dataset_id"], resource_id=resource_id,
                    download_url=resource["download_url"], gtfs_file_date=resource["gtfs_file_date"], status=status,
                )
                if coverage is None:
                    status = "missing_coverage"
            gtfs_sources.insert_gtfs_file(
                connection, country=self.country, provider=self.provider, dataset_id=resource["dataset_id"],
                resource_id=resource_id, title=resource["title"], download_url=resource["download_url"],
                gtfs_file_date=resource["gtfs_file_date"], gtfs_file_age_days=age,
                status=status, coverage_geometry=coverage,
            )

    @staticmethod
    def timetable_year(date):
        """Swiss timetable years start after December's second Saturday."""
        december = dt.date(date.year, 12, 1)
        changeover = december + dt.timedelta(days=(5 - december.weekday()) % 7 + 8)
        return date.year + int(date >= changeover)

    def select_gtfs_resource(self, timetable_year=None) -> dict[str, str] | None:
        """Select an archive available by the reference date for one service year."""
        timetable_year = timetable_year or self.timetable_year(self.reference_date)
        resources = []
        for dataset_url in (
            self.dataset_url_template.format(year=timetable_year),
            self.archive_listing_url,
        ):
            response = request_url(dataset_url, allowed_status_codes={404})
            try:
                if response.status_code != 404:
                    resources.extend(self.parse_gtfs_links(response.text, dataset_url))
            finally:
                response.close()
        eligible = [resource for resource in resources
                    if resource["gtfs_file_date"] <= self.reference_date.isoformat()
                    and resource["dataset_id"].startswith(f"timetable-{timetable_year}-")]
        return max(eligible, key=lambda resource: (resource["gtfs_file_date"], resource["download_url"]), default=None)

    def parse_gtfs_links(self, html: str, dataset_url: str) -> list[dict[str, str]]:
        """Parse dated GTFS ZIP links from Swiss archive or dataset pages."""
        resources = {}
        for href in re.findall(r'href=["\']([^"\']+\.zip[^"\']*)["\']', html, flags=re.IGNORECASE):
            href = urljoin(dataset_url, href)
            file_name = pathlib.PurePosixPath(href.split("?", 1)[0]).name
            if "GTFS" not in file_name.upper():
                continue

            gtfs_file_date = self.date_from_file_name(file_name)
            if gtfs_file_date is None:
                continue

            dataset_id = f"timetable-{self.reference_date.year}-gtfs2020"
            year_match = re.search(r"(?:fp|timetable[-_])(20\d{2})", href, re.IGNORECASE)
            if year_match is not None:
                dataset_id = f"timetable-{year_match.group(1)}-gtfs2020"
            path_parts = pathlib.PurePosixPath(href.split("?", 1)[0]).parts
            if len(path_parts) >= 2 and path_parts[-2].startswith("timetable-"):
                dataset_id = path_parts[-2]

            resources[href] = {
                "dataset_id": dataset_id,
                "resource_id": "-".join(dataset_id.split("-")[:2]),
                "title": file_name,
                "download_url": href,
                "gtfs_file_date": gtfs_file_date.isoformat(),
            }

        return list(resources.values())

    @staticmethod
    def date_from_file_name(file_name: str) -> dt.date | None:
        """Return a date embedded in a Swiss GTFS ZIP filename."""
        match = re.search(r"(20\d{2}-\d{2}-\d{2}|20\d{6})", file_name)
        if match is None:
            return None
        try:
            date_text = match.group(1)
            if "-" in date_text:
                return dt.date.fromisoformat(date_text)
            return dt.datetime.strptime(date_text, "%Y%m%d").date()
        except ValueError:
            return None
