import os
import pathlib
import logging
import zipfile
import hashlib
import shutil
import json
import tempfile

import polars as pl
from concurrent.futures import ThreadPoolExecutor

import requests

from mobility.runtime.assets.file_asset import FileAsset
from mobility.runtime.assets.input_hashing import hash_inputs
from mobility.runtime.io.download_file import clean_path, download_file
from .gtfs_feed import GTFSFeed

class GTFSData(FileAsset):
    """Cache a GTFS ZIP and its service dates; get() returns both output paths."""

    def __init__(
        self,
        provider: str,
        dataset_id: str,
        resource_id: str,
        download_url: str,
        gtfs_file_date: str | None,
        source_status: str,
        sources_created_at_utc: str,
        sha256: str | None = None,
    ):

        path = clean_path(download_url)
        live_sources_created_at_utc = None
        if source_status == "live":
            live_sources_created_at_utc = sources_created_at_utc

        # Keep existing ZIP names so previous downloads can be pinned without downloading again.
        source_key = "|".join(
            [
                provider,
                dataset_id,
                resource_id,
                download_url,
                str(gtfs_file_date),
                source_status,
                str(live_sources_created_at_utc),
            ]
        )
        name = hashlib.md5(source_key.encode('utf-8')).hexdigest() + "_" + path.name
        cache_path = pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / "gtfs" / name

        if cache_path.suffix != ".zip":
            cache_path = cache_path.with_suffix('.zip')

        self.name = name

        inputs = {
            "provider": provider,
            "dataset_id": dataset_id,
            "resource_id": resource_id,
            "download_url": download_url,
            "gtfs_file_date": gtfs_file_date,
            "source_status": source_status,
            "live_sources_created_at_utc": live_sources_created_at_utc,
        }
        if sha256 is not None:
            inputs["sha256"] = sha256

        zip_path = cache_path.with_name(hash_inputs(inputs) + "-" + cache_path.name)
        super().__init__({**inputs, "metadata_version": "1"}, {
            "metadata": cache_path.with_suffix(".json"), "zip": cache_path,
        })
        # Preserve the ZIP path from the previous single-output asset.
        self.cache_path["zip"] = zip_path


    def get_cached_asset(self):
        return self.cache_path

    def is_update_needed(self):
        if self.cache_path["zip"].exists():
            self.validate_file()
        return super().is_update_needed()

    def create_and_get_asset(self):
        # Snapshot creation already downloaded this URL to learn its checksum.
        if self.inputs.get("sha256") is not None and not self.cache_path["zip"].exists():
            downloaded = GTFSData(
                self.provider, self.dataset_id, self.resource_id, self.download_url,
                self.gtfs_file_date, self.source_status,
                self.live_sources_created_at_utc,
            )
            if downloaded.cache_path["zip"].exists() and self.file_sha256(downloaded.cache_path["zip"]) == self.sha256:
                temporary = self.cache_path["zip"].with_suffix(".zip.part")
                shutil.copyfile(downloaded.cache_path["zip"], temporary)
                temporary.replace(self.cache_path["zip"])
                if downloaded.cache_path["metadata"].exists():
                    shutil.copyfile(downloaded.cache_path["metadata"], self.cache_path["metadata"])
        try:
            download_file(self.download_url, self.cache_path["zip"], raise_on_error=True)
        except requests.RequestException as exc:
            source_id = f"{self.provider}:{self.resource_id}"
            raise ValueError(
                f"Cannot download GTFS source {source_id} from {self.download_url}: {exc}. "
                "Retry this URL or restore the cached ZIP. To omit this feed, create a new snapshot "
                f"with excluded_gtfs_sources=[{source_id!r}]."
            ) from exc

        checksum = self.validate_file()
        self.write_metadata(checksum)
        return self.cache_path

    def write_metadata(self, checksum=None):
        """Cache conservative service bounds; unknown dates never exclude a feed."""
        metadata = {"sha256": checksum or self.file_sha256(self.cache_path["zip"]),
                    "service_start_date": None, "service_end_date": None}
        if self.cache_path["metadata"].exists():
            saved = json.loads(self.cache_path["metadata"].read_text(encoding="utf-8"))
            if saved.get("sha256") == metadata["sha256"]:
                return
        # A previous snapshot may already have computed this pinned ZIP's metadata.
        if self.inputs.get("sha256") is None:
            pinned = GTFSData(
                self.provider, self.dataset_id, self.resource_id, self.download_url,
                self.gtfs_file_date, self.source_status, self.live_sources_created_at_utc,
                metadata["sha256"],
            )
            if pinned.cache_path["metadata"].exists():
                saved = json.loads(pinned.cache_path["metadata"].read_text(encoding="utf-8"))
                if saved.get("sha256") == metadata["sha256"]:
                    temporary = self.cache_path["metadata"].with_suffix(".json.part")
                    temporary.write_text(json.dumps(saved, sort_keys=True), encoding="utf-8")
                    temporary.replace(self.cache_path["metadata"])
                    return
        with tempfile.TemporaryDirectory(prefix="mobility-gtfs-calendar-") as folder:
            reader = GTFSFeed(self.cache_path["zip"], pathlib.Path(folder))
            reader.extract(("calendar", "calendar_dates"))
            try:
                calendar, exceptions = reader.read_calendars()
                bounds = []
                if not calendar.empty:
                    bounds.extend([calendar.start_date.min(), calendar.end_date.max()])
                if not exceptions.empty:
                    additions = exceptions.loc[exceptions.exception_type.eq(1), "date"]
                    if not additions.empty:
                        bounds.extend([additions.min(), additions.max()])
                if bounds:
                    metadata.update(service_start_date=min(bounds).strftime("%Y-%m-%d"),
                                    service_end_date=max(bounds).strftime("%Y-%m-%d"))
            except (ValueError, pl.exceptions.PolarsError) as error:
                metadata["coverage_error"] = str(error)
        temporary = self.cache_path["metadata"].with_suffix(".json.part")
        temporary.write_text(json.dumps(metadata, sort_keys=True), encoding="utf-8")
        temporary.replace(self.cache_path["metadata"])

    @classmethod
    def download_gtfs_files(cls, sources: list[dict]) -> list[dict[str, pathlib.Path]]:
        """Download selected GTFS sources in parallel and validate each file."""
        gtfs_files = [
            cls(
                provider=source["provider"],
                dataset_id=source["dataset_id"],
                resource_id=source["resource_id"],
                download_url=source["download_url"],
                gtfs_file_date=source["gtfs_file_date"],
                source_status=source["status"],
                sources_created_at_utc=source["sources_created_at_utc"],
                sha256=source.get("sha256"),
            )
            for source in sources
        ]

        with ThreadPoolExecutor(max_workers=4) as executor:
            return list(executor.map(GTFSData.get, gtfs_files))


    @staticmethod
    def file_sha256(path):
        """Hash file contents without loading the whole archive in memory."""
        digest = hashlib.sha256()
        with open(path, "rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()

    def validate_file(self):
        """Fail on a broken or changed archive instead of silently dropping a feed."""
        source_id = f"{self.provider}:{self.resource_id}"
        try:
            expected = self.inputs.get("sha256")
            with zipfile.ZipFile(self.cache_path["zip"]) as archive:
                names = set(GTFSFeed.table_paths(archive))
                if not names and any(name.lower().endswith((".zip", ".gtfs")) for name in archive.namelist()):
                    raise ValueError(
                        "this ZIP bundles several feeds instead of containing GTFS tables. "
                        "Bundles are not supported because they may duplicate separately selected feeds"
                    )
                required = {"agency.txt", "stops.txt", "routes.txt", "trips.txt", "stop_times.txt"}
                missing = sorted(required - names)
                if not names.intersection({"calendar.txt", "calendar_dates.txt"}):
                    missing.append("calendar.txt or calendar_dates.txt")
                if missing:
                    raise ValueError("missing required GTFS tables: " + ", ".join(missing))
                # A pinned checksum already covers every ZIP entry's bytes.
                if expected is None and archive.testzip() is not None:
                    raise ValueError("damaged ZIP entry")
            checksum = self.file_sha256(self.cache_path["zip"]) if expected is not None else None
            if expected is not None and checksum != expected:
                raise ValueError(f"SHA-256 differs from the frozen value {expected}")
            return checksum
        except (OSError, ValueError, zipfile.BadZipFile) as exc:
            raise ValueError(
                f"Invalid GTFS archive for {source_id}: {exc}. File: {self.cache_path['zip']}. "
                f"URL: {self.download_url}. Restore the frozen ZIP or remove this cached ZIP "
                "to retry the same URL. To omit this feed, create a new snapshot with "
                f"excluded_gtfs_sources=[{source_id!r}]."
            ) from exc

    @staticmethod
    def get_agencies_names(path):

        with zipfile.ZipFile(path, 'r') as gtfs_folder:
            with gtfs_folder.open("agency.txt") as agency:
                agencies = agency.read().decode('utf-8')
                logging.info(agencies)
                return agencies
