import datetime as dt
import json
import logging
import os
import pathlib
import re
import sqlite3
from contextlib import closing

import geopandas as gpd
import pandas as pd
from shapely import wkb
from shapely.ops import unary_union

from mobility.runtime.assets.file_asset import FileAsset
from mobility.runtime.assets.input_hashing import hash_inputs, normalize_for_hash
from mobility.transport.modes.public_transport.gtfs.gtfs_data import GTFSData
from mobility.transport.modes.public_transport.gtfs.gtfs_area_filter import (
    FrenchGTFSAreaFilter,
)
from mobility.transport.modes.public_transport.gtfs.countries import available_gtfs_sources
from mobility.transport.modes.public_transport.gtfs.gtfs_source_providers import SwissGTFS
from mobility.countries import normalize_country_codes


class GTFSSources(FileAsset):
    """Project SQLite file that freezes the GTFS sources used by a run."""

    schema_version = "4"
    def __init__(
        self,
        gtfs_reference_date: str,
        gtfs_sources_folder: str | pathlib.Path,
        countries: list[str] | tuple[str, ...],
        use_live_gtfs: bool = False,
        max_gtfs_file_age_days: int | None = None,
        transport_zones=None,
        excluded_gtfs_sources: list[str] | None = None,
        gtfs_service_start_date: str | None = None,
        gtfs_service_end_date: str | None = None,
    ):
        reference_date = self.parse_reference_date(gtfs_reference_date)
        countries = self.normalize_countries(countries)
        mode = "live" if use_live_gtfs else "archived"

        inputs = {
            "schema_version": self.schema_version,
            "gtfs_reference_date": reference_date.isoformat(),
            "countries": countries,
            "use_live_gtfs": use_live_gtfs,
            "max_gtfs_file_age_days": max_gtfs_file_age_days,
        }
        if transport_zones is not None:
            # Zones locate a new snapshot; missing zone caches must not refresh an existing one.
            inputs["transport_zones"] = normalize_for_hash(transport_zones)

        file_name = (
            f"gtfs_sources_{reference_date.isoformat()}_"
            f"{'-'.join(countries)}_{mode}.sqlite"
        )
        cache_path = pathlib.Path(gtfs_sources_folder) / file_name
        exclusions = sorted(set(excluded_gtfs_sources or []))
        if any(re.fullmatch(r"[^:]+:[^:]+", value) is None for value in exclusions):
            raise ValueError("excluded_gtfs_sources must contain provider:resource_id values.")
        if exclusions:
            inputs["excluded_gtfs_sources"] = exclusions
        self.legacy_paths = []
        for version in ("1", "2"):
            legacy_inputs = {**inputs, "schema_version": version,
                             "max_gtfs_file_age_days": max_gtfs_file_age_days if max_gtfs_file_age_days is not None else 30}
            self.legacy_paths.append(cache_path.with_name(f"{hash_inputs(legacy_inputs)}-{file_name}"))
        self.legacy_path = self.legacy_paths[0]
        start, end = self.resolve_service_window(gtfs_reference_date, gtfs_service_start_date, gtfs_service_end_date)
        inputs.update(gtfs_service_start_date=start.isoformat(), gtfs_service_end_date=end.isoformat())
        previous_inputs = {**inputs, "schema_version": "3"}
        self.previous_path = cache_path.with_name(f"{hash_inputs(previous_inputs)}-{file_name}")
        super().__init__(inputs, cache_path)
        self.transport_zones = transport_zones
        self.area_geometry = None

    def get_cached_asset(self):
        self.validate_metadata()
        return self.cache_path

    def is_update_needed(self) -> bool:
        """An existing snapshot is frozen, even if an upstream cache is missing."""
        if self.cache_path.exists():
            self.validate_metadata()
            return False
        if self.previous_path.exists():
            return True
        legacy_path = next((path for path in self.legacy_paths if path.exists()), None)
        if legacy_path is not None:
            raise ValueError(
                f"Legacy GTFS snapshot found at {legacy_path}. It uses previous GTFS selection rules. "
                "Keep it as a backup and choose a new gtfs_sources_folder to explicitly rebuild "
                "from complete archive history. Review the new sources before committing them."
            )
        return True

    def create_and_get_asset(self):
        """Create the project GTFS sources file."""
        if not self.is_update_needed():
            return self.get_cached_asset()
        logging.info("Creating frozen GTFS sources at %s", self.cache_path)
        reference_date = self.parse_reference_date(self.inputs["gtfs_reference_date"])
        sources_created_at_utc = self.current_utc_timestamp()
        temp_path = self.cache_path.with_name(f"{self.cache_path.name}.part")

        if temp_path.exists():
            temp_path.unlink()

        try:
            if self.previous_path.exists():
                # Enrich a copy of the frozen selection; never rediscover or edit it.
                self.validate_metadata(self.previous_path, schema_version="3")
                with closing(sqlite3.connect(self.previous_path.resolve().as_uri() + "?mode=ro", uri=True)) as old:
                    with closing(sqlite3.connect(temp_path)) as new:
                        old.backup(new)
                        new.execute("ALTER TABLE gtfs_files ADD COLUMN service_start_date TEXT")
                        new.execute("ALTER TABLE gtfs_files ADD COLUMN service_end_date TEXT")
                        self.store_feed_metadata(new)
                        new.execute("UPDATE metadata SET schema_version = ?", (self.schema_version,))
                        new.commit()
                self.validate_metadata(temp_path)
            else:
                self.write_sources_file(temp_path, reference_date, sources_created_at_utc)
            temp_path.replace(self.cache_path)
        except Exception:
            if temp_path.exists():
                temp_path.unlink()
            raise

        return self.cache_path

    def write_sources_file(
        self,
        temp_path: pathlib.Path,
        reference_date: dt.date,
        sources_created_at_utc: str,
    ) -> None:
        """Build the temporary SQLite sources file before it replaces the cache."""
        if self.transport_zones is not None:
            self.area_geometry = unary_union(self.transport_zones.get().to_crs(4326).geometry)
        with closing(sqlite3.connect(temp_path)) as connection:
            self.create_schema(connection)
            self.insert_country_sources(connection, reference_date, sources_created_at_utc)
            rows = pd.read_sql_query(
                "SELECT provider, resource_id, dataset_id, title, status, gtfs_file_date "
                "FROM gtfs_files ORDER BY provider, resource_id", connection,
            )
            exclusions = self.inputs.get("excluded_gtfs_sources", [])
            known_ids = set(rows["provider"] + ":" + rows["resource_id"])
            if "opentransportdata_swiss" in set(rows["provider"]):
                known_ids.add("opentransportdata_swiss:timetable")
            unknown = sorted(set(exclusions) - known_ids)
            if unknown:
                raise ValueError(f"Unknown excluded_gtfs_sources: {unknown}. Check the provider:resource_id values.")
            problems = rows.loc[~rows["status"].isin(["archived", "live", "excluded"])]
            if not problems.empty:
                raise ValueError(self.format_unusable_sources_message(problems))
            usable = rows.loc[rows["status"].isin(["archived", "live"])].copy()
            if usable.empty:
                raise ValueError("No usable GTFS source remains in this snapshot.")
            # Publish the SQLite only after every selected ZIP has been checked.
            self.store_feed_metadata(connection, sources_created_at_utc)
            self.insert_metadata(connection, sources_created_at_utc)
            connection.commit()
            logging.info(
                "Frozen %s candidate GTFS feeds; %s explicitly excluded. Service dates are filtered next.",
                len(usable), len(rows) - len(usable),
            )

    def store_feed_metadata(self, connection, sources_created_at_utc=None):
        """Copy cached archive checksums and calendar bounds into a new snapshot."""
        if sources_created_at_utc is None:
            sources_created_at_utc = connection.execute("SELECT sources_created_at_utc FROM metadata").fetchone()[0]
        rows = pd.read_sql_query(
            "SELECT provider, dataset_id, resource_id, download_url, gtfs_file_date, status, sha256 "
            "FROM gtfs_files WHERE status IN ('archived', 'live') ORDER BY provider, resource_id",
            connection,
        )
        rows["sources_created_at_utc"] = sources_created_at_utc
        sources = rows.to_dict("records")
        outputs = GTFSData.download_gtfs_files(sources)
        for row, output in zip(sources, outputs):
            metadata = json.loads(output["metadata"].read_text(encoding="utf-8"))
            connection.execute(
                "UPDATE gtfs_files SET sha256 = ?, service_start_date = ?, service_end_date = ? "
                "WHERE provider = ? AND resource_id = ?",
                (metadata["sha256"], metadata["service_start_date"], metadata["service_end_date"],
                 row["provider"], row["resource_id"]),
            )

    def insert_country_sources(
        self,
        connection: sqlite3.Connection,
        reference_date: dt.date,
        sources_created_at_utc: str,
    ) -> None:
        """Fetch and insert GTFS source rows for each requested country."""
        for country in self.inputs["countries"]:
            source = self.build_country_source(country, reference_date, sources_created_at_utc)
            source.insert_data(connection, self)

    def build_country_source(
        self,
        country: str,
        reference_date: dt.date,
        sources_created_at_utc: str,
    ):
        """Create the source used to fetch one country's GTFS metadata."""
        source_kwargs = {
            "use_live_gtfs": self.inputs["use_live_gtfs"],
            "max_gtfs_file_age_days": self.inputs["max_gtfs_file_age_days"],
        }
        if country == "fr":
            area_filter = self.build_french_area_filter()
            if area_filter is not None:
                source_kwargs["area_filter"] = area_filter

        return available_gtfs_sources()[country](
            reference_date,
            sources_created_at_utc,
            **source_kwargs,
        )

    def build_french_area_filter(self):
        """Return the optional French covered_area filter for this sources file."""
        transport_zones = self.transport_zones
        if transport_zones is None:
            return None
        if hasattr(transport_zones, "get"):
            transport_zones = transport_zones.get()
        return FrenchGTFSAreaFilter.from_transport_zones(transport_zones)

    def get_gtfs_resources_for_area(self, transport_zones: gpd.GeoDataFrame) -> pd.DataFrame:
        """Return GTFS sources whose declared coverage intersects the study area."""
        metadata = self.validate_metadata()
        area = transport_zones.to_crs(4326)
        area_geometry = unary_union(area.geometry)
        minx, miny, maxx, maxy = area_geometry.bounds

        connection = sqlite3.connect(self.cache_path)
        try:
            candidates = pd.read_sql_query(
                """
                SELECT
                    country,
                    provider,
                    dataset_id,
                    resource_id,
                    title,
                    download_url,
                    gtfs_file_date,
                    gtfs_file_age_days,
                    status,
                    sha256,
                    geometry_wkb
                FROM gtfs_files
                WHERE maxx >= ?
                    AND minx <= ?
                    AND maxy >= ?
                    AND miny <= ?
                    AND (service_end_date IS NULL OR service_end_date >= ?)
                    AND (service_start_date IS NULL OR service_start_date <= ?)
                """,
                connection,
                params=(minx, maxx, miny, maxy, self.gtfs_service_start_date, self.gtfs_service_end_date),
            )
        finally:
            connection.close()

        if candidates.empty:
            raise ValueError("No GTFS source intersects the study area with possible service in the requested window.")

        # The bbox pass is cheap. This second pass removes false positives,
        # for example a source polygon with a hole around the study area.
        intersects_area = []
        for geometry_wkb in candidates["geometry_wkb"]:
            geometry = wkb.loads(geometry_wkb)
            intersects_area.append(geometry.intersects(area_geometry))

        selected = candidates.loc[intersects_area].drop(columns=["geometry_wkb"])
        if selected.empty:
            raise ValueError("No GTFS source intersects the study area.")

        problems = selected.loc[~selected["status"].isin(["archived", "live", "excluded"])]
        if not problems.empty:
            raise ValueError(self.format_unusable_sources_message(problems))

        usable_sources = selected.loc[selected["status"].isin(["archived", "live"])]
        if usable_sources.empty:
            raise ValueError("All GTFS sources intersecting the study area are explicitly excluded.")

        usable_sources = usable_sources.sort_values(
            by=["country", "provider", "dataset_id", "resource_id", "download_url"],
            kind="mergesort",
        ).reset_index(drop=True)
        usable_sources["sources_created_at_utc"] = metadata["sources_created_at_utc"]

        return usable_sources

    def create_schema(self, connection: sqlite3.Connection) -> None:
        """Create the SQLite schema for a GTFS sources file."""
        connection.executescript(
            """
            CREATE TABLE metadata (
                schema_version TEXT NOT NULL,
                gtfs_reference_date TEXT NOT NULL,
                countries TEXT NOT NULL,
                use_live_gtfs INTEGER NOT NULL,
                max_gtfs_file_age_days INTEGER,
                sources_created_at_utc TEXT NOT NULL,
                excluded_gtfs_sources TEXT NOT NULL,
                gtfs_service_start_date TEXT NOT NULL,
                gtfs_service_end_date TEXT NOT NULL
            );

            CREATE TABLE gtfs_files (
                country TEXT NOT NULL,
                provider TEXT NOT NULL,
                dataset_id TEXT NOT NULL,
                resource_id TEXT NOT NULL,
                title TEXT,
                download_url TEXT,
                gtfs_file_date TEXT,
                gtfs_file_age_days INTEGER,
                status TEXT NOT NULL,
                sha256 TEXT,
                service_start_date TEXT,
                service_end_date TEXT,
                minx REAL,
                miny REAL,
                maxx REAL,
                maxy REAL,
                geometry_wkb BLOB,
                PRIMARY KEY (provider, resource_id)
            );

            CREATE INDEX coverage_bbox_idx
                ON gtfs_files (minx, miny, maxx, maxy);
            CREATE INDEX gtfs_files_dataset_idx
                ON gtfs_files (provider, dataset_id);
            CREATE INDEX gtfs_files_status_idx
                ON gtfs_files (status);
            """
        )

    def insert_metadata(self, connection: sqlite3.Connection, sources_created_at_utc: str) -> None:
        """Record how the GTFS sources file was built."""
        connection.execute(
            """
            INSERT INTO metadata (
                schema_version,
                gtfs_reference_date,
                countries,
                use_live_gtfs,
                max_gtfs_file_age_days,
                sources_created_at_utc,
                excluded_gtfs_sources,
                gtfs_service_start_date,
                gtfs_service_end_date
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                self.schema_version,
                self.inputs["gtfs_reference_date"],
                ",".join(self.inputs["countries"]),
                int(self.inputs["use_live_gtfs"]),
                self.inputs["max_gtfs_file_age_days"],
                sources_created_at_utc,
                json.dumps(self.inputs.get("excluded_gtfs_sources", [])),
                self.gtfs_service_start_date,
                self.gtfs_service_end_date,
            ),
        )

    def validate_metadata(self, path=None, schema_version=None) -> dict[str, str]:
        """Check that an existing SQLite file matches the requested run."""
        connection = sqlite3.connect((path or self.cache_path).resolve().as_uri() + "?mode=ro", uri=True)
        try:
            rows = connection.execute(
                """
                SELECT
                    schema_version,
                    gtfs_reference_date,
                    countries,
                    use_live_gtfs,
                    max_gtfs_file_age_days,
                    sources_created_at_utc,
                    excluded_gtfs_sources,
                    gtfs_service_start_date,
                    gtfs_service_end_date
                FROM metadata
                """
            ).fetchall()
            problems = pd.read_sql_query(
                "SELECT provider, resource_id, dataset_id, title, status, gtfs_file_date "
                "FROM gtfs_files WHERE status IS NULL OR status NOT IN ('archived', 'live', 'excluded')",
                connection,
            )
            if not problems.empty:
                raise ValueError(self.format_unusable_sources_message(problems))
            # Check completeness in SQLite without loading coverage polygons into Python.
            count, invalid = connection.execute(
                "SELECT COUNT(*), COALESCE(SUM("
                "download_url IS NULL OR download_url = '' OR geometry_wkb IS NULL "
                "OR sha256 IS NULL OR LENGTH(sha256) != 64 OR sha256 GLOB '*[^0-9a-f]*'), 0) "
                "FROM gtfs_files WHERE status IN ('archived', 'live')"
            ).fetchone()
            if count == 0 or invalid:
                raise ValueError(
                    f"Incomplete frozen GTFS snapshot {self.cache_path}: no included feed or missing URL, "
                    "coverage or SHA-256. Restore the committed SQLite or rebuild in a new gtfs_sources_folder."
                )
        except (sqlite3.DatabaseError, pd.errors.DatabaseError) as exc:
            raise ValueError(
                f"Invalid or incompatible frozen GTFS snapshot {self.cache_path}: {exc}. "
                "Restore the committed SQLite or use a new gtfs_sources_folder to rebuild explicitly."
            ) from exc
        finally:
            connection.close()

        if len(rows) != 1:
            raise ValueError(
                f"{self.cache_path} is not a valid GTFS sources file: "
                "metadata should contain exactly one row."
            )

        row = rows[0]
        metadata = {
            "schema_version": row[0],
            "gtfs_reference_date": row[1],
            "countries": row[2],
            "use_live_gtfs": bool(row[3]),
            "max_gtfs_file_age_days": row[4],
            "sources_created_at_utc": row[5],
            "excluded_gtfs_sources": json.loads(row[6]),
            "gtfs_service_start_date": row[7],
            "gtfs_service_end_date": row[8],
        }
        expected = {
            "schema_version": schema_version or self.schema_version,
            "gtfs_reference_date": self.inputs["gtfs_reference_date"],
            "countries": ",".join(self.inputs["countries"]),
            "use_live_gtfs": self.inputs["use_live_gtfs"],
            "max_gtfs_file_age_days": self.inputs["max_gtfs_file_age_days"],
            "excluded_gtfs_sources": self.inputs.get("excluded_gtfs_sources", []),
            "gtfs_service_start_date": self.gtfs_service_start_date,
            "gtfs_service_end_date": self.gtfs_service_end_date,
        }

        for key, expected_value in expected.items():
            if metadata[key] != expected_value:
                raise ValueError(
                    f"{self.cache_path} was built with {key}={metadata[key]!r}, "
                    f"but this run requested {expected_value!r}."
                )

        return metadata

    def insert_gtfs_file(
        self,
        connection: sqlite3.Connection,
        *,
        country: str,
        provider: str,
        dataset_id: str,
        resource_id: str,
        title: str | None,
        download_url: str | None,
        gtfs_file_date: str | None,
        gtfs_file_age_days: int | None,
        status: str,
        coverage_geometry,
        sha256: str | None = None,
    ) -> None:
        """Store one GTFS source and its spatial coverage."""
        if coverage_geometry is not None and self.area_geometry is not None:
            if not coverage_geometry.intersects(self.area_geometry):
                return
        source_id = f"{provider}:{resource_id}"
        if source_id in self.inputs.get("excluded_gtfs_sources", []):
            status = "excluded"
        minx, miny, maxx, maxy = coverage_geometry.bounds if coverage_geometry is not None else (None,) * 4

        connection.execute(
            """
            INSERT OR REPLACE INTO gtfs_files (
                country,
                provider,
                dataset_id,
                resource_id,
                title,
                download_url,
                gtfs_file_date,
                gtfs_file_age_days,
                status,
                sha256,
                minx,
                miny,
                maxx,
                maxy,
                geometry_wkb
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                country,
                provider,
                dataset_id,
                resource_id,
                title,
                download_url,
                gtfs_file_date,
                gtfs_file_age_days,
                status,
                sha256,
                minx,
                miny,
                maxx,
                maxy,
                coverage_geometry.wkb if coverage_geometry is not None else None,
            ),
        )

    def format_unusable_sources_message(self, rows: pd.DataFrame) -> str:
        """Explain failures and the deliberate opt-out for each missing feed."""
        lines = [
            f"Cannot prepare public transport data for reference date {self.inputs['gtfs_reference_date']}.",
            "Some feeds could not be used:",
        ]
        for row in rows.to_dict("records"):
            source_id = f"{row['provider']}:{row['resource_id']}"
            archive_date = row.get("gtfs_file_date")
            if pd.isna(archive_date):
                archive_date = "unavailable"
            if row["status"] == "missing_archive":
                reason = "No archived file was found on or before the reference date."
            elif row["status"] == "stale_archive":
                reason = (
                    f"The latest archive ({archive_date}) is older than your "
                    f"{self.max_gtfs_file_age_days}-day limit. "
                    "Increase max_gtfs_file_age_days or set it to None to remove this limit."
                )
            elif row["status"] == "missing_coverage":
                reason = "The feed's geographic coverage is missing, so we cannot tell whether it serves your study area."
            else:
                reason = "No usable file was found. Check the source page below."
            lines.append(
                f"\n- {row.get('title') or source_id} ({source_id})\n"
                f"  {reason}\n  {self.dataset_url(row)}"
            )
        excluded = sorted(set(self.inputs.get("excluded_gtfs_sources", [])) | set(rows["provider"] + ":" + rows["resource_id"]))
        lines.append(
            "\nTo continue without these feeds, add this to PublicTransportRoutingParameters:\n"
            f"    excluded_gtfs_sources={excluded!r}\n"
            "Their services will be omitted from the results.\n"
            "Your saved sources have not been changed."
        )
        return "\n".join(lines)

    @classmethod
    def normalize_countries(cls, countries: list[str] | tuple[str, ...]) -> list[str]:
        """Return sorted supported country codes."""
        normalized = normalize_country_codes(countries)
        unknown_countries = [
            country
            for country in normalized
            if country not in available_gtfs_sources()
        ]
        if len(unknown_countries) > 0:
            raise ValueError(f"Unsupported GTFS countries: {unknown_countries}.")
        return normalized

    @staticmethod
    def dataset_url(row: dict) -> str:
        """Return the provider page where the user can inspect a GTFS source."""
        if row["provider"] == "transport_data_gouv":
            return f"https://transport.data.gouv.fr/datasets/{row['dataset_id']}"
        if row["provider"] == "opentransportdata_swiss":
            return SwissGTFS.archive_page_url
        return ""

    @staticmethod
    def parse_reference_date(gtfs_reference_date: str) -> dt.date:
        """Parse a GTFS reference date in YYYY-MM-DD format."""
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", gtfs_reference_date) is None:
            raise ValueError("gtfs_reference_date should use YYYY-MM-DD format.")
        try:
            return dt.date.fromisoformat(gtfs_reference_date)
        except ValueError as exc:
            raise ValueError("gtfs_reference_date should use YYYY-MM-DD format.") from exc

    @staticmethod
    def current_utc_timestamp() -> str:
        """Return a precise UTC timestamp for this GTFS sources file."""
        timestamp = dt.datetime.now(dt.timezone.utc).replace(microsecond=0)
        return timestamp.isoformat().replace("+00:00", "Z")

    @staticmethod
    def resolve_service_window(reference_date, start_date=None, end_date=None):
        """Default to four weeks from the reference date, with at least one Tuesday."""
        if (start_date is None) != (end_date is None):
            raise ValueError("Provide both gtfs_service_start_date and gtfs_service_end_date.")
        start = GTFSSources.parse_reference_date(start_date or reference_date)
        end = GTFSSources.parse_reference_date(end_date) if end_date else start + dt.timedelta(days=27)
        if end < start:
            raise ValueError("gtfs_service_end_date must not precede gtfs_service_start_date.")
        if start + dt.timedelta(days=(1 - start.weekday()) % 7) > end:
            raise ValueError("The GTFS service window must contain a Tuesday.")
        return start, end


class GTFSSourceSelection(FileAsset):
    """Reuse the same frozen feed selection across scenario and iteration variants."""

    def __init__(self, sources: GTFSSources, transport_zones, sources_sha256: str):
        super().__init__(
            {"sources": sources, "transport_zones": transport_zones,
             "sources_sha256": sources_sha256},
            pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"]) / "gtfs_source_selection.parquet",
        )

    def get_cached_asset(self):
        return pd.read_parquet(self.cache_path)

    def create_and_get_asset(self):
        selected = self.sources.get_gtfs_resources_for_area(self.transport_zones.get())
        temporary = self.cache_path.with_suffix(".parquet.part")
        selected.to_parquet(temporary, index=False)
        temporary.replace(self.cache_path)
        return selected
