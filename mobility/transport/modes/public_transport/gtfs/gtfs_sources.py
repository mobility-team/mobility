import datetime as dt
import logging
import pathlib
import re
import sqlite3

import geopandas as gpd
import pandas as pd
from shapely import wkb
from shapely.ops import unary_union

from mobility.runtime.assets.file_asset import FileAsset
from mobility.transport.modes.public_transport.gtfs.gtfs_source_providers import (
    FrenchGTFS,
    SwissGTFS,
)


class GTFSSources(FileAsset):
    """Project SQLite file that freezes the GTFS sources used by a run."""

    schema_version = "1"
    data_sources_by_country = {
        "fr": FrenchGTFS,
        "ch": SwissGTFS,
    }

    def __init__(
        self,
        gtfs_reference_date: str,
        gtfs_sources_folder: str | pathlib.Path,
        countries: list[str] | tuple[str, ...],
        use_live_gtfs: bool = False,
        max_gtfs_file_age_days: int = 30,
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

        file_name = (
            f"gtfs_sources_{reference_date.isoformat()}_"
            f"{'-'.join(countries)}_{mode}.sqlite"
        )
        cache_path = pathlib.Path(gtfs_sources_folder) / file_name
        super().__init__(inputs, cache_path)

    def get_cached_asset(self):
        self.validate_metadata()
        return self.cache_path

    def is_update_needed(self) -> bool:
        """Return True when the GTFS sources file is missing, stale, or incomplete."""
        if super().is_update_needed():
            return True
        try:
            self.validate_metadata()
        except (sqlite3.DatabaseError, ValueError):
            return True
        return False

    def create_and_get_asset(self):
        """Create the project GTFS sources file."""
        reference_date = self.parse_reference_date(self.inputs["gtfs_reference_date"])
        sources_created_at_utc = self.current_utc_timestamp()
        temp_path = self.cache_path.with_name(f"{self.cache_path.name}.part")

        if temp_path.exists():
            temp_path.unlink()

        connection = None
        try:
            connection = sqlite3.connect(temp_path)
            try:
                self.create_schema(connection)

                for country in self.inputs["countries"]:
                    source = self.data_sources_by_country[country](
                        reference_date,
                        sources_created_at_utc,
                        use_live_gtfs=self.inputs["use_live_gtfs"],
                        max_gtfs_file_age_days=self.inputs["max_gtfs_file_age_days"],
                    )
                    source.insert_data(connection, self)

                self.insert_metadata(connection, sources_created_at_utc)
                connection.commit()
            finally:
                connection.close()

            temp_path.replace(self.cache_path)
        except Exception:
            if temp_path.exists():
                temp_path.unlink()
            raise

        return self.cache_path

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
                    geometry_wkb
                FROM gtfs_files
                WHERE maxx >= ?
                    AND minx <= ?
                    AND maxy >= ?
                    AND miny <= ?
                """,
                connection,
                params=(minx, maxx, miny, maxy),
            )
        finally:
            connection.close()

        if candidates.empty:
            raise ValueError("No GTFS source intersects the study area.")

        # The bbox pass is cheap. This second pass removes false positives,
        # for example a source polygon with a hole around the study area.
        intersects_area = []
        for geometry_wkb in candidates["geometry_wkb"]:
            geometry = wkb.loads(geometry_wkb)
            intersects_area.append(geometry.intersects(area_geometry))

        selected = candidates.loc[intersects_area].drop(columns=["geometry_wkb"])
        if selected.empty:
            raise ValueError("No GTFS source intersects the study area.")

        stale_archive = selected.loc[selected["status"] == "stale_archive"]
        if not stale_archive.empty:
            logging.warning(
                self.format_skipped_stale_sources_message(stale_archive, metadata)
            )

        missing_archive = selected.loc[selected["status"] == "missing_archive"]
        if not missing_archive.empty:
            logging.warning(
                self.format_skipped_missing_sources_message(missing_archive)
            )

        usable_sources = selected.loc[selected["status"].isin(["archived", "live"])]
        if usable_sources.empty:
            raise ValueError(self.format_no_usable_sources_message(selected, metadata))

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
                max_gtfs_file_age_days INTEGER NOT NULL,
                sources_created_at_utc TEXT NOT NULL
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
                minx REAL NOT NULL,
                miny REAL NOT NULL,
                maxx REAL NOT NULL,
                maxy REAL NOT NULL,
                geometry_wkb BLOB NOT NULL,
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
                sources_created_at_utc
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                self.schema_version,
                self.inputs["gtfs_reference_date"],
                ",".join(self.inputs["countries"]),
                int(self.inputs["use_live_gtfs"]),
                int(self.inputs["max_gtfs_file_age_days"]),
                sources_created_at_utc,
            ),
        )

    def validate_metadata(self) -> dict[str, str]:
        """Check that an existing SQLite file matches the requested run."""
        connection = sqlite3.connect(self.cache_path)
        try:
            rows = connection.execute(
                """
                SELECT
                    schema_version,
                    gtfs_reference_date,
                    countries,
                    use_live_gtfs,
                    max_gtfs_file_age_days,
                    sources_created_at_utc
                FROM metadata
                """
            ).fetchall()
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
            "max_gtfs_file_age_days": int(row[4]),
            "sources_created_at_utc": row[5],
        }
        expected = {
            "schema_version": self.schema_version,
            "gtfs_reference_date": self.inputs["gtfs_reference_date"],
            "countries": ",".join(self.inputs["countries"]),
            "use_live_gtfs": self.inputs["use_live_gtfs"],
            "max_gtfs_file_age_days": self.inputs["max_gtfs_file_age_days"],
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
    ) -> None:
        """Store one GTFS source and its spatial coverage."""
        minx, miny, maxx, maxy = coverage_geometry.bounds

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
                minx,
                miny,
                maxx,
                maxy,
                geometry_wkb
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                float(minx),
                float(miny),
                float(maxx),
                float(maxy),
                coverage_geometry.wkb,
            ),
        )

    @classmethod
    def normalize_countries(cls, countries: list[str] | tuple[str, ...]) -> list[str]:
        """Return sorted supported country codes."""
        normalized = sorted(set(str(country).lower() for country in countries))
        unknown_countries = [
            country
            for country in normalized
            if country not in cls.data_sources_by_country
        ]
        if len(unknown_countries) > 0:
            raise ValueError(f"Unsupported GTFS countries: {unknown_countries}.")
        return normalized

    def format_skipped_missing_sources_message(self, missing_archive: pd.DataFrame) -> str:
        """Return a warning message for live-only sources skipped in strict mode."""
        lines = [
            "Some GTFS sources intersect the study area but were skipped because "
            "transport.data.gouv.fr does not provide an archived GTFS URL for them.",
        ]
        lines.append("Skipped GTFS sources:")
        lines.extend(self.format_source_rows(missing_archive))
        return "\n".join(lines)

    def format_skipped_stale_sources_message(self, stale_archive: pd.DataFrame, metadata: dict) -> str:
        """Return a warning message for stale GTFS archives skipped by freshness filtering."""
        lines = [
            "Some GTFS sources intersect the study area but were skipped because "
            "their latest archived GTFS is older than "
            f"max_gtfs_file_age_days={metadata['max_gtfs_file_age_days']}.",
        ]
        lines.append("Skipped GTFS sources:")
        lines.extend(self.format_source_rows(stale_archive, include_age=True))
        return "\n".join(lines)

    def format_no_usable_sources_message(self, selected: pd.DataFrame, metadata: dict) -> str:
        """Return a clear error when no fresh reproducible GTFS can be used."""
        lines = [
            "GTFS sources intersect the study area, but none can be used after "
            "reproducibility and freshness filtering.",
        ]
        missing_archive = selected.loc[selected["status"] == "missing_archive"]
        if not missing_archive.empty:
            lines.append("")
            lines.append("No archived GTFS found:")
            lines.extend(self.format_source_rows(missing_archive))
        stale_archive = selected.loc[selected["status"] == "stale_archive"]
        if not stale_archive.empty:
            lines.append("")
            lines.append(
                "Latest archived GTFS is older than "
                f"max_gtfs_file_age_days={metadata['max_gtfs_file_age_days']}:"
            )
            lines.extend(self.format_source_rows(stale_archive, include_age=True))
        return "\n".join(lines)

    @staticmethod
    def format_source_rows(rows: pd.DataFrame, include_age: bool = False) -> list[str]:
        """Format GTFS source rows for error messages."""
        formatted_rows = []
        for row in rows.sort_values(by=["provider", "dataset_id", "resource_id"]).to_dict("records"):
            label = (
                f"- {row['provider']}:{row['dataset_id']}:{row['resource_id']}"
                f" ({row.get('title') or 'no title'})"
            )
            if include_age:
                label += (
                    f", gtfs_file_date={row.get('gtfs_file_date')}, "
                    f"age={row.get('gtfs_file_age_days')} days"
                )
            label += f", dataset={GTFSSources.dataset_url(row)}"
            formatted_rows.append(label)
        return formatted_rows

    @staticmethod
    def dataset_url(row: dict) -> str:
        """Return the provider page where the user can inspect a GTFS source."""
        if row["provider"] == "transport_data_gouv":
            return f"https://transport.data.gouv.fr/datasets/{row['dataset_id']}"
        if row["provider"] == "opentransportdata_swiss":
            year_match = re.search(r"timetable-(\d{4})-", str(row["dataset_id"]))
            if year_match is not None and int(year_match.group(1)) < 2026:
                return SwissGTFS.archive_page_url
            return f"https://data.opentransportdata.swiss/en/dataset/{row['dataset_id']}"
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
