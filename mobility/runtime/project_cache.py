import hashlib
import logging
import os
import pathlib
import sqlite3
import sys
import time
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any


logger = logging.getLogger(__name__)

_active_project_ref_version: ContextVar[str | None] = ContextVar(
    "active_project_ref_version",
    default=None,
)
_active_project_ref_folder: ContextVar[str | None] = ContextVar(
    "active_project_ref_folder",
    default=None,
)
_registries_by_project_folder: dict[str, "ProjectCacheRegistry"] = {}
_UNTRACKED_SCAN_BATCH_SIZE = 1000


@dataclass(frozen=True)
class ProjectCacheReport:
    """Cleanup summary.

    Modellers usually only need to print this object. The path lists are kept
    available for notebooks that need to inspect the files before deleting them.
    """

    preview: bool
    deleted_files: list[pathlib.Path]
    files_to_delete: list[pathlib.Path]
    kept_files_count: int
    protected_files_count: int
    protected_paths: list[pathlib.Path]
    untracked_files_count: int
    untracked_size_bytes: int
    include_untracked_files: bool
    tracked_files_to_delete_count: int
    tracked_bytes_to_delete: int
    untracked_files_to_delete_count: int
    untracked_bytes_to_delete: int
    bytes_to_delete: int
    blockers: list[str]

    def __str__(self) -> str:
        """Return a plain-language cleanup summary."""
        lines = []
        if self.blockers:
            if self.preview:
                lines.append("Preview: no files were deleted.")
            else:
                lines.append("Cleanup is blocked.")
            lines.append("Tracked cache cleanup is blocked.")
        elif self.preview:
            lines.append("Preview: no files were deleted.")
        elif self.include_untracked_files:
            lines.append(
                f"Deleted {len(self.deleted_files)} files not registered by Mobility."
            )
        else:
            lines.append(f"Deleted {len(self.deleted_files)} Mobility cache files.")

        if self.blockers:
            lines.append("")
            lines.append(
                "These cache sources need your attention before tracked cache "
                "files can be deleted:"
            )
            lines.extend(f"- {blocker}" for blocker in self.blockers)
            lines.append("")
            lines.append(
                "Run changed scripts again, or archive cache sources you no longer need."
            )

        lines.append("")
        lines.append("Cleanup candidates:")
        if self.include_untracked_files:
            lines.append(
                f"- {self.untracked_files_to_delete_count} files not registered "
                f"by Mobility "
                f"({format_bytes(self.untracked_bytes_to_delete)})"
            )
            lines.append(
                f"Mobility will keep {self.kept_files_count} registered cache files."
            )
            if self.untracked_files_to_delete_count:
                lines.append(
                    "These files are inside the project data folder but not in "
                    "Mobility's cache registry. On older projects, this can include "
                    "old Mobility cache files and user files."
                )
                lines.append(
                    "Review the list and protect input or output folders before "
                    "removing them."
                )
        else:
            lines.append(
                f"- {self.tracked_files_to_delete_count} unused registered cache files "
                f"({format_bytes(self.tracked_bytes_to_delete)})"
            )
            lines.append(
                f"Mobility will keep {self.kept_files_count} registered cache files."
            )

        if self.protected_files_count:
            lines.append(f"Skipped {self.protected_files_count} protected files.")
        if self.protected_paths:
            lines.append("")
            lines.append("Protected project data:")
            lines.extend(f"- {path}" for path in self.protected_paths)
        if self.untracked_files_count and not self.include_untracked_files:
            lines.append("")
            lines.append("Files not registered by Mobility:")
            lines.append(
                f"- {self.untracked_files_count} files "
                f"({format_bytes(self.untracked_size_bytes)})"
            )
            lines.append(
                "These files are not in Mobility's cache registry. "
                "They are not removed by normal cache cleanup."
            )
        return "\n".join(lines)

    def __repr__(self) -> str:
        """Show the readable report in notebooks instead of a huge path dump."""
        return str(self)


class ProjectCache:
    """User-facing project-data cache tools."""

    def __init__(self, project_folder: str | pathlib.Path | None = None) -> None:
        if project_folder is None:
            project_folder = _project_folder()
        self._registry = _registry_for_project_folder(pathlib.Path(project_folder))

    def register_cache_source(self, name: str) -> None:
        """Register a notebook or named cache source."""
        self._register_cache_source(name, ref_type="manual")

    def _register_cache_source(self, name: str, *, ref_type: str) -> str:
        version_id = self._registry.register_cache_source(
            name,
            source_type=ref_type,
        )
        _active_project_ref_version.set(version_id)
        _active_project_ref_folder.set(str(self._registry.project_folder))
        return version_id

    def unused_files_preview(
        self,
    ) -> ProjectCacheReport:
        """Show tracked cache files that no latest cache source uses."""
        return self._registry.clean_project_data(
            dry_run=True,
            include_untracked_files=False,
        )

    def remove_unused_files(
        self,
    ) -> ProjectCacheReport:
        """Delete cache files that no active cache source uses."""
        return self._registry.clean_project_data(
            dry_run=False,
            include_untracked_files=False,
        )

    def untracked_files_preview(self) -> ProjectCacheReport:
        """Show untracked project files that cleanup would delete."""
        return self._registry.clean_project_data(
            dry_run=True,
            include_untracked_files=True,
        )

    def remove_untracked_files(self) -> ProjectCacheReport:
        """Delete untracked project files."""
        return self._registry.clean_project_data(
            dry_run=False,
            include_untracked_files=True,
        )

    def cache_sources(self) -> list[dict[str, Any]]:
        """List scripts and notebooks that keep cache files alive."""
        return self._registry.list_cache_sources()

    def archive_cache_source(self, name: str) -> None:
        """Stop one cache source from keeping cache files alive."""
        self._registry.set_cache_source_status(name, "archived")

    def restore_cache_source(self, name: str) -> None:
        """Make one archived cache source active again."""
        self._registry.set_cache_source_status(name, "active")

    def protect(self, path: str | pathlib.Path) -> None:
        """Protect a project-data file or folder from cleanup."""
        self._registry.add_protected_path(path)

    def unprotect(self, path: str | pathlib.Path) -> None:
        """Remove one protected project-data path."""
        self._registry.remove_protected_path(path)

    def protected_paths(self) -> list[pathlib.Path]:
        """Return project-data files or folders protected from cleanup."""
        return self._registry.list_protected_paths()


def register_current_script_if_available() -> str | None:
    """Register the running Python script when Mobility can detect one."""
    main_module = sys.modules.get("__main__")
    script_path = getattr(main_module, "__file__", None)
    if not script_path:
        return None

    path = pathlib.Path(script_path)
    if not path.exists() or path.suffix.lower() != ".py":
        return None

    cache = ProjectCache()
    return cache._register_cache_source(
        str(path),
        ref_type="script",
    )


def active_project_ref_version() -> str | None:
    """Return the cache-source version currently linked to asset reads."""
    project_folder = os.environ.get("MOBILITY_PROJECT_DATA_FOLDER")
    if project_folder is None:
        return None
    if _active_project_ref_folder.get() != str(pathlib.Path(project_folder).resolve()):
        return None
    return _active_project_ref_version.get()


def record_file_asset_use(asset: Any) -> None:
    """Record one FileAsset in the project cache registry."""
    project_folder = os.environ.get("MOBILITY_PROJECT_DATA_FOLDER")
    if not project_folder:
        return

    registry = _registry_for_project_folder(pathlib.Path(project_folder))
    registry.record_asset(asset, version_id=active_project_ref_version())


def format_bytes(size_bytes: int) -> str:
    """Format a byte count for modeller-facing reports."""
    size = float(size_bytes)
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size < 1024 or unit == "TB":
            if unit == "B":
                return f"{int(size)} {unit}"
            return f"{size:.1f} {unit}"
        size = size / 1024
    return f"{size_bytes} B"


class ProjectCacheRegistry:
    """SQLite registry for Mobility project cache files."""

    def __init__(self, project_folder: pathlib.Path) -> None:
        self.project_folder = pathlib.Path(project_folder).resolve()
        self.registry_folder = self.project_folder / ".mobility"
        self.database_path = self.registry_folder / "cache.sqlite"
        self.registry_folder.mkdir(parents=True, exist_ok=True)
        self._ensure_schema()

    def register_cache_source(self, name: str, *, source_type: str) -> str:
        """Register one cache source version and return its version id."""
        now = _now()
        ref_type = str(source_type)
        source_path = None
        source_hash = None
        ref_name = str(name)

        if ref_type == "script":
            source_path = str(pathlib.Path(name).resolve())
            ref_name = _display_path(pathlib.Path(source_path), self.project_folder)
            source_hash = _hash_file(pathlib.Path(source_path))
        else:
            source_hash = _hash_text(f"{ref_type}:{ref_name}:{now}")

        ref_id = _hash_text(f"{ref_type}:{source_path or ref_name}")
        version_id = _hash_text(f"{ref_id}:{source_hash}")

        with self._connect() as con:
            con.execute(
                """
                INSERT INTO project_refs (
                    ref_id, ref_type, name, source_path, status, created_at, last_seen_at
                )
                VALUES (?, ?, ?, ?, 'active', ?, ?)
                ON CONFLICT(ref_id) DO UPDATE SET
                    name=excluded.name,
                    source_path=excluded.source_path,
                    last_seen_at=excluded.last_seen_at
                """,
                (ref_id, ref_type, ref_name, source_path, now, now),
            )
            con.execute(
                """
                INSERT INTO project_ref_versions (
                    version_id, ref_id, source_hash, created_at, is_latest
                )
                VALUES (?, ?, ?, ?, 0)
                ON CONFLICT(version_id) DO NOTHING
                """,
                (version_id, ref_id, source_hash, now),
            )
        return version_id

    def record_asset(self, asset: Any, *, version_id: str | None) -> None:
        """Record one file-backed asset and link it to the active ref version."""
        now = _now()
        asset_type = asset.__class__.__name__
        inputs_hash = str(getattr(asset, "inputs_hash", ""))
        paths = _asset_paths(asset)
        with self._connect() as con:
            version_marked_latest = False
            for role, path in paths:
                resolved = pathlib.Path(path).resolve()
                if not _is_inside(resolved, self.project_folder):
                    continue
                if not resolved.is_file():
                    continue
                size = resolved.stat().st_size
                asset_key = _hash_text(f"{asset_type}:{inputs_hash}:{resolved}")
                con.execute(
                    """
                    INSERT INTO asset_cache_entries (
                        path, asset_key, asset_type, inputs_hash, path_role,
                        project_data_folder, size_bytes, first_seen_at, last_seen_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(path) DO UPDATE SET
                        asset_key=excluded.asset_key,
                        asset_type=excluded.asset_type,
                        inputs_hash=excluded.inputs_hash,
                        path_role=excluded.path_role,
                        size_bytes=excluded.size_bytes,
                        last_seen_at=excluded.last_seen_at
                    """,
                    (
                        str(resolved),
                        asset_key,
                        asset_type,
                        inputs_hash,
                        role,
                        str(self.project_folder),
                        size,
                        now,
                        now,
                    ),
                )
                if version_id is not None:
                    if not version_marked_latest:
                        self._mark_version_latest(con, version_id)
                        version_marked_latest = True
                    con.execute(
                        """
                        INSERT OR IGNORE INTO ref_assets (version_id, path)
                        VALUES (?, ?)
                        """,
                        (version_id, str(resolved)),
                    )
                con.execute(
                    "DELETE FROM untracked_project_files WHERE path=?",
                    (str(resolved),),
                )

    def _mark_version_latest(self, con: sqlite3.Connection, version_id: str) -> None:
        row = con.execute(
            "SELECT ref_id FROM project_ref_versions WHERE version_id=?",
            (version_id,),
        ).fetchone()
        if row is None:
            return
        con.execute(
            """
            UPDATE project_ref_versions
            SET is_latest=0
            WHERE ref_id=? AND version_id<>?
            """,
            (row["ref_id"], version_id),
        )
        con.execute(
            """
            UPDATE project_ref_versions
            SET is_latest=1
            WHERE version_id=?
            """,
            (version_id,),
        )

    def clean_project_data(
        self,
        *,
        dry_run: bool,
        include_untracked_files: bool,
    ) -> ProjectCacheReport:
        """Return a cleanup report and optionally delete unreferenced files."""
        started_at = time.perf_counter()
        logger.debug(
            "Project cache cleanup started: project_folder=%s dry_run=%s "
            "include_untracked_files=%s",
            self.project_folder,
            dry_run,
            include_untracked_files,
        )

        # Untracked-file previews update Mobility's internal index, but never
        # delete project files.
        if include_untracked_files:
            self.scan_untracked_files()
        blockers = [] if include_untracked_files else self._cleanup_blockers()
        protected_paths = self.list_protected_paths()
        keep_paths = self._latest_active_ref_paths()
        source_paths = self._cache_source_paths()
        asset_rows = [] if include_untracked_files else self._asset_rows()
        untracked_rows = (
            self._untracked_file_rows()
            if include_untracked_files
            else []
        )
        project_folder_key = _resolved_path_key(self.project_folder)
        protected_path_keys = [_resolved_path_key(path) for path in protected_paths]
        keep_path_keys = {_stored_path_key(path) for path in keep_paths | source_paths}
        logger.debug(
            "Project cache cleanup loaded registry state: tracked_files=%d "
            "kept_files=%d untracked_files=%d protected_paths=%d blockers=%d",
            len(asset_rows),
            len(keep_paths),
            len(untracked_rows),
            len(protected_paths),
            len(blockers),
        )

        if blockers and not dry_run:
            blocker_text = "\n".join(f"- {blocker}" for blocker in blockers)
            raise RuntimeError(
                "Cleanup stopped because some cache sources need attention:\n"
                f"{blocker_text}"
            )

        files_to_delete = []
        tracked_files_to_delete = []
        untracked_files_to_delete = []
        protected_files_count = 0
        tracked_bytes_to_delete = 0
        untracked_bytes_to_delete = 0
        if not include_untracked_files and not blockers:
            for path, size_bytes in asset_rows:
                path_key = _stored_path_key(path)
                if path_key in keep_path_keys:
                    continue
                if _is_protected_key(path_key, protected_path_keys):
                    protected_files_count += 1
                    continue
                if not _is_inside_key(path_key, project_folder_key):
                    continue
                files_to_delete.append(path)
                tracked_files_to_delete.append(path)
                tracked_bytes_to_delete += size_bytes

        if include_untracked_files:
            filter_started_at = time.perf_counter()
            for path, size_bytes in untracked_rows:
                path_key = _stored_path_key(path)
                if path_key in keep_path_keys:
                    continue
                if _is_protected_key(path_key, protected_path_keys):
                    protected_files_count += 1
                    continue
                files_to_delete.append(path)
                untracked_files_to_delete.append(path)
                untracked_bytes_to_delete += size_bytes
            logger.debug(
                "Project cache cleanup filtered untracked files in %.2fs: "
                "untracked_to_delete=%d protected_skipped=%d",
                time.perf_counter() - filter_started_at,
                len(untracked_files_to_delete),
                protected_files_count,
            )

        deleted_files = []
        deleted_tracked_files = []
        deleted_untracked_files = []
        if not dry_run:
            tracked_delete_set = set(tracked_files_to_delete)
            untracked_delete_set = set(untracked_files_to_delete)
            for path in files_to_delete:
                if path.exists() and path.is_file():
                    path.unlink()
                    deleted_files.append(path)
                    if path in tracked_delete_set:
                        deleted_tracked_files.append(path)
                    if path in untracked_delete_set:
                        deleted_untracked_files.append(path)
            self._remove_tracked_files(deleted_tracked_files)
            self._remove_untracked_files(deleted_untracked_files)

        untracked_size = sum(size for _, size in untracked_rows)
        report = ProjectCacheReport(
            preview=dry_run,
            deleted_files=deleted_files,
            files_to_delete=files_to_delete,
            kept_files_count=len(keep_paths),
            protected_files_count=protected_files_count,
            protected_paths=protected_paths,
            untracked_files_count=len(untracked_rows),
            untracked_size_bytes=untracked_size,
            include_untracked_files=include_untracked_files,
            tracked_files_to_delete_count=len(tracked_files_to_delete),
            tracked_bytes_to_delete=tracked_bytes_to_delete,
            untracked_files_to_delete_count=len(untracked_files_to_delete),
            untracked_bytes_to_delete=untracked_bytes_to_delete,
            bytes_to_delete=tracked_bytes_to_delete + untracked_bytes_to_delete,
            blockers=blockers,
        )
        logger.debug(
            "Project cache cleanup finished in %.2fs: tracked_to_delete=%d "
            "untracked_to_delete=%d protected_skipped=%d deleted=%d",
            time.perf_counter() - started_at,
            len(tracked_files_to_delete),
            len(untracked_files_to_delete),
            protected_files_count,
            len(deleted_files),
        )
        return report

    def list_cache_sources(self) -> list[dict[str, Any]]:
        """Return one row per cache source with computed file status."""
        rows = []
        with self._connect() as con:
            for row in con.execute(
                """
                SELECT r.ref_id, r.ref_type, r.name, r.source_path, r.status,
                       r.created_at AS ref_created_at,
                       r.last_seen_at,
                       v.source_hash,
                       v.created_at AS version_created_at
                FROM project_refs r
                LEFT JOIN project_ref_versions v
                    ON r.ref_id = v.ref_id AND v.is_latest = 1
                ORDER BY r.name
                """
            ):
                status = row["status"]
                if status == "active" and row["ref_type"] == "script":
                    status = self._script_status(row["source_path"], row["source_hash"])
                rows.append(
                    {
                        "name": row["name"],
                        "source_type": row["ref_type"],
                        "source_path": row["source_path"],
                        "status": status,
                        "created_at": row["ref_created_at"],
                        "last_seen_at": row["last_seen_at"],
                        "latest_registered_at": row["version_created_at"],
                    }
                )
        return rows

    def set_cache_source_status(self, name: str, status: str) -> None:
        """Set one cache source status by display name or source path."""
        if status not in {"active", "archived"}:
            raise ValueError("Cache source status should be 'active' or 'archived'.")

        source_id = self._find_cache_source_id(name)
        if source_id is None:
            raise ValueError(f"Unknown cache source: {name}")

        with self._connect() as con:
            con.execute(
                "UPDATE project_refs SET status=? WHERE ref_id=?",
                (status, source_id),
            )

    def add_protected_path(self, path: str | pathlib.Path) -> None:
        """Store one protected project-data path."""
        resolved = self._resolve_project_path(path)
        if not _is_inside(resolved, self.project_folder):
            raise ValueError(
                "Protected paths must be inside MOBILITY_PROJECT_DATA_FOLDER."
            )
        with self._connect() as con:
            con.execute(
                """
                INSERT OR IGNORE INTO protected_paths (path, created_at)
                VALUES (?, ?)
                """,
                (str(resolved), _now()),
            )

    def remove_protected_path(self, path: str | pathlib.Path) -> None:
        """Remove one protected project-data path."""
        resolved = self._resolve_project_path(path)
        with self._connect() as con:
            con.execute("DELETE FROM protected_paths WHERE path=?", (str(resolved),))

    def list_protected_paths(self) -> list[pathlib.Path]:
        """Return protected project-data paths."""
        with self._connect() as con:
            return [
                pathlib.Path(row["path"])
                for row in con.execute("SELECT path FROM protected_paths ORDER BY path")
            ]

    def scan_untracked_files(self) -> None:
        """Index untracked project files without making them deletable by default."""
        started_at = time.perf_counter()
        scanned_files_count = 0
        registry_files_count = 0
        tracked_files_count = 0
        untracked_files_count = 0
        rows_to_upsert = []
        now = _now()
        registry_folder_key = _resolved_path_key(self.registry_folder)
        logger.debug(
            "Project cache untracked-file scan started: project_folder=%s",
            self.project_folder,
        )
        with self._connect() as con:
            self._forget_missing_untracked_files(con)
            indexed_paths = {
                _stored_path_key(pathlib.Path(row["path"]))
                for row in con.execute("SELECT path FROM asset_cache_entries")
            }
            for file_path, size_bytes in _walk_project_files(self.project_folder):
                scanned_files_count += 1
                file_key = os.path.normcase(file_path)
                if _is_inside_key(file_key, registry_folder_key):
                    registry_files_count += 1
                    continue
                if file_key in indexed_paths:
                    tracked_files_count += 1
                    continue
                untracked_files_count += 1
                rows_to_upsert.append((file_path, size_bytes, now, now))
                if len(rows_to_upsert) >= _UNTRACKED_SCAN_BATCH_SIZE:
                    self._upsert_untracked_file_rows(con, rows_to_upsert)
                    rows_to_upsert.clear()
                if (
                    logger.isEnabledFor(logging.DEBUG)
                    and scanned_files_count % 10000 == 0
                ):
                    logger.debug(
                        "Project cache untracked-file scan progress: scanned=%d "
                        "untracked=%d tracked=%d registry=%d elapsed=%.2fs",
                        scanned_files_count,
                        untracked_files_count,
                        tracked_files_count,
                        registry_files_count,
                        time.perf_counter() - started_at,
                    )
            if rows_to_upsert:
                self._upsert_untracked_file_rows(con, rows_to_upsert)
        logger.debug(
            "Project cache untracked-file scan finished in %.2fs: scanned=%d "
            "untracked=%d tracked=%d registry=%d",
            time.perf_counter() - started_at,
            scanned_files_count,
            untracked_files_count,
            tracked_files_count,
            registry_files_count,
        )

    def _upsert_untracked_file_rows(
        self,
        con: sqlite3.Connection,
        rows: list[tuple[str, int, str, str]],
    ) -> None:
        con.executemany(
            """
            INSERT INTO untracked_project_files (
                path, size_bytes, first_seen_at, last_seen_at
            )
            VALUES (?, ?, ?, ?)
            ON CONFLICT(path) DO UPDATE SET
                size_bytes=excluded.size_bytes,
                last_seen_at=excluded.last_seen_at
            """,
            rows,
        )

    def _connect(self) -> sqlite3.Connection:
        con = sqlite3.connect(self.database_path)
        con.row_factory = sqlite3.Row
        return con

    def _ensure_schema(self) -> None:
        with self._connect() as con:
            con.executescript(
                """
                CREATE TABLE IF NOT EXISTS project_refs (
                    ref_id TEXT PRIMARY KEY,
                    ref_type TEXT NOT NULL,
                    name TEXT NOT NULL,
                    source_path TEXT,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    last_seen_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS project_ref_versions (
                    version_id TEXT PRIMARY KEY,
                    ref_id TEXT NOT NULL,
                    source_hash TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    is_latest INTEGER NOT NULL,
                    FOREIGN KEY(ref_id) REFERENCES project_refs(ref_id)
                );

                CREATE TABLE IF NOT EXISTS asset_cache_entries (
                    path TEXT PRIMARY KEY,
                    asset_key TEXT NOT NULL,
                    asset_type TEXT NOT NULL,
                    inputs_hash TEXT NOT NULL,
                    path_role TEXT NOT NULL,
                    project_data_folder TEXT NOT NULL,
                    size_bytes INTEGER NOT NULL,
                    first_seen_at TEXT NOT NULL,
                    last_seen_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS ref_assets (
                    version_id TEXT NOT NULL,
                    path TEXT NOT NULL,
                    PRIMARY KEY(version_id, path),
                    FOREIGN KEY(version_id) REFERENCES project_ref_versions(version_id)
                );

                CREATE TABLE IF NOT EXISTS untracked_project_files (
                    path TEXT PRIMARY KEY,
                    size_bytes INTEGER NOT NULL,
                    first_seen_at TEXT NOT NULL,
                    last_seen_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS protected_paths (
                    path TEXT PRIMARY KEY,
                    created_at TEXT NOT NULL
                );
                """
            )
            self._remove_exists_on_disk_column(con)

    def _remove_exists_on_disk_column(self, con: sqlite3.Connection) -> None:
        columns = [
            row["name"]
            for row in con.execute("PRAGMA table_info(asset_cache_entries)")
        ]
        if "exists_on_disk" not in columns:
            return
        con.executescript(
            """
            CREATE TABLE asset_cache_entries_new (
                path TEXT PRIMARY KEY,
                asset_key TEXT NOT NULL,
                asset_type TEXT NOT NULL,
                inputs_hash TEXT NOT NULL,
                path_role TEXT NOT NULL,
                project_data_folder TEXT NOT NULL,
                size_bytes INTEGER NOT NULL,
                first_seen_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL
            );

            INSERT INTO asset_cache_entries_new (
                path, asset_key, asset_type, inputs_hash, path_role,
                project_data_folder, size_bytes, first_seen_at, last_seen_at
            )
            SELECT
                path, asset_key, asset_type, inputs_hash, path_role,
                project_data_folder, size_bytes, first_seen_at, last_seen_at
            FROM asset_cache_entries
            WHERE exists_on_disk = 1;

            DROP TABLE asset_cache_entries;
            ALTER TABLE asset_cache_entries_new RENAME TO asset_cache_entries;
            """
        )

    def _cleanup_blockers(self) -> list[str]:
        blockers = []
        with self._connect() as con:
            rows = con.execute(
                """
                SELECT r.ref_type, r.name, r.source_path, v.source_hash
                FROM project_refs r
                JOIN project_ref_versions v
                    ON r.ref_id = v.ref_id AND v.is_latest = 1
                WHERE r.status = 'active'
                ORDER BY r.name
                """
            )
            for row in rows:
                if row["ref_type"] != "script":
                    continue
                status = self._script_status(row["source_path"], row["source_hash"])
                if status == "missing":
                    blockers.append(
                        f"{row['name']} is missing. "
                        "Archive this cache source before removing cache files."
                    )
                elif status == "modified":
                    blockers.append(
                        f"{row['name']} changed since its last registered run. "
                        "Rerun it before removing cache files."
                    )
        return blockers

    def _script_status(self, source_path: str | None, source_hash: str | None) -> str:
        if not source_path:
            return "active"
        path = pathlib.Path(source_path)
        if not path.exists():
            return "missing"
        if source_hash and _hash_file(path) != source_hash:
            return "modified"
        return "active"

    def _latest_active_ref_paths(self) -> set[pathlib.Path]:
        with self._connect() as con:
            return {
                pathlib.Path(row["path"])
                for row in con.execute(
                    """
                    SELECT DISTINCT a.path
                    FROM ref_assets a
                    JOIN project_ref_versions v ON a.version_id = v.version_id
                    JOIN project_refs r ON v.ref_id = r.ref_id
                    WHERE r.status = 'active' AND v.is_latest = 1
                    """
                )
            }

    def _cache_source_paths(self) -> set[pathlib.Path]:
        with self._connect() as con:
            return {
                pathlib.Path(row["source_path"])
                for row in con.execute(
                    """
                    SELECT source_path
                    FROM project_refs
                    WHERE source_path IS NOT NULL
                    """
                )
            }

    def _asset_rows(self) -> list[tuple[pathlib.Path, int]]:
        with self._connect() as con:
            return [
                (pathlib.Path(row["path"]), int(row["size_bytes"]))
                for row in con.execute(
                    """
                    SELECT path, size_bytes
                    FROM asset_cache_entries
                    ORDER BY path
                    """
                )
            ]

    def _untracked_file_rows(self) -> list[tuple[pathlib.Path, int]]:
        with self._connect() as con:
            return [
                (pathlib.Path(row["path"]), int(row["size_bytes"]))
                for row in con.execute(
                    "SELECT path, size_bytes FROM untracked_project_files ORDER BY path"
                )
            ]

    def _forget_missing_untracked_files(self, con: sqlite3.Connection) -> None:
        rows = list(con.execute("SELECT path FROM untracked_project_files"))
        for row in rows:
            if not pathlib.Path(row["path"]).exists():
                con.execute(
                    "DELETE FROM untracked_project_files WHERE path=?",
                    (row["path"],),
                )

    def _remove_tracked_files(self, paths: list[pathlib.Path]) -> None:
        if not paths:
            return
        rows = [(str(path.resolve()),) for path in paths]
        with self._connect() as con:
            con.executemany("DELETE FROM ref_assets WHERE path=?", rows)
            con.executemany("DELETE FROM asset_cache_entries WHERE path=?", rows)

    def _remove_untracked_files(self, paths: list[pathlib.Path]) -> None:
        if not paths:
            return
        rows = [(str(path.resolve()),) for path in paths]
        with self._connect() as con:
            con.executemany("DELETE FROM untracked_project_files WHERE path=?", rows)

    def _resolve_project_path(self, path: str | pathlib.Path) -> pathlib.Path:
        candidate = pathlib.Path(path)
        if not candidate.is_absolute():
            candidate = self.project_folder / candidate
        return candidate.resolve()

    def _find_cache_source_id(self, name: str) -> str | None:
        target = str(name)
        target_path = (
            pathlib.Path(target).resolve()
            if pathlib.Path(target).exists()
            else None
        )
        with self._connect() as con:
            for row in con.execute("SELECT ref_id, name, source_path FROM project_refs"):
                if row["name"] == target or row["source_path"] == target:
                    return row["ref_id"]
                if target_path is not None and row["source_path"] == str(target_path):
                    return row["ref_id"]
        return None


def _asset_paths(asset: Any) -> list[tuple[str, pathlib.Path]]:
    paths = []
    cache_path = getattr(asset, "cache_path", None)
    if isinstance(cache_path, dict):
        for role, path in sorted(cache_path.items()):
            paths.append((str(role), pathlib.Path(path)))
    elif cache_path is not None:
        paths.append(("output", pathlib.Path(cache_path)))

    hash_path = getattr(asset, "hash_path", None)
    if hash_path is not None:
        paths.append(("inputs_hash", pathlib.Path(hash_path)))
    return paths


def _project_folder() -> pathlib.Path:
    value = os.environ.get("MOBILITY_PROJECT_DATA_FOLDER")
    if not value:
        raise RuntimeError(
            "MOBILITY_PROJECT_DATA_FOLDER is not set. "
            "Call mobility.set_params(...) before using project cache tools."
        )
    return pathlib.Path(value)


def _walk_project_files(project_folder: pathlib.Path):
    """Yield project files with sizes using a fast directory scanner."""
    folders = [str(project_folder)]
    while folders:
        folder = folders.pop()
        try:
            entries = list(os.scandir(folder))
        except OSError as error:
            logger.debug(
                "Skipping unreadable project-cache folder %s: %s",
                folder,
                error,
            )
            continue

        for entry in entries:
            try:
                if entry.is_dir(follow_symlinks=False):
                    folders.append(entry.path)
                elif entry.is_file(follow_symlinks=False):
                    yield entry.path, entry.stat(follow_symlinks=False).st_size
            except OSError as error:
                logger.debug(
                    "Skipping unreadable project-cache path %s: %s",
                    entry.path,
                    error,
                )


def _registry_for_project_folder(project_folder: pathlib.Path) -> ProjectCacheRegistry:
    resolved = str(pathlib.Path(project_folder).resolve())
    if resolved not in _registries_by_project_folder:
        _registries_by_project_folder[resolved] = ProjectCacheRegistry(
            pathlib.Path(resolved)
        )
    return _registries_by_project_folder[resolved]


def _hash_file(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _hash_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def _is_inside(path: pathlib.Path, folder: pathlib.Path) -> bool:
    try:
        path.resolve().relative_to(folder.resolve())
    except ValueError:
        return False
    return True


def _resolved_path_key(path: pathlib.Path) -> str:
    """Return a normalized absolute path string for fast path comparisons."""
    return os.path.normcase(str(path.resolve()))


def _stored_path_key(path: pathlib.Path) -> str:
    """Return a normalized path string for paths already resolved in the registry."""
    return os.path.normcase(str(path))


def _is_inside_key(path_key: str, folder_key: str) -> bool:
    return path_key == folder_key or path_key.startswith(folder_key + os.sep)


def _is_protected_key(path_key: str, protected_path_keys: list[str]) -> bool:
    return any(
        _is_inside_key(path_key, protected_key)
        for protected_key in protected_path_keys
    )


def _display_path(path: pathlib.Path, project_folder: pathlib.Path) -> str:
    try:
        return str(path.relative_to(project_folder))
    except ValueError:
        return str(path)
