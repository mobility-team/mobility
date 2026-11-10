"""Temporary folders for Mobility-owned files."""

import io
import os
import threading
from contextlib import contextmanager
from pathlib import Path

if os.name == "nt":
    import msvcrt
else:
    import fcntl

_temporary_write_lock = threading.RLock()
_MANAGED_TEMPORARY_PREFIXES = (
    "result-", "mobility-shared-assets-", "mobility-run-assets-",
    "mobility-mode-search-", "mobility-gtfs-", "mobility-gtfs-calendar-",
    "mobility-map-frames-",
)


class TemporaryDiskLimitExceeded(RuntimeError):
    """Mobility's managed temporary files reached their configured size limit."""


def temporary_folder() -> Path:
    """Keep temporary files alongside the configured project data."""
    project_folder = os.environ.get("MOBILITY_PROJECT_DATA_FOLDER")
    if project_folder is None:
        package_folder = Path(os.environ.get("MOBILITY_PACKAGE_DATA_FOLDER", Path.home() / ".mobility" / "data"))
        project_folder = package_folder / "projects"
    folder = Path(project_folder) / "temporary"
    folder.mkdir(parents=True, exist_ok=True)
    return folder


def temporary_disk_limit() -> int | None:
    """Return the optional project temporary-file limit in bytes."""
    value = os.environ.get("MOBILITY_TEMPORARY_DISK_LIMIT_BYTES")
    if value is None or value == "":
        return None
    try:
        limit = int(value)
    except ValueError as exc:
        raise ValueError("MOBILITY_TEMPORARY_DISK_LIMIT_BYTES must be a non-negative integer.") from exc
    if limit < 0:
        raise ValueError("MOBILITY_TEMPORARY_DISK_LIMIT_BYTES must be a non-negative integer.")
    return limit


def managed_temporary_disk_usage() -> int:
    """Count files in Mobility-owned temporary directories for this project."""
    total = 0
    for directory in temporary_folder().iterdir():
        if not directory.name.startswith(_MANAGED_TEMPORARY_PREFIXES) or not directory.is_dir():
            continue
        for root, _, names in os.walk(directory):
            for name in names:
                try:
                    total += (Path(root) / name).stat().st_size
                except FileNotFoundError:
                    # Another completed result may close while usage is read.
                    continue
    return total


@contextmanager
def _temporary_disk_lock():
    """Serialize managed writes from Mobility processes in the same project."""
    with _temporary_write_lock:
        lock_path = temporary_folder() / ".mobility-temporary.lock"
        with lock_path.open("a+b") as lock_file:
            if lock_file.tell() == 0:
                lock_file.write(b"\0")
                lock_file.flush()
            lock_file.seek(0)
            if os.name == "nt":
                msvcrt.locking(lock_file.fileno(), msvcrt.LK_LOCK, 1)
            else:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                lock_file.seek(0)
                if os.name == "nt":
                    msvcrt.locking(lock_file.fileno(), msvcrt.LK_UNLCK, 1)
                else:
                    fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


def check_temporary_disk_limit() -> None:
    """Check project usage after a tool that writes its own temporary file."""
    limit = temporary_disk_limit()
    if limit is None:
        return
    with _temporary_disk_lock():
        used = managed_temporary_disk_usage()
        if used > limit:
            raise TemporaryDiskLimitExceeded(
                f"Mobility temporary files used {used} bytes, exceeding the {limit}-byte limit. "
                "Close unused results or increase MOBILITY_TEMPORARY_DISK_LIMIT_BYTES."
            )


class _LimitedTemporaryWriter(io.BufferedIOBase):
    """Enforce the project limit before writing bytes to a temporary file."""

    def __init__(self, file, limit: int) -> None:
        self.file = file
        self.limit = limit
        self.limit_error: TemporaryDiskLimitExceeded | None = None

    def writable(self) -> bool:
        return True

    def tell(self) -> int:
        return self.file.tell()

    def write(self, data: bytes) -> int:
        with _temporary_disk_lock():
            used = managed_temporary_disk_usage()
            if used + len(data) > self.limit:
                self.limit_error = TemporaryDiskLimitExceeded(
                    f"Mobility temporary files reached the {self.limit}-byte limit "
                    f"({used} bytes already used). Set MOBILITY_TEMPORARY_DISK_LIMIT_BYTES "
                    "to a larger value or close unused results."
                )
                raise self.limit_error
            return self.file.write(data)

    def flush(self) -> None:
        if not self.file.closed:
            self.file.flush()


@contextmanager
def temporary_file_writer(path: Path):
    """Open a Mobility-owned temporary file with the configured write limit."""
    limit = temporary_disk_limit()
    try:
        with path.open("wb", buffering=0 if limit is not None else -1) as file:
            writer = _LimitedTemporaryWriter(file, limit) if limit is not None else file
            try:
                yield writer
            except Exception as exc:
                if isinstance(writer, _LimitedTemporaryWriter) and writer.limit_error is not None:
                    raise writer.limit_error from exc
                raise
    except BaseException:
        path.unlink(missing_ok=True)
        raise


def write_temporary_parquet(table, path: Path) -> None:
    """Write a temporary Polars table, enforcing the limit when configured."""
    limit = temporary_disk_limit()
    if limit is None:
        try:
            if hasattr(table, "sink_parquet"):
                table.sink_parquet(path, engine="streaming")
            else:
                table.write_parquet(path)
        except BaseException:
            # A failed spill must not look like a complete file on the next
            # read, including when an execution is interrupted.
            path.unlink(missing_ok=True)
            raise
        return
    with temporary_file_writer(path) as writer:
        if hasattr(table, "sink_parquet"):
            table.sink_parquet(writer, engine="streaming")
        else:
            table.write_parquet(writer)


def write_temporary_bytes(data: bytes, path: Path) -> None:
    """Write a small Mobility-owned temporary file under the same limit."""
    limit = temporary_disk_limit()
    if limit is None:
        path.write_bytes(data)
        return
    with temporary_file_writer(path) as writer:
        writer.write(data)
