from __future__ import annotations

import os
import tempfile
from pathlib import Path

import polars as pl

from mobility.runtime.temporary import temporary_folder, write_temporary_parquet


class ResultTable:
    """A transient table whose files remain available until it is closed.

    Table operations return another view with the same owner. Closing any view
    closes them all; collecting a view returns an independent DataFrame.
    """

    def __init__(self) -> None:
        self._directory = tempfile.TemporaryDirectory(
            prefix="result-", dir=temporary_folder()
        )
        self._paths: list[Path] = []
        self._closed = False
        self._operations = ()
        # The owner must not refer to itself: when the last table or view is
        # released, TemporaryDirectory can clean up without waiting for GC.
        self._owner = None

    @property
    def _storage(self) -> ResultTable:
        return self if self._owner is None else self._owner

    def _check_open(self) -> None:
        if self._storage._closed:
            raise RuntimeError("This result table is closed.")

    def _append(self, value: pl.DataFrame | pl.LazyFrame) -> None:
        self._check_open()
        if not isinstance(value, (pl.DataFrame, pl.LazyFrame)):
            raise TypeError("A result table must be a Polars DataFrame or LazyFrame.")
        path = Path(self._directory.name) / f"{len(self._paths)}.parquet"
        write_temporary_parquet(value.lazy() if isinstance(value, pl.DataFrame) else value, path)
        self._paths.append(path)

    def _scan(self) -> pl.LazyFrame:
        self._check_open()
        if not self._storage._paths:
            raise RuntimeError("This result table has no data.")
        value = pl.concat(
            [pl.scan_parquet(path) for path in self._storage._paths],
            how="vertical_relaxed",
        )
        for name, args, kwargs in self._operations:
            value = getattr(value, name)(*args, **kwargs)
        return value

    def _view(self, name, args, kwargs) -> ResultTable:
        self._check_open()
        view = object.__new__(ResultTable)
        view._owner = self._storage
        view._operations = (*self._operations, (name, args, kwargs))
        return view

    def sort(self, *args, **kwargs) -> ResultTable:
        return self._view("sort", args, kwargs)

    def filter(self, *args, **kwargs) -> ResultTable:
        return self._view("filter", args, kwargs)

    def select(self, *args, **kwargs) -> ResultTable:
        return self._view("select", args, kwargs)

    def group_by(self, *args, **kwargs) -> _GroupedResultTable:
        """Group rows for an aggregation without loading the full table."""
        self._check_open()
        return _GroupedResultTable(self, args, kwargs)

    def collect(self, *, engine="streaming") -> pl.DataFrame:
        """Read the table into memory while its owner is open."""
        return self._scan().collect(engine=engine)

    def save(self, path: str | Path) -> None:
        """Save the selected table as Parquet, replacing the target atomically."""
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, temporary_path = tempfile.mkstemp(
            prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
        )
        os.close(fd)
        try:
            self._scan().sink_parquet(temporary_path, engine="streaming")
            os.replace(temporary_path, path)
        finally:
            Path(temporary_path).unlink(missing_ok=True)

    def close(self) -> None:
        """Remove the temporary files backing this table and its views."""
        owner = self._storage
        if not owner._closed:
            owner._closed = True
            owner._directory.cleanup()

    def __enter__(self) -> ResultTable:
        self._check_open()
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.close()


class _GroupedResultTable:
    """Keep the source result alive until its grouped aggregation is declared."""

    def __init__(self, table: ResultTable, args, kwargs) -> None:
        self.table = table
        self.args = args
        self.kwargs = kwargs

    def agg(self, *args, **kwargs) -> ResultTable:
        """Return an owned view of the grouped result."""
        grouped = self.table._view("group_by", self.args, self.kwargs)
        return grouped._view("agg", args, kwargs)
