from __future__ import annotations

import os
import pathlib
import tempfile
from dataclasses import dataclass
from collections.abc import Callable, Sequence

import polars as pl

from mobility.runtime.assets.asset import Asset
from mobility.runtime.assets.file_asset import FileAsset


@dataclass
class MetricLaneResult:
    """Small per-replication inputs needed to finish a metric."""

    values: pl.DataFrame
    population: pl.DataFrame | None = None
    paired_values: pl.DataFrame | None = None
    reference_values: pl.DataFrame | None = None


class Metric(FileAsset):
    """Persist one requested result tied to its exact run assets and query."""

    def __init__(
        self,
        *,
        name: str,
        results,
        dependencies: Sequence[Asset],
        planned_outputs: Sequence[Asset] = (),
        query: dict,
        compute: Callable[[], pl.DataFrame | pl.LazyFrame],
        selected_iterations: Sequence[int] = (),
        compute_iteration: Callable[[int], pl.DataFrame | pl.LazyFrame] | None = None,
    ) -> None:
        self.compute = compute
        self.results = results
        # Runner metadata controls reuse and lifetime; result identity comes
        # from exact dependencies and the normalized query below.
        self.planned_outputs = tuple(planned_outputs)
        self.selected_iterations = tuple(selected_iterations)
        self.compute_iteration = compute_iteration
        # Some metrics can retain small per-replication totals and combine
        # them only after every independent simulation lane has finished.
        self.compute_lane = None
        self.finish_lanes = None
        self._has_precomputed_value = False
        self._precomputed_value: pl.DataFrame | pl.LazyFrame | None = None
        inputs = {
            "version": 5,
            "name": name,
            "dependencies": tuple(dependencies),
            "query": query,
        }
        project_folder = pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"])
        cache_path = project_folder / "group_day_trips" / "metrics" / f"{name}.parquet"
        super().__init__(inputs, cache_path)

    def get(self):
        """Resolve this metric as a single requested result."""
        return self.results.get_all(result=self)["result"]

    def get_cached_asset(self) -> pl.DataFrame | pl.LazyFrame:
        """Load the metric table previously computed for this exact query."""
        return pl.read_parquet(self.cache_path)

    def set_precomputed_value(self, value: pl.DataFrame | pl.LazyFrame) -> None:
        """Provide the runner's iteration-wise result to the asset writer."""
        self._precomputed_value = value
        self._has_precomputed_value = True

    def clear_precomputed_value(self) -> None:
        """Release the runner's value after the resolver has handled it."""
        self._precomputed_value = None
        self._has_precomputed_value = False

    def create_and_get_asset(self) -> pl.DataFrame | pl.LazyFrame:
        """Compute and persist the requested result table."""
        if self._has_precomputed_value:
            values = self._precomputed_value
            self._precomputed_value = None
            self._has_precomputed_value = False
        else:
            values = self.compute()
        if isinstance(values, pl.LazyFrame):
            values = values.collect(engine="streaming")
        if not isinstance(values, pl.DataFrame):
            raise TypeError("A result asset must return a Polars DataFrame or LazyFrame.")
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        fd, temporary_path = tempfile.mkstemp(
            prefix=f".{self.cache_path.name}.", suffix=".tmp", dir=self.cache_path.parent
        )
        os.close(fd)
        try:
            values.write_parquet(temporary_path)
            # Invalidate before replacing the data: an interruption after the
            # replacement must not leave the previous marker on the new file.
            # The resolver writes a fresh marker after this method returns.
            self.hash_path.unlink(missing_ok=True)
            os.replace(temporary_path, self.cache_path)
        finally:
            pathlib.Path(temporary_path).unlink(missing_ok=True)
        return values
