from __future__ import annotations

import os
import pathlib
from collections.abc import Callable, Sequence

import polars as pl

from mobility.runtime.assets.asset import Asset
from mobility.runtime.assets.file_asset import FileAsset


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
        stream_result: bool = False,
    ) -> None:
        self.compute = compute
        self.results = results
        # Planner metadata controls reuse and lifetime; result identity comes
        # from exact dependencies and the normalized query below.
        self.planned_outputs = tuple(planned_outputs)
        self.selected_iterations = tuple(selected_iterations)
        self.compute_iteration = compute_iteration
        self.stream_result = stream_result
        self._has_precomputed_value = False
        self._precomputed_value: pl.DataFrame | pl.LazyFrame | None = None
        inputs = {
            "version": 3,
            "name": name,
            "dependencies": tuple(dependencies),
            "query": query,
        }
        project_folder = pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"])
        cache_path = project_folder / "group_day_trips" / "metrics" / f"{name}.parquet"
        super().__init__(inputs, cache_path)

    def get(self):
        """Resolve this metric as a one-output result batch."""
        return self.results.get_all(result=self)["result"]

    def get_cached_asset(self) -> pl.DataFrame | pl.LazyFrame:
        """Load the metric table previously computed for this exact query."""
        if self.stream_result:
            return pl.scan_parquet(self.cache_path)
        return pl.read_parquet(self.cache_path)

    def set_precomputed_value(self, value: pl.DataFrame | pl.LazyFrame) -> None:
        """Provide the planner's iteration-wise result to the asset writer."""
        self._precomputed_value = value
        self._has_precomputed_value = True

    def clear_precomputed_value(self) -> None:
        """Release the planner's value after the resolver has handled it."""
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
            if self.stream_result:
                self.cache_path.parent.mkdir(parents=True, exist_ok=True)
                self.hash_path.unlink(missing_ok=True)
                values.sink_parquet(self.cache_path, engine="streaming")
                return pl.scan_parquet(self.cache_path)
            values = values.collect(engine="streaming")
        if not isinstance(values, pl.DataFrame):
            raise TypeError("A result asset must return a Polars DataFrame or LazyFrame.")
        if self.stream_result:
            self.cache_path.parent.mkdir(parents=True, exist_ok=True)
            self.hash_path.unlink(missing_ok=True)
            values.lazy().sink_parquet(self.cache_path, engine="streaming")
            return pl.scan_parquet(self.cache_path)
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        self.hash_path.unlink(missing_ok=True)
        values.write_parquet(self.cache_path)
        return values

    def persist_iteration_parts(self, part_paths: Sequence[pathlib.Path]) -> None:
        """Stream selected iteration tables into this result's durable Parquet."""
        if not self.stream_result:
            raise ValueError("Only streamed result assets can persist iteration parts.")
        if not part_paths:
            raise ValueError("A streamed result needs at least one iteration table.")

        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        self.hash_path.unlink(missing_ok=True)
        iteration_tables = [pl.scan_parquet(path) for path in part_paths]
        pl.concat(iteration_tables, how="vertical_relaxed").sink_parquet(
            self.cache_path,
            engine="streaming",
        )
        self.update_hash(self.inputs_hash)

    def persist_streamed_value(self, value: pl.DataFrame | pl.LazyFrame) -> None:
        """Write one selected table directly to its durable result file."""
        if not self.stream_result:
            raise ValueError("Only streamed result assets can persist a LazyFrame directly.")
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        lazy_value = value.lazy() if isinstance(value, pl.DataFrame) else value
        if not isinstance(lazy_value, pl.LazyFrame):
            raise TypeError("A streamed result must be a Polars DataFrame or LazyFrame.")
        self.hash_path.unlink(missing_ok=True)
        lazy_value.sink_parquet(self.cache_path, engine="streaming")
        self.update_hash(self.inputs_hash)
