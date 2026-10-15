from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence

import polars as pl

from mobility.runtime.assets.graph import asset_graph_key

from .assets.metric import Metric
from .assets.scoped_tables import ScopedRunTable, release_scoped_run_tables


class ResultExecutionPlan:
    """Resolve requested result assets together, advancing through iterations once."""

    def __init__(
        self,
        *,
        requests: Sequence[Metric | ScopedRunTable],
        resolver,
    ) -> None:
        self.requests = self._unique_requests(requests)
        self.resolver = resolver

    @staticmethod
    def _unique_requests(
        requests: Sequence[Metric | ScopedRunTable],
    ) -> list[Metric | ScopedRunTable]:
        """Keep one computation for every identical result asset key."""
        unique_requests = {}
        for request in requests:
            unique_requests.setdefault(asset_graph_key(request), request)
        return list(unique_requests.values())

    def execute(self) -> dict[tuple, pl.DataFrame | pl.LazyFrame]:
        """Cache-check result assets, run shared iteration work, and return values."""
        values_by_request: dict[tuple, pl.DataFrame | pl.LazyFrame] = {}
        pending = []

        # Check persistent metrics before loading iteration state. Raw tables
        # are always materialized because they are transient result assets.
        for request in self.requests:
            self.resolver.prepare_upstream_assets(request)
            if isinstance(request, Metric) and not request.is_update_needed():
                request_key = asset_graph_key(request)
                values_by_request[request_key] = self.resolver.get(request)
            else:
                pending.append(request)

        if not pending:
            return values_by_request

        planned_outputs = [
            output
            for request in pending
            for output in request.planned_outputs
        ]
        self.resolver.retain_assets_shared_by_roots(planned_outputs)

        iteration_requests = [
            request
            for request in pending
            if request.compute_iteration is not None
        ]
        fallback_metrics = [
            request
            for request in pending
            if isinstance(request, Metric) and request.compute_iteration is None
        ]
        selected_iterations = sorted(
            {
                iteration
                for request in iteration_requests
                for iteration in request.selected_iterations
            }
        )
        metric_values: dict[tuple, list[pl.DataFrame]] = defaultdict(list)
        table_values: dict[tuple, list[pl.LazyFrame]] = defaultdict(list)
        parts_by_metric: dict[tuple, list] = defaultdict(list)
        temporary_parts = []
        directly_persisted = set()
        scoped_tables = [
            request
            for request in pending
            if isinstance(request, ScopedRunTable)
        ]

        try:
            for iteration in selected_iterations:
                try:
                    for request in iteration_requests:
                        if iteration not in request.selected_iterations:
                            continue

                        request_key = asset_graph_key(request)
                        value = request.compute_iteration(iteration)
                        if isinstance(request, ScopedRunTable):
                            if isinstance(value, pl.DataFrame):
                                value = value.lazy()
                            if not isinstance(value, pl.LazyFrame):
                                raise TypeError(
                                    "A raw table iteration must return a Polars "
                                    "DataFrame or LazyFrame."
                                )
                            table_values[request_key].append(value)
                            continue

                        if request.stream_result:
                            if len(request.selected_iterations) == 1:
                                request.persist_streamed_value(value)
                                directly_persisted.add(request_key)
                                del value
                                continue
                            part_path = self.resolver.temporary_asset_path(
                                request,
                                suffix=f".iteration-{iteration}.parquet",
                            )
                            temporary_parts.append(part_path)
                            if isinstance(value, pl.LazyFrame):
                                value.sink_parquet(part_path, engine="streaming")
                            elif isinstance(value, pl.DataFrame):
                                value.write_parquet(part_path)
                            else:
                                raise TypeError(
                                    "A streamed result iteration must return a Polars "
                                    "DataFrame or LazyFrame."
                                )
                            parts_by_metric[request_key].append(part_path)
                            del value
                        else:
                            if isinstance(value, pl.LazyFrame):
                                value = value.collect(engine="streaming")
                            if not isinstance(value, pl.DataFrame):
                                raise TypeError(
                                    "A result iteration must return a Polars "
                                    "DataFrame or LazyFrame."
                                )
                            metric_values[request_key].append(value)
                finally:
                    # All metric and table consumers have used this iteration.
                    release_scoped_run_tables(iteration, scoped_tables)

            for metric in fallback_metrics:
                try:
                    values_by_request[asset_graph_key(metric)] = self.resolver.get(metric)
                finally:
                    metric.clear_precomputed_value()

            for request in iteration_requests:
                request_key = asset_graph_key(request)
                if isinstance(request, ScopedRunTable):
                    table_parts = table_values[request_key]
                    if not table_parts:
                        raise RuntimeError(
                            "The execution plan produced no values for "
                            f"{request.__class__.__name__}."
                        )
                    values_by_request[request_key] = (
                        table_parts[0]
                        if len(table_parts) == 1
                        else pl.concat(table_parts, how="vertical_relaxed")
                    )
                    continue

                if request.stream_result:
                    if request_key not in directly_persisted:
                        request.persist_iteration_parts(parts_by_metric[request_key])
                else:
                    values = metric_values[request_key]
                    if not values:
                        raise RuntimeError(
                            "The execution plan produced no values for "
                            f"{request.__class__.__name__}."
                        )
                    request.set_precomputed_value(
                        values[0]
                        if len(values) == 1
                        else pl.concat(values, how="diagonal_relaxed")
                    )

                values_by_request[request_key] = self.resolver.get(request)

            return values_by_request
        finally:
            for request in iteration_requests:
                if isinstance(request, Metric):
                    request.clear_precomputed_value()
            for part_path in temporary_parts:
                try:
                    part_path.unlink()
                except FileNotFoundError:
                    pass
