from __future__ import annotations

from collections import defaultdict

import polars as pl

from mobility.runtime.assets.graph import asset_graph_key

from .assets.metric import Metric
from .assets.scoped_tables import release_scoped_run_tables


class ResultExecutionPlan:
    """Resolve declared results together, advancing through iterations once."""

    def __init__(self, *, metrics: list[Metric], resolver) -> None:
        self.metrics = self._unique_metrics(metrics)
        self.resolver = resolver

    @staticmethod
    def _unique_metrics(metrics: list[Metric]) -> list[Metric]:
        """Keep one computation for every identical persisted result key."""
        unique_metrics = {}
        for metric in metrics:
            unique_metrics.setdefault(asset_graph_key(metric), metric)
        return list(unique_metrics.values())

    def execute(self, requested: Metric, args: tuple, kwargs: dict):
        """Cache-check results, run iteration work together, then persist them."""
        requested_key = asset_graph_key(requested)
        pending = []
        requested_value = None

        # Prepare upstream files first so a cached metric can return without
        # loading any iteration state. Its cache key contains its exact logical
        # dependencies; planner-only sharing metadata stays out of that key.
        for metric in self.metrics:
            self.resolver.prepare_upstream_assets(metric)
            if metric.is_update_needed():
                pending.append(metric)
            elif asset_graph_key(metric) == requested_key:
                requested_value = self.resolver.get(metric, *args, **kwargs)

        if not pending:
            return requested_value

        planned_outputs = [
            output
            for metric in pending
            for output in metric.planned_outputs
        ]
        self.resolver.retain_assets_shared_by_roots(planned_outputs)

        iteration_metrics = [
            metric
            for metric in pending
            if metric.compute_iteration is not None
        ]
        fallback_metrics = [
            metric
            for metric in pending
            if metric.compute_iteration is None
        ]
        selected_iterations = sorted(
            {
                iteration
                for metric in iteration_metrics
                for iteration in metric.selected_iterations
            }
        )
        values_by_metric: dict[tuple, list[pl.DataFrame]] = defaultdict(list)
        parts_by_metric: dict[tuple, list] = defaultdict(list)
        temporary_parts = []
        directly_persisted = set()

        try:
            for iteration in selected_iterations:
                try:
                    for metric in iteration_metrics:
                        if iteration not in metric.selected_iterations:
                            continue
                        value = metric.compute_iteration(iteration)
                        metric_key = asset_graph_key(metric)
                        if metric.stream_result:
                            if len(metric.selected_iterations) == 1:
                                metric.persist_streamed_value(value)
                                directly_persisted.add(metric_key)
                                del value
                                continue
                            part_path = self.resolver.temporary_asset_path(
                                metric,
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
                            parts_by_metric[metric_key].append(part_path)
                            del value
                        else:
                            if isinstance(value, pl.LazyFrame):
                                value = value.collect(engine="streaming")
                            if not isinstance(value, pl.DataFrame):
                                raise TypeError(
                                    "A result iteration must return a Polars "
                                    "DataFrame or LazyFrame."
                                )
                            values_by_metric[metric_key].append(value)
                finally:
                    # Every result for this iteration has consumed its tables.
                    # Drop them before asking the next iteration for its state.
                    release_scoped_run_tables(iteration)

            for metric in fallback_metrics:
                try:
                    value = self.resolver.get(
                        metric,
                        *(args if asset_graph_key(metric) == requested_key else ()),
                        **(kwargs if asset_graph_key(metric) == requested_key else {}),
                    )
                finally:
                    metric.clear_precomputed_value()
                if asset_graph_key(metric) == requested_key:
                    requested_value = value

            for metric in iteration_metrics:
                metric_key = asset_graph_key(metric)
                if metric.stream_result:
                    if metric_key not in directly_persisted:
                        metric.persist_iteration_parts(parts_by_metric[metric_key])
                else:
                    values = values_by_metric[metric_key]
                    if not values:
                        raise RuntimeError(
                            "The execution plan produced no values for "
                            f"{metric.__class__.__name__}."
                        )
                    metric.set_precomputed_value(
                        values[0]
                        if len(values) == 1
                        else pl.concat(values, how="diagonal_relaxed")
                    )

                value = self.resolver.get(
                    metric,
                    *(args if asset_graph_key(metric) == requested_key else ()),
                    **(kwargs if asset_graph_key(metric) == requested_key else {}),
                )
                if asset_graph_key(metric) == requested_key:
                    requested_value = value

            return requested_value
        finally:
            for metric in iteration_metrics:
                metric.clear_precomputed_value()
            for part_path in temporary_parts:
                try:
                    part_path.unlink()
                except FileNotFoundError:
                    pass
