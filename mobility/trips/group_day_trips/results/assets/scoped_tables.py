from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING

import polars as pl

from mobility.runtime.assets.in_memory_asset import InMemoryAsset
from mobility.runtime.assets.resolver import get_current_asset_resolver

if TYPE_CHECKING:
    from mobility.trips.group_day_trips.results import (
        GroupDayTripsResults,
    )

IterationSelector = str | int | list[int] | range


def release_scoped_run_tables(
    iteration: int,
    requested_tables: Sequence["ScopedRunTable"] = (),
) -> None:
    """Release large run tables once one iteration's result has been reduced."""
    resolver = get_current_asset_resolver()
    if resolver is None:
        return

    scoped_tables = {
        asset.inputs_hash: asset
        for asset in tuple(resolver.in_memory_assets.values())
        if isinstance(asset, ScopedRunTable)
    }
    scoped_tables.update({asset.inputs_hash: asset for asset in requested_tables})

    for asset in scoped_tables.values():
        if iteration not in asset.selected_iterations:
            continue

        resolver.release_in_memory_asset(asset)
        # Demand groups are invariant across iterations and are reused by every
        # metric. Keep their small table in this resolver until the plan ends.
        if asset.table_name == "demand_groups":
            continue
        output_index = (
            0
            if asset.uses_last_iteration
            else asset.selected_iterations.index(iteration)
        )
        for output_assets in asset.run_output_assets:
            if output_index < len(output_assets):
                resolver.release_in_memory_asset(output_assets[output_index])


class ScopedRunTable(InMemoryAsset):
    """Assemble one requested table across scenarios and replications."""

    def __init__(
        self,
        *,
        results: "GroupDayTripsResults",
        table_name: str,
        iterations: IterationSelector = "last",
        columns: list[str] | tuple[str, ...] | None = None,
    ) -> None:
        run_contexts = results.run_contexts
        if not run_contexts:
            raise ValueError("ScopedRunTable needs at least one run context.")

        self.results = results
        self.run_contexts = list(run_contexts)
        self.table_name = table_name
        self.columns = None if columns is None else tuple(dict.fromkeys(columns))
        self.iterations = results._validate_iterations(iterations)
        self.selected_iterations = results.selected_iterations(iterations)
        self.uses_last_iteration = self.selected_iterations == [results.last_iteration]
        self.run_output_assets = []
        for run_context in self.run_contexts:
            if self.table_name == "iteration_metrics":
                output_assets = [
                    run_context.run._table_asset(
                        "iteration_metrics",
                        iteration=iteration,
                    )
                    for iteration in self.selected_iterations
                ]
            elif self.table_name == "demand_groups":
                output_assets = [run_context.run._table_asset("demand_groups")]
            elif self.uses_last_iteration:
                output_assets = [run_context.run._table_asset(self.table_name)]
            elif self.table_name in {"plan_steps", "costs"}:
                output_assets = [
                    run_context.run._table_asset(
                        self.table_name,
                        iteration=iteration,
                    )
                    for iteration in self.selected_iterations
                ]
            else:
                raise ValueError(
                    f'iterations={self.iterations!r} is not available for '
                    f"`{self.table_name}`."
                )
            self.run_output_assets.append(output_assets)

        inputs = {
            "version": 6,
            "table_name": table_name,
            "iterations": tuple(self.selected_iterations),
            "uses_last_iteration": self.uses_last_iteration,
            "columns": self.columns,
            "run_outputs": tuple(
                asset
                for output_assets in self.run_output_assets
                for asset in output_assets
            ),
            "scope": tuple(
                (
                    run_context.scenario,
                    run_context.sensitivity_case_id,
                    run_context.day_type,
                    run_context.replication,
                )
                for run_context in self.run_contexts
            ),
        }
        super().__init__(inputs)

    @property
    def planned_outputs(self) -> tuple:
        """Return the concrete run output assets used by this table request."""
        return tuple(
            asset
            for output_assets in self.run_output_assets
            for asset in output_assets
        )

    def get(self) -> pl.LazyFrame:
        """Resolve a standalone table through the result planner."""
        if get_current_asset_resolver() is not None:
            return super().get()
        return self.results.get_all(table=self)["table"]

    def compute_iteration(self, iteration: int) -> pl.LazyFrame:
        """Build this table's scoped rows for one selected iteration."""
        if iteration not in self.selected_iterations:
            raise ValueError(
                f"Iteration {iteration} is not selected for {self.table_name}."
            )

        iteration_index = self.selected_iterations.index(iteration)
        scoped_tables = []
        for run_context, output_assets in zip(self.run_contexts, self.run_output_assets):
            if self.table_name == "demand_groups":
                output_asset = output_assets[0]
            else:
                output_index = 0 if self.uses_last_iteration else iteration_index
                output_asset = output_assets[output_index]

            table = output_asset.get().lazy()
            iteration_expr = (
                pl.col("iteration").cast(pl.Int32)
                if self.table_name == "iteration_metrics"
                else pl.lit(iteration, dtype=pl.Int32)
            )
            scoped_tables.append(
                self._select_columns(
                    table.with_columns(
                        scenario=pl.lit(run_context.scenario, dtype=pl.String),
                        sensitivity_case=pl.lit(run_context.sensitivity_case_id, dtype=pl.String),
                        day_type=pl.lit(run_context.day_type, dtype=pl.String),
                        iteration=iteration_expr,
                        replication=pl.lit(run_context.replication, dtype=pl.Int32),
                    )
                )
            )

        return pl.concat(scoped_tables, how="vertical_relaxed")

    def create_and_get_asset(self) -> pl.LazyFrame:
        """Build the requested scoped table without persisting the raw table."""
        resolver = get_current_asset_resolver()
        if resolver is not None:
            resolver.retain_assets_shared_by_roots(list(self.planned_outputs))

        scoped_tables = [
            self.compute_iteration(iteration)
            for iteration in self.selected_iterations
        ]
        return pl.concat(scoped_tables, how="vertical_relaxed")

    def _select_columns(self, table: pl.LazyFrame) -> pl.LazyFrame:
        """Return only requested columns when this scoped table is projected."""
        if self.columns is None:
            return table
        schema = table.collect_schema().names()
        columns = [column for column in self.columns if column in schema]
        return table.select(columns)

