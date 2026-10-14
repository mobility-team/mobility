from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl

from mobility.runtime.assets.in_memory_asset import InMemoryAsset
from mobility.runtime.assets.resolver import get_current_asset_resolver

if TYPE_CHECKING:
    from mobility.trips.group_day_trips.results import (
        GroupDayTripsResults,
    )

IterationSelector = str | int | list[int] | range


def release_scoped_run_tables(iteration: int) -> None:
    """Release large run tables once one iteration's result has been reduced."""
    resolver = get_current_asset_resolver()
    if resolver is None:
        return

    for asset in tuple(resolver.in_memory_assets.values()):
        if not isinstance(asset, ScopedRunTable):
            continue
        if asset.selected_iterations != [iteration]:
            continue

        resolver.release_in_memory_asset(asset)
        # Demand groups are invariant across iterations and are reused by every
        # metric. Keep their small table in this resolver until the plan ends.
        if asset.table_name == "demand_groups":
            continue
        for output_assets in asset.run_output_assets:
            for output_asset in output_assets:
                resolver.release_in_memory_asset(output_asset)


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

    def create_and_get_asset(self) -> pl.LazyFrame:
        """Build the requested scoped table without persisting the raw table."""
        resolver = get_current_asset_resolver()
        if resolver is not None:
            resolver.retain_assets_shared_by_roots(
                [
                    asset
                    for output_assets in self.run_output_assets
                    for asset in output_assets
                ]
            )

        if not self.uses_last_iteration:
            return self._create_saved_iteration_asset()

        scoped_tables: list[pl.LazyFrame] = []
        for run_context, output_assets in zip(self.run_contexts, self.run_output_assets):
            table = output_assets[0].get().lazy()
            if self.table_name == "iteration_metrics":
                iteration_expr = pl.col("iteration").cast(pl.Int32)
            else:
                iteration_expr = pl.lit(self.results.last_iteration, dtype=pl.Int32)
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

    def _create_saved_iteration_asset(self) -> pl.LazyFrame:
        """Build a table from the selected transient iteration outputs."""
        if self.table_name == "plan_steps":
            scoped_tables = self._iteration_plan_steps()
        elif self.table_name == "costs":
            scoped_tables = self._iteration_costs()
        elif self.table_name == "demand_groups":
            scoped_tables = self._iteration_demand_groups()
        elif self.table_name == "iteration_metrics":
            scoped_tables = self._iteration_metrics()
        else:
            raise ValueError(
                f'iterations={self.iterations!r} is not available for '
                f"`{self.table_name}` yet. Saved iteration artifacts currently "
                "support plan_steps, costs, demand_groups, and iteration_metrics."
            )

        return pl.concat(scoped_tables, how="vertical_relaxed")

    def _iteration_plan_steps(self) -> list[pl.LazyFrame]:
        """Return selected saved current plan steps with result scope columns."""
        scoped_tables = []
        for run_index, run_context in enumerate(self.run_contexts):
            for iteration_index, iteration in enumerate(self.selected_iterations):
                current_plan_steps = self.run_output_assets[run_index][
                    iteration_index
                ].get().lazy()
                scoped_tables.append(
                    self._select_columns(
                        current_plan_steps.with_columns(
                            scenario=pl.lit(run_context.scenario, dtype=pl.String),
                            sensitivity_case=pl.lit(run_context.sensitivity_case_id, dtype=pl.String),
                            day_type=pl.lit(run_context.day_type, dtype=pl.String),
                            replication=pl.lit(run_context.replication, dtype=pl.Int32),
                            iteration=pl.lit(iteration, dtype=pl.Int32),
                        )
                    )
                )
        return scoped_tables

    def _iteration_costs(self) -> list[pl.LazyFrame]:
        """Return selected saved transport costs with result scope columns."""
        scoped_tables = []
        for run_index, run_context in enumerate(self.run_contexts):
            for iteration_index, iteration in enumerate(self.selected_iterations):
                costs = self.run_output_assets[run_index][
                    iteration_index
                ].get().lazy()
                scoped_tables.append(
                    self._select_columns(
                        costs.with_columns(
                            scenario=pl.lit(run_context.scenario, dtype=pl.String),
                            sensitivity_case=pl.lit(run_context.sensitivity_case_id, dtype=pl.String),
                            day_type=pl.lit(run_context.day_type, dtype=pl.String),
                            replication=pl.lit(run_context.replication, dtype=pl.Int32),
                            iteration=pl.lit(iteration, dtype=pl.Int32),
                        )
                    )
                )
        return scoped_tables

    def _iteration_demand_groups(self) -> list[pl.LazyFrame]:
        """Return demand groups repeated for each selected iteration."""
        scoped_tables = []
        for run_context, output_assets in zip(self.run_contexts, self.run_output_assets):
            table = output_assets[0].get().lazy()
            for iteration in self.selected_iterations:
                scoped_tables.append(
                    self._select_columns(
                        table.with_columns(
                            scenario=pl.lit(run_context.scenario, dtype=pl.String),
                            sensitivity_case=pl.lit(run_context.sensitivity_case_id, dtype=pl.String),
                            day_type=pl.lit(run_context.day_type, dtype=pl.String),
                            replication=pl.lit(run_context.replication, dtype=pl.Int32),
                            iteration=pl.lit(iteration, dtype=pl.Int32),
                        )
                    )
                )
        return scoped_tables

    def _iteration_metrics(self) -> list[pl.LazyFrame]:
        """Return iteration diagnostics filtered to the selected iterations."""
        scoped_tables = []
        for run_context, output_assets in zip(self.run_contexts, self.run_output_assets):
            for table_asset in output_assets:
                table = table_asset.get().lazy()
                scoped_tables.append(
                    self._select_columns(
                        table.with_columns(
                            scenario=pl.lit(run_context.scenario, dtype=pl.String),
                            sensitivity_case=pl.lit(run_context.sensitivity_case_id, dtype=pl.String),
                            day_type=pl.lit(run_context.day_type, dtype=pl.String),
                            replication=pl.lit(run_context.replication, dtype=pl.Int32),
                            iteration=pl.col("iteration").cast(pl.Int32),
                        )
                    )
                )
        return scoped_tables

    def _select_columns(self, table: pl.LazyFrame) -> pl.LazyFrame:
        """Return only requested columns when this scoped table is projected."""
        if self.columns is None:
            return table
        schema = table.collect_schema().names()
        columns = [column for column in self.columns if column in schema]
        return table.select(columns)

