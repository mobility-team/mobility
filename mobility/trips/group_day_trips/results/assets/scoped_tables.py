from __future__ import annotations

import os
import pathlib
import hashlib
from typing import TYPE_CHECKING, Any

import polars as pl

from mobility.runtime.assets.file_asset import FileAsset

if TYPE_CHECKING:
    from mobility.trips.group_day_trips.results import (
        GroupDayTripsResults,
        ResultRun,
    )

IterationSelector = str | int | list[int] | range


class ScopedRunTable(FileAsset):
    """Persist one run output table with scenario, day type, iteration, and replication."""

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
        self.uses_last_iteration = self.iterations == "last"

        project_folder = pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"])
        if self.uses_last_iteration:
            iteration_suffix = "last"
        else:
            iteration_suffix = "-".join(str(iteration) for iteration in self.selected_iterations)
        if self.columns is None:
            column_suffix = "all-columns"
        else:
            column_key = "|".join(self.columns).encode("utf-8")
            column_suffix = f"cols-{hashlib.md5(column_key).hexdigest()[:8]}"
        cache_path = (
            project_folder
            / "group_day_trips"
            / "results"
            / f"{table_name}_{iteration_suffix}_{column_suffix}.parquet"
        )
        inputs = {
            "version": 4,
            "table_name": table_name,
            "iterations": tuple(self.selected_iterations),
            "uses_last_iteration": self.uses_last_iteration,
            "columns": self.columns,
            # Result tables only need to know which completed run outputs they
            # summarize. Keeping the full Run assets out of the hash inputs
            # avoids walking every simulation iteration asset before reading an
            # already-finalized result table.
            "run_outputs": tuple(
                self._run_output_descriptor(run_context)
                for run_context in self.run_contexts
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
        super().__init__(inputs, cache_path)

    @staticmethod
    def _run_output_descriptor(run_context: "ResultRun") -> dict[str, Any]:
        """Return the compact run identity needed by result-table caches."""
        run = run_context.run
        cache_path = getattr(run, "cache_path", {})
        if isinstance(cache_path, dict):
            output_paths = {
                name: str(path)
                for name, path in sorted(cache_path.items())
            }
        else:
            output_paths = {"default": str(cache_path)}

        return {
            "run_hash": str(getattr(run, "inputs_hash", "")),
            "scenario": str(run_context.scenario),
            "sensitivity_case": str(run_context.sensitivity_case_id),
            "day_type": str(run_context.day_type),
            "replication": str(run_context.replication),
            "output_paths": output_paths,
        }

    def get_cached_asset(self) -> pl.LazyFrame:
        """Return the cached scoped table as a lazy parquet scan."""
        return pl.scan_parquet(self.cache_path)

    def create_and_get_asset(self) -> pl.LazyFrame:
        """Build and cache the scoped table."""
        if not self.uses_last_iteration:
            return self._create_saved_iteration_asset()

        scoped_tables: list[pl.LazyFrame] = []
        for run_context in self.run_contexts:
            self._ensure_run_output_available(run_context, self.table_name)
            cached = run_context.run.get_cached_asset()
            if self.table_name not in cached:
                raise KeyError(
                    f"Run output table `{self.table_name}` is not available. "
                    f"Available tables: {sorted(cached)}."
                )

            table = cached[self.table_name]
            if isinstance(table, pl.DataFrame):
                table = table.lazy()
            if self.table_name == "iteration_metrics":
                table = table.filter(pl.col("iteration") == self.results.last_iteration)
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

        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        pl.concat(scoped_tables, how="vertical_relaxed").collect(engine="streaming").write_parquet(
            self.cache_path
        )
        return self.get_cached_asset()

    def _create_saved_iteration_asset(self) -> pl.LazyFrame:
        """Build and cache a table for selected saved iterations."""
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

        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        pl.concat(scoped_tables, how="vertical_relaxed").collect(engine="streaming").write_parquet(
            self.cache_path
        )
        return self.get_cached_asset()

    def _iteration_plan_steps(self) -> list[pl.LazyFrame]:
        """Return selected saved current plan steps with result scope columns."""
        scoped_tables = []
        for run_context in self.run_contexts:
            for iteration in self.selected_iterations:
                current_plan_steps = self._iteration_table_for_run(
                    run_context,
                    table_name="plan_steps",
                    iteration=iteration,
                )
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
        for run_context in self.run_contexts:
            for iteration in self.selected_iterations:
                costs = self._iteration_table_for_run(
                    run_context,
                    table_name="costs",
                    iteration=iteration,
                )
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

    def _iteration_table_for_run(
        self,
        run_context: "ResultRun",
        *,
        table_name: str,
        iteration: int,
    ) -> pl.LazyFrame:
        """Return one saved iteration table for a run."""
        run = run_context.run
        if not callable(getattr(run, "iteration_table", None)):
            raise TypeError(
                "Iteration-scoped result tables need runs exposing "
                "`iteration_table(table_name, iteration)`."
            )

        table = run.iteration_table(table_name, iteration)
        if isinstance(table, pl.DataFrame):
            return table.lazy()
        return table

    def _iteration_demand_groups(self) -> list[pl.LazyFrame]:
        """Return demand groups repeated for each selected iteration."""
        scoped_tables = []
        for run_context in self.run_contexts:
            self._ensure_run_output_available(run_context, "demand_groups")
            cached = run_context.run.get_cached_asset()
            if "demand_groups" not in cached:
                raise KeyError("Run output table `demand_groups` is not available.")
            table = cached["demand_groups"]
            if isinstance(table, pl.DataFrame):
                table = table.lazy()
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
        for run_context in self.run_contexts:
            self._ensure_run_output_available(run_context, "iteration_metrics")
            cached = run_context.run.get_cached_asset()
            if "iteration_metrics" not in cached:
                raise KeyError("Run output table `iteration_metrics` is not available.")
            table = cached["iteration_metrics"]
            if isinstance(table, pl.DataFrame):
                table = table.lazy()
            scoped_tables.append(
                self._select_columns(
                    table
                    .filter(pl.col("iteration").is_in(self.selected_iterations))
                    .with_columns(
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

    @staticmethod
    def _ensure_run_output_available(run_context: "ResultRun", table_name: str) -> None:
        """Materialize a run only when the requested final output is missing."""
        run = run_context.run
        cache_path = getattr(run, "cache_path", {})
        if isinstance(cache_path, dict) and table_name in cache_path:
            if pathlib.Path(cache_path[table_name]).exists():
                return
        run.get()

