from __future__ import annotations

import os
import pathlib
from typing import TYPE_CHECKING

import polars as pl

from mobility.runtime.assets.file_asset import FileAsset
from mobility.trips.group_day_trips.iterations import Iterations

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
    ) -> None:
        run_contexts = results.run_contexts
        if not run_contexts:
            raise ValueError("ScopedRunTable needs at least one run context.")

        self.results = results
        self.run_contexts = list(run_contexts)
        self.table_name = table_name
        self.iterations = results._validate_iterations(iterations)
        self.selected_iterations = results.selected_iterations(iterations)
        self.uses_last_iteration = self.iterations == "last"

        project_folder = pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"])
        if self.uses_last_iteration:
            iteration_suffix = "last"
        else:
            iteration_suffix = "-".join(str(iteration) for iteration in self.selected_iterations)
        cache_path = (
            project_folder
            / "group_day_trips"
            / "results"
            / f"{table_name}_{iteration_suffix}.parquet"
        )
        inputs = {
            "version": 3,
            "table_name": table_name,
            "iterations": tuple(self.selected_iterations),
            "uses_last_iteration": self.uses_last_iteration,
            "runs": tuple(run_context.run for run_context in self.run_contexts),
            "scope": tuple(
                (
                    run_context.scenario,
                    run_context.day_type,
                    run_context.replication,
                )
                for run_context in self.run_contexts
            ),
        }
        super().__init__(inputs, cache_path)

    def get_cached_asset(self) -> pl.LazyFrame:
        """Return the cached scoped table as a lazy parquet scan."""
        return pl.scan_parquet(self.cache_path)

    def create_and_get_asset(self) -> pl.LazyFrame:
        """Build and cache the scoped table."""
        if not self.uses_last_iteration:
            return self._create_saved_iteration_asset()

        scoped_tables: list[pl.LazyFrame] = []
        for run_context in self.run_contexts:
            run_context.run.get()
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
                table.with_columns(
                    scenario=pl.lit(run_context.scenario, dtype=pl.String),
                    day_type=pl.lit(run_context.day_type, dtype=pl.String),
                    iteration=iteration_expr,
                    replication=pl.lit(run_context.replication, dtype=pl.Int32),
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
        elif self.table_name == "demand_groups":
            scoped_tables = self._iteration_demand_groups()
        elif self.table_name == "iteration_metrics":
            scoped_tables = self._iteration_metrics()
        else:
            raise ValueError(
                f'iterations={self.iterations!r} is not available for '
                f"`{self.table_name}` yet. Saved iteration artifacts currently "
                "support plan_steps, demand_groups, and iteration_metrics."
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
            run_context.run.get()
            for iteration in self.selected_iterations:
                current_plan_steps = self._iteration_table_for_run(
                    run_context,
                    table_name="plan_steps",
                    iteration=iteration,
                )
                scoped_tables.append(
                    current_plan_steps.with_columns(
                        scenario=pl.lit(run_context.scenario, dtype=pl.String),
                        day_type=pl.lit(run_context.day_type, dtype=pl.String),
                        replication=pl.lit(run_context.replication, dtype=pl.Int32),
                        iteration=pl.lit(iteration, dtype=pl.Int32),
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
        if callable(getattr(run, "iteration_table", None)):
            table = run.iteration_table(table_name, iteration)
            if isinstance(table, pl.DataFrame):
                return table.lazy()
            return table

        if table_name == "plan_steps":
            return self._iterations_for_run(run_context).iteration(iteration).load_state().current_plan_steps.lazy()

        raise ValueError(
            f"Saved iteration table `{table_name}` is not available for this run."
        )

    def _iteration_demand_groups(self) -> list[pl.LazyFrame]:
        """Return demand groups repeated for each selected iteration."""
        scoped_tables = []
        for run_context in self.run_contexts:
            run_context.run.get()
            cached = run_context.run.get_cached_asset()
            if "demand_groups" not in cached:
                raise KeyError("Run output table `demand_groups` is not available.")
            table = cached["demand_groups"]
            if isinstance(table, pl.DataFrame):
                table = table.lazy()
            for iteration in self.selected_iterations:
                scoped_tables.append(
                    table.with_columns(
                        scenario=pl.lit(run_context.scenario, dtype=pl.String),
                        day_type=pl.lit(run_context.day_type, dtype=pl.String),
                        replication=pl.lit(run_context.replication, dtype=pl.Int32),
                        iteration=pl.lit(iteration, dtype=pl.Int32),
                    )
                )
        return scoped_tables

    def _iteration_metrics(self) -> list[pl.LazyFrame]:
        """Return iteration diagnostics filtered to the selected iterations."""
        scoped_tables = []
        for run_context in self.run_contexts:
            run_context.run.get()
            cached = run_context.run.get_cached_asset()
            if "iteration_metrics" not in cached:
                raise KeyError("Run output table `iteration_metrics` is not available.")
            table = cached["iteration_metrics"]
            if isinstance(table, pl.DataFrame):
                table = table.lazy()
            scoped_tables.append(
                table
                .filter(pl.col("iteration").is_in(self.selected_iterations))
                .with_columns(
                    scenario=pl.lit(run_context.scenario, dtype=pl.String),
                    day_type=pl.lit(run_context.day_type, dtype=pl.String),
                    replication=pl.lit(run_context.replication, dtype=pl.Int32),
                    iteration=pl.col("iteration").cast(pl.Int32),
                )
            )
        return scoped_tables

    def _iterations_for_run(self, run_context: "ResultRun") -> Iterations:
        """Return persisted iteration access for one run context."""
        run = run_context.run
        try:
            base_folder = pathlib.Path(run.cache_path["plan_steps"]).parent
            run_inputs_hash = run.inputs_hash
            is_weekday = bool(run.is_weekday)
        except (AttributeError, KeyError) as exc:
            raise TypeError(
                "Iteration-scoped results need runs with cache_path['plan_steps'], "
                "inputs_hash, and is_weekday."
            ) from exc
        return Iterations(
            run_inputs_hash=run_inputs_hash,
            is_weekday=is_weekday,
            base_folder=base_folder,
        )
