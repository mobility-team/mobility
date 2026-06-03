from __future__ import annotations

import polars as pl

from .assets.scoped_tables import ScopedRunTable

IterationSelector = str | int | list[int] | range


class GroupDayTripsResultTables:
    """Raw result tables for one or more scenarios and replications."""

    def __init__(self, results) -> None:
        self.results = results

    def plan_steps(self, *, iterations: IterationSelector = "last") -> pl.LazyFrame:
        """Return final plan steps with scenario, day type, and replication columns."""
        return self._table("plan_steps", iterations=iterations).get()

    def demand_groups(self, *, iterations: IterationSelector = "last") -> pl.LazyFrame:
        """Return demand groups with scenario, day type, and replication columns."""
        return self._table("demand_groups", iterations=iterations).get()

    def costs(self, *, iterations: IterationSelector = "last") -> pl.LazyFrame:
        """Return final transport costs with scenario, day type, and replication columns."""
        return self._table("costs", iterations=iterations).get()

    def transitions(self, *, iterations: IterationSelector = "last") -> pl.LazyFrame:
        """Return transition events with scenario, day type, and replication columns."""
        return self._table("transitions", iterations=iterations).get()

    def iteration_metrics(self, *, iterations: IterationSelector = "last") -> pl.LazyFrame:
        """Return per-iteration diagnostics with scenario, day type, and replication columns."""
        return self._table("iteration_metrics", iterations=iterations).get()

    def _table(self, table_name: str, *, iterations: IterationSelector) -> ScopedRunTable:
        """Build the cached scoped table asset for one run output."""
        return ScopedRunTable(
            results=self.results,
            table_name=table_name,
            iterations=iterations,
        )
