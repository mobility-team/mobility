from __future__ import annotations

from .assets.metric import Metric
from .assets.scoped_tables import ScopedRunTable

IterationSelector = str | int | list[int] | range


class GroupDayTripsResultTables:
    """Raw result tables for one or more scenarios and replications."""

    def __init__(self, results) -> None:
        self.results = results

    def plan_steps(
        self,
        *,
        iterations: IterationSelector = "last",
        columns: list[str] | tuple[str, ...] | None = None,
    ) -> ScopedRunTable:
        """Declare scoped plan steps with scenario, day type, and replication columns."""
        return self._table("plan_steps", iterations=iterations, columns=columns)

    def demand_groups(
        self,
        *,
        iterations: IterationSelector = "last",
        columns: list[str] | tuple[str, ...] | None = None,
    ) -> ScopedRunTable:
        """Declare scoped demand groups with scenario and run scope columns."""
        return self._table("demand_groups", iterations=iterations, columns=columns)

    def costs(self, *, iterations: IterationSelector = "last") -> Metric:
        """Declare persisted transport costs with scenario and run scope columns."""
        return self.results.metrics.travel_costs(iterations=iterations)

    def transitions(
        self,
        *,
        iterations: IterationSelector = "last",
        columns: list[str] | tuple[str, ...] | None = None,
    ) -> ScopedRunTable:
        """Declare scoped transition events with scenario and run scope columns."""
        return self._table("transitions", iterations=iterations, columns=columns)

    def iteration_metrics(
        self,
        *,
        iterations: IterationSelector = "last",
        columns: list[str] | tuple[str, ...] | None = None,
    ) -> ScopedRunTable:
        """Declare scoped per-iteration diagnostics."""
        return self._table("iteration_metrics", iterations=iterations, columns=columns)

    def _table(
        self,
        table_name: str,
        *,
        iterations: IterationSelector,
        columns: list[str] | tuple[str, ...] | None = None,
    ) -> ScopedRunTable:
        """Build a deferred scoped table request for one run output."""
        return ScopedRunTable(
            results=self.results,
            table_name=table_name,
            iterations=iterations,
            columns=columns,
        )
