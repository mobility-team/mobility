from __future__ import annotations

from .assets.scoped_tables import ScopedRunTable

IterationSelector = str | int | list[int] | range


class GroupDayTripsResultTables:
    """Raw result tables for one or more scenarios and replications."""

    def __init__(self, results) -> None:
        self.results = results

    def plan_steps(
        self,
        *,
        steps: IterationSelector = "last",
        columns: list[str] | tuple[str, ...] | None = None,
    ) -> ScopedRunTable:
        """Declare scoped plan steps with scenario, day type, and replication columns."""
        return self._table("plan_steps", iterations=steps, columns=columns, strict_columns=columns is not None)

    def demand_groups(
        self,
        *,
        steps: IterationSelector = "last",
        columns: list[str] | tuple[str, ...] | None = None,
    ) -> ScopedRunTable:
        """Declare scoped demand groups with scenario and run scope columns."""
        return self._table("demand_groups", iterations=steps, columns=columns, strict_columns=columns is not None)

    def costs(
        self,
        *,
        steps: IterationSelector = "last",
        columns: list[str] | tuple[str, ...] | None = None,
    ) -> ScopedRunTable:
        """Declare transient transport costs with scenario and run scope columns."""
        return self._table("costs", iterations=steps, columns=columns, strict_columns=columns is not None)

    def transitions(
        self,
        *,
        steps: IterationSelector = "last",
        columns: list[str] | tuple[str, ...] | None = None,
    ) -> ScopedRunTable:
        """Declare scoped transition events with scenario and run scope columns."""
        return self._table("transitions", iterations=steps, columns=columns, strict_columns=columns is not None)

    def step_metrics(
        self,
        *,
        steps: IterationSelector = "last",
        columns: list[str] | tuple[str, ...] | None = None,
    ) -> ScopedRunTable:
        """Declare diagnostics for each selected simulation step."""
        return self._table("iteration_metrics", iterations=steps, columns=columns, strict_columns=columns is not None)

    def adaptive_search_diagnostics(
        self,
        *,
        columns: list[str] | tuple[str, ...] | None = None,
    ) -> ScopedRunTable:
        """Declare inner- and outer-loop convergence diagnostics for each scenario."""
        return self._table(
            "adaptive_search_diagnostics",
            iterations="last",
            columns=columns,
            strict_columns=columns is not None,
        )

    def _table(
        self,
        table_name: str,
        *,
        iterations: IterationSelector,
        columns: list[str] | tuple[str, ...] | None = None,
        strict_columns: bool = False,
    ) -> ScopedRunTable:
        """Build a deferred scoped table request for one run output."""
        return ScopedRunTable(
            results=self.results,
            table_name=table_name,
            iterations=iterations,
            columns=columns,
            strict_columns=strict_columns,
        )

    def mnl_od_mode_flows(self) -> ScopedRunTable:
        """Return the last inner MNL estimate before final behavior and costs update.

        This diagnostic uses costs fixed at the start of the last outer
        iteration. It is not an estimate at the returned final transport costs.
        """
        return self._table("mnl_od_mode_flows", iterations="last")
