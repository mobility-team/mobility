from __future__ import annotations

from .assets.scoped_tables import ScopedRunTable

IterationSelector = str | int | list[int] | range


class GroupDayTripsResultDiagnostics:
    """Model diagnostics for one or more scenarios and replications."""

    def __init__(self, results) -> None:
        self.results = results

    def step_metrics(self, *, steps: IterationSelector = "last") -> ScopedRunTable:
        """Declare diagnostics for each selected simulation step."""
        return self.results.tables.step_metrics(steps=steps)
