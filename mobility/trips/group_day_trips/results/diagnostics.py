from __future__ import annotations

import polars as pl

IterationSelector = str | int | list[int] | range


class GroupDayTripsResultDiagnostics:
    """Model diagnostics for one or more scenarios and replications."""

    def __init__(self, results) -> None:
        self.results = results

    def iteration_metrics(self, *, iterations: IterationSelector = "last") -> pl.LazyFrame:
        """Return the persisted per-iteration diagnostics table."""
        return self.results.tables.iteration_metrics(iterations=iterations)
