from __future__ import annotations

import polars as pl
from mobility.surveys.programme_pooling import pooling_diagnostics
from mobility.trips.group_day_trips.evaluation.population_weighted_plan_steps import _get_demand_groups

IterationSelector = str | int | list[int] | range


class GroupDayTripsResultDiagnostics:
    """Model diagnostics for one or more scenarios and replications."""

    def __init__(self, results) -> None:
        self.results = results

    def iteration_metrics(self, *, iterations: IterationSelector = "last") -> pl.LazyFrame:
        """Return the persisted per-iteration diagnostics table."""
        return self.results.tables.iteration_metrics(iterations=iterations)

    def programme_pooling(self, *, assignments: bool = False) -> pl.DataFrame:
        """Return EMP segment coverage or the full donor assignment table.

        Coverage includes demand segments with no compatible donors and separates
        sampled support from original observed support. Donor reuse counts how
        many target segments can sample a day, not how often a run selected it.
        """
        tables = []
        keys = ["is_weekday", "city_category", "csp", "n_cars"]
        for context in self.results.run_contexts:
            run = context.run
            targets = _get_demand_groups(run.population).filter(pl.col("country") == "fr").with_columns(
                is_weekday=pl.lit(run.is_weekday)
            ).group_by(["country", *keys]).agg(pl.col("n_persons").sum())
            table = run.survey_plan_assets.get_programme_assignments(targets, run.parameters.programme_pooling)
            if "p_sampling" not in table.columns:
                continue
            if not assignments:
                table = targets.join(pooling_diagnostics(table), on=keys, how="left").with_columns(
                    sampling_mass=pl.col("sampling_mass").fill_null(0),
                    observed_days=pl.col("observed_days").fill_null(0),
                    available_donors=pl.col("available_donors").fill_null(0),
                )
            tables.append(table.with_columns(
                scenario=pl.lit(context.scenario), replication=pl.lit(context.replication),
                sensitivity_case_id=pl.lit(context.sensitivity_case_id),
            ))
        return pl.concat(tables, how="vertical_relaxed") if tables else pl.DataFrame()
