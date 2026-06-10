from __future__ import annotations

from typing import Any

import plotly.graph_objects as go
import polars as pl

from mobility.runtime.parameter_values import (
    SensitivityCase,
    SensitivityValue,
    collect_sensitivity_values,
)
from mobility.trips.group_day_trips.results.metrics import (
    METRIC_SPECS,
    GroupDayTripsResultMetrics,
    MetricName,
)
from mobility.trips.group_day_trips.results.assets.person_metrics import SCOPE_COLUMNS


class SensitivityResult:
    """Sensitivity metric table with plotting helpers."""

    def __init__(self, table: pl.DataFrame, *, metric_column: str) -> None:
        self.table = table
        self.metric_column = metric_column

    def filter(self, *predicates, **constraints) -> "SensitivityResult":
        """Return a filtered sensitivity result."""
        return SensitivityResult(
            self.table.filter(*predicates, **constraints),
            metric_column=self.metric_column,
        )

    def plot_tornado(
        self,
        *,
        value_column: str = "gap",
        width: int = 760,
        height: int = 460,
    ) -> go.Figure:
        """Plot one tornado chart from a one-row-per-case sensitivity table."""
        values = self.table
        if values.is_empty():
            raise ValueError("No sensitivity rows are available to plot.")

        case_counts = values.group_by("sensitivity_case").len()
        repeated_cases = case_counts.filter(pl.col("len") > 1)
        if not repeated_cases.is_empty():
            raise ValueError(
                "Tornado charts need one row per sensitivity case. "
                "Filter the sensitivity table first, then call plot_tornado()."
            )

        plot_values = (
            values
            .with_columns(abs_value=pl.col(value_column).abs())
            .sort("abs_value")
        )
        labels = [
            f"{parameter} ({variation})"
            for parameter, variation in zip(
                plot_values["parameter"].to_list(),
                plot_values["variation"].to_list(),
            )
        ]
        fig = go.Figure()
        fig.add_bar(
            x=plot_values[value_column].to_list(),
            y=labels,
            orientation="h",
            marker_color="#2B6CB0",
        )
        fig.update_layout(
            title="Sensitivity tornado chart",
            width=width,
            height=height,
            xaxis_title=value_column.replace("_", " "),
            yaxis_title="Parameter variation",
            template="plotly_white",
        )
        return fig


class GroupDayTripsSensitivityAnalysis:
    """Sensitivity analysis for grouped day-trip results."""

    def __init__(
        self,
        *,
        setup: Any,
        scenarios: list[str],
    ) -> None:
        self.setup = setup
        self.scenarios = scenarios
        self.cases = self._collect_cases()
        if len(self.cases) == 1:
            raise ValueError(
                "No sensitivity values were found. "
                "Use mobility.SensitivityValue.values(), "
                "mobility.SensitivityValue.relative(), or "
                "mobility.SensitivityValue.absolute() in the setup."
            )

    def trip_count(self, **kwargs) -> SensitivityResult:
        """Return trip-count sensitivity results."""
        return self.metric("trip_count", **kwargs)

    def travel_distance(self, **kwargs) -> SensitivityResult:
        """Return travel-distance sensitivity results."""
        return self.metric("travel_distance", **kwargs)

    def travel_time(self, **kwargs) -> SensitivityResult:
        """Return travel-time sensitivity results."""
        return self.metric("travel_time", **kwargs)

    def cost(self, **kwargs) -> SensitivityResult:
        """Return generalized-cost sensitivity results."""
        return self.metric("cost", **kwargs)

    def ghg_emissions(self, **kwargs) -> SensitivityResult:
        """Return greenhouse-gas-emission sensitivity results."""
        return self.metric("ghg_emissions", **kwargs)

    def metric(
        self,
        name: MetricName,
        *,
        day_type: str = "weekday",
        output: str = "table",
        **kwargs,
    ) -> SensitivityResult | go.Figure:
        """Return one metric compared to the base sensitivity case."""
        if output not in {"table", "tornado"}:
            raise ValueError('output should be either "table" or "tornado".')
        if "reference" in kwargs or "reference_view" in kwargs:
            raise ValueError(
                "Sensitivity metrics already compare each variation to the base case. "
                "Do not pass `reference` or `reference_view`."
            )
        if name not in METRIC_SPECS:
            raise ValueError(
                "Unknown metric name. Expected one of: "
                + ", ".join(sorted(METRIC_SPECS))
                + "."
            )

        spec = METRIC_SPECS[name]
        by_zone = kwargs.get("by_zone")
        by_variable = kwargs.get("by_variable")
        normalize_by = kwargs.get("normalize_by")
        normalize_scope = kwargs.get("normalize_scope")
        inner_zone_residents_only = kwargs.get("inner_zone_residents_only", False)
        iterations = kwargs.get("iterations", "last")
        metric_column = GroupDayTripsResultMetrics._metric_column(
            spec.quantity,
            normalize_by,
        )
        results = self.setup.results(
            day_type,
            scenarios=self.scenarios,
            sensitivity_cases=self.cases,
        )
        values = results.metrics.metric(
            name,
            output="table",
            **kwargs,
        )
        zone_columns = GroupDayTripsResultMetrics._zone_columns(by_zone)
        zone_column = zone_columns[0] if len(zone_columns) == 1 else None
        variable_column = (
            GroupDayTripsResultMetrics._variable_column(by_variable)
            if by_variable is not None
            else None
        )
        group_columns = zone_columns + ([variable_column] if variable_column is not None else [])
        per_replication_values = results.metrics._metric_by_replication(
            quantity=spec.quantity,
            quantity_column=spec.quantity_column,
            metric_column=metric_column,
            group_columns=group_columns,
            zone_column=zone_column,
            normalize_by=normalize_by,
            normalize_scope=normalize_scope,
            inner_zone_residents_only=inner_zone_residents_only,
            iterations=iterations,
        )
        table = self._with_base_gap(
            values,
            per_replication_values=per_replication_values,
            metric_column=metric_column,
        )
        result = SensitivityResult(table, metric_column=metric_column)
        if output == "tornado":
            return result.plot_tornado()
        return result

    def _collect_cases(self) -> list[SensitivityCase | None]:
        """Collect unique sensitivity cases from the setup."""
        sensitivity_values = collect_sensitivity_values(
            {
                "modes": self.setup.modes,
                "activities": self.setup.activities,
                "parameters": self.setup.parameters,
            }
        )
        by_name: dict[str, SensitivityValue] = {}
        for value in sensitivity_values:
            existing = by_name.get(value.name)
            if existing is None:
                by_name[value.name] = value
                continue
            if (
                existing.label != value.label
                or existing.variation_type != value.variation_type
                or existing.variation_values != value.variation_values
                or existing.variation_labels != value.variation_labels
            ):
                raise ValueError(
                    "SensitivityValue names should be unique unless all "
                    f"settings match. Conflicting name: {value.name!r}."
                )

        cases: list[SensitivityCase | None] = [None]
        for name in sorted(by_name):
            cases.extend(by_name[name].cases())
        return cases

    def _metadata(self) -> pl.DataFrame:
        """Return display metadata for non-base sensitivity cases."""
        rows = []
        for case in self.cases:
            if case is None:
                continue
            rows.append(
                {
                    "sensitivity_case": case.case_id,
                    "parameter": case.parameter_label or case.parameter_name,
                    "parameter_name": case.parameter_name,
                    "variation_type": case.variation_type,
                    "variation": case.variation_label,
                    "variation_value": case.variation_value,
                }
            )
        return pl.DataFrame(rows)

    def _with_base_gap(
        self,
        values: pl.DataFrame,
        *,
        per_replication_values: pl.DataFrame,
        metric_column: str,
    ) -> pl.DataFrame:
        """Compare every sensitivity case to the base case within each scenario."""
        std_column = f"{metric_column}_std"
        reference_column = f"{metric_column}_reference"
        excluded_columns = {
            "sensitivity_case",
            metric_column,
            std_column,
            "n_replications",
        }
        join_columns = [
            column
            for column in values.columns
            if column not in excluded_columns
        ]
        dimension_columns = [
            column
            for column in per_replication_values.columns
            if column not in SCOPE_COLUMNS + [metric_column]
        ]
        pairing_columns = [
            "scenario",
            "day_type",
            "iteration",
            "replication",
        ] + dimension_columns
        paired_output_columns = [
            "scenario",
            "sensitivity_case",
            "day_type",
            "iteration",
        ] + dimension_columns
        base_rows = (
            values
            .filter(pl.col("sensitivity_case") == "base")
            .select(join_columns + [pl.col(metric_column).alias(reference_column)])
        )
        sensitivity_rows = values.filter(pl.col("sensitivity_case") != "base")
        output_columns = [
            column
            for column in sensitivity_rows.columns
            if column not in {"n_replications"}
        ]
        if "n_replications" in sensitivity_rows.columns:
            output_columns.append("n_replications")

        base_replications = (
            per_replication_values
            .filter(pl.col("sensitivity_case") == "base")
            .select(pairing_columns + [pl.col(metric_column).alias(reference_column)])
        )
        sensitivity_replications = per_replication_values.filter(
            pl.col("sensitivity_case") != "base"
        )
        paired_gaps = (
            sensitivity_replications
            .join(base_replications, on=pairing_columns, how="inner")
            .with_columns(
                paired_gap=pl.col(metric_column) - pl.col(reference_column),
            )
            .group_by(paired_output_columns)
            .agg(
                pl.col("paired_gap").mean().alias("gap"),
                pl.col("paired_gap").std().alias("gap_std"),
                pl.col("replication").n_unique().cast(pl.UInt32).alias("n_paired_replications"),
            )
        )

        joined = (
            sensitivity_rows
            .join(base_rows, on=join_columns, how="left")
            .join(paired_gaps, on=paired_output_columns, how="left")
            .join(self._metadata(), on="sensitivity_case", how="left")
            .with_columns(
                relative_gap=(
                    pl.col("gap")
                    / pl.col(reference_column).abs().clip(1e-12)
                ),
            )
        )
        if "n_replications" in joined.columns:
            joined = (
                joined
                .with_columns(pl.col("n_paired_replications").alias("n_replications"))
                .drop("n_paired_replications")
            )

        return joined.select(
            [
                "scenario",
                "sensitivity_case",
                "parameter",
                "parameter_name",
                "variation",
                "variation_type",
                "variation_value",
            ]
            + [
                column
                for column in output_columns
                if column not in {"scenario", "sensitivity_case"}
            ]
            + [reference_column, "gap", "gap_std", "relative_gap"]
        )
