from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

import polars as pl

from mobility.reports import TransportZoneMaps

from .assets.final_state_metrics import Immobility, TripCountByDemandGroup
from .assets.person_metrics import (
    DEMAND_GROUP_COLUMNS,
    RESULT_COLUMNS,
    SCOPE_COLUMNS,
    add_demand_group_columns,
    complete_missing_replication_groups,
    filter_demand_groups_to_inner_zone_residents,
    filter_plan_steps_to_inner_zone_residents,
    with_analysis_dimensions,
)
from .assets.scoped_tables import ScopedRunTable
from .assets.survey_diagnostics import SurveyReferenceComparison, SurveyReferenceMarginal
from .assets.survey_time_diagnostics import ActivityDurationDistribution, ActivityTimeSeries
from .assets.trip_metrics import TripCountMetric
from .plots import (
    plot_activity_duration_distribution,
    plot_activity_time_series,
    plot_metric_flows_by_zone,
    plot_metric_by_dimension,
    plot_metric_by_zone,
    plot_metric_grid_by_zone,
    plot_metric_over_iterations,
    select_scenarios,
    validate_output,
)

ZoneDimension = Literal["home_zone", "origin_zone", "destination_zone"]
ZoneDimensions = ZoneDimension | list[ZoneDimension] | tuple[ZoneDimension, ...]
VariableDimension = Literal["mode", "activity", "distance_bin", "time_bin", "csp"]
MetricName = Literal["trip_count", "travel_distance", "travel_time", "cost", "ghg_emissions"]
MetricOutput = Literal["table", "plot"]
NormalizeBy = Literal["person_count", "metric_total"]
NormalizeScope = Literal["zone", "study_area"]
IterationSelector = str | int | list[int] | range
MetricReference = Literal["external"] | tuple[Literal["scenario"], str] | None
ReferenceView = Literal["gap", "values"]

REFERENCE_DIMENSIONS = {"mode", "activity", "distance_bin", "time_bin"}


@dataclass(frozen=True)
class MetricSpec:
    """Definition of one simple trip metric exposed by the public API."""

    quantity: str
    quantity_column: str | None
    indicator: str | None


METRIC_SPECS: dict[MetricName, MetricSpec] = {
    "trip_count": MetricSpec(
        quantity="trip_count",
        quantity_column=None,
        indicator="trip_count",
    ),
    "travel_distance": MetricSpec(
        quantity="travel_distance",
        quantity_column="distance",
        indicator="travel_distance",
    ),
    "travel_time": MetricSpec(
        quantity="travel_time",
        quantity_column="time",
        indicator="travel_time",
    ),
    "cost": MetricSpec(
        quantity="cost",
        quantity_column="cost",
        indicator=None,
    ),
    "ghg_emissions": MetricSpec(
        quantity="ghg_emissions",
        quantity_column="ghg_emissions_per_trip",
        indicator=None,
    ),
}


class GroupDayTripsResultMetrics:
    """Transport indicators for one or more scenarios and replications."""

    def __init__(self, results) -> None:
        self.results = results
        self._transport_zone_maps = None

    def _table(self, table_name: str, *, iterations: IterationSelector = "last") -> ScopedRunTable:
        """Return one scoped run output table for this result set."""
        return ScopedRunTable(
            results=self.results,
            table_name=table_name,
            iterations=iterations,
        )

    def metric(
        self,
        name: MetricName,
        *,
        by_zone: ZoneDimensions | None = None,
        by_variable: VariableDimension | None = None,
        normalize_by: NormalizeBy | None = None,
        normalize_scope: NormalizeScope | None = None,
        reference: MetricReference = None,
        reference_view: ReferenceView = "gap",
        inner_zone_residents_only: bool = False,
        iterations: IterationSelector = "last",
        output: MetricOutput = "table",
        scenarios: str | list[str] | tuple[str, ...] | None = None,
        width: int = 760,
        height: int = 460,
        labels: bool = True,
        n_largest: int | None = None,
        min_value: float | None = None,
        min_share: float | None = None,
        max_line_width: float = 8.0,
        min_line_width: float = 0.1,
    ) -> pl.DataFrame:
        """Return one simple trip metric, optionally split and normalized."""
        if name not in METRIC_SPECS:
            raise ValueError(
                "Unknown metric name. Expected one of: "
                + ", ".join(sorted(METRIC_SPECS))
                + "."
            )
        spec = METRIC_SPECS[name]
        return self._metric(
            quantity=spec.quantity,
            quantity_column=spec.quantity_column,
            indicator=spec.indicator,
            by_zone=by_zone,
            by_variable=by_variable,
            normalize_by=normalize_by,
            normalize_scope=normalize_scope,
            reference=reference,
            reference_view=reference_view,
            inner_zone_residents_only=inner_zone_residents_only,
            iterations=iterations,
            output=output,
            scenarios=scenarios,
            width=width,
            height=height,
            labels=labels,
            n_largest=n_largest,
            min_value=min_value,
            min_share=min_share,
            max_line_width=max_line_width,
            min_line_width=min_line_width,
        )

    def trip_count(self, **kwargs) -> pl.DataFrame:
        """Return trip counts, optionally split and normalized."""
        return self.metric("trip_count", **kwargs)

    def travel_distance(self, **kwargs) -> pl.DataFrame:
        """Return travelled distance, optionally split and normalized."""
        return self.metric("travel_distance", **kwargs)

    def travel_time(self, **kwargs) -> pl.DataFrame:
        """Return travelled time, optionally split and normalized."""
        return self.metric("travel_time", **kwargs)

    def cost(self, **kwargs) -> pl.DataFrame:
        """Return travelled cost, optionally split and normalized."""
        return self.metric("cost", **kwargs)

    def ghg_emissions(self, **kwargs) -> pl.DataFrame:
        """Return greenhouse gas emissions, optionally split and normalized."""
        return self.metric("ghg_emissions", **kwargs)

    def immobility(
        self,
        *,
        reference: MetricReference = None,
        iterations: IterationSelector = "last",
    ) -> pl.DataFrame:
        """Return model immobility by country and socio-professional category."""
        reference_kind = self._reference_kind(reference)
        if reference_kind == "scenario":
            raise ValueError("Scenario references are not available for immobility yet.")
        if reference_kind == "external" and not self.results.uses_last_iteration(iterations):
            raise ValueError("External references are only available for iterations='last' for now.")
        return Immobility(
            plan_steps=self._table("plan_steps", iterations=iterations),
            demand_groups=self._table("demand_groups", iterations=iterations),
            transport_zones=self.results.transport_zones,
            survey_immobility=self._survey_immobility() if reference_kind == "external" else None,
        ).get()

    def trip_count_by_demand_group(
        self,
        *,
        iterations: IterationSelector = "last",
    ) -> pl.DataFrame:
        """Return trip count and trips per person by demand group."""
        return TripCountByDemandGroup(
            plan_steps=self._table("plan_steps", iterations=iterations),
            demand_groups=self._table("demand_groups", iterations=iterations),
        ).get()

    def activity_duration_distribution(
        self,
        *,
        reference: MetricReference = None,
        iterations: IterationSelector = "last",
        bin_width_minutes: int = 15,
        output: MetricOutput = "table",
        width: int = 900,
        height: int = 520,
    ) -> pl.DataFrame:
        """Return model activity duration distributions."""
        validate_output(output)
        reference_kind = self._reference_kind(reference)
        if reference_kind == "scenario":
            raise ValueError("Scenario references are not available for activity duration distributions yet.")
        if reference_kind == "external" and not self.results.uses_last_iteration(iterations):
            raise ValueError("External references are only available for iterations='last' for now.")
        if output == "plot" and self.results.has_multiple_iterations(iterations):
            raise ValueError(
                'output="plot" currently supports only one iteration. '
                'Use iterations="last" or one iteration number, or use output="table".'
            )
        values = ActivityDurationDistribution(
            plan_steps=self._table("plan_steps", iterations=iterations),
            reference_plan_steps=self.results.survey_reference_plan_steps if reference_kind == "external" else None,
            scenarios=self.results.scenarios,
            day_type=self.results.day_type,
            iterations=self.results.selected_iterations(iterations),
            replications=self.results.replications,
            bin_width_minutes=bin_width_minutes,
        ).get()
        if output == "plot":
            return plot_activity_duration_distribution(
                values,
                scenario_titles=self.results.scenario_titles,
                width=width,
                height=height,
            )
        return values

    def activity_time_series(
        self,
        *,
        reference: MetricReference = None,
        iterations: IterationSelector = "last",
        interval_minutes: int = 15,
        output: MetricOutput = "table",
        width: int = 980,
        height: int = 560,
    ) -> pl.DataFrame:
        """Return model average occupancy by activity and time bin."""
        validate_output(output)
        reference_kind = self._reference_kind(reference)
        if reference_kind == "scenario":
            raise ValueError("Scenario references are not available for activity time series yet.")
        if reference_kind == "external" and not self.results.uses_last_iteration(iterations):
            raise ValueError("External references are only available for iterations='last' for now.")
        if output == "plot" and self.results.has_multiple_iterations(iterations):
            raise ValueError(
                'output="plot" currently supports only one iteration. '
                'Use iterations="last" or one iteration number, or use output="table".'
            )
        values = ActivityTimeSeries(
            plan_steps=self._table("plan_steps", iterations=iterations),
            demand_groups=self._table("demand_groups", iterations=iterations),
            reference_plan_steps=self.results.survey_reference_plan_steps if reference_kind == "external" else None,
            scenarios=self.results.scenarios,
            day_type=self.results.day_type,
            iterations=self.results.selected_iterations(iterations),
            replications=self.results.replications,
            interval_minutes=interval_minutes,
        ).get()
        if output == "plot":
            return plot_activity_time_series(
                values,
                scenario_titles=self.results.scenario_titles,
                width=width,
                height=height,
            )
        return values

    def opportunity_occupation(
        self,
        *,
        activity: str | None = None,
        inner_zone_residents_only: bool = True,
        iterations: IterationSelector = "last",
        output: MetricOutput = "table",
        scenarios: str | list[str] | tuple[str, ...] | None = None,
        width: int = 760,
        height: int = 460,
        labels: bool = True,
    ) -> pl.DataFrame:
        """Return opportunity occupation by destination zone and activity."""
        validate_output(output)
        values = self._opportunity_occupation(
            inner_zone_residents_only=inner_zone_residents_only,
            iterations=iterations,
        )
        values = self._filter_scenarios(values, scenarios=scenarios)
        if activity is not None:
            values = values.filter(pl.col("activity") == activity)
        if output == "plot":
            if activity is not None:
                return plot_metric_by_zone(
                    self._maps(),
                    values,
                    metric="opportunity_occupation",
                    zone_column="transport_zone_id",
                    title=f"Opportunity occupation for {activity}",
                    scenario_titles=self.results.scenario_titles,
                    width=width,
                    height=height,
                    labels=labels,
                )
            return plot_metric_grid_by_zone(
                self._maps(),
                values,
                metric="opportunity_occupation",
                zone_column="transport_zone_id",
                variable_column="activity",
                title="Opportunity occupation by destination zone and activity",
                scenario_titles=self.results.scenario_titles,
                width=width,
                height=height,
                labels=labels,
            )
        return values

    def _metric(
        self,
        *,
        quantity: str,
        quantity_column: str | None,
        indicator: str | None,
        by_zone: ZoneDimensions | None,
        by_variable: VariableDimension | None,
        normalize_by: NormalizeBy | None,
        normalize_scope: NormalizeScope | None,
        reference: MetricReference,
        reference_view: ReferenceView,
        inner_zone_residents_only: bool,
        iterations: IterationSelector,
        output: MetricOutput,
        scenarios: str | list[str] | tuple[str, ...] | None,
        width: int,
        height: int,
        labels: bool,
        n_largest: int | None,
        min_value: float | None,
        min_share: float | None,
        max_line_width: float,
        min_line_width: float,
    ) -> pl.DataFrame:
        """Return one compact metric query."""
        validate_output(output)
        self._validate_grouping(by_zone=by_zone, by_variable=by_variable)
        self._validate_normalization(
            normalize_by=normalize_by,
            normalize_scope=normalize_scope,
            by_zone=by_zone,
            by_variable=by_variable,
        )
        reference_kind = self._reference_kind(reference)
        self._validate_reference_view(reference_view, reference_kind=reference_kind)
        if reference_kind == "external" and indicator is None:
            raise ValueError(f"No reference is available for {quantity}.")
        if reference_kind == "external" and not self.results.uses_last_iteration(iterations):
            raise ValueError("External references are only available for iterations='last' for now.")
        if reference_kind == "external" and by_zone is not None:
            raise ValueError("External references are not available by zone.")
        if reference_kind == "external" and by_variable not in REFERENCE_DIMENSIONS and by_variable is not None:
            raise ValueError(
                "External references are available by mode, activity, distance_bin, "
                f"or time_bin. Received by_variable={by_variable!r}."
            )
        metric_column = self._metric_column(quantity, normalize_by)
        zone_columns = self._zone_columns(by_zone)
        zone_column = zone_columns[0] if len(zone_columns) == 1 else None
        variable_column = self._variable_column(by_variable) if by_variable is not None else None
        group_columns = zone_columns + ([variable_column] if variable_column is not None else [])

        if quantity == "trip_count":
            values = self._trip_count(
                output_column=(
                    "trip_count"
                    if normalize_by in {"person_count", "metric_total"}
                    else metric_column
                ),
                group_columns=group_columns,
                per_person=False,
                inner_zone_residents_only=inner_zone_residents_only,
                iterations=iterations,
            ).get()
            if normalize_by == "person_count":
                values = self._with_person_count_normalization(
                    values,
                    normalize_scope=normalize_scope,
                    zone_column=zone_column,
                    group_columns=group_columns,
                    value_column="trip_count",
                    output_column=metric_column,
                    inner_zone_residents_only=inner_zone_residents_only,
                    iterations=iterations,
                )
            elif normalize_by == "metric_total":
                values = self._with_metric_total_share(
                    values,
                    denominator_columns=self._denominator_columns(
                        normalize_scope=normalize_scope,
                        zone_column=zone_column,
                    ),
                    group_columns=group_columns,
                    value_column="trip_count",
                    output_column=metric_column,
                )
        else:
            values = self._weighted_quantity(
                quantity_column=quantity_column,
                output_column=quantity,
                group_columns=group_columns,
                inner_zone_residents_only=inner_zone_residents_only,
                iterations=iterations,
            )
            if normalize_by == "person_count":
                values = self._with_person_count_normalization(
                    values,
                    normalize_scope=normalize_scope,
                    zone_column=zone_column,
                    group_columns=group_columns,
                    value_column=quantity,
                    output_column=metric_column,
                    inner_zone_residents_only=inner_zone_residents_only,
                    iterations=iterations,
                )
            elif normalize_by == "metric_total":
                values = self._with_metric_total_share(
                    values,
                    denominator_columns=self._denominator_columns(
                        normalize_scope=normalize_scope,
                        zone_column=zone_column,
                    ),
                    group_columns=group_columns,
                    value_column=quantity,
                    output_column=metric_column,
                )

        if reference_kind == "scenario":
            reference_scenario = self._reference_scenario(reference)
            if reference_view == "gap":
                per_replication_values = self._metric_by_replication(
                    quantity=quantity,
                    quantity_column=quantity_column,
                    metric_column=metric_column,
                    group_columns=group_columns,
                    zone_column=zone_column,
                    normalize_by=normalize_by,
                    normalize_scope=normalize_scope,
                    inner_zone_residents_only=inner_zone_residents_only,
                    iterations=iterations,
                )
                values = self._with_scenario_reference(
                    values,
                    per_replication_values=per_replication_values,
                    reference_scenario=reference_scenario,
                    metric_column=metric_column,
                    scenarios=scenarios,
                )
            else:
                values = self._with_scenario_reference_values(
                    values,
                    reference_scenario=reference_scenario,
                    scenarios=scenarios,
                )
        elif reference_kind == "external":
            if reference_view == "gap":
                values = self._with_external_reference(
                    values,
                    by_variable=by_variable,
                    indicator=indicator,
                    metric_column=metric_column,
                    normalize_by=normalize_by,
                    normalize_scope=normalize_scope,
                    iterations=iterations,
                )
                values = self._filter_scenarios(values, scenarios=scenarios)
            else:
                values = self._with_external_reference_values(
                    values,
                    by_variable=by_variable,
                    indicator=indicator,
                    metric_column=metric_column,
                    normalize_by=normalize_by,
                    normalize_scope=normalize_scope,
                    iterations=iterations,
                    scenarios=scenarios,
                )
        else:
            values = self._filter_scenarios(values, scenarios=scenarios)
        if output == "plot":
            plot_metric_column = (
                "gap"
                if reference_kind != "none" and reference_view == "gap"
                else metric_column
            )
            plot_title = self._plot_title(
                quantity,
                by_zone=by_zone,
                by_variable=by_variable,
                normalize_by=normalize_by,
                reference_kind=reference_kind,
                reference_view=reference_view,
            )
            if "iteration" in values.columns and values["iteration"].n_unique() > 1:
                return plot_metric_over_iterations(
                    values,
                    metric=plot_metric_column,
                    title=plot_title,
                    dimension_columns=group_columns + (
                        ["series"] if "series" in values.columns else []
                    ),
                    scenario_titles=self.results.scenario_titles,
                    width=width,
                    height=height,
                )
            if by_zone is None and by_variable is None:
                raise ValueError('output="plot" needs `by_zone` or `by_variable`.')
            if len(zone_columns) > 1:
                if by_variable is not None:
                    raise ValueError("OD flow plots do not support `by_variable` yet.")
                if zone_columns != ["origin_zone_id", "destination_zone_id"]:
                    raise ValueError(
                        'output="plot" with multiple zone dimensions only supports '
                        'by_zone=["origin_zone", "destination_zone"].'
                    )
                return plot_metric_flows_by_zone(
                    self._maps(),
                    values,
                    metric=plot_metric_column,
                    origin_column="origin_zone_id",
                    destination_column="destination_zone_id",
                    title=plot_title,
                    scenario_titles=self.results.scenario_titles,
                    width=width,
                    height=height,
                    labels=labels,
                    n_largest=100 if n_largest is None else n_largest,
                    min_value=min_value,
                    min_share=min_share,
                    max_line_width=max_line_width,
                    min_line_width=min_line_width,
                )
            if zone_column is not None and by_variable is not None:
                return plot_metric_grid_by_zone(
                    self._maps(),
                    values,
                    metric=plot_metric_column,
                    zone_column=zone_column,
                    variable_column=variable_column,
                    title=plot_title,
                    scenario_titles=self.results.scenario_titles,
                    diverging_center=0.0 if plot_metric_column == "gap" else None,
                    width=width,
                    height=height,
                    labels=labels,
                )
            if zone_column is not None:
                return plot_metric_by_zone(
                    self._maps(),
                    values,
                    metric=plot_metric_column,
                    zone_column=zone_column,
                    title=plot_title,
                    scenario_titles=self.results.scenario_titles,
                    diverging_center=0.0 if plot_metric_column == "gap" else None,
                    width=width,
                    height=height,
                    labels=labels,
                )
            return plot_metric_by_dimension(
                values,
                dimension=variable_column,
                metric=plot_metric_column,
                yaxis_title=plot_metric_column.replace("_", " "),
                title=plot_title,
                scenarios=None if reference_kind != "none" else scenarios,
                scenario_titles=self.results.scenario_titles,
                width=width,
                height=height,
            )
        return values

    def _opportunity_occupation(
        self,
        *,
        inner_zone_residents_only: bool,
        iterations: IterationSelector,
    ) -> pl.DataFrame:
        """Return opportunity occupation aggregated over selected replications."""
        opportunities = self._table("opportunities", iterations=iterations).get()
        plan_steps = self._table("plan_steps", iterations=iterations).get()
        if inner_zone_residents_only:
            plan_steps = filter_plan_steps_to_inner_zone_residents(
                plan_steps,
                self.results.transport_zones,
            )

        plan_schema = set(plan_steps.collect_schema().names())
        duration_expr = self._activity_duration_expr(plan_schema)
        if "activity_seq_id" in plan_schema:
            plan_steps = plan_steps.filter(pl.col("activity_seq_id") != 0)

        transport_zones = pl.DataFrame(
            self.results.transport_zones.get().drop("geometry", axis=1, errors="ignore")
        ).with_columns(pl.col("transport_zone_id").cast(pl.String))
        destination_flags = transport_zones.select(
            pl.col("transport_zone_id").alias("to"),
            pl.col("is_inner_zone").alias("destination_is_inner_zone"),
        ).lazy()

        opportunity_schema = set(opportunities.collect_schema().names())
        missing_columns = {"to", "activity", "opportunity_capacity"}.difference(
            opportunity_schema
        )
        if missing_columns:
            raise ValueError(
                "Opportunity occupation needs these missing opportunity column(s): "
                + ", ".join(sorted(missing_columns))
                + "."
            )

        scoped_opportunities = (
            opportunities
            .select(SCOPE_COLUMNS + ["to", "activity", "opportunity_capacity"])
            .with_columns(
                pl.col("to").cast(pl.String),
                pl.col("activity").cast(pl.String),
                pl.col("opportunity_capacity").cast(pl.Float64),
            )
            .group_by(SCOPE_COLUMNS + ["to", "activity"])
            .agg(opportunity_capacity=pl.col("opportunity_capacity").sum())
        )
        occupied_duration = (
            plan_steps
            .with_columns(
                pl.col("to").cast(pl.String),
                pl.col("activity").cast(pl.String),
                duration=duration_expr,
            )
            .group_by(SCOPE_COLUMNS + ["to", "activity"])
            .agg(duration=pl.col("duration").sum())
        )
        per_replication = (
            scoped_opportunities
            .join(destination_flags, on="to", how="left")
            .join(occupied_duration, on=SCOPE_COLUMNS + ["to", "activity"], how="left")
            .with_columns(
                duration=pl.col("duration").fill_null(0.0),
                opportunity_occupation=(
                    pl.col("duration").fill_null(0.0)
                    / pl.col("opportunity_capacity").clip(1e-12)
                ),
            )
        )
        output_columns = RESULT_COLUMNS + [
            "transport_zone_id",
            "activity",
            "destination_is_inner_zone",
        ]
        return (
            per_replication
            .rename({"to": "transport_zone_id"})
            .group_by(output_columns)
            .agg(
                opportunity_capacity=pl.col("opportunity_capacity").mean(),
                duration=pl.col("duration").mean(),
                opportunity_occupation=pl.col("opportunity_occupation").mean(),
                opportunity_occupation_std=pl.col("opportunity_occupation").std(),
                n_replications=pl.col("replication").n_unique().cast(pl.UInt32),
            )
            .sort(output_columns)
            .collect(engine="streaming")
        )

    def _maps(self) -> TransportZoneMaps:
        """Return cached transport-zone map helpers for this result set."""
        if self._transport_zone_maps is None:
            population = getattr(self.results.first_run, "population", None)
            if not callable(getattr(population, "get", None)):
                population = None
            self._transport_zone_maps = TransportZoneMaps(
                self.results.transport_zones,
                population=population,
            )
        return self._transport_zone_maps

    def _with_survey_reference(
        self,
        *,
        by_variable: VariableDimension | None,
        indicator: str,
        metric_column: str,
        normalize_by: NormalizeBy | None,
        normalize_scope: NormalizeScope | None,
        iterations: IterationSelector,
    ) -> pl.DataFrame:
        """Return one metric with its natural external reference."""
        if normalize_by == "metric_total":
            if by_variable is None:
                raise ValueError('normalize_by="metric_total" needs `by_variable` for references.')
            return self._share_with_survey_reference(
                by_variable=by_variable,
                indicator=indicator,
                metric_column=metric_column,
                iterations=iterations,
            )

        if by_variable is None:
            comparison = (
                self._survey_reference_marginal([])
                .get()
                .filter(pl.col("indicator") == indicator)
                .group_by(RESULT_COLUMNS + ["reference_source"])
                .agg(
                    model_value=pl.col("model_value").sum(),
                    model_value_std=(pl.col("model_value_std").pow(2).sum()).sqrt(),
                    reference_value=pl.col("reference_value").sum(),
                    n_replications=pl.col("n_replications").max(),
                )
            )
            join_columns = RESULT_COLUMNS
        else:
            group_column = self._variable_column(by_variable)
            comparison = (
                self._survey_reference_marginal([group_column])
                .get()
                .filter(pl.col("indicator") == indicator)
            )
            join_columns = RESULT_COLUMNS + ["country"]

        if normalize_by == "person_count":
            if normalize_scope != "study_area":
                raise ValueError('External references only support normalize_scope="study_area".')
            population = (
                self._total_population(iterations=iterations)
                if by_variable is None
                else self._country_population(iterations=iterations)
            )
            comparison = (
                comparison
                .join(population, on=join_columns, how="left")
                .with_columns(
                    model_value=pl.col("model_value") / pl.col("n_persons").clip(1e-12),
                    model_value_std=pl.col("model_value_std") / pl.col("n_persons").clip(1e-12),
                    reference_value=pl.col("reference_value") / pl.col("n_persons").clip(1e-12),
                )
                .drop("n_persons")
            )

        return self._format_reference_table(
            comparison,
            by_variable=by_variable,
            metric_column=metric_column,
        )

    def _share_with_survey_reference(
        self,
        *,
        by_variable: VariableDimension,
        indicator: str,
        metric_column: str,
        iterations: IterationSelector,
    ) -> pl.DataFrame:
        """Return one quantity share with its external reference share."""
        group_column = self._variable_column(by_variable)
        comparison = (
            self._survey_reference_marginal([group_column])
            .get()
            .filter(pl.col("indicator") == indicator)
        )
        group_columns = RESULT_COLUMNS + ["country"]
        return self._format_reference_table(
            comparison
            .with_columns(
                model_total=pl.col("model_value").sum().over(group_columns),
                reference_total=pl.col("reference_value").sum().over(group_columns),
            )
            .with_columns(
                model_value=pl.col("model_value") / pl.col("model_total").clip(1e-12),
                model_value_std=pl.col("model_value_std") / pl.col("model_total").clip(1e-12),
                reference_value=pl.col("reference_value") / pl.col("reference_total").clip(1e-12),
            ),
            by_variable=by_variable,
            metric_column=metric_column,
        )

    def _format_reference_table(
        self,
        comparison: pl.DataFrame,
        *,
        by_variable: VariableDimension | None,
        metric_column: str,
    ) -> pl.DataFrame:
        """Rename model/reference columns to the public metric schema."""
        reference_column = f"{metric_column}_reference"
        columns = list(RESULT_COLUMNS)
        sort_columns = list(RESULT_COLUMNS)
        if by_variable is not None:
            group_column = self._variable_column(by_variable)
            columns.extend(["country", group_column])
            sort_columns.extend(["country", group_column])

        return (
            comparison
            .rename(
                {
                    "model_value": metric_column,
                    "model_value_std": f"{metric_column}_std",
                    "reference_value": reference_column,
                }
            )
            .with_columns(
                gap=pl.col(metric_column) - pl.col(reference_column),
                gap_std=pl.col(f"{metric_column}_std"),
                relative_gap=(
                    (pl.col(metric_column) - pl.col(reference_column))
                    / pl.col(reference_column).abs().clip(1e-12)
                ),
            )
            .select(
                columns
                + [
                    metric_column,
                    f"{metric_column}_std",
                    "reference_source",
                    reference_column,
                    "gap",
                    "gap_std",
                    "relative_gap",
                    "n_replications",
                ]
            )
            .sort(sort_columns)
        )

    def _with_scenario_reference(
        self,
        values: pl.DataFrame,
        *,
        per_replication_values: pl.DataFrame,
        reference_scenario: str,
        metric_column: str,
        scenarios: str | list[str] | tuple[str, ...] | None,
    ) -> pl.DataFrame:
        """Compare metric rows to one scenario from the same result set."""
        if reference_scenario not in self.results.scenarios:
            raise ValueError(
                "Scenario references should be included in the result set. "
                f"Missing scenario: {reference_scenario!r}."
            )

        selected_scenarios = select_scenarios(self.results, scenarios)
        model_scenarios = [
            scenario
            for scenario in selected_scenarios
            if scenario != reference_scenario
        ]
        if not model_scenarios:
            raise ValueError(
                "Scenario reference output needs at least one non-reference scenario."
            )

        std_column = f"{metric_column}_std"
        reference_column = f"{metric_column}_reference"
        non_key_columns = {"scenario", metric_column, std_column, "n_replications"}
        join_columns = [
            column
            for column in values.columns
            if column not in non_key_columns
        ]
        dimension_columns = [
            column
            for column in per_replication_values.columns
            if column not in SCOPE_COLUMNS + [metric_column]
        ]
        pairing_columns = ["day_type", "iteration", "replication"] + dimension_columns
        paired_output_columns = RESULT_COLUMNS + dimension_columns

        reference_rows = (
            values
            .filter(pl.col("scenario") == reference_scenario)
            .select(
                join_columns
                + [pl.col(metric_column).alias(reference_column)]
            )
        )
        model_rows = values.filter(pl.col("scenario").is_in(model_scenarios))
        output_columns = [
            column
            for column in model_rows.columns
            if column not in {"n_replications"}
        ]
        if "n_replications" in model_rows.columns:
            output_columns.append("n_replications")

        reference_replications = (
            per_replication_values
            .filter(pl.col("scenario") == reference_scenario)
            .select(
                pairing_columns
                + [pl.col(metric_column).alias(reference_column)]
            )
        )
        model_replications = per_replication_values.filter(
            pl.col("scenario").is_in(model_scenarios)
        )
        paired_gaps = (
            model_replications
            .join(reference_replications, on=pairing_columns, how="inner")
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
            model_rows
            .join(reference_rows, on=join_columns, how="left")
            .join(paired_gaps, on=paired_output_columns, how="left")
            .with_columns(
                reference_source=pl.lit(f"scenario:{reference_scenario}"),
                relative_gap=(
                    pl.col("gap") / pl.col(reference_column).abs().clip(1e-12)
                ),
            )
        )
        if "n_replications" in joined.columns:
            joined = (
                joined
                .with_columns(pl.col("n_paired_replications").alias("n_replications"))
                .drop("n_paired_replications")
            )

        return (
            joined
            .select(
                output_columns
                + [
                    "reference_source",
                    reference_column,
                    "gap",
                    "gap_std",
                    "relative_gap",
                ]
            )
            .sort([column for column in output_columns if column != "n_replications"])
        )

    def _with_scenario_reference_values(
        self,
        values: pl.DataFrame,
        *,
        reference_scenario: str,
        scenarios: str | list[str] | tuple[str, ...] | None,
    ) -> pl.DataFrame:
        """Return model rows and the selected scenario reference as value rows."""
        if reference_scenario not in self.results.scenarios:
            raise ValueError(
                "Scenario references should be included in the result set. "
                f"Missing scenario: {reference_scenario!r}."
            )

        selected_scenarios = select_scenarios(self.results, scenarios)
        model_scenarios = [
            scenario
            for scenario in selected_scenarios
            if scenario != reference_scenario
        ]
        if not model_scenarios:
            raise ValueError(
                "Scenario reference output needs at least one non-reference scenario."
            )

        output_columns = list(values.columns)
        model_rows = (
            values
            .filter(pl.col("scenario").is_in(model_scenarios))
            .with_columns(
                series=pl.lit("model"),
                reference_source=pl.lit(None, dtype=pl.String),
            )
            .select(output_columns + ["series", "reference_source"])
        )
        reference_rows = (
            values
            .filter(pl.col("scenario") == reference_scenario)
            .with_columns(
                series=pl.lit("reference"),
                reference_source=pl.lit(f"scenario:{reference_scenario}"),
            )
            .select(output_columns + ["series", "reference_source"])
        )
        return (
            pl.concat([model_rows, reference_rows], how="vertical_relaxed")
            .sort([column for column in output_columns if column != "n_replications"] + ["series"])
        )

    def _with_external_reference(
        self,
        values: pl.DataFrame,
        *,
        by_variable: VariableDimension | None,
        indicator: str,
        metric_column: str,
        normalize_by: NormalizeBy | None,
        normalize_scope: NormalizeScope | None,
        iterations: IterationSelector,
    ) -> pl.DataFrame:
        """Add the natural external reference to a normal metric table."""
        reference_values = self._external_reference_values(
            by_variable=by_variable,
            indicator=indicator,
            metric_column=metric_column,
            normalize_by=normalize_by,
            normalize_scope=normalize_scope,
            iterations=iterations,
        )
        reference_column = f"{metric_column}_reference"
        join_columns = list(RESULT_COLUMNS)
        if by_variable is not None:
            join_columns.append(self._variable_column(by_variable))
        std_column = f"{metric_column}_std"
        joined = (
            values
            .join(reference_values, on=join_columns, how="left")
            .with_columns(
                gap=pl.col(metric_column) - pl.col(reference_column),
                relative_gap=(
                    (pl.col(metric_column) - pl.col(reference_column))
                    / pl.col(reference_column).abs().clip(1e-12)
                ),
            )
        )
        if std_column in joined.columns:
            joined = joined.with_columns(gap_std=pl.col(std_column))
        else:
            joined = joined.with_columns(gap_std=pl.lit(None, dtype=pl.Float64))

        output_columns = list(values.columns)
        return (
            joined
            .select(
                output_columns
                + [
                    "reference_source",
                    reference_column,
                    "gap",
                    "gap_std",
                    "relative_gap",
                ]
            )
            .sort([column for column in output_columns if column != "n_replications"])
        )

    def _with_external_reference_values(
        self,
        values: pl.DataFrame,
        *,
        by_variable: VariableDimension | None,
        indicator: str,
        metric_column: str,
        normalize_by: NormalizeBy | None,
        normalize_scope: NormalizeScope | None,
        iterations: IterationSelector,
        scenarios: str | list[str] | tuple[str, ...] | None,
    ) -> pl.DataFrame:
        """Return model rows and external reference rows in one value table."""
        model_rows = self._filter_scenarios(values, scenarios=scenarios)
        output_columns = list(model_rows.columns)
        reference_column = f"{metric_column}_reference"
        reference_rows = self._external_reference_values(
            by_variable=by_variable,
            indicator=indicator,
            metric_column=metric_column,
            normalize_by=normalize_by,
            normalize_scope=normalize_scope,
            iterations=iterations,
        )
        reference_rows = self._filter_scenarios(reference_rows, scenarios=scenarios)
        reference_rows = reference_rows.rename({reference_column: metric_column})

        missing_expressions = []
        if f"{metric_column}_std" in output_columns and f"{metric_column}_std" not in reference_rows.columns:
            missing_expressions.append(pl.lit(None, dtype=pl.Float64).alias(f"{metric_column}_std"))
        if "n_replications" in output_columns and "n_replications" not in reference_rows.columns:
            missing_expressions.append(pl.lit(None, dtype=pl.UInt32).alias("n_replications"))
        if missing_expressions:
            reference_rows = reference_rows.with_columns(missing_expressions)

        model_rows = (
            model_rows
            .with_columns(
                series=pl.lit("model"),
                reference_source=pl.lit(None, dtype=pl.String),
            )
            .select(output_columns + ["series", "reference_source"])
        )
        reference_rows = (
            reference_rows
            .with_columns(series=pl.lit("reference"))
            .select(output_columns + ["series", "reference_source"])
        )
        return (
            pl.concat([model_rows, reference_rows], how="vertical_relaxed")
            .sort([column for column in output_columns if column != "n_replications"] + ["series"])
        )

    def _external_reference_values(
        self,
        *,
        by_variable: VariableDimension | None,
        indicator: str,
        metric_column: str,
        normalize_by: NormalizeBy | None,
        normalize_scope: NormalizeScope | None,
        iterations: IterationSelector,
    ) -> pl.DataFrame:
        """Return external reference values with the same dimensions as metric values."""
        if by_variable is None:
            comparison = (
                self._survey_reference_marginal([])
                .get()
                .filter(pl.col("indicator") == indicator)
                .group_by(RESULT_COLUMNS + ["reference_source"])
                .agg(reference_value=pl.col("reference_value").sum())
            )
            join_columns = RESULT_COLUMNS
            group_columns = RESULT_COLUMNS + ["reference_source"]
        else:
            group_column = self._variable_column(by_variable)
            comparison = (
                self._survey_reference_marginal([group_column])
                .get()
                .filter(pl.col("indicator") == indicator)
                .group_by(RESULT_COLUMNS + ["reference_source", group_column])
                .agg(reference_value=pl.col("reference_value").sum())
            )
            join_columns = RESULT_COLUMNS
            group_columns = RESULT_COLUMNS + ["reference_source", group_column]

        if normalize_by == "person_count":
            if normalize_scope != "study_area":
                raise ValueError('External references only support normalize_scope="study_area".')
            population = self._external_reference_population(iterations=iterations)
            comparison = (
                comparison
                .join(population, on=join_columns, how="left")
                .with_columns(
                    reference_value=pl.col("reference_value") / pl.col("n_persons_reference").clip(1e-12),
                )
                .drop("n_persons_reference")
            )
        elif normalize_by == "metric_total":
            if by_variable is None:
                raise ValueError('normalize_by="metric_total" needs `by_variable` for references.')
            comparison = comparison.with_columns(
                reference_total=pl.col("reference_value").sum().over(RESULT_COLUMNS),
            ).with_columns(
                reference_value=pl.col("reference_value") / pl.col("reference_total").clip(1e-12),
            )

        reference_column = f"{metric_column}_reference"
        return (
            comparison
            .with_columns(pl.col("reference_value").alias(reference_column))
            .select(group_columns + [reference_column])
            .sort(group_columns)
        )

    def _external_reference_population(
        self,
        *,
        iterations: IterationSelector,
    ) -> pl.DataFrame:
        """Return the population represented by the natural external reference."""
        reference_plan_steps = self.results.survey_reference_plan_steps.get()
        reference_schema = reference_plan_steps.collect_schema().names()
        if "home_zone_id" not in reference_schema or "n_persons" not in reference_schema:
            raise ValueError(
                "External references need reference plan steps with home_zone_id and n_persons."
            )

        zones = self.results.transport_zones.get()
        if "geometry" in zones.columns:
            zones = zones.drop(columns="geometry")
        inner_zones = (
            pl.from_pandas(zones)
            .filter(pl.col("is_inner_zone"))
            .select(pl.col("transport_zone_id").cast(pl.String).alias("home_zone_id"))
            .lazy()
        )
        plan_columns = [
            column
            for column in [
                "country",
                "home_zone_id",
                "city_category",
                "csp",
                "n_cars",
                "activity_seq_id",
                "time_seq_id",
            ]
            if column in reference_schema
        ]
        if not plan_columns:
            raise ValueError("External reference population could not identify reference plans.")

        reference_population = (
            reference_plan_steps
            .with_columns(pl.col("home_zone_id").cast(pl.String))
            .join(inner_zones, on="home_zone_id", how="inner")
            .select(plan_columns + ["n_persons"])
            .unique()
            .select(n_persons_reference=pl.col("n_persons").cast(pl.Float64).sum())
            .collect(engine="streaming")
        )
        scope = (
            self._table("plan_steps", iterations=iterations)
            .get()
            .select(RESULT_COLUMNS)
            .unique()
            .collect(engine="streaming")
        )
        return scope.join(reference_population, how="cross")

    def _weighted_quantity(
        self,
        *,
        quantity_column: str,
        output_column: str,
        group_columns: list[str],
        inner_zone_residents_only: bool,
        iterations: IterationSelector,
    ) -> pl.DataFrame:
        """Return an absolute weighted quantity averaged over replications."""
        plan_steps = self._plan_steps_with_quantity(
            quantity_column,
            iterations=iterations,
        )
        needed_demand_columns = [
            column
            for column in group_columns
            if column in DEMAND_GROUP_COLUMNS
        ]
        if inner_zone_residents_only and "home_zone_id" not in needed_demand_columns:
            needed_demand_columns.append("home_zone_id")
        if needed_demand_columns:
            plan_steps = add_demand_group_columns(
                plan_steps,
                self._table("demand_groups", iterations=iterations).get(),
                needed_demand_columns,
            )
        if inner_zone_residents_only:
            plan_steps = filter_plan_steps_to_inner_zone_residents(
                plan_steps,
                self.results.transport_zones,
            )
        plan_steps = with_analysis_dimensions(plan_steps, group_columns)
        required_columns = set(SCOPE_COLUMNS + group_columns + ["activity_seq_id", quantity_column, "n_persons"])
        missing_columns = required_columns.difference(plan_steps.collect_schema().names())
        if missing_columns:
            raise ValueError(
                "Metric plan steps are missing columns: "
                f"{sorted(missing_columns)}."
            )

        output_columns = RESULT_COLUMNS + group_columns
        return (
            plan_steps
            .filter(pl.col("activity_seq_id") != 0)
            .group_by(SCOPE_COLUMNS + group_columns)
            .agg(
                value=(
                    pl.col(quantity_column).cast(pl.Float64)
                    * pl.col("n_persons").cast(pl.Float64)
                ).sum()
            )
            .group_by(output_columns)
            .agg(
                pl.col("value").mean().alias(output_column),
                pl.col("value").std().alias(f"{output_column}_std"),
                pl.col("replication").n_unique().cast(pl.UInt32).alias("n_replications"),
            )
            .sort(output_columns)
            .collect(engine="streaming")
        )

    def _metric_by_replication(
        self,
        *,
        quantity: str,
        quantity_column: str | None,
        metric_column: str,
        group_columns: list[str],
        zone_column: str | None,
        normalize_by: NormalizeBy | None,
        normalize_scope: NormalizeScope | None,
        inner_zone_residents_only: bool,
        iterations: IterationSelector,
    ) -> pl.DataFrame:
        """Return metric values before averaging replications."""
        if quantity == "trip_count":
            value_column = "trip_count"
            per_replication = self._trip_count_by_replication(
                group_columns=group_columns,
                inner_zone_residents_only=inner_zone_residents_only,
                iterations=iterations,
                value_column=value_column,
            )
        else:
            value_column = quantity
            per_replication = self._weighted_quantity_by_replication(
                quantity_column=quantity_column,
                output_column=value_column,
                group_columns=group_columns,
                inner_zone_residents_only=inner_zone_residents_only,
                iterations=iterations,
            )

        if normalize_by == "person_count":
            per_replication = self._with_person_count_normalization_by_replication(
                per_replication,
                normalize_scope=normalize_scope,
                zone_column=zone_column,
                group_columns=group_columns,
                value_column=value_column,
                output_column=metric_column,
                inner_zone_residents_only=inner_zone_residents_only,
                iterations=iterations,
            )
        elif normalize_by == "metric_total":
            per_replication = self._with_metric_total_share_by_replication(
                per_replication,
                denominator_columns=self._denominator_columns(
                    normalize_scope=normalize_scope,
                    zone_column=zone_column,
                ),
                group_columns=group_columns,
                value_column=value_column,
                output_column=metric_column,
            )
        elif value_column != metric_column:
            per_replication = per_replication.rename({value_column: metric_column})

        return per_replication.select(SCOPE_COLUMNS + list(group_columns) + [metric_column])

    def _trip_count_by_replication(
        self,
        *,
        group_columns: list[str],
        inner_zone_residents_only: bool,
        iterations: IterationSelector,
        value_column: str,
    ) -> pl.DataFrame:
        """Return weighted trip counts by replication."""
        needed_demand_columns = [
            column
            for column in group_columns
            if column in DEMAND_GROUP_COLUMNS
        ]
        if inner_zone_residents_only and "home_zone_id" not in needed_demand_columns:
            needed_demand_columns.append("home_zone_id")
        plan_steps = add_demand_group_columns(
            self._table("plan_steps", iterations=iterations).get(),
            self._table("demand_groups", iterations=iterations).get(),
            needed_demand_columns,
        )
        if inner_zone_residents_only:
            plan_steps = filter_plan_steps_to_inner_zone_residents(
                plan_steps,
                self.results.transport_zones,
            )
        plan_steps = with_analysis_dimensions(plan_steps, group_columns)
        return (
            plan_steps
            .filter(pl.col("activity_seq_id") != 0)
            .group_by(SCOPE_COLUMNS + list(group_columns))
            .agg(pl.col("n_persons").cast(pl.Float64).sum().alias(value_column))
            .pipe(
                complete_missing_replication_groups,
                group_columns=group_columns,
                value_column=value_column,
            )
            .collect(engine="streaming")
        )

    def _weighted_quantity_by_replication(
        self,
        *,
        quantity_column: str,
        output_column: str,
        group_columns: list[str],
        inner_zone_residents_only: bool,
        iterations: IterationSelector,
    ) -> pl.DataFrame:
        """Return one weighted quantity by replication."""
        plan_steps = self._plan_steps_with_quantity(
            quantity_column,
            iterations=iterations,
        )
        needed_demand_columns = [
            column
            for column in group_columns
            if column in DEMAND_GROUP_COLUMNS
        ]
        if inner_zone_residents_only and "home_zone_id" not in needed_demand_columns:
            needed_demand_columns.append("home_zone_id")
        if needed_demand_columns:
            plan_steps = add_demand_group_columns(
                plan_steps,
                self._table("demand_groups", iterations=iterations).get(),
                needed_demand_columns,
            )
        if inner_zone_residents_only:
            plan_steps = filter_plan_steps_to_inner_zone_residents(
                plan_steps,
                self.results.transport_zones,
            )
        plan_steps = with_analysis_dimensions(plan_steps, group_columns)
        return (
            plan_steps
            .filter(pl.col("activity_seq_id") != 0)
            .group_by(SCOPE_COLUMNS + list(group_columns))
            .agg(
                (
                    pl.col(quantity_column).cast(pl.Float64)
                    * pl.col("n_persons").cast(pl.Float64)
                ).sum().alias(output_column)
            )
            .collect(engine="streaming")
        )

    def _with_person_count_normalization_by_replication(
        self,
        values: pl.DataFrame,
        *,
        normalize_scope: NormalizeScope | None,
        zone_column: str | None,
        group_columns: list[str],
        value_column: str,
        output_column: str,
        inner_zone_residents_only: bool,
        iterations: IterationSelector,
    ) -> pl.DataFrame:
        """Normalize one per-replication metric by population."""
        if normalize_scope == "zone":
            if zone_column is None:
                raise ValueError('normalize_scope="zone" needs `by_zone`.')
            values = values.with_columns(pl.col(zone_column).cast(pl.String))
            population = self._population(
                zone_column=zone_column,
                by_replication=True,
                inner_zone_residents_only=inner_zone_residents_only,
                iterations=iterations,
            )
            join_columns = SCOPE_COLUMNS + [zone_column]
        else:
            population = self._population(
                by_replication=True,
                inner_zone_residents_only=inner_zone_residents_only,
                iterations=iterations,
            )
            join_columns = SCOPE_COLUMNS

        return (
            values
            .join(population, on=join_columns, how="left")
            .with_columns(
                pl.col(value_column).truediv(pl.col("n_persons").clip(1e-12)).alias(output_column)
            )
            .select(SCOPE_COLUMNS + list(group_columns) + [output_column])
        )

    @staticmethod
    def _with_metric_total_share_by_replication(
        values: pl.DataFrame,
        *,
        denominator_columns: list[str],
        group_columns: list[str],
        value_column: str,
        output_column: str,
    ) -> pl.DataFrame:
        """Normalize one per-replication metric by its total."""
        total_scope_columns = SCOPE_COLUMNS + list(denominator_columns)
        return (
            values
            .with_columns(total=pl.col(value_column).sum().over(total_scope_columns))
            .with_columns(
                pl.col(value_column).truediv(pl.col("total").clip(1e-12)).alias(output_column)
            )
            .select(SCOPE_COLUMNS + list(group_columns) + [output_column])
        )

    def _with_person_count_normalization(
        self,
        values: pl.DataFrame,
        *,
        normalize_scope: NormalizeScope | None,
        zone_column: str | None,
        group_columns: list[str],
        value_column: str,
        output_column: str,
        inner_zone_residents_only: bool,
        iterations: IterationSelector,
    ) -> pl.DataFrame:
        """Normalize a metric by population at the requested scope."""
        if normalize_scope == "zone":
            if zone_column is None:
                raise ValueError('normalize_scope="zone" needs `by_zone`.')
            values = values.with_columns(pl.col(zone_column).cast(pl.String))
            population = self._population(
                zone_column=zone_column,
                inner_zone_residents_only=inner_zone_residents_only,
                iterations=iterations,
            )
            join_columns = RESULT_COLUMNS + [zone_column]
        else:
            population = self._population(
                inner_zone_residents_only=inner_zone_residents_only,
                iterations=iterations,
            )
            join_columns = RESULT_COLUMNS

        output_columns = RESULT_COLUMNS + list(group_columns)
        return (
            values
            .join(population, on=join_columns, how="left")
            .with_columns(
                pl.col(value_column).truediv(pl.col("n_persons").clip(1e-12)).alias(output_column),
                pl.col(f"{value_column}_std").truediv(pl.col("n_persons").clip(1e-12)).alias(f"{output_column}_std"),
            )
            .select(output_columns + [output_column, f"{output_column}_std", "n_replications"])
            .sort(output_columns)
        )

    def _with_metric_total_share(
        self,
        values: pl.DataFrame,
        *,
        denominator_columns: list[str],
        group_columns: list[str],
        value_column: str,
        output_column: str,
    ) -> pl.DataFrame:
        """Normalize a metric by its total at the requested scope."""
        total_scope_columns = RESULT_COLUMNS + list(denominator_columns)
        output_columns = RESULT_COLUMNS + list(group_columns)
        return (
            values
            .with_columns(
                total=pl.col(value_column).sum().over(total_scope_columns),
            )
            .with_columns(
                pl.col(value_column).truediv(pl.col("total").clip(1e-12)).alias(output_column),
                pl.col(f"{value_column}_std").truediv(pl.col("total").clip(1e-12)).alias(f"{output_column}_std"),
            )
            .select(output_columns + [output_column, f"{output_column}_std", "n_replications"])
            .sort(output_columns)
        )

    def _plan_steps_with_quantity(
        self,
        quantity_column: str,
        *,
        iterations: IterationSelector,
    ) -> pl.LazyFrame:
        """Return plan steps, joining costs when the requested quantity is absent."""
        plan_steps = self._table("plan_steps", iterations=iterations).get()
        if quantity_column in plan_steps.collect_schema().names():
            return plan_steps

        costs = self._table("costs", iterations=iterations).get()
        join_columns = SCOPE_COLUMNS + ["from", "to", "mode"]
        return plan_steps.join(
            costs.select(join_columns + [quantity_column]),
            on=join_columns,
            how="left",
        )

    def _filter_scenarios(
        self,
        values: pl.DataFrame,
        *,
        scenarios: str | list[str] | tuple[str, ...] | None,
    ) -> pl.DataFrame:
        """Filter a result table to selected scenarios."""
        if scenarios is None:
            return values
        selected_scenarios = select_scenarios(self.results, scenarios)
        return values.filter(pl.col("scenario").is_in(selected_scenarios))

    def _survey_reference_marginal(self, marginal_columns: list[str]) -> SurveyReferenceMarginal:
        """Build one cached survey-reference marginal."""
        return SurveyReferenceMarginal(
            comparison=SurveyReferenceComparison(
                plan_steps=self._table("plan_steps"),
                reference_plan_steps=self.results.survey_reference_plan_steps,
                transport_zones=self.results.transport_zones,
            ),
            marginal_columns=marginal_columns,
        )

    def _country_population(self, *, iterations: IterationSelector) -> pl.DataFrame:
        """Return population by scenario, day type, and country."""
        zones = self.results.transport_zones.get()
        if "geometry" in zones.columns:
            zones = zones.drop(columns="geometry")
        study_area = self.results.transport_zones.study_area.get()
        if "geometry" in study_area.columns:
            study_area = study_area.drop(columns="geometry")

        zone_country = (
            pl.from_pandas(zones)
            .filter(pl.col("is_inner_zone"))
            .join(
                pl.from_pandas(study_area)
                .select([
                    "local_admin_unit_id",
                    pl.col("country").cast(pl.String),
                ]),
                on="local_admin_unit_id",
                how="left",
            )
            .select([
                pl.col("transport_zone_id").cast(pl.String).alias("home_zone_id"),
                pl.col("country").cast(pl.String),
            ])
        )
        demand_groups = self._table("demand_groups", iterations=iterations).get().with_columns(
            pl.col("home_zone_id").cast(pl.String)
        )
        return (
            demand_groups
            .join(zone_country.lazy(), on="home_zone_id", how="inner")
            .group_by(SCOPE_COLUMNS + ["country"])
            .agg(n_persons=pl.col("n_persons").cast(pl.Float64).sum())
            .group_by(RESULT_COLUMNS + ["country"])
            .agg(n_persons=pl.col("n_persons").mean())
            .with_columns(pl.col("country").cast(pl.String))
            .collect(engine="streaming")
        )

    def _population(
        self,
        *,
        zone_column: str | None = None,
        by_replication: bool = False,
        inner_zone_residents_only: bool = False,
        iterations: IterationSelector,
    ) -> pl.DataFrame:
        """Return resident population for the requested result scope."""
        demand_groups = self._table("demand_groups", iterations=iterations).get()
        if inner_zone_residents_only:
            demand_groups = filter_demand_groups_to_inner_zone_residents(
                demand_groups,
                self.results.transport_zones,
            )
        extra_columns = []
        if zone_column is not None:
            extra_columns.append(zone_column)
            demand_groups = demand_groups.with_columns(
                pl.col("home_zone_id").cast(pl.String).alias(zone_column)
            )

        per_replication = (
            demand_groups
            .group_by(SCOPE_COLUMNS + extra_columns)
            .agg(n_persons=pl.col("n_persons").cast(pl.Float64).sum())
        )
        if by_replication:
            return per_replication.collect(engine="streaming")

        return (
            per_replication
            .group_by(RESULT_COLUMNS + extra_columns)
            .agg(n_persons=pl.col("n_persons").mean())
            .collect(engine="streaming")
        )

    def _survey_immobility(self) -> pl.DataFrame:
        """Return survey immobility probabilities by country and CSP."""
        column_name = "immobility_weekday" if self.results.is_weekday else "immobility_weekend"
        survey_tables = [
            pl.DataFrame(survey.get()["p_immobility"].reset_index()).with_columns(
                country=pl.lit(survey.inputs["parameters"].country, dtype=pl.String)
            )
            for survey in self.results.surveys
        ]
        if not survey_tables:
            raise ValueError("Immobility needs at least one survey with immobility probabilities.")
        return (
            pl.concat(survey_tables, how="vertical_relaxed")
            .with_columns(
                country=pl.col("country").cast(pl.String),
                csp=pl.col("csp").cast(pl.String),
                p_immobility_ref=pl.col(column_name).cast(pl.Float64),
            )
            .select(["country", "csp", "p_immobility_ref"])
        )

    def _trip_count(
        self,
        *,
        output_column: str,
        group_columns: list[str],
        per_person: bool,
        inner_zone_residents_only: bool = False,
        iterations: IterationSelector = "last",
    ) -> TripCountMetric:
        """Build one cached trip-count asset."""
        return TripCountMetric(
            plan_steps=self._table("plan_steps", iterations=iterations),
            demand_groups=self._table("demand_groups", iterations=iterations),
            transport_zones=self.results.transport_zones,
            output_column=output_column,
            group_columns=group_columns,
            per_person=per_person,
            inner_zone_residents_only=inner_zone_residents_only,
        )

    @staticmethod
    def _zone_column(by_zone: ZoneDimension) -> str:
        """Return the plan-step/demand-group column for one zone dimension."""
        if by_zone == "home_zone":
            return "home_zone_id"
        if by_zone == "origin_zone":
            return "origin_zone_id"
        if by_zone == "destination_zone":
            return "destination_zone_id"
        raise ValueError(
            "by_zone should be one of: 'home_zone', 'origin_zone', "
            f"'destination_zone'. Received {by_zone!r}."
        )

    @staticmethod
    def _zone_columns(by_zone: ZoneDimensions | None) -> list[str]:
        """Return public zone-id columns for requested zone dimensions."""
        if by_zone is None:
            return []
        if isinstance(by_zone, str):
            zone_dimensions = [by_zone]
        elif isinstance(by_zone, (list, tuple)):
            zone_dimensions = list(by_zone)
        else:
            raise TypeError("by_zone should be None, one zone dimension, or a list of zone dimensions.")
        if not zone_dimensions:
            raise ValueError("by_zone should not be an empty list.")
        duplicate_zones = sorted(
            zone
            for zone in set(zone_dimensions)
            if zone_dimensions.count(zone) > 1
        )
        if duplicate_zones:
            raise ValueError(
                "by_zone should not contain duplicate dimensions. "
                f"Received duplicates: {duplicate_zones}."
            )
        return [
            GroupDayTripsResultMetrics._zone_column(zone)
            for zone in zone_dimensions
        ]

    @staticmethod
    def _variable_column(by_variable: VariableDimension) -> str:
        """Return the plan-step column for one non-spatial dimension."""
        if by_variable in {"mode", "activity", "distance_bin", "time_bin", "csp"}:
            return by_variable
        raise ValueError(
            "by_variable should be one of: 'activity', 'csp', 'distance_bin', "
            f"'mode', 'time_bin'. Received {by_variable!r}."
        )

    @staticmethod
    def _denominator_columns(
        *,
        normalize_scope: NormalizeScope | None,
        zone_column: str | None,
    ) -> list[str]:
        """Return extra denominator grouping columns for metric-total shares."""
        if normalize_scope == "zone":
            if zone_column is None:
                raise ValueError('normalize_scope="zone" needs `by_zone`.')
            return [zone_column]
        return []

    @staticmethod
    def _reference_kind(reference: MetricReference) -> Literal["none", "external", "scenario"]:
        """Return the kind of reference requested by a metric call."""
        if reference is None:
            return "none"
        if reference == "external":
            return "external"
        if (
            isinstance(reference, tuple)
            and len(reference) == 2
            and reference[0] == "scenario"
            and isinstance(reference[1], str)
        ):
            return "scenario"
        raise ValueError('reference should be None, "external", or ("scenario", scenario_name).')

    @staticmethod
    def _reference_scenario(reference: MetricReference) -> str:
        """Return the scenario name from a scenario reference selector."""
        if (
            isinstance(reference, tuple)
            and len(reference) == 2
            and reference[0] == "scenario"
            and isinstance(reference[1], str)
        ):
            return reference[1]
        raise ValueError('reference should be None, "external", or ("scenario", scenario_name).')

    @staticmethod
    def _metric_column(quantity: str, normalize_by: NormalizeBy | None) -> str:
        """Return the public value column name."""
        if normalize_by is None:
            return quantity
        if normalize_by == "person_count":
            return f"{quantity}_per_person"
        return f"{quantity}_share"

    @staticmethod
    def _activity_duration_expr(schema: set[str]) -> pl.Expr:
        """Return total person-hours for one activity step."""
        if "duration" in schema:
            return pl.col("duration").cast(pl.Float64)
        if {"duration_per_pers", "n_persons"} <= schema:
            return (
                pl.col("duration_per_pers").cast(pl.Float64)
                * pl.col("n_persons").cast(pl.Float64)
            )
        raise ValueError(
            "Opportunity occupation needs plan steps with `duration` or "
            "`duration_per_pers` and `n_persons`."
        )

    @staticmethod
    def _title(
        quantity: str,
        by_zone: ZoneDimensions | None,
        by_variable: VariableDimension | None,
        normalize_by: NormalizeBy | None,
    ) -> str:
        """Return a simple plot title."""
        label = GroupDayTripsResultMetrics._metric_column(quantity, normalize_by).replace("_", " ")
        zone_dimensions = []
        if isinstance(by_zone, str):
            zone_dimensions = [by_zone]
        elif isinstance(by_zone, (list, tuple)):
            zone_dimensions = list(by_zone)
        dimensions = [
            value.replace("_", " ")
            for value in zone_dimensions + ([by_variable] if by_variable is not None else [])
            if value is not None
        ]
        if not dimensions:
            return label.capitalize()
        return f"{label.capitalize()} by {' and '.join(dimensions)}"

    @staticmethod
    def _plot_title(
        quantity: str,
        *,
        by_zone: ZoneDimensions | None,
        by_variable: VariableDimension | None,
        normalize_by: NormalizeBy | None,
        reference_kind: Literal["none", "external", "scenario"],
        reference_view: ReferenceView,
    ) -> str:
        """Return the plot title for direct metrics or reference gaps."""
        title = GroupDayTripsResultMetrics._title(
            quantity,
            by_zone=by_zone,
            by_variable=by_variable,
            normalize_by=normalize_by,
        )
        if reference_kind == "none":
            return title
        if reference_view == "values":
            return f"Metric and reference: {title}"
        return f"Gap to reference: {title}"

    @staticmethod
    def _validate_reference_view(
        reference_view: ReferenceView,
        *,
        reference_kind: Literal["none", "external", "scenario"],
    ) -> None:
        """Check how reference values should be returned."""
        if reference_view not in {"gap", "values"}:
            raise ValueError('reference_view should be either "gap" or "values".')
        if reference_kind == "none" and reference_view != "gap":
            raise ValueError('reference_view="values" needs a reference.')

    @staticmethod
    def _validate_grouping(
        *,
        by_zone: ZoneDimensions | None,
        by_variable: VariableDimension | None,
    ) -> None:
        """Check that requested grouping dimensions are supported."""
        GroupDayTripsResultMetrics._zone_columns(by_zone)
        if by_variable is not None:
            GroupDayTripsResultMetrics._variable_column(by_variable)

    @staticmethod
    def _validate_normalization(
        *,
        normalize_by: NormalizeBy | None,
        normalize_scope: NormalizeScope | None,
        by_zone: ZoneDimensions | None,
        by_variable: VariableDimension | None,
    ) -> None:
        """Check that the requested normalization is supported."""
        zone_columns = GroupDayTripsResultMetrics._zone_columns(by_zone)
        if normalize_by is None:
            if normalize_scope is not None:
                raise ValueError("normalize_scope should be None when normalize_by is None.")
            return
        if len(zone_columns) > 1:
            raise ValueError("Normalization is not supported when grouping by multiple zone dimensions.")
        if normalize_by not in {"person_count", "metric_total"}:
            raise ValueError('normalize_by should be None, "person_count", or "metric_total".')
        if normalize_scope not in {"zone", "study_area"}:
            raise ValueError('normalize_scope should be "zone" or "study_area" when normalize_by is set.')
        if normalize_scope == "zone" and by_zone is None:
            raise ValueError('normalize_scope="zone" needs `by_zone`.')
        if normalize_by == "metric_total" and by_zone is None and by_variable is None:
            raise ValueError('normalize_by="metric_total" needs `by_zone` or `by_variable`.')
