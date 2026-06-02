from __future__ import annotations

import plotly.graph_objects as go
import polars as pl
import plotly.express as px
from plotly.subplots import make_subplots

from mobility.reports.theme import MOBILITY_COLORS, apply_report_layout

SCENARIO_COLORS = [
    MOBILITY_COLORS["model"],
    "#2B6CB0",
    "#2F855A",
    "#805AD5",
    "#B7791F",
    "#319795",
]
REFERENCE_COLOR = "#8A8F98"


def validate_output(output: str) -> None:
    """Check that a result method output is supported."""
    if output not in {"table", "plot"}:
        raise ValueError('output should be either "table" or "plot".')


def plot_metric_by_dimension(
    values: pl.DataFrame,
    *,
    dimension: str,
    metric: str,
    yaxis_title: str,
    title: str,
    scenarios: str | list[str] | tuple[str, ...] | None = None,
    scenario_titles: dict[str, str] | None = None,
    width: int = 760,
    height: int = 460,
) -> go.Figure:
    """Plot one metric by an analysis dimension and scenario."""
    if f"{metric}_reference" in values.columns:
        return plot_reference_metric_by_dimension(
            values,
            dimension=dimension,
            metric=metric,
            title=title,
            scenarios=scenarios,
            scenario_titles=scenario_titles,
            width=width,
            height=height,
        )

    if "series" in values.columns:
        return plot_metric_series_by_dimension(
            values,
            dimension=dimension,
            metric=metric,
            yaxis_title=yaxis_title,
            title=title,
            scenarios=scenarios,
            scenario_titles=scenario_titles,
            width=width,
            height=height,
        )

    if scenarios is not None:
        selected_scenarios = _selected_scenarios_from_values(values, scenarios)
        values = values.filter(pl.col("scenario").is_in(selected_scenarios))
    else:
        selected_scenarios = values["scenario"].unique(maintain_order=True).to_list()

    scenario_labels = _scenario_label_map(selected_scenarios, scenario_titles)
    dimensions = values[dimension].unique(maintain_order=True).to_list()
    labels = [_dimension_label(value) for value in dimensions]
    fig = go.Figure()
    for scenario_index, scenario in enumerate(selected_scenarios):
        scenario_values = values.filter(pl.col("scenario") == scenario)
        metric_values = dict(
            zip(
                scenario_values[dimension].to_list(),
                scenario_values[metric].to_list(),
            )
        )
        metric_errors = dict(
            zip(
                scenario_values[dimension].to_list(),
                scenario_values[f"{metric}_std"].to_list(),
            )
        )
        fig.add_bar(
            x=labels,
            y=[metric_values.get(value, 0.0) for value in dimensions],
            error_y={
                "type": "data",
                "array": [metric_errors.get(value) for value in dimensions],
                "visible": True,
            },
            marker_color=_scenario_color(scenario_index),
            name=scenario_labels[scenario],
        )
    _apply_vertical_bar_layout(
        fig,
        labels=labels,
        title=title,
        width=width,
        height=height,
        yaxis_title=yaxis_title,
        showlegend=True,
    )
    if metric.endswith("_share"):
        fig.update_yaxes(tickformat=".0%")
    return fig


def plot_metric_series_by_dimension(
    values: pl.DataFrame,
    *,
    dimension: str,
    metric: str,
    yaxis_title: str,
    title: str,
    scenarios: str | list[str] | tuple[str, ...] | None = None,
    scenario_titles: dict[str, str] | None = None,
    width: int = 760,
    height: int = 460,
) -> go.Figure:
    """Plot model and reference value rows by dimension."""
    if scenarios is not None:
        selected_scenarios = _selected_scenarios_from_values(values, scenarios)
        values = values.filter(pl.col("scenario").is_in(selected_scenarios))
    else:
        selected_scenarios = values["scenario"].unique(maintain_order=True).to_list()

    scenario_labels = _scenario_label_map(selected_scenarios, scenario_titles)
    dimensions = values[dimension].unique(maintain_order=True).to_list()
    labels = [_dimension_label(value) for value in dimensions]
    std_column = f"{metric}_std"
    fig = go.Figure()
    reference_rows = values.filter(pl.col("series") == "reference")
    if not reference_rows.is_empty():
        aggregate_expressions = [pl.col(metric).mean().alias(metric)]
        if std_column in reference_rows.columns:
            aggregate_expressions.append(pl.col(std_column).mean().alias(std_column))
        reference_values = reference_rows.group_by(dimension).agg(aggregate_expressions)
        metric_values = dict(zip(reference_values[dimension].to_list(), reference_values[metric].to_list()))
        if std_column in reference_values.columns:
            metric_errors = dict(zip(reference_values[dimension].to_list(), reference_values[std_column].to_list()))
            error_values = [metric_errors.get(value) for value in dimensions]
            error_y = {
                "type": "data",
                "array": error_values,
                "visible": any(value is not None for value in error_values),
            }
        else:
            error_y = None
        fig.add_bar(
            x=labels,
            y=[metric_values.get(value, 0.0) for value in dimensions],
            error_y=error_y,
            marker_color=REFERENCE_COLOR,
            name="Reference",
        )

    for scenario_index, scenario in enumerate(selected_scenarios):
        trace_values = values.filter(
            (pl.col("scenario") == scenario)
            & (pl.col("series") == "model")
        )
        if trace_values.is_empty():
            continue
        metric_values = dict(zip(trace_values[dimension].to_list(), trace_values[metric].to_list()))
        if std_column in trace_values.columns:
            metric_errors = dict(zip(trace_values[dimension].to_list(), trace_values[std_column].to_list()))
            error_y = {
                "type": "data",
                "array": [metric_errors.get(value) for value in dimensions],
                "visible": True,
            }
        else:
            error_y = None
        fig.add_bar(
            x=labels,
            y=[metric_values.get(value, 0.0) for value in dimensions],
            error_y=error_y,
            marker_color=_scenario_color(scenario_index),
            name=scenario_labels[scenario],
        )

    _apply_vertical_bar_layout(
        fig,
        labels=labels,
        title=title,
        width=width,
        height=height,
        yaxis_title=yaxis_title,
        showlegend=True,
    )
    if metric.endswith("_share"):
        fig.update_yaxes(tickformat=".0%")
    return fig


def plot_metric_over_iterations(
    values: pl.DataFrame,
    *,
    metric: str,
    title: str,
    dimension_columns: list[str],
    scenario_titles: dict[str, str] | None = None,
    width: int = 760,
    height: int = 460,
) -> go.Figure:
    """Plot one metric over model iterations."""
    if values.is_empty():
        raise ValueError("No rows are available to plot.")

    selected_scenarios = values["scenario"].unique(maintain_order=True).to_list()
    scenario_labels = _scenario_label_map(selected_scenarios, scenario_titles)
    plot_values = values
    dimension_column = "_dimension_label"
    if dimension_columns:
        plot_values = plot_values.with_columns(
            pl.concat_str(
                [pl.col(column).cast(pl.String) for column in dimension_columns],
                separator=" / ",
            ).alias(dimension_column)
        )
    else:
        plot_values = plot_values.with_columns(pl.lit("Total").alias(dimension_column))

    has_dimensions = bool(dimension_columns)
    line_dashes = ["solid", "dash", "dot", "dashdot", "longdash", "longdashdot"]
    std_column = f"{metric}_std"
    fig = go.Figure()
    for scenario_index, scenario in enumerate(selected_scenarios):
        scenario_values = plot_values.filter(pl.col("scenario") == scenario)
        dimensions = scenario_values[dimension_column].unique(maintain_order=True).to_list()
        for dimension_index, dimension in enumerate(dimensions):
            trace_values = (
                scenario_values
                .filter(pl.col(dimension_column) == dimension)
                .sort("iteration")
            )
            if has_dimensions and len(selected_scenarios) == 1:
                trace_name = _dimension_label(dimension)
                color = _scenario_color(dimension_index)
                dash = "solid"
            elif has_dimensions:
                trace_name = f"{scenario_labels[scenario]} - {_dimension_label(dimension)}"
                color = _scenario_color(scenario_index)
                dash = line_dashes[dimension_index % len(line_dashes)]
            else:
                trace_name = scenario_labels[scenario]
                color = _scenario_color(scenario_index)
                dash = "solid"

            iterations = trace_values["iteration"].to_list()
            metric_values = trace_values[metric].to_list()
            if std_column in trace_values.columns:
                metric_std = trace_values[std_column].to_list()
                if any(value is not None for value in metric_std):
                    std_values = [0.0 if value is None else value for value in metric_std]
                    lower_values = [
                        value - std
                        for value, std in zip(metric_values, std_values)
                    ]
                    upper_values = [
                        value + std
                        for value, std in zip(metric_values, std_values)
                    ]
                    fig.add_scatter(
                        x=iterations + list(reversed(iterations)),
                        y=upper_values + list(reversed(lower_values)),
                        fill="toself",
                        fillcolor=_transparent_color(color, 0.18),
                        line={"color": "rgba(0,0,0,0)"},
                        hoverinfo="skip",
                        mode="lines",
                        name=f"{trace_name} uncertainty",
                        showlegend=False,
                    )
            fig.add_scatter(
                x=iterations,
                y=metric_values,
                mode="lines+markers",
                line={"color": color, "dash": dash},
                marker={"color": color},
                name=trace_name,
            )

    apply_report_layout(fig, title=title)
    fig.update_layout(
        width=width,
        height=height,
        xaxis_title="Iteration",
        yaxis_title=metric.replace("_", " "),
        showlegend=True,
        margin={"l": 70, "r": 45, "t": 95, "b": 80},
        legend={
            "orientation": "h",
            "yanchor": "bottom",
            "y": 1.03,
            "xanchor": "left",
            "x": 0.0,
        },
    )
    fig.update_xaxes(dtick=1, automargin=True)
    fig.update_yaxes(automargin=True)
    if metric.endswith("_share"):
        fig.update_yaxes(tickformat=".0%")
    return fig


def plot_metric_by_zone(
    maps,
    values: pl.DataFrame,
    *,
    metric: str,
    zone_column: str,
    title: str,
    scenario_titles: dict[str, str] | None = None,
    diverging_center: float | None = None,
    width: int = 760,
    height: int = 460,
    labels: bool = True,
) -> go.Figure:
    """Plot a zone metric as one map per scenario."""
    scenarios = values["scenario"].unique(maintain_order=True).to_list()
    if not scenarios:
        raise ValueError("No rows are available to plot.")

    map_values = (
        values
        .rename({zone_column: "transport_zone_id"})
        .with_columns(pl.col("transport_zone_id").cast(pl.String))
    )
    facet_column = "scenario"
    if scenario_titles:
        facet_column = "scenario_label"
        map_values = _with_scenario_label_column(map_values, scenario_titles)
    hover_columns = []
    std_column = f"{metric}_std"
    if std_column in map_values.columns:
        hover_columns.append(std_column)
    range_color = _symmetric_range(map_values, metric, diverging_center)
    return maps.metric_facets(
        map_values,
        value_column=metric,
        facet_column=facet_column,
        save_name=f"{metric}-by-zone-map",
        id_column="transport_zone_id",
        labels=labels,
        width=width,
        height=height,
        hover_columns=hover_columns,
        legend_label=metric.replace("_", " "),
        frame_title=title,
        classify=False,
        color_continuous_scale=(
            _blue_white_red_color_scale()
            if diverging_center is not None
            else _ylorrd_color_scale()
        ),
        color_continuous_midpoint=diverging_center,
        range_color=range_color,
        colorbar_tickformat=".0%" if metric.endswith("_share") else None,
    )


def plot_metric_grid_by_zone(
    maps,
    values: pl.DataFrame,
    *,
    metric: str,
    zone_column: str,
    variable_column: str,
    title: str,
    scenario_titles: dict[str, str] | None = None,
    diverging_center: float | None = None,
    width: int = 760,
    height: int = 460,
    labels: bool = True,
) -> go.Figure:
    """Plot a zone metric as variable rows by scenario columns."""
    if values.is_empty():
        raise ValueError("No rows are available to plot.")

    map_values = (
        values
        .rename({zone_column: "transport_zone_id"})
        .with_columns(pl.col("transport_zone_id").cast(pl.String))
    )
    scenario_column = "scenario"
    if scenario_titles:
        scenario_column = "scenario_label"
        map_values = _with_scenario_label_column(map_values, scenario_titles)
    hover_columns = []
    std_column = f"{metric}_std"
    if std_column in map_values.columns:
        hover_columns.append(std_column)
    range_color = _symmetric_range(map_values, metric, diverging_center)
    return maps.metric_grid(
        map_values,
        value_column=metric,
        row_column=variable_column,
        column_column=scenario_column,
        save_name=f"{metric}-by-zone-and-{variable_column}-map",
        id_column="transport_zone_id",
        labels=labels,
        width=width,
        height=height,
        hover_columns=hover_columns,
        legend_label=metric.replace("_", " "),
        frame_title=title,
        classify=False,
        color_continuous_scale=(
            _blue_white_red_color_scale()
            if diverging_center is not None
            else _ylorrd_color_scale()
        ),
        color_continuous_midpoint=diverging_center,
        range_color=range_color if diverging_center is not None else (
            (0.0, 1.0) if metric.endswith("_share") else None
        ),
        colorbar_tickformat=".0%" if metric.endswith("_share") else None,
    )


def plot_metric_flows_by_zone(
    maps,
    values: pl.DataFrame,
    *,
    metric: str,
    origin_column: str,
    destination_column: str,
    title: str,
    scenario_titles: dict[str, str] | None = None,
    width: int = 760,
    height: int = 460,
    labels: bool = True,
    n_largest: int | None = 100,
    min_value: float | None = None,
    min_share: float | None = None,
    max_line_width: float = 8.0,
    min_line_width: float = 0.1,
) -> go.Figure:
    """Plot an OD metric as proportional-width flow lines."""
    if values.is_empty():
        raise ValueError("No rows are available to plot.")

    map_values = values.with_columns(
        pl.col(origin_column).cast(pl.String),
        pl.col(destination_column).cast(pl.String),
    )
    facet_column = "scenario"
    if scenario_titles:
        facet_column = "scenario_label"
        map_values = _with_scenario_label_column(map_values, scenario_titles)
    hover_columns = []
    std_column = f"{metric}_std"
    if std_column in map_values.columns:
        hover_columns.append(std_column)
    return maps.metric_flows(
        map_values,
        value_column=metric,
        origin_column=origin_column,
        destination_column=destination_column,
        facet_column=facet_column,
        save_name=f"{metric}-by-origin-and-destination-map",
        labels=labels,
        width=width,
        height=height,
        hover_columns=hover_columns,
        legend_label=metric.replace("_", " "),
        frame_title=title,
        n_largest=n_largest,
        min_value=min_value,
        min_share=min_share,
        max_line_width=max_line_width,
        min_line_width=min_line_width,
    )


def plot_activity_duration_distribution(
    values: pl.DataFrame,
    *,
    scenario_titles: dict[str, str] | None = None,
    width: int = 900,
    height: int = 520,
) -> go.Figure:
    """Plot activity-duration probabilities by activity and source."""
    if values.is_empty():
        raise ValueError("No rows are available to plot.")
    plot_values = _with_scenario_label_column(values, scenario_titles)
    fig = px.line(
        plot_values.to_pandas(),
        x="duration_bin_mid",
        y="probability",
        color="source",
        line_dash="scenario_label",
        facet_col="activity",
        facet_col_wrap=3,
        error_y="probability_std" if "probability_std" in plot_values.columns else None,
        labels={
            "duration_bin_mid": "Duration (h)",
            "probability": "Probability",
            "source": "Source",
            "scenario_label": "Scenario",
        },
    )
    fig.for_each_annotation(
        lambda annotation: annotation.update(text=annotation.text.replace("activity=", ""))
    )
    apply_report_layout(fig, title="Activity duration distribution")
    fig.update_layout(width=width, height=height, legend_title_text=None)
    fig.update_yaxes(tickformat=".0%")
    return fig


def plot_activity_time_series(
    values: pl.DataFrame,
    *,
    scenario_titles: dict[str, str] | None = None,
    width: int = 980,
    height: int = 560,
) -> go.Figure:
    """Plot activity occupancy over the day."""
    if values.is_empty():
        raise ValueError("No rows are available to plot.")
    plot_values = _with_scenario_label_column(values, scenario_titles)
    fig = px.area(
        plot_values.to_pandas(),
        x="time_label",
        y="n_persons",
        color="label",
        facet_row="source",
        facet_col="scenario_label",
        labels={
            "time_label": "Time",
            "n_persons": "Persons",
            "label": "Activity",
            "scenario_label": "Scenario",
        },
    )
    fig.for_each_annotation(
        lambda annotation: annotation.update(
            text=annotation.text.replace("source=", "").replace("scenario_label=", "")
        )
    )
    apply_report_layout(fig, title="Activity time series")
    fig.update_layout(width=width, height=height, legend_title_text=None)
    fig.update_xaxes(tickangle=0, nticks=8)
    return fig


def plot_reference_metric_by_dimension(
    values: pl.DataFrame,
    *,
    dimension: str,
    metric: str,
    title: str,
    scenarios: str | list[str] | tuple[str, ...] | None = None,
    scenario_titles: dict[str, str] | None = None,
    width: int = 820,
    height: int = 480,
) -> go.Figure:
    """Plot one metric against its reference by country, dimension, and scenario."""
    if scenarios is not None:
        selected_scenarios = _selected_scenarios_from_values(values, scenarios)
        values = values.filter(pl.col("scenario").is_in(selected_scenarios))
    else:
        selected_scenarios = values["scenario"].unique(maintain_order=True).to_list()

    scenario_labels = _scenario_label_map(selected_scenarios, scenario_titles)
    countries = values["country"].unique(maintain_order=True).to_list()
    if not countries:
        raise ValueError("No reference rows are available to plot.")

    max_dimensions = max(values.filter(pl.col("country") == country)[dimension].n_unique() for country in countries)
    fig = make_subplots(
        rows=1,
        cols=len(countries),
        subplot_titles=countries,
        shared_yaxes=True,
        horizontal_spacing=0.08 if len(countries) > 1 else 0.0,
    )
    reference_column = f"{metric}_reference"
    error_column = f"{metric}_std"
    for column_index, country in enumerate(countries, start=1):
        country_values = values.filter(pl.col("country") == country).sort(dimension)
        dimensions = country_values[dimension].unique(maintain_order=True).to_list()
        labels = [_dimension_label(value) for value in dimensions]
        reference_rows = country_values.select([dimension, reference_column]).unique(
            subset=[dimension],
            maintain_order=True,
        )
        reference_values = dict(zip(reference_rows[dimension].to_list(), reference_rows[reference_column].to_list()))
        fig.add_bar(
            x=labels,
            y=[reference_values.get(value, 0.0) for value in dimensions],
            marker_color=MOBILITY_COLORS["survey"],
            name="Reference",
            showlegend=column_index == 1,
            row=1,
            col=column_index,
        )
        for scenario_index, scenario in enumerate(selected_scenarios):
            scenario_values = country_values.filter(pl.col("scenario") == scenario)
            metric_values = dict(zip(scenario_values[dimension].to_list(), scenario_values[metric].to_list()))
            metric_errors = dict(zip(scenario_values[dimension].to_list(), scenario_values[error_column].to_list()))
            fig.add_bar(
                x=labels,
                y=[metric_values.get(value, 0.0) for value in dimensions],
                error_y={
                    "type": "data",
                    "array": [metric_errors.get(value) for value in dimensions],
                    "visible": True,
                },
                marker_color=_scenario_color(scenario_index),
                name=scenario_labels[scenario],
                showlegend=column_index == 1,
                row=1,
                col=column_index,
            )
        fig.update_xaxes(automargin=True, row=1, col=column_index)
        fig.update_yaxes(automargin=True, row=1, col=column_index)

    apply_report_layout(fig, title=title)
    fig.update_layout(
        width=max(width, 390 * len(countries)),
        height=max(height, 480 + 18 * max_dimensions),
        barmode="group",
        bargap=0.22,
        bargroupgap=0.16,
        yaxis_title=metric.replace("_", " "),
        showlegend=True,
        margin={"l": 70, "r": 45, "t": 105, "b": 125},
        legend={
            "orientation": "h",
            "yanchor": "bottom",
            "y": 1.04,
            "xanchor": "left",
            "x": 0.0,
        },
    )
    if metric.endswith("_share"):
        fig.update_yaxes(tickformat=".0%")
    return fig


def select_scenarios(
    results,
    scenarios: str | list[str] | tuple[str, ...] | None,
) -> list[str]:
    """Return scenario names after checking they are in this result set."""
    if scenarios is None:
        selected_scenarios = list(results.scenarios)
    elif isinstance(scenarios, str):
        selected_scenarios = [scenarios]
    elif isinstance(scenarios, (list, tuple)):
        selected_scenarios = list(scenarios)
    else:
        raise TypeError("scenarios should be None, one scenario name, or a list of scenario names.")

    if not selected_scenarios:
        raise ValueError("At least one scenario is needed.")
    if not all(isinstance(scenario, str) for scenario in selected_scenarios):
        raise TypeError("scenarios should only contain scenario names.")

    missing_scenarios = [
        scenario
        for scenario in selected_scenarios
        if scenario not in results.scenarios
    ]
    if missing_scenarios:
        raise ValueError(
            "Selected scenarios should be included in the result set. "
            f"Missing scenarios: {missing_scenarios}."
        )

    duplicate_scenarios = sorted(
        scenario
        for scenario in set(selected_scenarios)
        if selected_scenarios.count(scenario) > 1
    )
    if duplicate_scenarios:
        raise ValueError(
            "scenarios should not contain duplicate names. "
            f"Received duplicates: {duplicate_scenarios}."
        )

    return selected_scenarios


def _selected_scenarios_from_values(
    values: pl.DataFrame,
    scenarios: str | list[str] | tuple[str, ...],
) -> list[str]:
    """Return scenario names after checking they are in a value table."""
    if isinstance(scenarios, str):
        selected_scenarios = [scenarios]
    elif isinstance(scenarios, (list, tuple)):
        selected_scenarios = list(scenarios)
    else:
        raise TypeError("scenarios should be one scenario name or a list of scenario names.")

    if not selected_scenarios:
        raise ValueError("At least one scenario is needed.")
    available_scenarios = set(values["scenario"].unique().to_list())
    missing_scenarios = [
        scenario
        for scenario in selected_scenarios
        if scenario not in available_scenarios
    ]
    if missing_scenarios:
        raise ValueError(
            "Selected scenarios should be included in the result table. "
            f"Missing scenarios: {missing_scenarios}."
        )
    return selected_scenarios


def _with_scenario_label_column(
    values: pl.DataFrame,
    scenario_titles: dict[str, str] | None,
) -> pl.DataFrame:
    """Return values with a display-only scenario label column."""
    scenarios = values["scenario"].unique(maintain_order=True).to_list()
    labels = _scenario_label_map(scenarios, scenario_titles)
    label_frame = pl.DataFrame(
        {
            "scenario": list(labels),
            "scenario_label": list(labels.values()),
        }
    )
    return values.join(label_frame, on="scenario", how="left")


def _scenario_label_map(
    scenarios: list[str],
    scenario_titles: dict[str, str] | None,
) -> dict[str, str]:
    """Return unique display labels for scenario names."""
    raw_labels = {
        scenario: (scenario_titles or {}).get(scenario, scenario)
        for scenario in scenarios
    }
    duplicated_labels = {
        label
        for label in raw_labels.values()
        if list(raw_labels.values()).count(label) > 1
    }
    return {
        scenario: (
            f"{label} ({scenario})"
            if label in duplicated_labels
            else label
        )
        for scenario, label in raw_labels.items()
    }


def _dimension_label(value: object) -> str:
    """Return a readable dimension value label for plots."""
    return str(value).replace("_", " ").replace("/", "<br>")


def _scenario_color(index: int) -> str:
    """Return a stable color for one scenario trace."""
    return SCENARIO_COLORS[index % len(SCENARIO_COLORS)]


def _transparent_color(color: str, alpha: float) -> str:
    """Return a transparent CSS color from a hex color."""
    if not color.startswith("#") or len(color) != 7:
        return color
    red = int(color[1:3], 16)
    green = int(color[3:5], 16)
    blue = int(color[5:7], 16)
    return f"rgba({red},{green},{blue},{alpha})"


def _ylorrd_color_scale() -> list[list[float | str]]:
    """Return the positive-value map color scale used in report maps."""
    return [
        [0.0, "#ffffcc"],
        [0.125, "#ffeda0"],
        [0.25, "#fed976"],
        [0.375, "#feb24c"],
        [0.5, "#fd8d3c"],
        [0.625, "#fc4e2a"],
        [0.75, "#e31a1c"],
        [0.875, "#bd0026"],
        [1.0, "#800026"],
    ]


def _blue_white_red_color_scale() -> list[list[float | str]]:
    """Return a diverging color scale for positive and negative gaps."""
    return [
        [0.0, "#2166AC"],
        [0.5, "#FFFFFF"],
        [1.0, "#B2182B"],
    ]


def _symmetric_range(
    values: pl.DataFrame,
    metric: str,
    center: float | None,
) -> tuple[float, float] | None:
    """Return a color range symmetric around the requested center."""
    if center is None or metric not in values.columns or values.is_empty():
        return None
    metric_values = values[metric].drop_nulls()
    if metric_values.is_empty():
        return (center - 1.0, center + 1.0)
    lower = float(metric_values.min())
    upper = float(metric_values.max())
    span = max(abs(lower - center), abs(upper - center), 1e-12)
    return (center - span, center + span)


def _apply_vertical_bar_layout(
    fig: go.Figure,
    *,
    labels: list[str],
    title: str,
    width: int,
    height: int,
    yaxis_title: str,
    showlegend: bool,
    left_margin: int | None = None,
) -> None:
    """Apply a consistent layout for vertical result bar charts."""
    figure_width = max(width, 220 + 130 * len(labels))
    if left_margin is None:
        left_margin = 70
    apply_report_layout(fig, title=title)
    fig.update_layout(
        width=figure_width,
        height=height,
        barmode="group",
        bargap=0.22,
        bargroupgap=0.16,
        xaxis_title=None,
        yaxis_title=yaxis_title,
        showlegend=showlegend,
        margin={"l": left_margin, "r": 45, "t": 95, "b": 125},
        legend={
            "orientation": "h",
            "yanchor": "bottom",
            "y": 1.03,
            "xanchor": "left",
            "x": 0.0,
        },
    )
    fig.update_xaxes(automargin=True)
    fig.update_yaxes(automargin=True)
