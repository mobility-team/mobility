"""Report-ready transport-zone map."""

from __future__ import annotations

import os
import math
from pathlib import Path
from typing import Any

import geopandas as gpd
import mapclassify
import matplotlib
import matplotlib.colors as mcolors
import matplotlib.ticker as mticker

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
import polars as pl
import plotly.express as px
import plotly.graph_objects as go
from matplotlib.patches import Patch
from matplotlib import patheffects

from .theme import MOBILITY_COLORS, apply_report_layout


_LABEL_LON_OFFSET_RATIO = 0.003
_LABEL_LAT_OFFSET_RATIO = 0.004
_SVG_LABEL_OFFSET_POINTS = 2
_INTERNAL_CRS = "EPSG:3035"
_PLOTLY_CRS = "EPSG:4326"
_FACET_MIN_WIDTH = 620
_FACET_MIN_HEIGHT = 620
_EUROPE_EQUAL_AREA_GEO = {
    "projection_type": "azimuthal equal area",
    "projection_rotation_lon": 10,
    "projection_rotation_lat": 52,
}


class TransportZoneMaps:
    """Build several report maps from the same transport-zone layer."""

    def __init__(
        self,
        transport_zones: Any,
        population: Any | None = None,
        max_labels: int = 30,
        simplify_tolerance: float | None = 50.0,
    ) -> None:
        self._transport_zones = transport_zones
        self._map_data = _ZoneMapData(
            transport_zones=transport_zones,
            population=population,
            max_labels=max_labels,
            simplify_tolerance=simplify_tolerance,
        )

    def transport_zones(
        self,
        save_to_file: bool = False,
        labels: bool = True,
        title: str | None = None,
        width: int = 1200,
        height: int = 850,
    ) -> go.Figure:
        """Create a report map of inner and outer transport zones."""
        zones = self._map_data.zones.copy()
        zones["zone_type"] = zones["is_inner_zone"].map(
            {True: "Inner zone", False: "Outer zone"}
        )

        return _zone_choropleth_map(
            map_data=self._map_data,
            zones=zones,
            color_column="zone_type",
            save_name="transport-zones-map",
            save_to_file=save_to_file,
            labels=labels,
            width=width,
            height=height,
            frame_title=title,
            discrete_colors={
                "Inner zone": MOBILITY_COLORS["inner_zone"],
                "Outer zone": MOBILITY_COLORS["outer_zone"],
            },
        )

    def population_density(
        self,
        population: Any | None = None,
        save_to_file: bool = False,
        labels: bool = True,
        width: int = 1200,
        height: int = 850,
    ) -> go.Figure:
        """Create a report map of population density by transport zone."""
        if population is None:
            population = self._map_data.population
        if population is None:
            raise ValueError(
                "population_density needs a Population asset. Pass it to "
                "TransportZoneMaps or to population_density()."
            )

        zones = self._map_data.zones.copy()
        population_by_zone = _population_by_zone(population).rename(
            columns={"_population": "population"}
        )
        zones = zones.merge(population_by_zone, on="transport_zone_id", how="left")
        zones["population"] = zones["population"].fillna(0.0)
        zones["area_km2"] = _area_km2(zones)
        zones["population_density"] = zones["population"] / zones["area_km2"]

        return _zone_choropleth_map(
            map_data=self._map_data,
            zones=zones,
            color_column="population_density",
            save_name="population-density-map",
            save_to_file=save_to_file,
            labels=labels,
            width=width,
            height=height,
            hover_columns=["population"],
            legend_label="Population/km2",
        )

    def opportunity_density(
        self,
        activity: Any,
        model_run: Any,
        save_to_file: bool = False,
        labels: bool = True,
        width: int = 1200,
        height: int = 850,
    ) -> go.Figure:
        """Create a report map of opportunity density by transport zone."""
        if not getattr(activity, "has_opportunities", True):
            raise ValueError(f"Activity `{activity.name}` has no destination opportunities.")

        zones = self._map_data.zones.copy()
        opportunities = _model_opportunities_by_zone(
            model_run,
            activity_name=activity.name,
        )
        value_column = "opportunity_capacity"
        density_column = "opportunity_capacity_density"

        zones = zones.merge(
            opportunities[["transport_zone_id", value_column]],
            on="transport_zone_id",
            how="left",
        )
        zones[value_column] = zones[value_column].fillna(0.0)
        zones["area_km2"] = _area_km2(zones)
        zones[density_column] = zones[value_column] / zones["area_km2"]

        return _zone_choropleth_map(
            map_data=self._map_data,
            zones=zones,
            color_column=density_column,
            save_name=f"{activity.name}-opportunity-density-map",
            save_to_file=save_to_file,
            labels=labels,
            width=width,
            height=height,
            hover_columns=[value_column],
            legend_label="Opportunity hours/km2",
        )

    def metric(
        self,
        values: Any,
        value_column: str,
        save_name: str,
        id_column: str = "transport_zone_id",
        save_to_file: bool = False,
        output_path: str | Path | None = None,
        inner_zones_only: bool = False,
        labels: bool = True,
        width: int = 1200,
        height: int = 850,
        hover_columns: list[str] | None = None,
        legend_label: str | None = None,
        frame_title: str | None = None,
        classify: bool = True,
        color_continuous_scale: Any | None = None,
        color_continuous_midpoint: float | None = None,
        range_color: tuple[float, float] | None = None,
        colorbar_tickformat: str | None = None,
    ) -> go.Figure:
        """Create a report map for one transport-zone metric table."""
        zones = self._map_data.zones.copy()
        value_frame = _metric_values_frame(
            values=values,
            id_column=id_column,
            value_column=value_column,
        )
        value_frame = _align_metric_zone_ids(value_frame, zones)
        zones = zones.merge(value_frame, on="transport_zone_id", how="left")
        if inner_zones_only:
            zones = zones[zones["is_inner_zone"]].copy()

        return _zone_choropleth_map(
            map_data=self._map_data,
            zones=zones,
            color_column=value_column,
            save_name=save_name,
            save_to_file=save_to_file,
            labels=labels,
            width=width,
            height=height,
            hover_columns=hover_columns,
            legend_label=legend_label,
            frame_title=frame_title,
            classify=classify,
            color_continuous_scale=color_continuous_scale,
            color_continuous_midpoint=color_continuous_midpoint,
            range_color=range_color,
            colorbar_tickformat=colorbar_tickformat,
            output_path=Path(output_path) if output_path is not None else None,
        )

    def metric_facets(
        self,
        values: Any,
        value_column: str,
        facet_column: str,
        save_name: str,
        id_column: str = "transport_zone_id",
        labels: bool = True,
        width: int = 1200,
        height: int = 850,
        hover_columns: list[str] | None = None,
        legend_label: str | None = None,
        frame_title: str | None = None,
        classify: bool = False,
        color_continuous_scale: Any | None = None,
        color_continuous_midpoint: float | None = None,
        range_color: tuple[float, float] | None = None,
        colorbar_tickformat: str | None = None,
    ) -> go.Figure:
        """Create report maps for one metric, split by a facet column."""
        value_frame = _metric_values_frame(
            values=values,
            id_column=id_column,
            value_column=value_column,
        )
        value_frame = _align_metric_zone_ids(value_frame, self._map_data.zones)
        if facet_column not in value_frame.columns:
            raise ValueError(f"Metric map values need a `{facet_column}` column.")

        facet_values = value_frame[facet_column].drop_duplicates().to_list()
        if not facet_values:
            raise ValueError("Metric map values need at least one facet value.")

        map_zones = _faceted_metric_zones(
            map_data=self._map_data,
            value_frame=value_frame,
            facet_column=facet_column,
        )

        plot_color_column = value_column
        plot_discrete_colors = None
        if classify:
            map_zones, plot_color_column, plot_discrete_colors = _add_head_tail_classes(
                map_zones,
                value_column,
            )

        hover_data = {"transport_zone_id": True, value_column: True}
        hover_data[facet_column] = False
        for column in hover_columns or []:
            hover_data[column] = True

        hover_name = None
        if "local_admin_unit_name" in map_zones.columns:
            hover_name = "local_admin_unit_name"
            hover_data["local_admin_unit_name"] = False
        elif "local_admin_unit_id" in map_zones.columns:
            hover_data["local_admin_unit_id"] = True

        facet_argument = facet_column if len(facet_values) > 1 else None
        figure_kwargs = {
            "data_frame": map_zones.drop(columns="geometry"),
            "geojson": self._map_data.plotly_zones.__geo_interface__,
            "locations": "transport_zone_id",
            "featureidkey": "properties.transport_zone_id",
            "color": plot_color_column,
            "facet_col": facet_argument,
            "facet_col_wrap": 2 if len(facet_values) > 2 else 0,
            "hover_name": hover_name,
            "hover_data": hover_data,
            "labels": {
                value_column: legend_label or value_column,
                plot_color_column: legend_label or value_column,
            },
        }
        if plot_discrete_colors is not None:
            figure_kwargs["color_discrete_map"] = plot_discrete_colors
            figure_kwargs["category_orders"] = {plot_color_column: list(plot_discrete_colors)}
        else:
            if color_continuous_scale is not None:
                figure_kwargs["color_continuous_scale"] = color_continuous_scale
            if color_continuous_midpoint is not None:
                figure_kwargs["color_continuous_midpoint"] = color_continuous_midpoint
            if range_color is not None:
                figure_kwargs["range_color"] = range_color

        fig = px.choropleth(**figure_kwargs)
        if plot_discrete_colors is None and colorbar_tickformat is not None:
            fig.update_coloraxes(colorbar_tickformat=colorbar_tickformat)
        fig.update_traces(
            marker_line_color=MOBILITY_COLORS["zone_border"],
            marker_line_width=0.5,
        )
        _apply_plotly_geo_layout(fig)
        fig.update_layout(
            width=max(width, _FACET_MIN_WIDTH * min(len(facet_values), 2)),
            height=max(height, _FACET_MIN_HEIGHT * math.ceil(len(facet_values) / 2)),
        )
        apply_report_layout(fig, title=frame_title)
        fig.for_each_annotation(
            lambda annotation: annotation.update(
                text=annotation.text.replace(f"{facet_column}=", "")
            )
        )

        if labels:
            label_zones = _metric_value_zones(
                self._map_data.plotly_zones,
                value_frame=value_frame,
                value_column=value_column,
            )
            label_df = _filter_labels_to_zones(self._map_data.labels, label_zones)
            geo_ids = [
                geo_id
                for geo_id in dict.fromkeys(getattr(trace, "geo", "geo") for trace in fig.data)
                if geo_id is not None
            ]
            for geo_id in geo_ids:
                _add_label_traces(fig, label_df, map_zones.total_bounds, geo=geo_id)

        return fig

    def metric_grid(
        self,
        values: Any,
        value_column: str,
        row_column: str,
        column_column: str,
        save_name: str,
        id_column: str = "transport_zone_id",
        labels: bool = False,
        width: int = 1200,
        height: int = 850,
        hover_columns: list[str] | None = None,
        legend_label: str | None = None,
        frame_title: str | None = None,
        classify: bool = False,
        color_continuous_scale: Any | None = None,
        color_continuous_midpoint: float | None = None,
        range_color: tuple[float, float] | None = None,
        colorbar_tickformat: str | None = None,
    ) -> go.Figure:
        """Create report maps for one metric, split by row and column facets."""
        value_frame = _metric_values_frame(
            values=values,
            id_column=id_column,
            value_column=value_column,
        )
        value_frame = _align_metric_zone_ids(value_frame, self._map_data.zones)
        missing_columns = [
            column
            for column in [row_column, column_column]
            if column not in value_frame.columns
        ]
        if missing_columns:
            raise ValueError(
                "Metric grid values need these missing column(s): "
                + ", ".join(missing_columns)
                + "."
            )

        row_values = value_frame[row_column].drop_duplicates().to_list()
        column_values = value_frame[column_column].drop_duplicates().to_list()
        if not row_values or not column_values:
            raise ValueError("Metric grid values need at least one row and one column value.")

        grid_column = "__metric_grid_facet__"
        value_frame = value_frame.copy()
        value_frame[grid_column] = value_frame[column_column]
        map_zones = _faceted_metric_zones(
            map_data=self._map_data,
            value_frame=value_frame,
            facet_column=grid_column,
        )
        map_zones[row_column] = map_zones[row_column].astype(str)
        map_zones[grid_column] = map_zones[grid_column].astype(str)

        plot_color_column = value_column
        plot_discrete_colors = None
        if classify:
            map_zones, plot_color_column, plot_discrete_colors = _add_head_tail_classes(
                map_zones,
                value_column,
            )

        hover_data = {"transport_zone_id": True, value_column: True}
        hover_data[row_column] = False
        hover_data[grid_column] = False
        for column in hover_columns or []:
            hover_data[column] = True

        hover_name = None
        if "local_admin_unit_name" in map_zones.columns:
            hover_name = "local_admin_unit_name"
            hover_data["local_admin_unit_name"] = False
        elif "local_admin_unit_id" in map_zones.columns:
            hover_data["local_admin_unit_id"] = True

        figure_kwargs = {
            "data_frame": map_zones.drop(columns="geometry"),
            "geojson": self._map_data.plotly_zones.__geo_interface__,
            "locations": "transport_zone_id",
            "featureidkey": "properties.transport_zone_id",
            "color": plot_color_column,
            "facet_row": row_column,
            "facet_col": grid_column,
            "hover_name": hover_name,
            "hover_data": hover_data,
            "labels": {
                value_column: legend_label or value_column,
                plot_color_column: legend_label or value_column,
            },
            "category_orders": {
                row_column: [str(value) for value in row_values],
                grid_column: [str(value) for value in column_values],
            },
        }
        if plot_discrete_colors is not None:
            figure_kwargs["color_discrete_map"] = plot_discrete_colors
            figure_kwargs["category_orders"][plot_color_column] = list(plot_discrete_colors)
        else:
            if color_continuous_scale is not None:
                figure_kwargs["color_continuous_scale"] = color_continuous_scale
            if color_continuous_midpoint is not None:
                figure_kwargs["color_continuous_midpoint"] = color_continuous_midpoint
            if range_color is not None:
                figure_kwargs["range_color"] = range_color

        fig = px.choropleth(**figure_kwargs)
        if plot_discrete_colors is None and colorbar_tickformat is not None:
            fig.update_coloraxes(colorbar_tickformat=colorbar_tickformat)
        fig.update_traces(
            marker_line_color=MOBILITY_COLORS["zone_border"],
            marker_line_width=0.5,
        )
        _apply_plotly_geo_layout(fig)
        fig.update_layout(
            width=max(width, _FACET_MIN_WIDTH * len(column_values)),
            height=max(height, _FACET_MIN_HEIGHT * len(row_values)),
        )
        apply_report_layout(fig, title=frame_title)
        fig.for_each_annotation(
            lambda annotation: annotation.update(
                text=annotation.text.replace(f"{row_column}=", "").replace(f"{grid_column}=", "")
            )
        )

        if labels:
            label_zones = _metric_value_zones(
                self._map_data.plotly_zones,
                value_frame=value_frame,
                value_column=value_column,
            )
            label_df = _filter_labels_to_zones(self._map_data.labels, label_zones)
            geo_ids = [
                geo_id
                for geo_id in dict.fromkeys(getattr(trace, "geo", "geo") for trace in fig.data)
                if geo_id is not None
            ]
            for geo_id in geo_ids:
                _add_label_traces(fig, label_df, map_zones.total_bounds, geo=geo_id)

        return fig

    def metric_flows(
        self,
        values: Any,
        value_column: str,
        origin_column: str,
        destination_column: str,
        save_name: str,
        facet_column: str = "scenario",
        labels: bool = False,
        width: int = 1200,
        height: int = 850,
        hover_columns: list[str] | None = None,
        legend_label: str | None = None,
        frame_title: str | None = None,
        n_largest: int | None = 100,
        min_value: float | None = None,
        min_share: float | None = None,
        max_line_width: float = 8.0,
        min_line_width: float = 0.1,
    ) -> go.Figure:
        """Create report maps for OD-like flows with proportional line widths."""
        flow_frame = _metric_flow_values_frame(
            values=values,
            origin_column=origin_column,
            destination_column=destination_column,
            value_column=value_column,
        )
        flow_frame = _align_flow_zone_ids(
            flow_frame,
            self._map_data.zones,
            origin_column=origin_column,
            destination_column=destination_column,
        )
        if facet_column not in flow_frame.columns:
            raise ValueError(f"Metric flow values need a `{facet_column}` column.")

        facet_values = flow_frame[facet_column].drop_duplicates().to_list()
        if not facet_values:
            raise ValueError("Metric flow values need at least one facet value.")

        plot_flows = _prepare_metric_flow_lines(
            flow_frame,
            map_data=self._map_data,
            value_column=value_column,
            origin_column=origin_column,
            destination_column=destination_column,
            facet_column=facet_column,
            n_largest=n_largest,
            min_value=min_value,
            min_share=min_share,
            max_line_width=max_line_width,
            min_line_width=min_line_width,
        )
        if plot_flows.empty:
            raise ValueError("No positive metric flows are available to plot after filtering.")

        base_rows = self._map_data.plotly_zones[["transport_zone_id"]].merge(
            pd.DataFrame({facet_column: facet_values}),
            how="cross",
        )
        base_rows["__flow_base__"] = "zone"
        fig = px.choropleth(
            base_rows,
            geojson=self._map_data.plotly_zones.__geo_interface__,
            locations="transport_zone_id",
            featureidkey="properties.transport_zone_id",
            color="__flow_base__",
            facet_col=facet_column if len(facet_values) > 1 else None,
            facet_col_wrap=2 if len(facet_values) > 2 else 0,
            color_discrete_map={"zone": MOBILITY_COLORS["outer_zone"]},
            hover_data={"transport_zone_id": True, "__flow_base__": False},
            category_orders={facet_column: [str(value) for value in facet_values]},
        )
        fig.update_traces(
            marker_line_color=MOBILITY_COLORS["zone_border"],
            marker_line_width=0.5,
            showscale=False,
            showlegend=False,
        )

        geo_by_facet = _facet_geo_ids(fig, facet_values)
        flow_color = MOBILITY_COLORS.get("model", "#355CDE")
        for _, flow in plot_flows.iterrows():
            is_intrazonal = flow[origin_column] == flow[destination_column]
            hover_lines = [
                f"{origin_column}: {flow[origin_column]}",
                f"{destination_column}: {flow[destination_column]}",
                f"{legend_label or value_column}: {flow[value_column]:,.3g}",
            ]
            for column in hover_columns or []:
                if column in flow.index and pd.notna(flow[column]):
                    hover_lines.append(f"{column}: {flow[column]:,.3g}")
            if is_intrazonal:
                fig.add_trace(
                    go.Scattergeo(
                        lon=[flow["_origin_lon"]],
                        lat=[flow["_origin_lat"]],
                        mode="markers",
                        marker={
                            "size": float(flow["_line_width"]),
                            "color": flow_color,
                            "opacity": 0.35,
                        },
                        hoverinfo="text",
                        text="<br>".join(hover_lines),
                        showlegend=False,
                        geo=geo_by_facet[str(flow[facet_column])],
                    )
                )
                continue
            fig.add_trace(
                go.Scattergeo(
                    lon=[flow["_origin_lon"], flow["_destination_lon"]],
                    lat=[flow["_origin_lat"], flow["_destination_lat"]],
                    mode="lines",
                    line={"width": float(flow["_line_width"]), "color": flow_color},
                    opacity=0.35,
                    hoverinfo="text",
                    text="<br>".join(hover_lines),
                    showlegend=False,
                    geo=geo_by_facet[str(flow[facet_column])],
                )
            )
        _add_flow_width_legend(
            fig,
            plot_flows,
            value_column=value_column,
            legend_label=legend_label or value_column,
            max_line_width=max_line_width,
            flow_color=flow_color,
            geo=next(iter(geo_by_facet.values())),
        )

        _apply_plotly_geo_layout(fig)
        fig.update_layout(
            width=max(width, _FACET_MIN_WIDTH * min(len(facet_values), 2)),
            height=max(height, _FACET_MIN_HEIGHT * math.ceil(len(facet_values) / 2)),
        )
        apply_report_layout(fig, title=frame_title)
        fig.for_each_annotation(
            lambda annotation: annotation.update(
                text=annotation.text.replace(f"{facet_column}=", "")
            )
        )
        fig.update_layout(
            showlegend=True,
            legend={
                "title": {"text": legend_label or value_column},
                "orientation": "v",
                "x": 1.01,
                "y": 0.5,
                "xanchor": "left",
                "yanchor": "middle",
                "bgcolor": "rgba(255,255,255,0.85)",
            },
        )

        if labels:
            label_df = _filter_labels_to_zones(self._map_data.labels, self._map_data.plotly_zones)
            geo_ids = [
                geo_id
                for geo_id in dict.fromkeys(getattr(trace, "geo", "geo") for trace in fig.data)
                if geo_id is not None
            ]
            for geo_id in geo_ids:
                _add_label_traces(fig, label_df, self._map_data.plotly_zones.total_bounds, geo=geo_id)

        return fig


def transport_zones_map(
    transport_zones: Any,
    population: Any | None = None,
    save_to_file: bool = False,
    labels: bool = True,
    max_labels: int = 30,
    title: str | None = None,
    width: int = 1200,
    height: int = 850,
    simplify_tolerance: float | None = 50.0,
) -> go.Figure:
    """Create a report map of inner and outer transport zones.

    Parameters
    ----------
    transport_zones:
        A ``TransportZones`` asset or a GeoDataFrame with transport zones.
    population:
        Optional ``Population`` asset. When provided, city labels are ranked by
        estimated population from ``population_groups.parquet``.
    save_to_file:
        If ``True``, save the map as an SVG in ``MOBILITY_PROJECT_DATA_FOLDER``.
        The file name includes the transport-zone asset hash.
    labels:
        If ``True``, add a small set of prominent city labels.

    Returns
    -------
    plotly.graph_objects.Figure
        Plotly figure that can still be adjusted by the caller.
    """
    return TransportZoneMaps(
        transport_zones=transport_zones,
        population=population,
        max_labels=max_labels,
        simplify_tolerance=simplify_tolerance,
    ).transport_zones(
        save_to_file=save_to_file,
        labels=labels,
        title=title,
        width=width,
        height=height,
    )


def population_density_map(
    transport_zones: Any,
    population: Any,
    save_to_file: bool = False,
    labels: bool = True,
    max_labels: int = 30,
    width: int = 1200,
    height: int = 850,
    simplify_tolerance: float | None = 50.0,
) -> go.Figure:
    """Create a report map of population density by transport zone.

    The population asset should expose a ``population_groups`` parquet file
    through ``population.get()``.
    """
    return TransportZoneMaps(
        transport_zones=transport_zones,
        population=population,
        max_labels=max_labels,
        simplify_tolerance=simplify_tolerance,
    ).population_density(
        population=population,
        save_to_file=save_to_file,
        labels=labels,
        width=width,
        height=height,
    )


def opportunity_density_map(
    transport_zones: Any,
    activity: Any,
    model_run: Any,
    population: Any | None = None,
    save_to_file: bool = False,
    labels: bool = True,
    max_labels: int = 30,
    width: int = 1200,
    height: int = 850,
    simplify_tolerance: float | None = 50.0,
) -> go.Figure:
    """Create a report map of opportunity density by transport zone.

    The model run should expose its grouped opportunity table through
    ``model_run.cache_path["opportunities"]``.
    """
    return TransportZoneMaps(
        transport_zones=transport_zones,
        population=population,
        max_labels=max_labels,
        simplify_tolerance=simplify_tolerance,
    ).opportunity_density(
        activity=activity,
        model_run=model_run,
        save_to_file=save_to_file,
        labels=labels,
        width=width,
        height=height,
    )


class _ZoneMapData:
    """Cache shared map layers for one transport-zone asset."""

    def __init__(
        self,
        transport_zones: Any,
        population: Any | None,
        max_labels: int,
        simplify_tolerance: float | None,
    ) -> None:
        self.transport_zones = transport_zones
        self.population = population
        self.max_labels = max_labels
        self.simplify_tolerance = simplify_tolerance
        self._zones = None
        self._plotly_zones = None
        self._svg_zones = None
        self._labels = None

    @property
    def zones(self) -> gpd.GeoDataFrame:
        if self._zones is None:
            zones = _get_zones(self.transport_zones)
            self._zones = _add_local_admin_unit_names(zones, self.transport_zones)
        return self._zones

    @property
    def plotly_zones(self) -> gpd.GeoDataFrame:
        if self._plotly_zones is None:
            zones = self.zones.copy()
            if self.simplify_tolerance is not None and (
                zones.crs is None or zones.crs.is_projected
            ):
                zones["geometry"] = zones.geometry.simplify(self.simplify_tolerance)
            zones = zones.to_crs(_PLOTLY_CRS)
            self._plotly_zones = zones.copy()
        return self._plotly_zones

    @property
    def svg_zones(self) -> gpd.GeoDataFrame:
        if self._svg_zones is None:
            zones = self.zones.copy()
            if self.simplify_tolerance is not None and (
                zones.crs is None or zones.crs.is_projected
            ):
                zones["geometry"] = zones.geometry.simplify(self.simplify_tolerance)
            self._svg_zones = zones
        return self._svg_zones

    @property
    def labels(self) -> pd.DataFrame:
        if self._labels is None:
            self._labels = _select_city_labels(
                self.zones,
                population=self.population,
                max_labels=self.max_labels,
            )
        return self._labels

def _get_zones(transport_zones: Any) -> gpd.GeoDataFrame:
    if isinstance(transport_zones, gpd.GeoDataFrame):
        zones = transport_zones.copy()
    elif callable(getattr(transport_zones, "get", None)):
        zones = transport_zones.get()
        if not isinstance(zones, gpd.GeoDataFrame):
            raise TypeError("transport_zones.get() should return a GeoDataFrame.")
        zones = zones.copy()
    else:
        raise TypeError("transport_zones should be a TransportZones asset or a GeoDataFrame.")

    required_columns = {"transport_zone_id", "is_inner_zone", "geometry"}
    missing_columns = sorted(required_columns.difference(zones.columns))
    if missing_columns:
        raise ValueError(
            "Transport-zone map needs these missing column(s): "
            + ", ".join(missing_columns)
            + "."
        )
    if zones.crs is None:
        zones = zones.set_crs(_INTERNAL_CRS)
    elif zones.crs.to_epsg() != 3035:
        zones = zones.to_crs(_INTERNAL_CRS)
    return zones


def _add_local_admin_unit_names(
    zones: gpd.GeoDataFrame,
    transport_zones: Any,
) -> gpd.GeoDataFrame:
    if (
        "local_admin_unit_name" in zones.columns
        or "local_admin_unit_id" not in zones.columns
        or not callable(getattr(getattr(transport_zones, "study_area", None), "get", None))
    ):
        return zones

    study_area = transport_zones.study_area.get()
    return zones.merge(
        study_area[["local_admin_unit_id", "local_admin_unit_name"]].drop_duplicates(),
        on="local_admin_unit_id",
        how="left",
    )


def _zone_choropleth_map(
    map_data: _ZoneMapData,
    zones: gpd.GeoDataFrame,
    color_column: str,
    save_name: str,
    save_to_file: bool,
    labels: bool,
    width: int,
    height: int,
    discrete_colors: dict[str, str] | None = None,
    hover_columns: list[str] | None = None,
    legend_label: str | None = None,
    frame_title: str | None = None,
    classify: bool = True,
    color_continuous_scale: Any | None = None,
    color_continuous_midpoint: float | None = None,
    range_color: tuple[float, float] | None = None,
    colorbar_tickformat: str | None = None,
    output_path: Path | None = None,
) -> go.Figure:
    map_zones = map_data.plotly_zones[["transport_zone_id", "geometry"]].merge(
        zones.drop(columns="geometry"),
        on="transport_zone_id",
        how="inner",
    )

    plot_color_column = color_column
    plot_discrete_colors = discrete_colors
    save_zones = zones
    if discrete_colors is None and classify:
        map_zones, plot_color_column, plot_discrete_colors = _add_head_tail_classes(
            map_zones,
            color_column,
        )
        save_zones = zones.merge(
            map_zones[["transport_zone_id", plot_color_column]],
            on="transport_zone_id",
            how="left",
        )

    hover_data = {"transport_zone_id": True, color_column: True}
    for column in hover_columns or []:
        hover_data[column] = True

    hover_name = None
    if "local_admin_unit_name" in map_zones.columns:
        hover_name = "local_admin_unit_name"
        hover_data["local_admin_unit_name"] = False
    elif "local_admin_unit_id" in map_zones.columns:
        hover_data["local_admin_unit_id"] = True

    figure_kwargs = {
        "data_frame": map_zones.drop(columns="geometry"),
        "geojson": map_zones.__geo_interface__,
        "locations": "transport_zone_id",
        "featureidkey": "properties.transport_zone_id",
        "color": plot_color_column,
        "hover_name": hover_name,
        "hover_data": hover_data,
        "labels": {
            color_column: legend_label or color_column,
            plot_color_column: legend_label or color_column,
        },
    }
    if plot_discrete_colors is not None:
        figure_kwargs["color_discrete_map"] = plot_discrete_colors
        figure_kwargs["category_orders"] = {plot_color_column: list(plot_discrete_colors)}
    else:
        if color_continuous_scale is not None:
            figure_kwargs["color_continuous_scale"] = color_continuous_scale
        if color_continuous_midpoint is not None:
            figure_kwargs["color_continuous_midpoint"] = color_continuous_midpoint
        if range_color is not None:
            figure_kwargs["range_color"] = range_color

    fig = px.choropleth(**figure_kwargs)
    if plot_discrete_colors is None and colorbar_tickformat is not None:
        fig.update_coloraxes(colorbar_tickformat=colorbar_tickformat)
    fig.update_traces(
        marker_line_color=MOBILITY_COLORS["zone_border"],
        marker_line_width=0.5,
    )
    _apply_plotly_geo_layout(fig)
    fig.update_layout(width=width, height=height)
    apply_report_layout(fig, title=frame_title)

    label_df = None
    if labels:
        label_df = _filter_labels_to_zones(map_data.labels, map_zones)
        _add_label_traces(fig, label_df, map_zones.total_bounds)

    static_output_path = output_path
    if save_to_file:
        inputs_hash = getattr(map_data.transport_zones, "inputs_hash", None)
        if not inputs_hash:
            raise ValueError(
                "save_to_file=True needs a TransportZones file asset with an input hash."
            )
        project_folder = os.environ.get("MOBILITY_PROJECT_DATA_FOLDER")
        if project_folder is None:
            raise ValueError(
                "save_to_file=True needs MOBILITY_PROJECT_DATA_FOLDER to be defined."
            )
        static_output_path = Path(project_folder) / f"{inputs_hash}-{save_name}.svg"

    if static_output_path is not None:
        _save_zone_choropleth_svg(
            map_data=map_data,
            zones=save_zones,
            output_path=static_output_path,
            color_column=plot_color_column,
            label_df=label_df,
            discrete_colors=plot_discrete_colors,
            legend_label=legend_label,
            frame_title=frame_title,
            color_continuous_scale=color_continuous_scale,
            color_continuous_midpoint=color_continuous_midpoint,
            range_color=range_color,
            colorbar_tickformat=colorbar_tickformat,
        )

    return fig


def _metric_values_frame(values: Any, id_column: str, value_column: str) -> pd.DataFrame:
    """Return a pandas metric table keyed by transport_zone_id."""
    if isinstance(values, pl.DataFrame):
        value_frame = values.to_pandas()
    elif isinstance(values, pd.DataFrame):
        value_frame = values.copy()
    else:
        raise TypeError("Metric map values should be a pandas or polars DataFrame.")

    missing_columns = [
        column
        for column in [id_column, value_column]
        if column not in value_frame.columns
    ]
    if missing_columns:
        raise ValueError(
            "Metric map values need these missing column(s): "
            + ", ".join(missing_columns)
            + "."
        )

    keep_columns = [id_column, value_column]
    for column in value_frame.columns:
        if column not in keep_columns:
            keep_columns.append(column)
    value_frame = value_frame[keep_columns].rename(columns={id_column: "transport_zone_id"})
    return value_frame


def _metric_value_zones(
    zones: gpd.GeoDataFrame,
    *,
    value_frame: pd.DataFrame,
    value_column: str,
) -> gpd.GeoDataFrame:
    """Return zones joined with metric rows that have a value."""
    if value_column not in value_frame.columns:
        return zones
    valued_rows = value_frame[value_frame[value_column].notna()]
    return zones.merge(
        valued_rows[["transport_zone_id"]].drop_duplicates(),
        on="transport_zone_id",
        how="inner",
    )


def _metric_flow_values_frame(
    values: Any,
    origin_column: str,
    destination_column: str,
    value_column: str,
) -> pd.DataFrame:
    """Return a pandas metric table keyed by origin and destination zone ids."""
    if isinstance(values, pl.DataFrame):
        flow_frame = values.to_pandas()
    elif isinstance(values, pd.DataFrame):
        flow_frame = values.copy()
    else:
        raise TypeError("Metric flow values should be a pandas or polars DataFrame.")

    missing_columns = [
        column
        for column in [origin_column, destination_column, value_column]
        if column not in flow_frame.columns
    ]
    if missing_columns:
        raise ValueError(
            "Metric flow values need these missing column(s): "
            + ", ".join(missing_columns)
            + "."
        )

    keep_columns = [origin_column, destination_column, value_column]
    for column in flow_frame.columns:
        if column not in keep_columns:
            keep_columns.append(column)
    return flow_frame[keep_columns]


def _align_metric_zone_ids(
    value_frame: pd.DataFrame,
    zone_frame: pd.DataFrame,
) -> pd.DataFrame:
    """Return metric values with ids matching the map zone id dtype."""
    zone_ids = zone_frame["transport_zone_id"]
    value_ids = value_frame["transport_zone_id"]
    if value_ids.dtype == zone_ids.dtype:
        return value_frame

    aligned_frame = value_frame.copy()
    if pd.api.types.is_integer_dtype(zone_ids.dtype):
        aligned_frame["transport_zone_id"] = pd.to_numeric(
            aligned_frame["transport_zone_id"],
            errors="raise",
        ).astype(zone_ids.dtype)
        return aligned_frame

    inferred_zone_type = pd.api.types.infer_dtype(zone_ids.dropna(), skipna=True)
    if inferred_zone_type in {"string", "unicode"}:
        aligned_frame["transport_zone_id"] = aligned_frame["transport_zone_id"].astype(str)
        return aligned_frame

    aligned_frame["transport_zone_id"] = aligned_frame["transport_zone_id"].astype(
        zone_ids.dtype
    )
    return aligned_frame


def _align_flow_zone_ids(
    flow_frame: pd.DataFrame,
    zone_frame: pd.DataFrame,
    *,
    origin_column: str,
    destination_column: str,
) -> pd.DataFrame:
    """Return flow values with ids matching the map zone id dtype."""
    aligned_frame = flow_frame.copy()
    for column in [origin_column, destination_column]:
        if column in aligned_frame.columns:
            temp = aligned_frame.rename(columns={column: "transport_zone_id"})
            temp = _align_metric_zone_ids(temp, zone_frame)
            aligned_frame[column] = temp["transport_zone_id"]
    return aligned_frame


def _facet_geo_ids(fig: go.Figure, facet_values: list[Any]) -> dict[str, str]:
    """Return the Plotly geo subplot id used by each facet value."""
    geo_ids = [
        geo_id
        for geo_id in dict.fromkeys(getattr(trace, "geo", "geo") for trace in fig.data)
        if geo_id is not None
    ]
    return {
        str(facet_value): geo_ids[min(index, len(geo_ids) - 1)]
        for index, facet_value in enumerate(facet_values)
    }


def _apply_plotly_geo_layout(fig: go.Figure) -> None:
    """Use a Europe equal-area projection and fit each geo subplot to data."""
    fig.update_geos(
        fitbounds="locations",
        visible=False,
        **_EUROPE_EQUAL_AREA_GEO,
    )


def _prepare_metric_flow_lines(
    flow_frame: pd.DataFrame,
    *,
    map_data: _ZoneMapData,
    value_column: str,
    origin_column: str,
    destination_column: str,
    facet_column: str,
    n_largest: int | None,
    min_value: float | None,
    min_share: float | None,
    max_line_width: float,
    min_line_width: float,
) -> pd.DataFrame:
    """Filter flow values and attach centroid coordinates plus line widths."""
    if max_line_width <= 0.0:
        raise ValueError("max_line_width should be positive.")
    if min_line_width < 0.0:
        raise ValueError("min_line_width should be non-negative.")
    if n_largest is not None and n_largest <= 0:
        raise ValueError("n_largest should be positive when provided.")
    if min_share is not None and not 0.0 <= min_share <= 1.0:
        raise ValueError("min_share should be between 0 and 1 when provided.")

    flows = flow_frame.copy()
    flows[value_column] = pd.to_numeric(flows[value_column], errors="coerce")
    flows = flows[flows[value_column].notna()]
    flows = flows[flows[value_column].map(math.isfinite)]
    flows = flows[flows[value_column] > 0.0]
    if min_value is not None:
        flows = flows[flows[value_column] >= float(min_value)]
    if min_share is not None:
        totals = flows.groupby(facet_column)[value_column].transform("sum")
        flows = flows[(flows[value_column] / totals.where(totals != 0.0)) >= float(min_share)]
    if n_largest is not None:
        flows = (
            flows
            .sort_values([facet_column, value_column], ascending=[True, False])
            .groupby(facet_column, group_keys=False)
            .head(n_largest)
        )
    if flows.empty:
        return flows

    max_value = float(flows[value_column].max())
    if not math.isfinite(max_value) or max_value <= 0.0:
        return flows.iloc[0:0]
    flows["_line_width"] = max_line_width * flows[value_column] / max_value
    flows = flows[flows["_line_width"] >= min_line_width]
    if flows.empty:
        return flows

    centroids = map_data.zones[["transport_zone_id", "geometry"]].copy()
    centroids["geometry"] = centroids.geometry.centroid
    centroids = centroids.to_crs(_PLOTLY_CRS)
    centroid_frame = pd.DataFrame(
        {
            "transport_zone_id": centroids["transport_zone_id"],
            "_lon": centroids.geometry.x,
            "_lat": centroids.geometry.y,
        }
    )
    flows = flows.merge(
        centroid_frame.rename(
            columns={
                "transport_zone_id": origin_column,
                "_lon": "_origin_lon",
                "_lat": "_origin_lat",
            }
        ),
        on=origin_column,
        how="inner",
    )
    flows = flows.merge(
        centroid_frame.rename(
            columns={
                "transport_zone_id": destination_column,
                "_lon": "_destination_lon",
                "_lat": "_destination_lat",
            }
        ),
        on=destination_column,
        how="inner",
    )
    return flows


def _add_flow_width_legend(
    fig: go.Figure,
    flows: pd.DataFrame,
    *,
    value_column: str,
    legend_label: str,
    max_line_width: float,
    flow_color: str,
    geo: str,
) -> None:
    """Add legend-only traces that explain proportional flow width."""
    max_value = float(flows[value_column].max())
    if not math.isfinite(max_value) or max_value <= 0.0:
        return

    legend_values = []
    for fraction in [0.25, 0.5, 1.0]:
        value = max_value * fraction
        if value not in legend_values:
            legend_values.append(value)

    for value in legend_values:
        size = max_line_width * value / max_value
        fig.add_trace(
            go.Scattergeo(
                lon=[None, None],
                lat=[None, None],
                mode="lines+markers",
                line={"width": size, "color": flow_color},
                marker={"size": size, "color": flow_color, "opacity": 0.35},
                opacity=0.35,
                name=_format_significant(value),
                legendgroup="flow_width",
                showlegend=True,
                visible="legendonly",
                hoverinfo="skip",
                geo=geo,
            )
        )


def _faceted_metric_zones(
    map_data: _ZoneMapData,
    value_frame: pd.DataFrame,
    facet_column: str,
) -> gpd.GeoDataFrame:
    """Return one map row per zone and facet value."""
    zone_columns = [
        column
        for column in map_data.plotly_zones.columns
        if column != "geometry"
    ]
    zone_frame = map_data.plotly_zones[zone_columns].copy()
    facets = pd.DataFrame(
        {facet_column: value_frame[facet_column].drop_duplicates().to_list()}
    )
    map_rows = zone_frame.merge(facets, how="cross")
    map_rows = map_rows.merge(
        value_frame,
        on=["transport_zone_id", facet_column],
        how="left",
    )
    return gpd.GeoDataFrame(
        map_data.plotly_zones[["transport_zone_id", "geometry"]].merge(
            map_rows,
            on="transport_zone_id",
            how="inner",
        ),
        geometry="geometry",
        crs=map_data.plotly_zones.crs,
    )


def _add_head_tail_classes(
    zones: gpd.GeoDataFrame,
    color_column: str,
) -> tuple[gpd.GeoDataFrame, str, dict[str, str]]:
    values = zones[color_column].fillna(0.0).to_numpy()
    classifier = mapclassify.HeadTailBreaks(values)
    bins = classifier.bins.tolist()
    if len(bins) > 5:
        bins = bins[:4] + [max(values)]
    class_column = f"{color_column}_class"
    lower_bound = min(values)
    labels = []
    for upper_bound in bins:
        labels.append(
            f"{_format_significant(lower_bound)} - {_format_significant(upper_bound)}"
        )
        lower_bound = upper_bound

    classed_zones = zones.copy()
    classed_zones[class_column] = [
        labels[min(i, len(labels) - 1)]
        for i in classifier.yb
    ]
    palette = [
        _plotly_color_to_hex(color)
        for color in px.colors.sample_colorscale(
            "YlOrRd",
            [
                i / max(len(labels) - 1, 1)
                for i in range(len(labels))
            ],
        )
    ]
    return classed_zones, class_column, dict(zip(labels, palette))


def _plotly_color_to_hex(color: str) -> str:
    if color.startswith("rgb("):
        rgb_values = [
            int(value.strip())
            for value in color.removeprefix("rgb(").removesuffix(")").split(",")
        ]
        return mcolors.to_hex([value / 255.0 for value in rgb_values])
    return mcolors.to_hex(color)


def _format_significant(value: float) -> str:
    if value == 0:
        return "0"

    digits = 2 - math.floor(math.log10(abs(value))) - 1
    rounded_value = round(value, digits)
    decimals = max(digits, 0)
    formatted_value = f"{rounded_value:,.{decimals}f}"
    if "." in formatted_value:
        formatted_value = formatted_value.rstrip("0").rstrip(".")
    return formatted_value


def _add_label_traces(
    fig: go.Figure,
    label_df: pd.DataFrame | None,
    bounds: tuple[float, float, float, float],
    geo: str | None = None,
) -> None:
    if label_df is None or label_df.empty:
        return

    min_lon, min_lat, max_lon, max_lat = bounds
    lon_offset = (max_lon - min_lon) * _LABEL_LON_OFFSET_RATIO
    lat_offset = (max_lat - min_lat) * _LABEL_LAT_OFFSET_RATIO
    label_lons = (label_df["lon"] + lon_offset).to_list()
    label_lats = (label_df["lat"] + lat_offset).to_list()

    fig.add_trace(
        go.Scattergeo(
            lon=label_df["lon"].to_list(),
            lat=label_df["lat"].to_list(),
            mode="markers",
            marker={
                "size": 3,
                "color": MOBILITY_COLORS["label"],
                "opacity": 0.75,
            },
            geo=geo,
            hoverinfo="skip",
            showlegend=False,
        )
    )
    for color in ["white", MOBILITY_COLORS["label"]]:
        fig.add_trace(
            go.Scattergeo(
                lon=label_lons,
                lat=label_lats,
                text=label_df["label"].to_list(),
                mode="text",
                textposition="middle left",
                textfont={
                    "size": label_df["font_size"].to_list(),
                    "color": color,
                },
                geo=geo,
                hoverinfo="skip",
                showlegend=False,
            )
        )


def _filter_labels_to_zones(
    label_df: pd.DataFrame,
    zones: gpd.GeoDataFrame,
) -> pd.DataFrame:
    """Keep labels whose area identifier is present in the plotted zones."""
    if label_df.empty or zones.empty:
        return label_df

    for id_column in ["local_admin_unit_id", "transport_zone_id"]:
        if id_column in label_df.columns and id_column in zones.columns:
            plotted_ids = set(zones[id_column].dropna().astype(str))
            return label_df.loc[
                label_df[id_column].astype(str).isin(plotted_ids)
            ].reset_index(drop=True)

    return label_df


def _save_zone_choropleth_svg(
    map_data: _ZoneMapData,
    zones: gpd.GeoDataFrame,
    output_path: Path,
    color_column: str,
    label_df: pd.DataFrame | None,
    discrete_colors: dict[str, str] | None,
    legend_label: str | None,
    frame_title: str | None = None,
    color_continuous_scale: Any | None = None,
    color_continuous_midpoint: float | None = None,
    range_color: tuple[float, float] | None = None,
    colorbar_tickformat: str | None = None,
) -> None:
    """Save the map as SVG without going through Plotly/Kaleido/Chrome."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    plot_zones = map_data.svg_zones[["transport_zone_id", "geometry"]].merge(
        zones.drop(columns="geometry"),
        on="transport_zone_id",
        how="inner",
    )
    if discrete_colors is None:
        zone_colors, color_map, color_norm = _continuous_zone_colors(
            plot_zones[color_column],
            color_continuous_scale=color_continuous_scale,
            color_continuous_midpoint=color_continuous_midpoint,
            range_color=range_color,
        )
        plot_zones["zone_color"] = zone_colors
    else:
        plot_zones["zone_color"] = plot_zones[color_column].map(discrete_colors)

    fig = plt.figure(figsize=(7.3, 5.9))
    ax = fig.add_axes([0.0, 0.18, 1.0, 0.82])
    plot_zones.plot(
        ax=ax,
        color=plot_zones["zone_color"],
        edgecolor=MOBILITY_COLORS["zone_border"],
        linewidth=0.4,
        zorder=1,
    )
    ax.set_axis_off()

    if frame_title is not None:
        fig.text(
            0.04,
            0.965,
            frame_title,
            ha="left",
            va="top",
            fontsize=12,
            color=MOBILITY_COLORS["label"],
        )

    if label_df is not None and not label_df.empty:
        label_points = gpd.GeoDataFrame(
            label_df[["label", "font_size"]],
            geometry=gpd.points_from_xy(label_df["lon"], label_df["lat"], crs="EPSG:4326"),
        )
        if zones.crs is not None:
            label_points = label_points.to_crs(zones.crs)
        label_points.plot(
            ax=ax,
            color=MOBILITY_COLORS["label"],
            markersize=4,
            alpha=0.75,
            zorder=9,
        )
        for _, row in label_points.iterrows():
            ax.annotate(
                row["label"],
                xy=(row.geometry.x, row.geometry.y),
                xytext=(_SVG_LABEL_OFFSET_POINTS, _SVG_LABEL_OFFSET_POINTS),
                textcoords="offset points",
                color=MOBILITY_COLORS["label"],
                fontsize=row["font_size"],
                ha="left",
                va="bottom",
                path_effects=[
                    patheffects.withStroke(linewidth=1.8, foreground="white")
                ],
                zorder=10,
            )

    if discrete_colors is not None:
        fig.legend(
            handles=[
                Patch(facecolor=color, label=label)
                for label, color in discrete_colors.items()
            ],
            loc="lower left",
            bbox_to_anchor=(0.04, 0.03),
            frameon=True,
            title=legend_label,
        )
    else:
        colorbar_axis = fig.add_axes([0.05, 0.065, 0.34, 0.025])
        scalar_map = matplotlib.cm.ScalarMappable(norm=color_norm, cmap=color_map)
        scalar_map.set_array([])
        colorbar = fig.colorbar(scalar_map, cax=colorbar_axis, orientation="horizontal")
        colorbar_axis.set_title(legend_label or color_column, fontsize=10, pad=5)
        if colorbar_tickformat is not None:
            _format_svg_colorbar_ticks(colorbar, colorbar_tickformat)
        colorbar.outline.set_linewidth(0.4)
    output_format = output_path.suffix.removeprefix(".") or "svg"
    fig.savefig(output_path, format=output_format)
    plt.close(fig)


def _format_svg_colorbar_ticks(colorbar, colorbar_tickformat: str) -> None:
    """Apply the small set of Plotly-like tick formats used by report maps."""
    if colorbar_tickformat.endswith("%"):
        decimals = 0
        if colorbar_tickformat.startswith(".") and colorbar_tickformat[1:-1].isdigit():
            decimals = int(colorbar_tickformat[1:-1])
        colorbar.ax.xaxis.set_major_formatter(
            mticker.PercentFormatter(xmax=1.0, decimals=decimals)
        )


def _continuous_zone_colors(
    values: pd.Series,
    color_continuous_scale: Any | None,
    color_continuous_midpoint: float | None,
    range_color: tuple[float, float] | None,
) -> tuple[list[str], mcolors.Colormap, mcolors.Normalize]:
    """Map continuous values to SVG fill colors."""
    scale = color_continuous_scale or [
        [0.0, "#ffffcc"],
        [1.0, "#800026"],
    ]
    if isinstance(scale, str):
        if scale not in plt.colormaps():
            raise ValueError(
                "Continuous SVG export needs a matplotlib colormap name or "
                "an explicit Plotly-style color scale."
            )
        cmap = plt.get_cmap(scale)
    else:
        cmap = mcolors.LinearSegmentedColormap.from_list(
            "mobility_continuous_map",
            [(float(stop), color) for stop, color in scale],
        )

    numeric_values = pd.to_numeric(values, errors="coerce")
    if range_color is None:
        valid_values = numeric_values.dropna()
        if valid_values.empty:
            lower, upper = 0.0, 1.0
        else:
            lower = float(valid_values.min())
            upper = float(valid_values.max())
    else:
        lower, upper = range_color

    if color_continuous_midpoint is not None:
        midpoint = float(color_continuous_midpoint)
        span = max(abs(float(upper) - midpoint), abs(float(lower) - midpoint), 1e-9)
        lower = midpoint - span
        upper = midpoint + span
    elif lower == upper:
        lower -= 0.5
        upper += 0.5

    normalized = ((numeric_values - lower) / (upper - lower)).clip(0.0, 1.0).fillna(0.5)
    norm = mcolors.Normalize(vmin=lower, vmax=upper)
    return [mcolors.to_hex(cmap(value)) for value in normalized], cmap, norm


def _model_opportunities_by_zone(
    model_run: Any,
    activity_name: str,
) -> pd.DataFrame:
    """Read model opportunity capacity from a group-day-trips run."""
    if not hasattr(model_run, "cache_path") or "opportunities" not in model_run.cache_path:
        raise TypeError(
            "Opportunity density needs a group-day-trips run with "
            "`cache_path['opportunities']`."
        )

    opportunities_path = Path(model_run.cache_path["opportunities"])
    if not opportunities_path.exists():
        if not callable(getattr(model_run, "get", None)):
            raise FileNotFoundError(
                "Could not find the model opportunities file: "
                f"{opportunities_path}."
            )
        model_run.get()

    if not opportunities_path.exists():
        raise FileNotFoundError(
            "Could not find the model opportunities file after running the model: "
            f"{opportunities_path}."
        )

    opportunities = pl.read_parquet(opportunities_path)
    columns = set(opportunities.columns)
    if "opportunity_capacity" not in columns:
        raise ValueError(
            "Model opportunity density needs an `opportunity_capacity` column. "
            "Pass the group-day-trips opportunities output, not raw activity opportunities."
        )
    if "to" not in columns:
        raise ValueError("Model opportunity density needs a `to` destination-zone column.")

    if "activity" in columns:
        opportunities = opportunities.filter(pl.col("activity").cast(pl.String()) == activity_name)

    return (
        opportunities
        .group_by("to")
        .agg(pl.col("opportunity_capacity").sum())
        .rename({"to": "transport_zone_id"})
        .to_pandas()
    )


def _area_km2(zones: gpd.GeoDataFrame) -> pd.Series:
    area_zones = zones
    if area_zones.crs is not None and not area_zones.crs.is_projected:
        area_zones = area_zones.to_crs("EPSG:3035")
    return area_zones.geometry.area / 1_000_000.0


def _select_city_labels(
    zones: gpd.GeoDataFrame,
    population: Any | None,
    max_labels: int,
) -> pd.DataFrame:
    if max_labels <= 0 or "local_admin_unit_id" not in zones.columns:
        return pd.DataFrame(
            columns=["local_admin_unit_id", "label", "lon", "lat", "font_size"]
        )

    label_name_column = (
        "local_admin_unit_name"
        if "local_admin_unit_name" in zones.columns
        else "local_admin_unit_id"
    )
    label_source = zones.copy()
    label_source["_label"] = label_source[label_name_column]
    label_source["_inner_score"] = label_source["is_inner_zone"].astype(int)
    label_source["_area"] = label_source.geometry.area
    label_source["_zone_count"] = 1
    if population is not None:
        label_source = label_source.merge(
            _population_by_zone(population),
            on="transport_zone_id",
            how="left",
        )
        label_source["_population"] = label_source["_population"].fillna(0.0)
    else:
        label_source["_population"] = 0.0

    city_labels = label_source.dissolve(
        by="local_admin_unit_id",
        aggfunc={
            "_label": "first",
            "_inner_score": "max",
            "_area": "sum",
            "_population": "sum",
            "_zone_count": "sum",
        },
    ).reset_index()

    if city_labels["_population"].sum() > 0.0:
        city_labels["_score"] = city_labels["_population"]
    else:
        city_labels["_score"] = (
            city_labels["_inner_score"] * max(city_labels["_zone_count"].max(), 1)
            + city_labels["_zone_count"]
        )
    city_labels = city_labels.sort_values(
        ["_score", "_zone_count", "_area", "_label"],
        ascending=[False, False, False, True],
        kind="mergesort",
    )

    label_points = city_labels.geometry.representative_point()
    city_labels["_x"] = label_points.x
    city_labels["_y"] = label_points.y

    minx, miny, maxx, maxy = city_labels.total_bounds
    min_distance = ((maxx - minx) ** 2 + (maxy - miny) ** 2) ** 0.5 / 24.0
    selected_rows = []
    selected_points = []
    for _, row in city_labels.iterrows():
        if len(selected_rows) >= max_labels:
            break
        point = row.geometry.representative_point()
        if any(point.distance(other) < min_distance for other in selected_points):
            continue
        selected_rows.append(row)
        selected_points.append(point)

    if not selected_rows:
        return pd.DataFrame(
            columns=["local_admin_unit_id", "label", "lon", "lat", "font_size"]
        )

    selected = gpd.GeoDataFrame(selected_rows, geometry="geometry", crs=zones.crs)
    selected_points = selected.geometry.representative_point()
    selected = selected.assign(_x=selected_points.x, _y=selected_points.y)
    selected_wgs84 = gpd.GeoDataFrame(
        selected[["local_admin_unit_id", "_label", "_score"]],
        geometry=gpd.points_from_xy(selected["_x"], selected["_y"], crs=zones.crs),
    )
    if selected_wgs84.crs is not None:
        selected_wgs84 = selected_wgs84.to_crs("EPSG:4326")
    coords = selected_wgs84.geometry.get_coordinates()

    result = pd.DataFrame(
        {
            "local_admin_unit_id": selected_wgs84["local_admin_unit_id"].to_list(),
            "label": selected_wgs84["_label"].to_list(),
            "lon": coords["x"].to_list(),
            "lat": coords["y"].to_list(),
        }
    )
    result["font_size"] = [9 if i == 0 else 8 if i < 5 else 7 for i in range(len(result))]
    return result


def _population_by_zone(population: Any) -> pd.DataFrame:
    population_groups = pd.read_parquet(population.get()["population_groups"])
    return (
        population_groups.groupby("transport_zone_id", as_index=False)["weight"]
        .sum()
        .rename(columns={"weight": "_population"})
    )
