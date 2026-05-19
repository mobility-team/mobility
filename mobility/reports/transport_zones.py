"""Report-ready transport-zone map."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import geopandas as gpd
import matplotlib.pyplot as plt
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from matplotlib.patches import Patch
from matplotlib import patheffects

from .theme import MOBILITY_COLORS, apply_report_layout


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

    if (
        "local_admin_unit_name" not in zones.columns
        and "local_admin_unit_id" in zones.columns
        and callable(getattr(getattr(transport_zones, "study_area", None), "get", None))
    ):
        study_area = transport_zones.study_area.get()
        zones = zones.merge(
            study_area[["local_admin_unit_id", "local_admin_unit_name"]].drop_duplicates(),
            on="local_admin_unit_id",
            how="left",
        )

    map_zones = zones.copy()
    if simplify_tolerance is not None and (
        map_zones.crs is None or map_zones.crs.is_projected
    ):
        map_zones["geometry"] = map_zones.geometry.simplify(simplify_tolerance)
    if map_zones.crs is not None:
        map_zones = map_zones.to_crs("EPSG:4326")
    map_zones = map_zones.copy()
    map_zones["zone_type"] = map_zones["is_inner_zone"].map(
        {True: "Inner zone", False: "Outer zone"}
    )

    hover_data = {
        "transport_zone_id": True,
        "zone_type": True,
    }
    hover_name = None
    if "local_admin_unit_name" in map_zones.columns:
        hover_name = "local_admin_unit_name"
        hover_data["local_admin_unit_name"] = False
    elif "local_admin_unit_id" in map_zones.columns:
        hover_data["local_admin_unit_id"] = True

    fig = px.choropleth(
        map_zones.drop(columns="geometry"),
        geojson=map_zones.__geo_interface__,
        locations="transport_zone_id",
        featureidkey="properties.transport_zone_id",
        color="zone_type",
        hover_name=hover_name,
        color_discrete_map={
            "Inner zone": MOBILITY_COLORS["inner_zone"],
            "Outer zone": MOBILITY_COLORS["outer_zone"],
        },
        category_orders={"zone_type": ["Inner zone", "Outer zone"]},
        hover_data=hover_data,
    )
    fig.update_traces(
        marker_line_color=MOBILITY_COLORS["zone_border"],
        marker_line_width=0.5,
    )
    fig.update_geos(fitbounds="locations", visible=False)
    fig.update_layout(width=width, height=height)
    apply_report_layout(fig)

    label_df = None
    if labels:
        label_df = _select_city_labels(zones, population=population, max_labels=max_labels)
        if not label_df.empty:
            fig.add_trace(
                go.Scattergeo(
                    lon=label_df["lon"].to_list(),
                    lat=label_df["lat"].to_list(),
                    text=label_df["label"].to_list(),
                    mode="text",
                    textfont={
                        "size": label_df["font_size"].to_list(),
                        "color": "white",
                    },
                    hoverinfo="skip",
                    showlegend=False,
                )
            )
            fig.add_trace(
                go.Scattergeo(
                    lon=label_df["lon"].to_list(),
                    lat=label_df["lat"].to_list(),
                    text=label_df["label"].to_list(),
                    mode="text",
                    textfont={
                        "size": label_df["font_size"].to_list(),
                        "color": MOBILITY_COLORS["label"],
                    },
                    hoverinfo="skip",
                    showlegend=False,
                )
            )

    if save_to_file:
        inputs_hash = getattr(transport_zones, "inputs_hash", None)
        if not inputs_hash:
            raise ValueError(
                "save_to_file=True needs a TransportZones file asset with an input hash."
            )
        project_folder = os.environ.get("MOBILITY_PROJECT_DATA_FOLDER")
        if project_folder is None:
            raise ValueError(
                "save_to_file=True needs MOBILITY_PROJECT_DATA_FOLDER to be defined."
            )
        output_path = Path(project_folder) / f"{inputs_hash}-transport-zones-map.svg"
        _save_transport_zones_svg(zones, output_path, label_df=label_df)

    return fig


def _save_transport_zones_svg(
    zones: gpd.GeoDataFrame,
    output_path: Path,
    label_df: pd.DataFrame | None,
) -> None:
    """Save the map as SVG without going through Plotly/Kaleido/Chrome."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    plot_zones = zones.copy()
    plot_zones["zone_color"] = plot_zones["is_inner_zone"].map(
        {True: MOBILITY_COLORS["inner_zone"], False: MOBILITY_COLORS["outer_zone"]}
    )

    fig, ax = plt.subplots(figsize=(11, 8), constrained_layout=True)
    plot_zones.plot(
        ax=ax,
        color=plot_zones["zone_color"],
        edgecolor=MOBILITY_COLORS["zone_border"],
        linewidth=0.4,
    )
    ax.set_axis_off()

    if label_df is not None and not label_df.empty:
        label_points = gpd.GeoDataFrame(
            label_df[["label", "font_size"]],
            geometry=gpd.points_from_xy(label_df["lon"], label_df["lat"], crs="EPSG:4326"),
        )
        if zones.crs is not None:
            label_points = label_points.to_crs(zones.crs)
        for _, row in label_points.iterrows():
            ax.text(
                row.geometry.x,
                row.geometry.y,
                row["label"],
                color=MOBILITY_COLORS["label"],
                fontsize=row["font_size"],
                ha="center",
                va="center",
                path_effects=[
                    patheffects.withStroke(linewidth=3.5, foreground="white")
                ],
            )

    ax.legend(
        handles=[
            Patch(facecolor=MOBILITY_COLORS["inner_zone"], label="Inner zone"),
            Patch(facecolor=MOBILITY_COLORS["outer_zone"], label="Outer zone"),
        ],
        loc="lower left",
        frameon=True,
    )
    fig.savefig(output_path, format="svg")
    plt.close(fig)


def _select_city_labels(
    zones: gpd.GeoDataFrame,
    population: Any | None,
    max_labels: int,
) -> pd.DataFrame:
    if max_labels <= 0 or "local_admin_unit_id" not in zones.columns:
        return pd.DataFrame(columns=["label", "lon", "lat", "font_size"])

    label_name_column = (
        "local_admin_unit_name"
        if "local_admin_unit_name" in zones.columns
        else "local_admin_unit_id"
    )
    label_source = zones.copy()
    label_source["_label"] = label_source[label_name_column]
    label_source["_inner_score"] = label_source["is_inner_zone"].astype(int)
    label_source["_area"] = label_source.geometry.area

    city_labels = label_source.dissolve(
        by="local_admin_unit_id",
        aggfunc={
            "_label": "first",
            "_inner_score": "max",
            "_area": "sum",
        },
    ).reset_index()

    if population is not None:
        population_groups = pd.read_parquet(population.get()["population_groups"])
        population_score = (
            population_groups.groupby("transport_zone_id", as_index=False)["weight"]
            .sum()
            .rename(columns={"weight": "population"})
        )
        city_population = (
            zones[["transport_zone_id", "local_admin_unit_id"]]
            .merge(population_score, on="transport_zone_id", how="left")
            .groupby("local_admin_unit_id", as_index=False)["population"]
            .sum()
        )
        city_labels = city_labels.merge(city_population, on="local_admin_unit_id", how="left")
        city_labels["population"] = city_labels["population"].fillna(0.0)
        population_max = max(city_labels["population"].max(), 1.0)
        city_labels["_score"] = (
            city_labels["population"]
            + city_labels["_inner_score"] * population_max * 0.01
            + city_labels["_area"] / max(city_labels["_area"].max(), 1.0)
        )
    else:
        city_labels["population"] = 0.0
        city_labels["_score"] = (
            city_labels["_inner_score"] * max(city_labels["_area"].max(), 1.0)
            + city_labels["_area"] / max(city_labels["_area"].max(), 1.0)
        )
    city_labels = city_labels.sort_values("_score", ascending=False)

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
        return pd.DataFrame(columns=["label", "lon", "lat", "font_size"])

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
            "label": selected_wgs84["_label"].to_list(),
            "lon": coords["x"].to_list(),
            "lat": coords["y"].to_list(),
        }
    )
    result["font_size"] = [14 if i == 0 else 11 if i < 5 else 9 for i in range(len(result))]
    return result
