import itertools
import os
from pathlib import Path

import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import plotly.express as px
import polars as pl

from mobility.reports.theme import MOBILITY_COLORS, apply_report_layout

from urllib.parse import unquote
from typing import Dict, List

class TravelCostsEvaluation:
    """
    Evaluate and compare modeled travel costs (time, distance) against reference data 
    such as Google Maps results.
    """
    
    def __init__(self, results):
        """
        Initialize the evaluator with model results.
        
        Parameters
        ----------
        results : object
            Object containing model outputs, including weekday and weekend costs 
            and transport zones.
        """
        self.results = results
    
    
    def get(
            self,
            ref_costs: List,
            variable: str = "time",
            plot: bool = True,
            iteration: int | None = None,
            plot_method: str = "browser",
            save_to_file: bool = False,
            output_path: str | Path | None = None,
            width: int = 760,
            height: int = 560,
        ):
        """
        Compares the travel costs (time, distance) of the model with reference 
        data (from google maps for example). 
        
        Parameters
        ----------
        ref_costs : list
            List of dicts with the following structure : 
                {
                     "url": "https://www.google.com/maps/dir/46.1987752,6.14416/46.0672248,6.3121397/@46.1336438,6.1540326,12z/data=!4m9!4m8!1m0!1m1!4e1!2m3!6e0!7e2!8j1761033600!3e0?entry=ttu&g_ep=EgoyMDI1MTAxNC4wIKXMDSoASAFQAw%3D%3D",
                     "hour": 8.0,
                     "travel_costs": [
                         {
                             "mode": "car",
                             "time": (24.0+40.0)/2.0/60.0,
                             "distance": 27.4
                         },
                         {
                             "mode": "walk/public_transport/walk",
                             "time": 64.0/60.0,
                             "distance": None
                         },
                         {
                             "mode": "bicycle",
                             "time": 105.0/60.0,
                             "distance": 26.5
                         }
                     ]
                }
                 
                
        variable: str
            Controls wether the comparison is made on "time" or "distance".
            
        plot: bool
            Should a scatter plot of the results be displayed.
        
        Returns
        -------
        pl.DataFrame
            Input ref_costs dataframe with a distance_model and a time_model column.
        """
        if iteration is None:
            iteration = int(self.results.parameters.run.n_iterations)

        costs = self.results.run.iteration_transport_cost_assets[
            iteration - 1
        ].get_costs_by_od_and_mode(["time", "distance"])
        transport_zones = self.results.transport_zones.get()
        
        ref_costs = self.convert_to_dataframe(ref_costs)
        ref_costs = self.join_transport_zone_ids(ref_costs, transport_zones)
        
        ref_costs = (
            ref_costs
            .join(costs, on=["from", "to", "mode"], suffix="_model")
        )
        
        if plot:
            fig = px.scatter(
                ref_costs,
                x=variable,
                y=variable + "_model",
                color="mode",
                color_discrete_map=_mode_color_map(ref_costs["mode"].unique().to_list()),
                hover_data={
                    "origin": True,
                    "destination": True,
                    "mode": True,
                    "time": True,
                    "time_model": True,
                    "distance": True,
                    "distance_model": True
                }
            )
            apply_report_layout(fig)
            fig.update_layout(
                title_text=None,
                paper_bgcolor=MOBILITY_COLORS["background"],
                plot_bgcolor=MOBILITY_COLORS["background"],
                width=width,
                height=height,
                margin={"l": 65, "r": 20, "t": 35, "b": 80},
                legend={
                    "orientation": "h",
                    "x": 0.0,
                    "y": -0.18,
                    "xanchor": "left",
                    "yanchor": "top",
                    "bgcolor": "rgba(255,255,255,0.9)",
                },
            )
            fig.update_xaxes(
                title_text=_travel_cost_axis_label(variable, source="reference"),
                showgrid=False,
                zeroline=False,
            )
            fig.update_yaxes(
                title_text=_travel_cost_axis_label(variable, source="model"),
                showgrid=True,
                gridcolor=MOBILITY_COLORS["grid"],
                zeroline=False,
            )
            
            fig.show(plot_method)

        if save_to_file or output_path is not None:
            _save_travel_costs_svg(
                ref_costs,
                variable=variable,
                output_path=(
                    Path(output_path)
                    if output_path is not None
                    else self._travel_costs_svg_path(variable, iteration)
                ),
                width=width,
                height=height,
            )
        
        return ref_costs

    def _travel_costs_svg_path(self, variable: str, iteration: int) -> Path:
        """Return the default SVG output path for a travel-cost plot."""
        project_folder = os.environ.get("MOBILITY_PROJECT_DATA_FOLDER")
        if project_folder is None:
            raise ValueError(
                "save_to_file=True needs MOBILITY_PROJECT_DATA_FOLDER to be defined."
            )
        inputs_hash = getattr(self.results, "inputs_hash", "travel-costs")
        return (
            Path(project_folder)
            / f"{inputs_hash}-travel-costs-{variable}-iteration-{iteration}.svg"
        )

    
    def convert_to_dataframe(self, ref_costs):
        """
        Convert structured reference cost data into a Polars DataFrame.
        
        Parameters
        ----------
        ref_costs : list of dict
            List of structured reference cost entries.
        
        Returns
        -------
        pl.DataFrame
            Flattened DataFrame with one row per OD-mode combination and an index column.
        """
        
        ref_costs = [
            self.flatten_travel_costs(**c)
            for c in ref_costs
        ]
        ref_costs = list(itertools.chain.from_iterable(ref_costs))
        ref_costs = pl.DataFrame(ref_costs)
        
        ref_costs = ( 
            ref_costs
            .with_row_index("ref_index")
        )
        
        return ref_costs
    

    def flatten_travel_costs(self, source: str, origin: Dict, destination: Dict, departure_hour: float, travel_costs: Dict):
        """
        Convert a single reference route definition into row entries.
        
        Parameters
        ----------
        source: str
            Description of the data source (example : Google Maps, 21-04-2026, manual extraction)
        origin : dict
            name, lon, and lat of the origin.
        destination : dict
            name, lon, and lat of the destination.
        url : str
            Google Maps directions URL.
        departure_hour : float
            Departure hour in decimal format.
        travel_costs : dict
            Dictionary of mode/time/distance entries.
        
        Returns
        -------
        list of dict
            Each dict contains travel attributes and coordinates for a specific mode.
        """
        
        travel_costs = [
            {
                "source": source,
                "origin": origin["name"],
                "destination": destination["name"],
                "from_lon": origin["lon"],
                "from_lat": origin["lat"],
                "to_lon":  destination["lon"],
                "to_lat": destination["lat"],
                "departure_hour": departure_hour,
                "time": tc["time"],
                "distance": tc["distance"],  
                "mode": tc["mode"]
            } 
            for tc in travel_costs
        ]
        
        return travel_costs
    

    def join_transport_zone_ids(self, ref_costs, transport_zones):
        """
        Assign origin and destination transport zone IDs to each reference trip.
        
        Parameters
        ----------
        ref_costs : pl.DataFrame
            Reference travel cost DataFrame with coordinates.
        transport_zones : gpd.GeoDataFrame
            Transport zone polygons with an ID column.
        
        Returns
        -------
        pl.DataFrame
            Input DataFrame with added 'from' and 'to' zone identifiers.
        """
        
        origins = gpd.GeoDataFrame(
            ref_costs.to_pandas()[["ref_index"]],
            geometry=gpd.points_from_xy(
                ref_costs["from_lon"],
                ref_costs["from_lat"]
            ),
            crs="EPSG:4326"
        )
        
        origins = origins.to_crs("EPSG:3035")
        
        destinations = gpd.GeoDataFrame(
            ref_costs.to_pandas()[["ref_index"]],
            geometry=gpd.points_from_xy(
                ref_costs["to_lon"],
                ref_costs["to_lat"]
            ),
            crs="EPSG:4326"
        )
        
        destinations = destinations.to_crs("EPSG:3035")
        
        origins = gpd.sjoin(origins, transport_zones[["transport_zone_id", "geometry"]])
        destinations = gpd.sjoin(destinations, transport_zones[["transport_zone_id", "geometry"]])
        
        origins = ( 
            pl.DataFrame(origins.drop(["geometry", "index_right"], axis=1))
            .rename({"transport_zone_id": "from"})
        )
        
        destinations = ( 
            pl.DataFrame(destinations.drop(["geometry", "index_right"], axis=1))
            .rename({"transport_zone_id": "to"})
        )
        
        ref_costs = (
            ref_costs
            .join(origins, on="ref_index")
            .join(destinations, on="ref_index")
        )
        
        return ref_costs


def _travel_cost_axis_label(variable: str, *, source: str) -> str:
    """Return a plain axis label for a travel-cost comparison plot."""
    label = {
        "time": "Travel time (h)",
        "distance": "Distance (km)",
    }.get(variable, variable.replace("_", " ").capitalize())
    if source == "model":
        return f"Model {label.lower()}"
    return f"Reference {label.lower()}"


def _mode_color_map(modes: list[str]) -> dict[str, str]:
    """Return report colors for raw mode names."""
    fixed_colors = {
        "walk": "#5F7F73",
        "bicycle": "#0B5A66",
        "public_transport": "#EF4B3E",
        "walk/public_transport/walk": "#F06A5A",
        "car/public_transport/walk": "#EF4B3E",
        "bicycle/public_transport/walk": "#D7191C",
        "car": "#4D4D4D",
        "carpool": "#8C8C8C",
        "other": "#7E6F9A",
    }
    fallback_palette = (
        px.colors.qualitative.Safe
        + px.colors.qualitative.Bold
        + px.colors.qualitative.Set3
    )

    color_map: dict[str, str] = {}
    fallback_index = 0
    for mode in modes:
        if mode in fixed_colors:
            color_map[mode] = fixed_colors[mode]
            continue

        while fallback_index < len(fallback_palette) and fallback_palette[fallback_index] in color_map.values():
            fallback_index += 1
        if fallback_index >= len(fallback_palette):
            fallback_index = 0
        color_map[mode] = fallback_palette[fallback_index]
        fallback_index += 1

    return color_map


def _mode_label(mode: str) -> str:
    """Return a readable transport mode label."""
    return mode.replace("_", " ").replace("/", " / ").capitalize()


def _save_travel_costs_svg(
    costs: pl.DataFrame,
    *,
    variable: str,
    output_path: Path,
    width: int,
    height: int,
) -> None:
    """Save a travel-cost comparison scatter plot as SVG."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    frame = costs.to_pandas()
    model_variable = f"{variable}_model"
    frame[variable] = frame[variable].astype(float)
    frame[model_variable] = frame[model_variable].astype(float)

    modes = sorted(frame["mode"].dropna().unique().tolist())
    color_map = _mode_color_map(modes)
    fig, axis = plt.subplots(
        figsize=(max(width / 100.0, 7.6), max(height / 100.0, 5.6)),
    )
    for mode in modes:
        panel = frame[frame["mode"] == mode]
        axis.scatter(
            panel[variable],
            panel[model_variable],
            color=color_map[mode],
            label=mode,
            s=28,
            alpha=0.9,
            edgecolors="none",
        )

    values = np.concatenate(
        [
            frame[variable].dropna().to_numpy(),
            frame[model_variable].dropna().to_numpy(),
        ]
    )
    if values.size > 0:
        lower = float(values.min())
        upper = float(values.max())
        if lower == upper:
            margin = max(abs(upper) * 0.05, 0.01)
            lower -= margin
            upper += margin
        padding = max((upper - lower) * 0.04, 0.02)
        lower -= padding
        upper += padding
        axis.set_xlim(lower, upper)
        axis.set_ylim(lower, upper)

    axis.set_xlabel(_travel_cost_axis_label(variable, source="reference"))
    axis.set_ylabel(_travel_cost_axis_label(variable, source="model"))
    axis.grid(axis="y", color=MOBILITY_COLORS["grid"], linewidth=1.0)
    axis.grid(axis="x", visible=False)
    axis.set_facecolor(MOBILITY_COLORS["background"])
    for spine in axis.spines.values():
        spine.set_visible(False)
    axis.legend(
        title="mode",
        loc="lower center",
        bbox_to_anchor=(0.5, -0.28),
        ncol=max(1, len(modes)),
        frameon=False,
        columnspacing=1.4,
        handletextpad=0.5,
    )
    fig.patch.set_facecolor(MOBILITY_COLORS["background"])
    fig.subplots_adjust(left=0.11, right=0.98, top=0.95, bottom=0.28)
    fig.savefig(output_path, format="svg")
    plt.close(fig)
