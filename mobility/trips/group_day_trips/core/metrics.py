from __future__ import annotations

import json
import logging
import math
import os
import re
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

import geopandas as gpd
import matplotlib.ticker as mticker
import matplotlib.pyplot as plt
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import polars as pl
from plotly.subplots import make_subplots

from mobility.reports import TransportZoneMaps
from mobility.reports.theme import MOBILITY_COLORS, apply_report_layout

from ..iterations import Iterations
from ..evaluation.car_traffic_evaluation import CarTrafficEvaluation
from ..evaluation.public_transport_network_evaluation import (
    PublicTransportNetworkEvaluation,
)
from ..evaluation.routing_evaluation import RoutingEvaluation
from ..evaluation.travel_costs_evaluation import TravelCostsEvaluation

if TYPE_CHECKING:
    from .results import RunResults


class RunMetrics:
    """Grouped access to descriptive run-level metrics and visual summaries."""

    def __init__(self, results: "RunResults") -> None:
        self.results = results

    def aggregate(self, normalize: bool = True):
        """Compute high-level trip, time, and distance metrics for this run."""
        ref_plan_steps = (
            self.results.population_weighted_plan_steps.rename({"travel_time": "time"})
            .with_columns(country=pl.col("country").cast(pl.String()))
        )

        transport_zones_df = (
            pl.DataFrame(self.results.transport_zones.get().drop("geometry", axis=1))
            .filter(pl.col("is_inner_zone"))
            .lazy()
        )
        study_area_df = pl.DataFrame(self.results.transport_zones.study_area.get().drop("geometry", axis=1)).lazy()

        n_persons = (
            self.results.demand_groups.rename({"home_zone_id": "transport_zone_id"})
            .join(transport_zones_df.select(["transport_zone_id", "local_admin_unit_id"]), on=["transport_zone_id"])
            .join(study_area_df.select(["local_admin_unit_id", "country"]), on=["local_admin_unit_id"])
            .group_by("country")
            .agg(pl.col("n_persons").sum())
            .with_columns(country=pl.col("country").cast(pl.String()))
            .collect(engine="streaming")
        )

        def aggregate(df):
            return (
                df.filter(pl.col("activity_seq_id") != 0)
                .rename({"home_zone_id": "transport_zone_id"})
                .join(transport_zones_df.select(["transport_zone_id", "local_admin_unit_id"]), on=["transport_zone_id"])
                .join(study_area_df.select(["local_admin_unit_id", "country"]), on=["local_admin_unit_id"])
                .group_by("country")
                .agg(
                    n_trips=pl.col("n_persons").sum(),
                    time=(pl.col("time") * pl.col("n_persons")).sum(),
                    distance=(pl.col("distance") * pl.col("n_persons")).sum(),
                )
                .unpivot(
                    index="country",
                    on=["n_trips", "time", "distance"],
                    variable_name="variable",
                    value_name="value",
                )
                .collect(engine="streaming")
            )

        trip_count = aggregate(self.results.plan_steps)
        trip_count_ref = aggregate(ref_plan_steps)

        comparison = trip_count.join(trip_count_ref, on=["country", "variable"], suffix="_ref")

        if normalize:
            comparison = (
                comparison.join(n_persons, on=["country"])
                .with_columns(
                    value=pl.col("value") / pl.col("n_persons"),
                    value_ref=pl.col("value_ref") / pl.col("n_persons"),
                )
            )

        return (
            comparison.with_columns(delta=pl.col("value") - pl.col("value_ref"))
            .with_columns(delta_relative=pl.col("delta") / pl.col("value_ref"))
            .select(["country", "variable", "value", "value_ref", "delta", "delta_relative"])
        )

    def travel_indicators_by(
        self,
        variable: Literal["mode", "activity", "time_bin", "distance_bin"] = None,
        normalize: bool = True,
        plot: bool = False,
        plot_method: str = "browser",
        save_to_file: bool = False,
        output_path: str | Path | None = None,
        width: int = 980,
        height: int = 560,
        return_figure: bool = False,
    ):
        """Compare trips, travel time, and travel distance with the survey reference by one grouping variable."""
        ref_plan_steps = (
            self.results.population_weighted_plan_steps.rename({"travel_time": "time"})
            .with_columns(mode=pl.col("mode").cast(pl.String()))
        )

        transport_zones_df = (
            pl.DataFrame(self.results.transport_zones.get().drop("geometry", axis=1))
            .filter(pl.col("is_inner_zone"))
            .lazy()
        )

        n_persons = (
            self.results.demand_groups.rename({"home_zone_id": "transport_zone_id"})
            .join(transport_zones_df.select(["transport_zone_id", "local_admin_unit_id"]), on=["transport_zone_id"])
            .collect()["n_persons"]
            .sum()
        )

        def aggregate(df):
            aggregated = (
                df.filter(pl.col("activity_seq_id") != 0)
                .rename({"home_zone_id": "transport_zone_id"})
                .join(transport_zones_df.select(["transport_zone_id", "local_admin_unit_id"]), on=["transport_zone_id"])
                .with_columns(
                    time_bin=(pl.col("time") * 60.0).cut([0.0, 5.0, 10, 20, 30.0, 45.0, 60.0, 1e6], left_closed=True),
                    distance_bin=pl.col("distance").cut([0.0, 1.0, 5.0, 10.0, 20.0, 40.0, 80.0, 1e6], left_closed=True),
                )
                .group_by(variable)
                .agg(
                    n_trips=pl.col("n_persons").sum(),
                    time=(pl.col("time") * pl.col("n_persons")).sum(),
                    distance=(pl.col("distance") * pl.col("n_persons")).sum(),
                )
                .unpivot(
                    index=variable,
                    on=["n_trips", "time", "distance"],
                    variable_name="variable",
                    value_name="value",
                )
            )
            if variable in {"mode", "activity"}:
                aggregated = aggregated.with_columns(pl.col(variable).cast(pl.String()))
            return aggregated.collect(engine="streaming")

        with pl.StringCache():
            trip_count = aggregate(self.results.plan_steps)
            trip_count_ref = aggregate(ref_plan_steps)

        comparison = trip_count.join(
            trip_count_ref,
            on=["variable", variable],
            suffix="_ref",
            how="full",
            coalesce=True,
        )

        if normalize:
            comparison = comparison.with_columns(
                value=pl.col("value") / n_persons,
                value_ref=pl.col("value_ref") / n_persons,
            )

        comparison = (
            comparison.with_columns(delta=pl.col("value") - pl.col("value_ref"))
            .with_columns(delta_relative=pl.col("delta") / pl.col("value_ref"))
            .select(["variable", variable, "value", "value_ref", "delta", "delta_relative"])
        )

        fig = None
        if plot or save_to_file or output_path is not None or return_figure:
            fig = self._plot_travel_indicators_by(
                comparison,
                group_variable=variable,
                width=width,
                height=height,
            )
        if save_to_file or output_path is not None:
            svg_path = (
                Path(output_path)
                if output_path is not None
                else self._svg_report_path(f"{self.results.period}-travel-indicators-by-{variable}")
            )
            self._save_travel_indicators_by_svg(
                comparison,
                group_variable=variable,
                output_path=svg_path,
                width=width,
                height=height,
            )

        if plot and fig is not None:
            fig.show(plot_method)

        if return_figure:
            return comparison, fig

        return comparison

    def _travel_indicators_plot_table(
        self,
        comparison: pl.DataFrame,
        *,
        group_variable: str,
    ) -> pl.DataFrame:
        """Return a long table used by travel-indicator plots."""
        indicator_labels = self._travel_indicator_labels()
        return (
            comparison
            .select(["variable", group_variable, "value", "value_ref"])
            .unpivot(
                index=["variable", group_variable],
                on=["value", "value_ref"],
                variable_name="value_type",
                value_name="value",
            )
            .with_columns(
                pl.col(group_variable).cast(pl.String),
                source=(
                    pl.when(pl.col("value_type") == "value_ref")
                    .then(pl.lit("Survey"))
                    .otherwise(pl.lit("Model"))
                ),
                indicator=pl.col("variable").replace(indicator_labels),
                value=pl.col("value").fill_null(0.0),
            )
            .select([group_variable, "indicator", "source", "value"])
            .sort([group_variable, "indicator", "source"])
        )

    def _plot_travel_indicators_by(
        self,
        comparison: pl.DataFrame,
        *,
        group_variable: str,
        width: int,
        height: int,
    ) -> go.Figure:
        """Render survey/model travel indicators with the report style."""
        plot_table = self._travel_indicators_plot_table(
            comparison,
            group_variable=group_variable,
        )
        indicator_order = list(self._travel_indicator_labels().values())
        fig = px.bar(
            plot_table.to_pandas(),
            x=group_variable,
            y="value",
            color="source",
            facet_col="indicator",
            barmode="group",
            facet_col_spacing=0.06,
            category_orders={
                "source": ["Survey", "Model"],
                "indicator": indicator_order,
            },
            color_discrete_map={
                "Survey": MOBILITY_COLORS["survey"],
                "Model": MOBILITY_COLORS["model"],
            },
            labels={
                group_variable: group_variable.replace("_", " ").capitalize(),
                "value": "Value",
                "source": "",
            },
            width=width,
            height=height,
        )
        apply_report_layout(fig)
        fig.update_layout(
            title_text=None,
            paper_bgcolor=MOBILITY_COLORS["background"],
            plot_bgcolor=MOBILITY_COLORS["background"],
            margin={"l": 65, "r": 20, "t": 35, "b": 120},
            legend={
                "orientation": "h",
                "x": 0.0,
                "y": -0.22,
                "xanchor": "left",
                "yanchor": "top",
                "bgcolor": "rgba(255,255,255,0.9)",
            },
        )
        fig.for_each_annotation(lambda annotation: annotation.update(text=annotation.text.split("=")[-1], font_size=12))
        fig.update_yaxes(matches=None, showticklabels=True, showgrid=True, gridcolor=MOBILITY_COLORS["grid"], zeroline=False)
        fig.update_xaxes(showgrid=False, zeroline=False, tickangle=45)
        return fig

    def _save_travel_indicators_by_svg(
        self,
        comparison: pl.DataFrame,
        *,
        group_variable: str,
        output_path: Path,
        width: int,
        height: int,
    ) -> None:
        """Save travel indicators as SVG with the same report style."""
        output_path.parent.mkdir(parents=True, exist_ok=True)
        plot_table = self._travel_indicators_plot_table(
            comparison,
            group_variable=group_variable,
        ).to_pandas()
        indicators = list(self._travel_indicator_labels().values())
        groups = sorted(plot_table[group_variable].dropna().unique().tolist())
        if not groups:
            return

        fig_width = max(width / 100.0, 7.0)
        fig_height = max(height / 100.0, 4.5)
        fig, axes = plt.subplots(
            1,
            len(indicators),
            figsize=(fig_width, fig_height),
            squeeze=False,
        )
        bar_width = 0.38
        x_positions = np.arange(len(groups))
        source_colors = {
            "Survey": MOBILITY_COLORS["survey"],
            "Model": MOBILITY_COLORS["model"],
        }

        for axis, indicator in zip(axes[0], indicators, strict=True):
            panel = plot_table[plot_table["indicator"] == indicator]
            for index, source in enumerate(["Survey", "Model"]):
                values = (
                    panel[panel["source"] == source]
                    .set_index(group_variable)["value"]
                    .reindex(groups, fill_value=0.0)
                    .to_numpy()
                )
                axis.bar(
                    x_positions + (index - 0.5) * bar_width,
                    values,
                    width=bar_width,
                    color=source_colors[source],
                    label=source,
                )
            axis.set_title(indicator, fontsize=12)
            axis.set_xticks(x_positions, groups, rotation=45, ha="right")
            axis.grid(axis="y", color=MOBILITY_COLORS["grid"], linewidth=0.8)
            axis.grid(axis="x", visible=False)
            axis.set_facecolor(MOBILITY_COLORS["background"])

        axes[0][0].set_ylabel("Value")
        handles, legend_labels = axes[0][0].get_legend_handles_labels()
        fig.legend(
            handles,
            legend_labels,
            loc="lower left",
            bbox_to_anchor=(0.06, 0.02),
            ncol=2,
            frameon=False,
        )
        fig.patch.set_facecolor(MOBILITY_COLORS["background"])
        fig.subplots_adjust(left=0.07, right=0.98, top=0.90, bottom=0.28, wspace=0.22)
        fig.savefig(output_path, format="svg")
        plt.close(fig)

    @staticmethod
    def _travel_indicator_labels() -> dict[str, str]:
        """Return display labels for travel indicator facets."""
        return {
            "n_trips": "Trips",
            "time": "Travel time",
            "distance": "Distance",
        }

    def activity_duration_distribution(
        self,
        bin_width_minutes: int = 15,
        plot: bool = True,
        inner_zone_residents_only: bool = False,
    ) -> pl.DataFrame:
        """Compare weighted activity-duration distributions between model and survey."""
        if bin_width_minutes <= 0:
            raise ValueError("bin_width_minutes must be positive.")

        model_plan_steps = self.results.plan_steps
        survey_plan_steps = self.results.population_weighted_plan_steps
        if inner_zone_residents_only:
            model_plan_steps = self._filter_plan_steps_to_inner_zone_residents(model_plan_steps)
            survey_plan_steps = self._filter_plan_steps_to_inner_zone_residents(survey_plan_steps)

        durations = pl.concat(
            [
                self._activity_duration_samples(model_plan_steps, source="model"),
                self._activity_duration_samples(survey_plan_steps, source="survey"),
            ],
            how="vertical_relaxed",
        )
        bin_width_hours = bin_width_minutes / 60.0

        distribution = (
            durations.lazy()
            .with_columns(
                duration_bin_start=(pl.col("duration") / bin_width_hours).floor() * bin_width_hours,
            )
            .with_columns(
                duration_bin_end=pl.col("duration_bin_start") + bin_width_hours,
                duration_bin_mid=pl.col("duration_bin_start") + bin_width_hours / 2.0,
                duration_label=pl.format(
                    "{}-{} h",
                    pl.col("duration_bin_start").round(2),
                    (pl.col("duration_bin_start") + bin_width_hours).round(2),
                ),
            )
            .group_by(
                [
                    "source",
                    "activity",
                    "duration_bin_start",
                    "duration_bin_end",
                    "duration_bin_mid",
                    "duration_label",
                ]
            )
            .agg(
                weighted_visits=pl.col("weight").sum(),
                person_hours=(pl.col("duration") * pl.col("weight")).sum(),
            )
            .with_columns(
                probability=pl.col("weighted_visits") / pl.col("weighted_visits").sum().over(["source", "activity"])
            )
            .sort(["activity", "source", "duration_bin_start"])
            .collect(engine="streaming")
        )

        if plot:
            self._plot_activity_duration_distribution(distribution)

        return distribution

    @staticmethod
    def _activity_duration_samples(
        plan_steps: pl.LazyFrame | pl.DataFrame,
        source: str,
    ) -> pl.DataFrame:
        """Extract one weighted activity-duration sample table from plan steps."""
        if isinstance(plan_steps, pl.DataFrame):
            plan_steps = plan_steps.lazy()

        column_names = set(plan_steps.collect_schema().names())
        required_columns = {"activity", "n_persons"}
        missing_columns = required_columns - column_names
        if missing_columns:
            missing = ", ".join(sorted(missing_columns))
            raise ValueError(f"Plan steps are missing required duration-distribution columns: {missing}.")

        if "duration_per_pers" in column_names:
            duration_expr = pl.col("duration_per_pers").cast(pl.Float64)
        elif {"duration", "n_persons"} <= column_names:
            duration_expr = pl.col("duration").cast(pl.Float64) / pl.col("n_persons").cast(pl.Float64)
        else:
            raise ValueError("Plan steps need either duration_per_pers or duration and n_persons.")

        samples = plan_steps.with_columns(
            source=pl.lit(source),
            activity=pl.col("activity").cast(pl.String),
            duration=duration_expr,
            weight=pl.col("n_persons").cast(pl.Float64),
        )
        samples = RunMetrics._filter_terminal_home_steps(samples, column_names)

        return (
            samples.filter(
                pl.col("duration").is_not_null()
                & pl.col("duration").is_finite()
                & (pl.col("duration") >= 0.0)
                & pl.col("weight").is_not_null()
                & pl.col("weight").is_finite()
                & (pl.col("weight") > 0.0)
            )
            .select(["source", "activity", "duration", "weight"])
            .collect(engine="streaming")
        )

    @staticmethod
    def _filter_terminal_home_steps(plan_steps: pl.LazyFrame, column_names: set[str]) -> pl.LazyFrame:
        """Remove the final home stay and keep home stops reached during the day."""
        if "seq_step_index" in column_names:
            plan_key_candidates = [
                "country",
                "home_zone_id",
                "city_category",
                "csp",
                "n_cars",
                "activity_seq_id",
                "time_seq_id",
                "dest_seq_id",
                "mode_seq_id",
            ]
            plan_keys = [column for column in plan_key_candidates if column in column_names]
            if plan_keys:
                return (
                    plan_steps.with_columns(
                        is_terminal_step=pl.col("seq_step_index") == pl.col("seq_step_index").max().over(plan_keys)
                    )
                    .filter(~((pl.col("activity") == "home") & pl.col("is_terminal_step")))
                    .drop("is_terminal_step")
                )

        if "next_departure_time" in column_names:
            return plan_steps.filter(~((pl.col("activity") == "home") & (pl.col("next_departure_time") >= 24.0)))

        return plan_steps

    @staticmethod
    def _plot_activity_duration_distribution(distribution: pl.DataFrame) -> None:
        """Plot the weighted duration distributions with one panel per activity."""
        if distribution.is_empty():
            return

        fig = px.line(
            distribution.to_pandas(),
            x="duration_bin_mid",
            y="probability",
            color="source",
            facet_col="activity",
            facet_col_wrap=3,
            markers=True,
            labels={
                "duration_bin_mid": "activity duration (h)",
                "probability": "share of weighted visits",
                "source": "source",
                "activity": "activity",
            },
        )
        fig.update_yaxes(matches=None, showticklabels=True)
        fig.update_xaxes(matches=None, showticklabels=True)
        fig.show("browser")

    def immobility(self, plot: bool = True):
        """Compute immobility by country and socio-professional category."""
        surveys_immobility = [
            pl.DataFrame(s.get()["p_immobility"].reset_index()).with_columns(
                country=pl.lit(s.inputs["parameters"].country, pl.String())
            )
            for s in self.results.surveys
        ]
        column_name = "immobility_weekday" if self.results.is_weekday else "immobility_weekend"
        surveys_immobility = (
            pl.concat(surveys_immobility)
            .with_columns(p_immobility=pl.col(column_name))
            .with_columns(
                country=pl.col("country").cast(pl.String()),
                csp=pl.col("csp").cast(pl.String()),
            )
            .select(["country", "csp", "p_immobility"])
        )

        transport_zones_df = (
            pl.DataFrame(self.results.transport_zones.get().drop("geometry", axis=1))
            .filter(pl.col("is_inner_zone"))
            .lazy()
        )
        study_area_df = (
            pl.DataFrame(self.results.transport_zones.study_area.get().drop("geometry", axis=1)[["local_admin_unit_id", "country"]]).lazy()
        )

        immobility = (
            self.results.plan_steps.filter(pl.col("activity_seq_id") == 0)
            .rename({"home_zone_id": "transport_zone_id"})
            .join(transport_zones_df.select(["transport_zone_id", "local_admin_unit_id"]), on=["transport_zone_id"])
            .with_columns(pl.col("csp").cast(pl.String()))
            .join(
                self.results.demand_groups.rename({"n_persons": "n_persons_dem_grp", "home_zone_id": "transport_zone_id"})
                .with_columns(pl.col("csp").cast(pl.String())),
                on=["transport_zone_id", "csp", "n_cars"],
                how="right",
            )
            .join(transport_zones_df, on="transport_zone_id")
            .join(study_area_df, on="local_admin_unit_id")
            .group_by(["country", "csp"])
            .agg(
                n_persons_imm=pl.col("n_persons").fill_null(0.0).sum(),
                n_persons_dem_grp=pl.col("n_persons_dem_grp").sum(),
            )
            .with_columns(
                country=pl.col("country").cast(pl.String()),
                csp=pl.col("csp").cast(pl.String()),
                p_immobility=pl.col("n_persons_imm") / pl.col("n_persons_dem_grp"),
            )
            .join(surveys_immobility.lazy(), on=["country", "csp"], suffix="_ref")
            .with_columns(n_persons_imm_ref=pl.col("n_persons_dem_grp") * pl.col("p_immobility_ref"))
            .collect(engine="streaming")
        )

        if plot:
            immobility_m = (
                immobility.select(["country", "csp", "n_persons_imm", "n_persons_imm_ref"])
                .unpivot(
                    index=["country", "csp"],
                    on=["n_persons_imm", "n_persons_imm_ref"],
                    variable_name="variable",
                    value_name="n_pers_immobility",
                )
                .sort("csp")
            )
            fig = px.bar(
                immobility_m,
                x="csp",
                y="n_pers_immobility",
                color="variable",
                barmode="group",
                facet_col="country",
            )
            fig = fig.update_xaxes(matches=None)
            fig.show("browser")

        return immobility

    def activity_time_series(
        self,
        interval_minutes: int = 15,
        plot: bool = False,
        inner_zone_residents_only: bool = False,
        plot_mode: Literal["stacked", "activity_panels"] = "stacked",
        save_to_file: bool = False,
    ) -> pl.DataFrame:
        """Aggregate average occupancy by activity and in-transit mode over time bins."""
        observed_time_series = self._add_residual_home_time(
            self._activity_time_series_raw(
                interval_minutes=interval_minutes,
                inner_zone_residents_only=inner_zone_residents_only,
            ),
            bins=self._time_bins(interval_minutes=interval_minutes),
            total_population=self._modeled_total_population(inner_zone_residents_only=inner_zone_residents_only),
        ).with_columns(source=pl.lit("observed"))
        survey_time_series = self._survey_activity_time_series(
            interval_minutes=interval_minutes,
            inner_zone_residents_only=inner_zone_residents_only,
        )
        time_series = pl.concat(
            [
                observed_time_series,
                survey_time_series.with_columns(source=pl.lit("survey")),
            ],
            how="vertical_relaxed",
        ).select(
            ["source", "time_bin_start", "time_label", "label", "n_persons"]
        )
        if plot:
            if plot_mode == "stacked":
                self._plot_activity_time_series(time_series, save_to_file=save_to_file)
            else:
                self._plot_activity_time_series(
                    time_series,
                    mode=plot_mode,
                    save_to_file=save_to_file,
                )

        return time_series

    def _activity_time_series_raw(
        self,
        interval_minutes: int = 15,
        inner_zone_residents_only: bool = False,
    ) -> pl.DataFrame:
        """Aggregate average occupancy by activity and in-transit mode over time bins."""
        plan_steps = self.results.plan_steps
        if inner_zone_residents_only:
            plan_steps = self._filter_plan_steps_to_inner_zone_residents(plan_steps)
        return self._time_series_from_plan_steps(plan_steps, interval_minutes=interval_minutes)

    def _survey_activity_time_series(
        self,
        interval_minutes: int = 15,
        inner_zone_residents_only: bool = False,
    ) -> pl.DataFrame:
        """Build the same occupancy time series from the canonical survey reference asset."""
        weighted_survey_plan_steps = self.results.population_weighted_plan_steps
        if inner_zone_residents_only:
            weighted_survey_plan_steps = self._filter_plan_steps_to_inner_zone_residents(weighted_survey_plan_steps)

        total_population = self._modeled_total_population(inner_zone_residents_only=inner_zone_residents_only)
        explicit_time_series = self._time_series_from_plan_steps(
            weighted_survey_plan_steps,
            interval_minutes=interval_minutes,
        )
        return self._add_residual_home_time(
            explicit_time_series,
            bins=self._time_bins(interval_minutes=interval_minutes),
            total_population=total_population,
        )

    def _modeled_total_population(self, inner_zone_residents_only: bool = False) -> float:
        """Return the total modeled population represented in the demand groups."""
        demand_groups = self.results.demand_groups
        if inner_zone_residents_only:
            demand_groups = self._filter_demand_groups_to_inner_zone_residents(demand_groups)
        total_population = demand_groups.select(pl.col("n_persons").sum().alias("total_population")).collect(engine="streaming").item()
        return float(total_population)

    def _inner_zone_transport_zone_ids(self) -> pl.DataFrame:
        """Return the transport-zone ids marked as inner zones."""
        return (
            pl.DataFrame(self.results.transport_zones.get().drop("geometry", axis=1, errors="ignore"))
            .filter(pl.col("is_inner_zone"))
            .select(pl.col("transport_zone_id").cast(pl.Int32))
            .unique()
        )

    def _filter_demand_groups_to_inner_zone_residents(self, demand_groups: pl.LazyFrame) -> pl.LazyFrame:
        """Keep only demand groups whose home zone is an inner zone."""
        inner_zone_ids = self._inner_zone_transport_zone_ids()
        return demand_groups.join(
            inner_zone_ids.lazy(),
            left_on=pl.col("home_zone_id").cast(pl.Int32),
            right_on="transport_zone_id",
            how="inner",
        ).drop("transport_zone_id")

    def _filter_plan_steps_to_inner_zone_residents(self, plan_steps: pl.LazyFrame) -> pl.LazyFrame:
        """Keep only plan steps belonging to residents of inner zones."""
        inner_zone_ids = self._inner_zone_transport_zone_ids()
        return plan_steps.join(
            inner_zone_ids.lazy(),
            left_on=pl.col("home_zone_id").cast(pl.Int32),
            right_on="transport_zone_id",
            how="inner",
        ).drop("transport_zone_id")

    def _time_series_from_plan_steps(
        self,
        plan_steps: pl.LazyFrame,
        *,
        interval_minutes: int = 15,
    ) -> pl.DataFrame:
        """Aggregate only the explicit states stored in plan steps over time bins."""
        if interval_minutes <= 0 or 1440 % interval_minutes != 0:
            raise ValueError("interval_minutes must be a positive divisor of 1440.")

        bins = self._time_bins(interval_minutes=interval_minutes)
        bin_duration_hours = interval_minutes / 60.0
        n_bins = bins.height

        plan_steps = (
            plan_steps
            .select(
                [
                    "activity",
                    "mode",
                    "n_persons",
                    "departure_time",
                    "arrival_time",
                    "next_departure_time",
                ]
            )
            .with_columns(
                activity=pl.col("activity").cast(pl.String),
                mode=pl.col("mode").cast(pl.String),
            )
        )

        activity_intervals = plan_steps.select(
            label=pl.col("activity"),
            interval_start=pl.col("arrival_time"),
            interval_end=pl.col("next_departure_time"),
            n_persons=pl.col("n_persons"),
        )
        transit_intervals = plan_steps.select(
            label=pl.format("in_transit:{}", pl.col("mode")),
            interval_start=pl.col("departure_time"),
            interval_end=pl.col("arrival_time"),
            n_persons=pl.col("n_persons"),
        )

        intervals = pl.concat([activity_intervals, transit_intervals], how="vertical_relaxed")

        return (
            intervals.lazy()
            .filter(pl.col("interval_end") > pl.col("interval_start"))
            .with_columns(
                start_bin_index=(
                    (pl.col("interval_start") / pl.lit(bin_duration_hours))
                    .floor()
                    .clip(0, n_bins - 1)
                    .cast(pl.UInt32)
                ),
                end_bin_index=(
                    (pl.col("interval_end") / pl.lit(bin_duration_hours))
                    .ceil()
                    .clip(0, n_bins)
                    .cast(pl.UInt32)
                ),
            )
            .with_columns(
                bin_index=pl.int_ranges(
                    "start_bin_index",
                    "end_bin_index",
                    step=1,
                    dtype=pl.UInt32,
                )
            )
            .explode("bin_index")
            .join(
                bins.with_row_index("bin_index").lazy(),
                on="bin_index",
                how="inner",
            )
            .with_columns(
                overlap_start=pl.max_horizontal("interval_start", "time_bin_start"),
                overlap_end=pl.min_horizontal("interval_end", "time_bin_end"),
            )
            .with_columns(
                overlap_hours=(pl.col("overlap_end") - pl.col("overlap_start")).clip(0.0, bin_duration_hours),
            )
            .filter(pl.col("overlap_hours") > 0.0)
            .with_columns(
                n_persons=(pl.col("n_persons") * pl.col("overlap_hours") / pl.lit(bin_duration_hours)),
            )
            .group_by(["time_bin_start", "time_label", "label"])
            .agg(n_persons=pl.col("n_persons").sum())
            .sort(["time_bin_start", "label"])
            .collect(engine="streaming")
        )

    def _time_bins(self, *, interval_minutes: int) -> pl.DataFrame:
        """Return one full-day time-bin table shared by modeled and survey series."""
        bin_duration_hours = interval_minutes / 60.0
        bin_starts = np.arange(0.0, 24.0, bin_duration_hours, dtype=float)
        return pl.DataFrame(
            {
                "time_bin_start": bin_starts,
                "time_bin_end": bin_starts + bin_duration_hours,
                "time_label": [
                    f"{int(hour):02d}:{int(round((hour * 60.0) % 60.0)):02d}"
                    for hour in bin_starts
                ],
            }
        )

    def plot_activity_time_series(
        self,
        interval_minutes: int = 15,
        inner_zone_residents_only: bool = False,
        plot_mode: Literal["stacked", "activity_panels"] = "stacked",
        save_to_file: bool = False,
    ):
        """Plot average occupancy by time of day."""
        time_series = self.activity_time_series(
            interval_minutes=interval_minutes,
            inner_zone_residents_only=inner_zone_residents_only,
        )
        if plot_mode == "stacked":
            return self._plot_activity_time_series(time_series, save_to_file=save_to_file)
        return self._plot_activity_time_series(
            time_series,
            mode=plot_mode,
            save_to_file=save_to_file,
        )

    def _add_residual_home_time(
        self,
        time_series: pl.DataFrame,
        *,
        bins: pl.DataFrame,
        total_population: float,
    ) -> pl.DataFrame:
        """Fill missing occupancy in each bin as home time so totals stay constant."""
        residual_home = (
            bins.lazy()
            .join(
                time_series.lazy()
                .group_by(["time_bin_start", "time_label"])
                .agg(stacked_total=pl.col("n_persons").sum()),
                on=["time_bin_start", "time_label"],
                how="left",
            )
            .with_columns(
                stacked_total=pl.col("stacked_total").fill_null(0.0),
                label=pl.lit("home"),
                n_persons=pl.lit(total_population) - pl.col("stacked_total"),
            )
            .filter(pl.col("n_persons") != 0.0)
            .select(["time_bin_start", "time_label", "label", "n_persons"])
            .collect(engine="streaming")
        )

        return (
            pl.concat([time_series, residual_home], how="vertical_relaxed")
            .group_by(["time_bin_start", "time_label", "label"])
            .agg(n_persons=pl.col("n_persons").sum())
            .sort(["time_bin_start", "label"])
        )

    def _plot_activity_time_series(
        self,
        time_series: pl.DataFrame,
        mode: Literal["stacked", "activity_panels"] = "stacked",
        save_to_file: bool = False,
    ):
        """Render stacked occupancy charts, with survey on the left when available."""
        if mode == "activity_panels":
            return self._plot_activity_time_series_activity_panels(
                time_series,
                save_to_file=save_to_file,
            )
        if mode != "stacked":
            raise ValueError("mode must be 'stacked' or 'activity_panels'.")

        source_titles = {"survey": "Survey", "observed": "Model"}
        available_sources = [source for source in ["survey", "observed"] if source in time_series["source"].unique().to_list()]
        panel_series = [
            (source_titles[source], time_series.filter(pl.col("source") == source).drop("source"))
            for source in available_sources
        ]
        category_orders = {
            "time_label": (
                time_series
                .select(["time_bin_start", "time_label"])
                .unique()
                .sort("time_bin_start")
                .get_column("time_label")
                .to_list()
            )
        }
        tick_labels = self._activity_time_tick_labels(category_orders["time_label"])
        labels = sorted({label for _, series in panel_series for label in series["label"].to_list()})
        color_map = self._activity_time_series_color_map(labels)

        fig = make_subplots(
            rows=1,
            cols=len(panel_series),
            subplot_titles=[title for title, _ in panel_series],
            shared_yaxes=True,
        )

        for col_index, (_, series) in enumerate(panel_series, start=1):
            pivoted = (
                series
                .pivot(index="time_label", on="label", values="n_persons", aggregate_function="sum")
                .sort("time_label")
                .fill_null(0.0)
            )
            time_labels = pivoted["time_label"].to_list()
            for label in labels:
                values = pivoted.get_column(label).to_list() if label in pivoted.columns else [0.0] * len(time_labels)
                fig.add_trace(
                    go.Bar(
                        x=time_labels,
                        y=values,
                        name=label,
                        legendgroup=label,
                        marker_color=color_map[label],
                        showlegend=(col_index == 1),
                    ),
                    row=1,
                    col=col_index,
                )

        fig.update_layout(
            barmode="stack",
            yaxis_title="Nombre de personnes",
            paper_bgcolor=MOBILITY_COLORS["background"],
            plot_bgcolor=MOBILITY_COLORS["background"],
            width=980,
            height=560,
            margin={"l": 65, "r": 20, "t": 35, "b": 95},
            legend={
                "orientation": "h",
                "x": 0.0,
                "y": -0.18,
                "xanchor": "left",
                "yanchor": "top",
                "bgcolor": "rgba(255,255,255,0.9)",
            },
        )
        apply_report_layout(fig)
        fig.update_layout(title_text=None)
        fig.update_annotations(font_size=13)
        fig.update_xaxes(
            showgrid=False,
            zeroline=False,
            tickmode="array",
            tickvals=tick_labels,
            ticktext=tick_labels,
            title_text=None,
        )
        fig.update_yaxes(showgrid=True, gridcolor=MOBILITY_COLORS["grid"], zeroline=False)
        for axis_name in [f"xaxis{suffix}" for suffix in ([""] + [str(index) for index in range(2, len(panel_series) + 1)])]:
            fig.layout[axis_name].update(categoryorder="array", categoryarray=category_orders["time_label"])
        if save_to_file:
            self._save_activity_time_series_svg(
                time_series,
                mode="stacked",
                output_path=self._svg_report_path("activity-time-series"),
            )
        fig.show("browser")
        return fig

    def _plot_activity_time_series_activity_panels(
        self,
        time_series: pl.DataFrame,
        save_to_file: bool = False,
    ):
        """Render one panel per activity, comparing survey and model occupancy."""
        activity_series = (
            time_series
            .filter(~pl.col("label").str.starts_with("in_transit:"))
            .with_columns(
                source=(
                    pl.when(pl.col("source") == "observed")
                    .then(pl.lit("Model"))
                    .when(pl.col("source") == "survey")
                    .then(pl.lit("Survey"))
                    .otherwise(pl.col("source"))
                ),
                label=pl.col("label").cast(pl.String),
            )
            .sort(["label", "source", "time_bin_start"])
        )
        if activity_series.is_empty():
            return None

        time_labels = (
            time_series
            .select(["time_bin_start", "time_label"])
            .unique()
            .sort("time_bin_start")
            .get_column("time_label")
            .to_list()
        )
        activities = sorted(activity_series["label"].unique().to_list())
        tick_labels = self._activity_time_tick_labels(time_labels)

        fig = px.line(
            activity_series.to_pandas(),
            x="time_label",
            y="n_persons",
            color="source",
            facet_col="label",
            facet_col_wrap=2,
            facet_col_spacing=0.08,
            facet_row_spacing=0.16,
            markers=True,
            category_orders={"time_label": time_labels, "source": ["Survey", "Model"]},
            color_discrete_map={
                "Survey": MOBILITY_COLORS["survey"],
                "Model": MOBILITY_COLORS["model"],
            },
            labels={
                "time_label": "Time of day",
                "n_persons": "Average persons in bin",
                "source": "source",
                "label": "activity",
            },
            title=f"Average occupancy by activity on {self.results.period}",
            width=980,
            height=max(520, 260 * math.ceil(len(activities) / 2)),
        )
        apply_report_layout(fig)
        fig.update_layout(
            paper_bgcolor=MOBILITY_COLORS["background"],
            plot_bgcolor=MOBILITY_COLORS["background"],
            title_text=None,
            margin={"l": 65, "r": 20, "t": 35, "b": 95},
            legend={
                "orientation": "h",
                "x": 0.0,
                "y": -0.12,
                "xanchor": "left",
                "yanchor": "top",
                "bgcolor": "rgba(255,255,255,0.9)",
            },
        )
        fig.for_each_annotation(lambda annotation: annotation.update(text=annotation.text.split("=")[-1], font_size=12))
        fig.update_yaxes(matches=None, showticklabels=True, title_text=None)
        fig.update_xaxes(
            matches=None,
            showticklabels=True,
            categoryorder="array",
            categoryarray=time_labels,
            showgrid=False,
            zeroline=False,
            tickmode="array",
            tickvals=tick_labels,
            ticktext=tick_labels,
            title_text=None,
        )
        fig.update_yaxes(showgrid=True, gridcolor=MOBILITY_COLORS["grid"], zeroline=False)
        if save_to_file:
            self._save_activity_time_series_svg(
                time_series,
                mode="activity_panels",
                output_path=self._svg_report_path("activity-time-series-activity-panels"),
            )
        fig.show("browser")
        return fig

    @staticmethod
    def _activity_time_tick_labels(time_labels: list[str]) -> list[str]:
        """Return a sparse set of time labels so plot axes stay readable."""
        if len(time_labels) <= 12:
            return time_labels
        step = max(1, math.ceil(len(time_labels) / 12))
        return time_labels[::step]

    def _svg_report_path(self, save_name: str) -> Path:
        """Return the standard report SVG path for this run."""
        return self._report_folder() / f"{self.results.inputs_hash}-{save_name}.svg"

    @staticmethod
    def _report_folder() -> Path:
        """Return the configured report output folder."""
        project_folder = os.environ.get("MOBILITY_PROJECT_DATA_FOLDER")
        if project_folder is None:
            raise ValueError(
                "save_to_file=True needs MOBILITY_PROJECT_DATA_FOLDER to be defined."
            )
        return Path(project_folder)

    def _save_activity_time_series_svg(
        self,
        time_series: pl.DataFrame,
        *,
        mode: Literal["stacked", "activity_panels"],
        output_path: Path,
    ) -> None:
        """Save the activity time-series plot as SVG without using a browser."""
        if mode == "activity_panels":
            self._save_activity_time_series_activity_panels_svg(time_series, output_path)
        else:
            self._save_activity_time_series_stacked_svg(time_series, output_path)

    def _save_activity_time_series_stacked_svg(
        self,
        time_series: pl.DataFrame,
        output_path: Path,
    ) -> None:
        """Save the stacked activity time-series plot as SVG."""
        output_path.parent.mkdir(parents=True, exist_ok=True)
        source_titles = {"survey": "Survey", "observed": "Model"}
        available_sources = [
            source
            for source in ["survey", "observed"]
            if source in time_series["source"].unique().to_list()
        ]
        time_labels = (
            time_series
            .select(["time_bin_start", "time_label"])
            .unique()
            .sort("time_bin_start")
            .get_column("time_label")
            .to_list()
        )
        labels = sorted(time_series["label"].unique().to_list())
        color_map = self._activity_time_series_color_map(labels)
        tick_labels = self._activity_time_tick_labels(time_labels)
        tick_positions = [time_labels.index(label) for label in tick_labels]

        fig, axes = plt.subplots(
            1,
            len(available_sources),
            sharey=True,
            figsize=(9.8, 5.6),
            squeeze=False,
        )
        for axis, source in zip(axes[0], available_sources, strict=True):
            source_series = (
                time_series
                .filter(pl.col("source") == source)
                .pivot(index="time_label", on="label", values="n_persons", aggregate_function="sum")
                .sort("time_label")
                .fill_null(0.0)
            )
            source_series = (
                source_series
                .to_pandas()
                .set_index("time_label")
                .reindex(time_labels, fill_value=0.0)
            )
            bottom = np.zeros(len(time_labels))
            for label in labels:
                values = (
                    source_series[label].to_numpy()
                    if label in source_series
                    else np.zeros(len(time_labels))
                )
                axis.bar(
                    np.arange(len(time_labels)),
                    values,
                    bottom=bottom,
                    color=color_map[label],
                    width=0.9,
                    label=label,
                )
                bottom += values
            axis.set_title(source_titles[source], fontsize=13)
            axis.set_xticks(tick_positions, tick_labels, rotation=0)
            axis.grid(axis="y", color=MOBILITY_COLORS["grid"], linewidth=0.8)
            axis.grid(axis="x", visible=False)
            axis.set_facecolor(MOBILITY_COLORS["background"])

        axes[0][0].set_ylabel("Nombre de personnes")
        handles, legend_labels = axes[0][0].get_legend_handles_labels()
        fig.legend(
            handles,
            legend_labels,
            loc="lower left",
            bbox_to_anchor=(0.06, 0.02),
            ncol=min(4, max(1, len(legend_labels))),
            frameon=False,
        )
        fig.patch.set_facecolor(MOBILITY_COLORS["background"])
        fig.subplots_adjust(left=0.07, right=0.98, top=0.92, bottom=0.18, wspace=0.08)
        fig.savefig(output_path, format="svg")
        plt.close(fig)

    def _save_activity_time_series_activity_panels_svg(
        self,
        time_series: pl.DataFrame,
        output_path: Path,
    ) -> None:
        """Save the activity-panel time-series plot as SVG."""
        output_path.parent.mkdir(parents=True, exist_ok=True)
        activity_series = (
            time_series
            .filter(~pl.col("label").str.starts_with("in_transit:"))
            .with_columns(
                source=(
                    pl.when(pl.col("source") == "observed")
                    .then(pl.lit("Model"))
                    .when(pl.col("source") == "survey")
                    .then(pl.lit("Survey"))
                    .otherwise(pl.col("source"))
                )
            )
        )
        if activity_series.is_empty():
            return

        time_labels = (
            time_series
            .select(["time_bin_start", "time_label"])
            .unique()
            .sort("time_bin_start")
            .get_column("time_label")
            .to_list()
        )
        activities = sorted(activity_series["label"].unique().to_list())
        tick_labels = self._activity_time_tick_labels(time_labels)
        tick_positions = [time_labels.index(label) for label in tick_labels]
        n_cols = 2
        n_rows = math.ceil(len(activities) / n_cols)
        fig, axes = plt.subplots(
            n_rows,
            n_cols,
            sharex=True,
            figsize=(9.8, max(5.2, 2.6 * n_rows)),
            squeeze=False,
        )
        colors = {"Survey": MOBILITY_COLORS["survey"], "Model": MOBILITY_COLORS["model"]}

        for axis, activity in zip(axes.ravel(), activities, strict=False):
            panel = (
                activity_series
                .filter(pl.col("label") == activity)
                .pivot(index="time_label", on="source", values="n_persons", aggregate_function="sum")
                .sort("time_label")
                .fill_null(0.0)
                .to_pandas()
                .set_index("time_label")
                .reindex(time_labels, fill_value=0.0)
            )
            for source in ["Survey", "Model"]:
                if source in panel:
                    axis.plot(
                        np.arange(len(time_labels)),
                        panel[source].to_numpy(),
                        color=colors[source],
                        linewidth=1.8,
                        label=source,
                    )
            axis.set_title(activity, fontsize=12)
            axis.set_xticks(tick_positions, tick_labels, rotation=0)
            axis.grid(axis="y", color=MOBILITY_COLORS["grid"], linewidth=0.8)
            axis.grid(axis="x", visible=False)
            axis.set_facecolor(MOBILITY_COLORS["background"])

        for axis in axes.ravel()[len(activities):]:
            axis.set_axis_off()

        handles, legend_labels = axes[0][0].get_legend_handles_labels()
        fig.legend(
            handles,
            legend_labels,
            loc="lower left",
            bbox_to_anchor=(0.06, 0.02),
            ncol=2,
            frameon=False,
        )
        fig.patch.set_facecolor(MOBILITY_COLORS["background"])
        fig.subplots_adjust(
            left=0.07,
            right=0.98,
            top=0.92,
            bottom=0.18,
            hspace=0.35,
            wspace=0.12,
        )
        fig.savefig(output_path, format="svg")
        plt.close(fig)

    @staticmethod
    def _activity_time_series_color_map(labels: list[str]) -> dict[str, str]:
        """Return stable, distinct colors for activity time-series labels."""
        fixed_colors = {
            "home": "#7f8c8d",
            "work": "#355CDE",
            "studies": "#FFBE3D",
            "shopping": "#E88AF2",
            "other": "#B8E986",
            "leisure": "#FF5C8A",
            "in_transit:car": "#4D4D4D",
            "in_transit:carpool": "#8C8C8C",
            "in_transit:walk": "#5F7F73",
            "in_transit:bicycle": "#0B5A66",
            "in_transit:public_transport": "#EF4B3E",
            "in_transit:car/public_transport/walk": "#EF4B3E",
            "in_transit:bicycle/public_transport/walk": "#D7191C",
            "in_transit:walk/public_transport/walk": "#F06A5A",
            "in_transit:other": "#7E6F9A",
        }
        fallback_palette = (
            px.colors.qualitative.Safe
            + px.colors.qualitative.Bold
            + px.colors.qualitative.Set3
        )

        color_map: dict[str, str] = {}
        fallback_index = 0
        for label in labels:
            if label in fixed_colors:
                color_map[label] = fixed_colors[label]
                continue

            while fallback_index < len(fallback_palette) and fallback_palette[fallback_index] in color_map.values():
                fallback_index += 1
            if fallback_index >= len(fallback_palette):
                fallback_index = 0
            color_map[label] = fallback_palette[fallback_index]
            fallback_index += 1

        return color_map

    def opportunity_occupation(
        self,
        plot_activity: str = None,
        mask_outliers: bool = False,
        inner_zone_residents_only: bool = True,
    ):
        """Compute opportunity occupation per destination and activity for this run.

        The returned table always includes the full modeled opportunity stock.
        Destinations with capacity but no realized occupation are kept with
        ``duration = 0`` and ``opportunity_occupation = 0``.
        """
        transport_zones_df = pl.DataFrame(
            self.results.transport_zones.get().drop("geometry", axis=1, errors="ignore")
        )
        resident_zone_scope = (
            transport_zones_df.filter(pl.col("is_inner_zone")).lazy()
            if inner_zone_residents_only
            else transport_zones_df.lazy()
        )
        destination_zone_flags = (
            transport_zones_df
            .select(
                pl.col("transport_zone_id").alias("to"),
                pl.col("is_inner_zone").alias("destination_is_inner_zone"),
            )
            .lazy()
        )

        opportunity_occupation = (
            self.results.opportunities
            .select(["to", "activity", "opportunity_capacity"])
            .with_columns(pl.col("activity").cast(pl.String()))
            .join(destination_zone_flags, on="to", how="left")
            .join(
                self.results.plan_steps.filter(pl.col("activity_seq_id") != 0)
                .rename({"home_zone_id": "transport_zone_id"})
                .join(
                    resident_zone_scope.select("transport_zone_id"),
                    on=["transport_zone_id"],
                    how="inner",
                )
                .with_columns(pl.col("activity").cast(pl.String()))
                .group_by(["to", "activity"])
                .agg(pl.col("duration").sum()),
                on=["to", "activity"],
                how="left",
            )
            .with_columns(
                duration=pl.col("duration").fill_null(0.0),
                opportunity_occupation=(
                    pl.col("duration").fill_null(0.0) / pl.col("opportunity_capacity")
                ),
            )
            .rename({"to": "transport_zone_id"})
            .collect(engine="streaming")
        )

        if plot_activity:
            tz = self.results.transport_zones.get().to_crs(4326)
            tz = tz.merge(transport_zones_df.to_pandas(), on="transport_zone_id")
            tz = tz.merge(
                opportunity_occupation
                .filter(pl.col("activity") == plot_activity)
                .to_pandas(),
                on="transport_zone_id",
                how="left",
            )
            tz["opportunity_occupation"] = tz["opportunity_occupation"].fillna(0.0)
            if mask_outliers:
                tz["opportunity_occupation"] = self.mask_outliers(tz["opportunity_occupation"])
            self.plot_map(tz, "opportunity_occupation", plot_activity)

        return opportunity_occupation

    def trip_count_by_demand_group(self, plot: bool = False, mask_outliers: bool = False):
        """Count trips and trips per person by demand group for this run."""
        transport_zones_df = (
            pl.DataFrame(self.results.transport_zones.get().drop("geometry", axis=1))
            .filter(pl.col("is_inner_zone"))
            .lazy()
        )

        trip_count = (
            self.results.plan_steps.filter(pl.col("activity_seq_id") != 0)
            .rename({"home_zone_id": "transport_zone_id"})
            .join(transport_zones_df.select(["transport_zone_id", "local_admin_unit_id"]), on=["transport_zone_id"])
            .group_by(["transport_zone_id", "csp", "n_cars"])
            .agg(n_trips=pl.col("n_persons").sum())
            .join(self.results.demand_groups.rename({"home_zone_id": "transport_zone_id"}), on=["transport_zone_id", "csp", "n_cars"])
            .with_columns(n_trips_per_person=pl.col("n_trips") / pl.col("n_persons"))
            .collect(engine="streaming")
        )

        if plot:
            tz = self.results.transport_zones.get().to_crs(4326)
            tz = tz.merge(transport_zones_df.collect().to_pandas(), on="transport_zone_id")
            tz = tz.merge(
                trip_count.group_by(["transport_zone_id"])
                .agg(n_trips_per_person=pl.col("n_trips").sum() / pl.col("n_persons").sum())
                .to_pandas(),
                on="transport_zone_id",
                how="left",
            )
            tz["n_trips_per_person"] = tz["n_trips_per_person"].fillna(0.0)
            if mask_outliers:
                tz["n_trips_per_person"] = self.mask_outliers(tz["n_trips_per_person"])
            self.plot_map(tz, "n_trips_per_person")

        return trip_count

    def metric_per_person(
        self,
        metric: str,
        plot: bool = False,
        mask_outliers: bool = False,
        compare_with=None,
        plot_delta: bool = False,
    ):
        """Aggregate a metric and metric-per-person by demand group for this run."""
        transport_zones_df = (
            pl.DataFrame(self.results.transport_zones.get().drop("geometry", axis=1))
            .filter(pl.col("is_inner_zone"))
            .lazy()
        )

        metric_per_person = metric + "_per_person"

        metric_per_groups_and_transport_zones = (
            self.results.plan_steps.filter(pl.col("activity_seq_id") != 0)
            .rename({"home_zone_id": "transport_zone_id"})
            .join(self.results.costs, on=["from", "to", "mode"])
            .group_by(["transport_zone_id", "csp", "n_cars"])
            .agg(metric=(pl.col(metric) * pl.col("n_persons")).sum())
            .join(transport_zones_df.select(["transport_zone_id", "local_admin_unit_id"]), on=["transport_zone_id"])
            .join(self.results.demand_groups.rename({"home_zone_id": "transport_zone_id"}), on=["transport_zone_id", "csp", "n_cars"])
            .with_columns(metric_per_person=pl.col("metric") / pl.col("n_persons"))
            .rename({"metric": metric, "metric_per_person": metric_per_person})
            .collect(engine="streaming")
        )

        if compare_with is not None:
            comparison = self._resolve_run_context(compare_with, include_costs=True)
            reference_run = comparison["run"]
            plan_steps_path = reference_run.cache_path.get("plan_steps")
            if plan_steps_path is None:
                raise TypeError("compare_with run needs `cache_path['plan_steps']`.")
            if Path(plan_steps_path).exists() is False and callable(getattr(reference_run, "get", None)):
                reference_run.get()
            plan_steps_comp = pl.scan_parquet(plan_steps_path)
            costs_comp = comparison["costs"]
            demand_groups_comp = comparison["demand_groups"]
            if isinstance(costs_comp, pl.DataFrame):
                costs_comp = costs_comp.lazy()
            if isinstance(demand_groups_comp, pl.DataFrame):
                demand_groups_comp = demand_groups_comp.lazy()

            metric_comp = metric + "_comp"
            metric_per_person_comp = metric + "_per_person_comp"
            metric_per_groups_and_transport_zones_comp = (
                plan_steps_comp.filter(pl.col("activity_seq_id") != 0)
                .rename({"home_zone_id": "transport_zone_id"})
                .join(costs_comp, on=["from", "to", "mode"])
                .group_by(["transport_zone_id", "csp", "n_cars"])
                .agg(metric=(pl.col(metric) * pl.col("n_persons")).sum())
                .join(transport_zones_df.select(["transport_zone_id", "local_admin_unit_id"]), on=["transport_zone_id"])
                .join(
                    demand_groups_comp.rename({"home_zone_id": "transport_zone_id"}),
                    on=["transport_zone_id", "csp", "n_cars"],
                )
                .with_columns(metric_per_person=pl.col("metric") / pl.col("n_persons"))
                .rename({"metric": metric_comp, "metric_per_person": metric_per_person_comp})
                .collect(engine="streaming")
            )
            metric_per_groups_and_transport_zones = (
                metric_per_groups_and_transport_zones.join(
                    metric_per_groups_and_transport_zones_comp.select(
                        [
                            "transport_zone_id",
                            "csp",
                            "n_cars",
                            "n_persons",
                            "local_admin_unit_id",
                            metric_comp,
                            metric_per_person_comp,
                        ]
                    ),
                    on=["transport_zone_id", "csp", "n_cars"],
                )
                .with_columns(delta=pl.col(metric_per_person) - pl.col(metric_per_person_comp))
            )

        if plot or plot_delta:
            tz = self.results.transport_zones.get().to_crs(4326)
            tz = tz.merge(transport_zones_df.collect().to_pandas(), on="transport_zone_id")
            tz = tz.merge(
                metric_per_groups_and_transport_zones.group_by(["transport_zone_id"])
                .agg(metric_per_person=pl.col(metric).sum() / pl.col("n_persons").sum())
                .rename({"metric_per_person": metric_per_person})
                .to_pandas(),
                on="transport_zone_id",
                how="left",
            )

            if plot_delta:
                tz = tz.merge(
                    metric_per_groups_and_transport_zones.group_by(["transport_zone_id"])
                    .agg(metric_per_person_comp=pl.col(metric_comp).sum() / pl.col("n_persons").sum())
                    .rename({"metric_per_person_comp": metric_per_person_comp})
                    .to_pandas(),
                    on="transport_zone_id",
                    how="left",
                )
                tz["delta"] = tz[metric_per_person] - tz[metric_per_person_comp]

            if plot:
                tz[metric_per_person] = tz[metric_per_person].fillna(0.0)
                if mask_outliers:
                    tz[metric_per_person] = self.mask_outliers(tz[metric_per_person])
                self.plot_map(tz, metric_per_person)

            if plot_delta:
                tz["delta"] = tz["delta"].fillna(0.0)
                if mask_outliers:
                    tz["delta"] = self.mask_outliers(tz["delta"])
                self.plot_map(tz, "delta", color_continuous_scale="RdBu_r", color_continuous_midpoint=0)

        return metric_per_groups_and_transport_zones

    def distance_per_person(self, *args, **kwargs):
        return self.metric_per_person("distance", *args, **kwargs)

    def ghg_per_person(self, *args, **kwargs):
        return self.metric_per_person("ghg_emissions_per_trip", *args, **kwargs)

    def time_per_person(self, *args, **kwargs):
        return self.metric_per_person("time", *args, **kwargs)

    def cost_per_person(self, *args, **kwargs):
        return self.metric_per_person("cost", *args, **kwargs)

    def modal_share_evolution_by_iteration(
        self,
        iterations: list[int] | range | None = None,
        modes: list[str] | None = None,
        aggregate_public_transport: bool = True,
        inner_zones_only: bool = False,
        chart_type: Literal["stacked_bar", "line"] = "stacked_bar",
        plot: bool = True,
        plot_method: str = "browser",
        save_to_file: bool = False,
        output_path: str | Path | None = None,
        width: int = 980,
        height: int = 560,
        return_figure: bool = False,
    ) -> pl.DataFrame | tuple[pl.DataFrame, go.Figure]:
        """Plot modal-share evolution as stacked bars over saved iterations."""
        iterations = self._modal_share_evolution_iterations(iterations)
        if modes is not None and not modes:
            raise ValueError("modes should contain at least one mode.")
        if chart_type not in {"stacked_bar", "line"}:
            raise ValueError("chart_type should be one of: stacked_bar, line.")

        evolution = self._modal_share_evolution_table(
            iterations=iterations,
            modes=modes,
            aggregate_public_transport=aggregate_public_transport,
            inner_zones_only=inner_zones_only,
        )

        fig = None
        if plot or save_to_file or output_path is not None or return_figure:
            fig = self._plot_modal_share_evolution(
                evolution,
                width=width,
                height=height,
                chart_type=chart_type,
            )

        if save_to_file or output_path is not None:
            svg_path = (
                Path(output_path)
                if output_path is not None
                else self._svg_report_path(f"{self.results.period}-modal-share-evolution-by-iteration")
            )
            self._save_modal_share_evolution_svg(
                evolution,
                output_path=svg_path,
                width=width,
                height=height,
                chart_type=chart_type,
            )

        if plot and fig is not None:
            fig.show(plot_method)

        if return_figure:
            return evolution, fig
        return evolution

    def modal_share_delta_evolution_by_iteration(
        self,
        compare_with: Any,
        iterations: list[int] | range | None = None,
        modes: list[str] | None = None,
        aggregate_public_transport: bool = True,
        inner_zones_only: bool = False,
        plot: bool = True,
        plot_method: str = "browser",
        save_to_file: bool = False,
        output_path: str | Path | None = None,
        width: int = 980,
        height: int = 560,
        return_figure: bool = False,
    ) -> pl.DataFrame | tuple[pl.DataFrame, go.Figure]:
        """Plot modal-share deltas over iterations against another scenario."""
        iterations = self._modal_share_evolution_iterations(iterations)
        if modes is not None and not modes:
            raise ValueError("modes should contain at least one mode.")

        reference_run = self._resolve_comparison_run(compare_with)
        reference_demand_groups = self._resolve_comparison_demand_groups(
            compare_with=compare_with,
            reference_run=reference_run,
        )
        current_raw = self._modal_share_evolution_raw_table(
            iterations=iterations,
            run=self.results.run,
            demand_groups=self.results.demand_groups,
            aggregate_public_transport=aggregate_public_transport,
            inner_zones_only=inner_zones_only,
        )
        reference_raw = self._modal_share_evolution_raw_table(
            iterations=iterations,
            run=reference_run,
            demand_groups=reference_demand_groups,
            aggregate_public_transport=aggregate_public_transport,
            inner_zones_only=inner_zones_only,
        )
        if modes is None:
            modes = self._ordered_modes(
                list(dict.fromkeys(current_raw["mode"].to_list() + reference_raw["mode"].to_list()))
            )
        else:
            modes = [
                self._modal_share_mode_key(
                    mode,
                    aggregate_public_transport=aggregate_public_transport,
                )
                for mode in modes
            ]
            modes = self._ordered_modes(list(dict.fromkeys(modes)))

        current = self._complete_modal_share_evolution_table(
            current_raw,
            iterations=iterations,
            modes=modes,
        )
        reference = self._complete_modal_share_evolution_table(
            reference_raw,
            iterations=iterations,
            modes=modes,
        )
        delta = (
            current
            .join(
                reference.rename(
                    {
                        "n_trips": "n_trips_reference",
                        "modal_share": "modal_share_reference",
                    }
                ),
                on=["iteration", "mode", "mode_label"],
                how="inner",
            )
            .with_columns(
                n_trips_delta=pl.col("n_trips") - pl.col("n_trips_reference"),
                modal_share_delta=pl.col("modal_share") - pl.col("modal_share_reference"),
            )
            .select(
                [
                    "iteration",
                    "mode",
                    "mode_label",
                    "n_trips",
                    "n_trips_reference",
                    "n_trips_delta",
                    "modal_share",
                    "modal_share_reference",
                    "modal_share_delta",
                ]
            )
        )

        fig = None
        if plot or save_to_file or output_path is not None or return_figure:
            fig = self._plot_modal_share_delta_evolution(
                delta,
                width=width,
                height=height,
            )

        if save_to_file or output_path is not None:
            svg_path = (
                Path(output_path)
                if output_path is not None
                else self._svg_report_path(f"{self.results.period}-modal-share-delta-evolution-by-iteration")
            )
            self._save_modal_share_delta_evolution_svg(
                delta,
                output_path=svg_path,
                width=width,
                height=height,
            )

        if plot and fig is not None:
            fig.show(plot_method)

        if return_figure:
            return delta, fig
        return delta

    def _modal_share_evolution_iterations(
        self,
        iterations: list[int] | range | None,
    ) -> list[int]:
        """Return the iterations used by modal-share evolution plots."""
        if iterations is None:
            run_parameters = getattr(self.results.parameters, "run", None)
            n_iterations = getattr(run_parameters, "n_iterations", None)
            if n_iterations is None:
                raise ValueError(
                    "iterations should be provided when results.parameters.run has no n_iterations."
                )
            iterations = range(1, int(n_iterations) + 1)

        iterations = [int(iteration) for iteration in iterations]
        if not iterations:
            raise ValueError("iterations should contain at least one iteration.")
        if any(iteration < 1 for iteration in iterations):
            raise ValueError("iterations should be greater than or equal to 1.")
        return iterations

    def _modal_share_evolution_table(
        self,
        *,
        iterations: list[int],
        modes: list[str] | None,
        aggregate_public_transport: bool,
        inner_zones_only: bool,
    ) -> pl.DataFrame:
        """Return one modal-share row per iteration and mode."""
        modal_share = self._modal_share_evolution_raw_table(
            iterations=iterations,
            run=self.results.run,
            demand_groups=self.results.demand_groups,
            aggregate_public_transport=aggregate_public_transport,
            inner_zones_only=inner_zones_only,
        )
        if modes is None:
            modes = self._ordered_modes(modal_share["mode"].unique().to_list())
        else:
            modes = [
                self._modal_share_mode_key(mode, aggregate_public_transport=aggregate_public_transport)
                for mode in modes
            ]
            modes = self._ordered_modes(list(dict.fromkeys(modes)))

        return self._complete_modal_share_evolution_table(
            modal_share,
            iterations=iterations,
            modes=modes,
        )

    def _modal_share_evolution_raw_table(
        self,
        *,
        iterations: list[int],
        run: Any,
        demand_groups: pl.DataFrame | pl.LazyFrame,
        aggregate_public_transport: bool,
        inner_zones_only: bool,
    ) -> pl.DataFrame:
        """Return observed modal shares without adding missing mode rows."""
        iteration_tables = [
            self._modal_share_for_iteration(
                iteration,
                run=run,
                demand_groups=demand_groups,
                aggregate_public_transport=aggregate_public_transport,
                inner_zones_only=inner_zones_only,
            ).with_columns(iteration=pl.lit(iteration))
            for iteration in iterations
        ]
        return pl.concat(iteration_tables, how="vertical_relaxed")

    def _complete_modal_share_evolution_table(
        self,
        modal_share: pl.DataFrame,
        *,
        iterations: list[int],
        modes: list[str],
    ) -> pl.DataFrame:
        """Add missing iteration-mode rows with zero shares."""
        iteration_mode_grid = pl.DataFrame(
            {
                "iteration": [
                    iteration
                    for iteration in iterations
                    for _mode in modes
                ],
                "mode": [
                    mode
                    for _iteration in iterations
                    for mode in modes
                ],
                "mode_order": [
                    mode_order
                    for _iteration in iterations
                    for mode_order, _mode in enumerate(modes)
                ],
            }
        )
        return (
            iteration_mode_grid
            .join(modal_share, on=["iteration", "mode"], how="left")
            .with_columns(
                pl.col("n_trips").fill_null(0.0),
                pl.col("modal_share").fill_null(0.0),
                mode_label=pl.col("mode").map_elements(
                    self._mode_label,
                    return_dtype=pl.String,
                ),
            )
            .sort(["iteration", "mode_order"])
            .select(["iteration", "mode", "mode_label", "n_trips", "modal_share"])
        )

    def _modal_share_for_iteration(
        self,
        iteration: int,
        *,
        run: Any,
        demand_groups: pl.DataFrame | pl.LazyFrame,
        aggregate_public_transport: bool,
        inner_zones_only: bool,
    ) -> pl.DataFrame:
        """Compute modal shares over mobile trips in one saved iteration."""
        plan_steps = self._saved_iteration_plan_steps(run, iteration).lazy()
        if inner_zones_only:
            plan_steps = self._join_home_zone(plan_steps, demand_groups)

        required_columns = {"activity_seq_id", "mode", "n_persons"}
        if inner_zones_only:
            required_columns.add("home_zone_id")
        missing_columns = required_columns.difference(plan_steps.collect_schema().names())
        if missing_columns:
            missing = ", ".join(sorted(missing_columns))
            raise ValueError(f"Saved iteration plan steps are missing: {missing}.")

        mobile_steps = (
            plan_steps
            .filter(pl.col("activity_seq_id") != 0)
            .with_columns(pl.col("mode").cast(pl.String))
        )
        if inner_zones_only:
            mobile_steps = mobile_steps.filter(
                pl.col("home_zone_id").is_in(self._inner_transport_zone_ids())
            )
        if aggregate_public_transport:
            mobile_steps = mobile_steps.with_columns(
                mode=pl.when(pl.col("mode").str.contains("public_transport"))
                .then(pl.lit("public_transport"))
                .otherwise(pl.col("mode"))
            )

        return (
            mobile_steps
            .group_by("mode")
            .agg(n_trips=pl.col("n_persons").sum())
            .with_columns(total_trips=pl.col("n_trips").sum())
            .with_columns(
                modal_share=pl.when(pl.col("total_trips") > 0.0)
                .then(pl.col("n_trips") / pl.col("total_trips"))
                .otherwise(0.0)
            )
            .select(["mode", "n_trips", "modal_share"])
            .collect(engine="streaming")
        )

    def _plot_modal_share_evolution(
        self,
        modal_share: pl.DataFrame,
        *,
        width: int,
        height: int,
        chart_type: Literal["stacked_bar", "line"],
    ) -> go.Figure:
        """Render a modal-share evolution chart."""
        mode_labels = modal_share["mode_label"].unique(maintain_order=True).to_list()
        color_map = self._mode_label_color_map(mode_labels)
        figure_kwargs = {
            "data_frame": modal_share.to_pandas(),
            "x": "iteration",
            "y": "modal_share",
            "color": "mode_label",
            "color_discrete_map": color_map,
            "category_orders": {"mode_label": mode_labels},
            "labels": {
                "iteration": "Iteration",
                "modal_share": "Modal share",
                "mode_label": "Mode",
            },
            "width": width,
            "height": height,
        }
        if chart_type == "line":
            fig = px.line(**figure_kwargs, markers=True)
        else:
            fig = px.bar(**figure_kwargs)
        apply_report_layout(fig)
        fig.update_layout(
            title_text=None,
            xaxis_title="Iteration",
            yaxis_title="Modal share",
            legend_title_text=None,
            margin={"l": 65, "r": 20, "t": 35, "b": 95},
        )
        if chart_type == "stacked_bar":
            fig.update_layout(barmode="stack")
        fig.update_xaxes(dtick=1, showgrid=False, zeroline=False)
        fig.update_yaxes(
            range=[0.0, 1.0],
            tickformat=".0%",
            showgrid=True,
            gridcolor=MOBILITY_COLORS["grid"],
            zeroline=False,
        )
        return fig

    def _plot_modal_share_delta_evolution(
        self,
        modal_share_delta: pl.DataFrame,
        *,
        width: int,
        height: int,
    ) -> go.Figure:
        """Render modal-share deltas over iterations as a line chart."""
        mode_labels = modal_share_delta["mode_label"].unique(maintain_order=True).to_list()
        color_map = self._mode_label_color_map(mode_labels)
        fig = px.line(
            modal_share_delta.to_pandas(),
            x="iteration",
            y="modal_share_delta",
            color="mode_label",
            markers=True,
            color_discrete_map=color_map,
            category_orders={"mode_label": mode_labels},
            labels={
                "iteration": "Iteration",
                "modal_share_delta": "Modal share difference",
                "mode_label": "Mode",
            },
            width=width,
            height=height,
        )
        apply_report_layout(fig)
        fig.update_layout(
            title_text=None,
            xaxis_title="Iteration",
            yaxis_title="Modal share difference",
            legend_title_text=None,
            margin={"l": 65, "r": 20, "t": 35, "b": 95},
        )
        fig.update_xaxes(dtick=1, showgrid=False, zeroline=False)
        fig.update_yaxes(
            tickformat=".1%",
            showgrid=True,
            gridcolor=MOBILITY_COLORS["grid"],
            zeroline=True,
            zerolinecolor=MOBILITY_COLORS["label"],
            zerolinewidth=1,
        )
        fig.add_hline(
            y=0.0,
            line_color=MOBILITY_COLORS["label"],
            line_width=1,
        )
        return fig

    def _save_modal_share_evolution_svg(
        self,
        modal_share: pl.DataFrame,
        *,
        output_path: Path,
        width: int,
        height: int,
        chart_type: Literal["stacked_bar", "line"],
    ) -> None:
        """Save a modal-share evolution chart as SVG."""
        output_path.parent.mkdir(parents=True, exist_ok=True)
        share_df = modal_share.to_pandas()
        iterations = sorted(share_df["iteration"].unique().tolist())
        mode_labels = share_df["mode_label"].drop_duplicates().tolist()
        color_map = self._mode_label_color_map(mode_labels)
        pivot = (
            share_df
            .pivot_table(
                index="iteration",
                columns="mode_label",
                values="modal_share",
                aggfunc="sum",
                fill_value=0.0,
            )
            .reindex(iterations, fill_value=0.0)
        )

        fig, axis = plt.subplots(figsize=(width / 100.0, height / 100.0))
        x = np.arange(len(iterations))
        if chart_type == "line":
            for mode_label in mode_labels:
                values = (
                    pivot[mode_label].to_numpy()
                    if mode_label in pivot
                    else np.zeros(len(iterations))
                )
                axis.plot(
                    x,
                    values,
                    color=color_map[mode_label],
                    label=mode_label,
                    linewidth=1.9,
                    marker="o",
                    markersize=4.0,
                )
        else:
            bottom = np.zeros(len(iterations))
            for mode_label in mode_labels:
                values = (
                    pivot[mode_label].to_numpy()
                    if mode_label in pivot
                    else np.zeros(len(iterations))
                )
                axis.bar(
                    x,
                    values,
                    bottom=bottom,
                    color=color_map[mode_label],
                    label=mode_label,
                    width=0.75,
                )
                bottom += values

        axis.set_ylim(0.0, 1.0)
        axis.set_ylabel("Modal share")
        axis.set_xlabel("Iteration")
        axis.set_xticks(x, [str(iteration) for iteration in iterations])
        tick_values = np.linspace(0.0, 1.0, 6)
        axis.set_yticks(tick_values, [f"{int(value * 100)}%" for value in tick_values])
        axis.grid(axis="y", color=MOBILITY_COLORS["grid"], linewidth=0.8)
        axis.grid(axis="x", visible=False)
        axis.set_facecolor(MOBILITY_COLORS["background"])
        axis.legend(
            loc="lower left",
            bbox_to_anchor=(0.0, -0.23),
            ncol=min(4, max(1, len(mode_labels))),
            frameon=False,
        )
        fig.patch.set_facecolor(MOBILITY_COLORS["background"])
        fig.subplots_adjust(left=0.08, right=0.98, top=0.94, bottom=0.24)
        fig.savefig(output_path, format="svg")
        plt.close(fig)

    def _save_modal_share_delta_evolution_svg(
        self,
        modal_share_delta: pl.DataFrame,
        *,
        output_path: Path,
        width: int,
        height: int,
    ) -> None:
        """Save a modal-share-delta line chart as SVG."""
        output_path.parent.mkdir(parents=True, exist_ok=True)
        delta_df = modal_share_delta.to_pandas()
        iterations = sorted(delta_df["iteration"].unique().tolist())
        mode_labels = delta_df["mode_label"].drop_duplicates().tolist()
        color_map = self._mode_label_color_map(mode_labels)
        pivot = (
            delta_df
            .pivot_table(
                index="iteration",
                columns="mode_label",
                values="modal_share_delta",
                aggfunc="sum",
                fill_value=0.0,
            )
            .reindex(iterations, fill_value=0.0)
        )

        fig, axis = plt.subplots(figsize=(width / 100.0, height / 100.0))
        x = np.arange(len(iterations))
        for mode_label in mode_labels:
            values = (
                pivot[mode_label].to_numpy()
                if mode_label in pivot
                else np.zeros(len(iterations))
            )
            axis.plot(
                x,
                values,
                color=color_map[mode_label],
                label=mode_label,
                linewidth=1.9,
                marker="o",
                markersize=4.0,
            )

        axis.axhline(0.0, color=MOBILITY_COLORS["label"], linewidth=1.0)
        axis.set_ylabel("Modal share difference")
        axis.set_xlabel("Iteration")
        axis.set_xticks(x, [str(iteration) for iteration in iterations])
        axis.yaxis.set_major_formatter(mticker.PercentFormatter(xmax=1.0))
        axis.grid(axis="y", color=MOBILITY_COLORS["grid"], linewidth=0.8)
        axis.grid(axis="x", visible=False)
        axis.set_facecolor(MOBILITY_COLORS["background"])
        axis.legend(
            loc="lower left",
            bbox_to_anchor=(0.0, -0.23),
            ncol=min(4, max(1, len(mode_labels))),
            frameon=False,
        )
        fig.patch.set_facecolor(MOBILITY_COLORS["background"])
        fig.subplots_adjust(left=0.08, right=0.98, top=0.94, bottom=0.24)
        fig.savefig(output_path, format="svg")
        plt.close(fig)

    def plot_delta_by_iteration(
        self,
        compare_with: Any | None,
        iteration: int,
        value: Literal["modal_share", "n_trips", "mean_utility", "ghg_emissions"],
        mode: str = "car",
        zone: Literal["home", "origin", "destination"] = "home",
        plot: bool = True,
        plot_method: str = "browser",
        save_to_file: bool = False,
        inner_zones_only: bool = False,
        relative_delta: bool = False,
        paired_runs: list[tuple[Any, Any]] | None = None,
        color_range: float | tuple[float, float] | None = None,
        auto_cap_outliers: bool = False,
        outlier_quantile: float = 0.98,
        labels: bool = True,
        width: int = 1200,
        height: int = 850,
        max_labels: int = 30,
        simplify_tolerance: float | None = 50.0,
        return_figure: bool = False,
    ) -> pl.DataFrame | tuple[pl.DataFrame, go.Figure]:
        """Plot a transport-zone delta map for one saved iteration."""
        if iteration < 1:
            raise ValueError("iteration must be greater than or equal to 1.")
        if zone not in {"home", "origin", "destination"}:
            raise ValueError("zone should be one of: home, origin, destination.")
        if value not in {"modal_share", "n_trips", "mean_utility", "ghg_emissions"}:
            raise ValueError(
                "value should be one of: modal_share, n_trips, mean_utility, ghg_emissions."
            )
        if value == "mean_utility" and zone != "home":
            raise ValueError("mean_utility can only be mapped by home zone.")
        if value == "modal_share" and relative_delta:
            raise ValueError("relative_delta=True is not available for modal_share.")
        if not 0.0 < outlier_quantile <= 1.0:
            raise ValueError("outlier_quantile should be greater than 0 and at most 1.")

        if paired_runs is None:
            if compare_with is None:
                raise ValueError("compare_with should be provided when paired_runs is not used.")
            reference_run = self._resolve_comparison_run(compare_with)
            reference_demand_groups = self._resolve_comparison_demand_groups(
                compare_with=compare_with,
                reference_run=reference_run,
            )
            current_costs = self.results.costs if value == "ghg_emissions" else pl.DataFrame()
            reference_costs = (
                self._resolve_comparison_costs(
                    compare_with=compare_with,
                    reference_run=reference_run,
                )
                if value == "ghg_emissions"
                else pl.DataFrame()
            )
            delta = self._delta_value_by_iteration(
                reference_run=reference_run,
                reference_demand_groups=reference_demand_groups,
                current_costs=current_costs,
                reference_costs=reference_costs,
                iteration=iteration,
                value=value,
                mode=mode,
                zone=zone,
                relative_delta=relative_delta,
            )
            global_delta = None
        else:
            delta, global_delta = self._paired_delta_by_iteration(
                paired_runs=paired_runs,
                iteration=iteration,
                value=value,
                mode=mode,
                zone=zone,
                inner_zones_only=inner_zones_only,
                relative_delta=relative_delta,
            )
        value_settings = self._plot_delta_value_settings(
            value=value,
            mode=mode,
            relative_delta=relative_delta,
        )

        fig = None
        if plot or save_to_file or return_figure:
            if global_delta is None:
                global_delta = self._global_plot_delta_by_iteration(
                    reference_run=reference_run,
                    reference_demand_groups=reference_demand_groups,
                    current_costs=current_costs,
                    reference_costs=reference_costs,
                    iteration=iteration,
                    value=value,
                    mode=mode,
                    zone=zone,
                    inner_zones_only=inner_zones_only,
                    relative_delta=relative_delta,
                )
            range_color = self._modal_share_delta_color_range(
                delta,
                self._automatic_delta_color_range(
                    delta,
                    color_range=color_range,
                    auto_cap_outliers=auto_cap_outliers,
                    outlier_quantile=outlier_quantile,
                ),
                value_column="value_delta",
            )
            fig = TransportZoneMaps(
                self.results.transport_zones,
                population=getattr(self.results.run, "population", None),
                max_labels=max_labels,
                simplify_tolerance=simplify_tolerance,
            ).metric(
                delta,
                value_column="value_delta",
                save_name=(
                    f"{self.results.period}-{value_settings['save_name']}"
                    f"-iteration-{iteration}-map"
                ),
                save_to_file=save_to_file,
                inner_zones_only=inner_zones_only,
                labels=labels,
                width=width,
                height=height,
                hover_columns=[
                    column
                    for column in ["value", "value_reference", "value_delta_std", "n_pairs"]
                    if column in delta.columns
                ],
                legend_label=value_settings["legend_label"],
                frame_title=self._plot_delta_frame_title(
                    iteration=iteration,
                    global_delta=global_delta,
                    value=value,
                    relative_delta=relative_delta,
                ),
                classify=False,
                color_continuous_scale=[
                    [0.0, "#44546A"],
                    [0.5, "#FFFFFF"],
                    [1.0, "#D71A1C"],
                ],
                color_continuous_midpoint=0.0,
                range_color=range_color,
                colorbar_tickformat=value_settings["colorbar_tickformat"],
            )
            if plot:
                fig.show(plot_method)

        if return_figure:
            return delta, fig
        return delta

    def _delta_value_by_iteration(
        self,
        *,
        reference_run: Any,
        reference_demand_groups: pl.DataFrame | pl.LazyFrame,
        current_costs: pl.DataFrame | pl.LazyFrame,
        reference_costs: pl.DataFrame | pl.LazyFrame,
        iteration: int,
        value: Literal["modal_share", "n_trips", "mean_utility", "ghg_emissions"],
        mode: str,
        zone: Literal["home", "origin", "destination"],
        relative_delta: bool,
    ) -> pl.DataFrame:
        """Return current, reference and delta values by transport zone."""
        return self._delta_value_between_runs_by_iteration(
            current_run=self.results.run,
            current_demand_groups=self.results.demand_groups,
            reference_run=reference_run,
            reference_demand_groups=reference_demand_groups,
            current_costs=current_costs,
            reference_costs=reference_costs,
            iteration=iteration,
            value=value,
            mode=mode,
            zone=zone,
            relative_delta=relative_delta,
        )

    def _delta_value_between_runs_by_iteration(
        self,
        *,
        current_run: Any,
        current_demand_groups: pl.DataFrame | pl.LazyFrame,
        reference_run: Any,
        reference_demand_groups: pl.DataFrame | pl.LazyFrame,
        current_costs: pl.DataFrame | pl.LazyFrame,
        reference_costs: pl.DataFrame | pl.LazyFrame,
        iteration: int,
        value: Literal["modal_share", "n_trips", "mean_utility", "ghg_emissions"],
        mode: str,
        zone: Literal["home", "origin", "destination"],
        relative_delta: bool,
    ) -> pl.DataFrame:
        """Return current, reference and delta values between two run contexts."""
        current = self._value_by_zone_for_saved_iteration(
            run=current_run,
            demand_groups=current_demand_groups,
            costs=current_costs,
            iteration=iteration,
            value=value,
            mode=mode,
            zone=zone,
        )
        reference = self._value_by_zone_for_saved_iteration(
            run=reference_run,
            demand_groups=reference_demand_groups,
            costs=reference_costs,
            iteration=iteration,
            value=value,
            mode=mode,
            zone=zone,
        )

        return (
            self._transport_zone_ids()
            .join(current, on="transport_zone_id", how="left")
            .join(
                reference.rename({"value": "value_reference"}),
                on="transport_zone_id",
                how="left",
            )
            .with_columns(
                pl.col("value").fill_null(0.0),
                pl.col("value_reference").fill_null(0.0),
            )
            .with_columns(
                value_delta=self._plot_delta_expression(relative_delta=relative_delta),
                value_name=pl.lit(value),
            )
            .sort("transport_zone_id")
        )

    def _value_by_zone_for_saved_iteration(
        self,
        *,
        run: Any,
        demand_groups: pl.DataFrame | pl.LazyFrame,
        costs: pl.DataFrame | pl.LazyFrame,
        iteration: int,
        value: Literal["modal_share", "n_trips", "mean_utility", "ghg_emissions"],
        mode: str,
        zone: Literal["home", "origin", "destination"],
    ) -> pl.DataFrame:
        """Compute one selected value by transport zone for a saved iteration."""
        if value in {"modal_share", "n_trips"}:
            column = "modal_share" if value == "modal_share" else "n_trips"
            return self._modal_share_for_saved_iteration(
                run=run,
                demand_groups=demand_groups,
                iteration=iteration,
                zone=zone,
                mode=mode,
            ).select(["transport_zone_id", pl.col(column).alias("value")])
        if value == "mean_utility":
            return self._mean_utility_by_home_zone_for_saved_iteration(
                run=run,
                demand_groups=demand_groups,
                iteration=iteration,
            )
        return self._ghg_emissions_by_zone_for_saved_iteration(
            run=run,
            demand_groups=demand_groups,
            costs=costs,
            iteration=iteration,
            zone=zone,
        )

    def _metric_by_zone_for_saved_iteration(
        self,
        *,
        run: Any,
        demand_groups: pl.DataFrame | pl.LazyFrame,
        iteration: int,
        quantity: Literal["trip_count", "distance", "travel_time", "modal_share"],
        mode: str,
        zone: Literal["home", "origin", "destination"],
    ) -> pl.DataFrame:
        """Compute one plotted metric by transport zone for a saved iteration."""
        if quantity == "modal_share":
            return (
                self._transport_zone_ids()
                .join(
                    self._modal_share_for_saved_iteration(
                        run=run,
                        demand_groups=demand_groups,
                        iteration=iteration,
                        zone=zone,
                        mode=mode,
                    ),
                    on="transport_zone_id",
                    how="left",
                )
                .with_columns(
                    pl.col("modal_share").fill_null(0.0),
                    pl.col("n_trips").fill_null(0.0),
                    mode=pl.lit(mode),
                    quantity=pl.lit(quantity),
                )
                .rename({"modal_share": "value"})
                .select(["transport_zone_id", "value", "n_trips", "mode", "quantity"])
                .sort("transport_zone_id")
            )

        return self._average_trip_metric_by_home_zone_for_saved_iteration(
            run=run,
            demand_groups=demand_groups,
            iteration=iteration,
            quantity=quantity,
        )

    def _average_trip_metric_by_home_zone_for_saved_iteration(
        self,
        *,
        run: Any,
        demand_groups: pl.DataFrame | pl.LazyFrame,
        iteration: int,
        quantity: Literal["trip_count", "distance", "travel_time"],
    ) -> pl.DataFrame:
        """Compute a trip metric average per resident by home transport zone."""
        plan_steps = self._saved_iteration_plan_steps(run, iteration).lazy()
        plan_steps = self._join_home_zone(plan_steps, demand_groups)
        schema_names = plan_steps.collect_schema().names()
        metric_column = self._saved_iteration_metric_column(quantity, schema_names)

        required_columns = {"activity_seq_id", "home_zone_id", "n_persons", metric_column}
        missing_columns = required_columns.difference(schema_names)
        if missing_columns:
            missing = ", ".join(sorted(missing_columns))
            raise ValueError(f"Saved iteration plan steps are missing: {missing}.")

        if quantity == "trip_count":
            total_expr = pl.col("n_persons").sum()
        else:
            total_expr = (pl.col(metric_column).fill_null(0.0) * pl.col("n_persons")).sum()

        totals_by_zone = (
            plan_steps
            .filter(pl.col("activity_seq_id") != 0)
            .group_by("home_zone_id")
            .agg(total=total_expr)
            .rename({"home_zone_id": "transport_zone_id"})
        )
        population_by_zone = self._population_by_home_zone(demand_groups)

        return (
            self._transport_zone_ids()
            .lazy()
            .join(totals_by_zone, on="transport_zone_id", how="left")
            .join(population_by_zone, on="transport_zone_id", how="left")
            .with_columns(
                pl.col("total").fill_null(0.0),
                pl.col("population").fill_null(0.0),
            )
            .with_columns(
                value=pl.when(pl.col("population") > 0.0)
                .then(pl.col("total") / pl.col("population"))
                .otherwise(0.0),
                quantity=pl.lit(quantity),
            )
            .select(["transport_zone_id", "value", "total", "population", "quantity"])
            .sort("transport_zone_id")
            .collect(engine="streaming")
        )

    @staticmethod
    def _saved_iteration_metric_column(
        quantity: Literal["trip_count", "distance", "travel_time"],
        schema_names: list[str],
    ) -> str:
        """Return the saved plan-step column used for one plotted quantity."""
        if quantity == "trip_count":
            return "n_persons"
        if quantity == "travel_time" and "time" in schema_names:
            return "time"
        return quantity

    @staticmethod
    def _plot_delta_expression(*, relative_delta: bool) -> pl.Expr:
        """Return the map delta expression."""
        absolute_delta = pl.col("value") - pl.col("value_reference")
        if relative_delta:
            return (
                pl.when(pl.col("value_reference") != 0.0)
                .then(absolute_delta / pl.col("value_reference"))
                .otherwise(0.0)
            )
        return absolute_delta

    @staticmethod
    def _automatic_delta_color_range(
        delta: pl.DataFrame,
        *,
        color_range: float | tuple[float, float] | None,
        auto_cap_outliers: bool,
        outlier_quantile: float,
    ) -> float | tuple[float, float] | None:
        """Return an optional symmetric color cap for a delta map."""
        if color_range is not None or not auto_cap_outliers:
            return color_range

        cap = (
            delta
            .select(pl.col("value_delta").abs().quantile(outlier_quantile))
            .item()
        )
        if cap is None or cap <= 0.0:
            return None
        return float(cap)

    def _mean_utility_by_home_zone_for_saved_iteration(
        self,
        *,
        run: Any,
        demand_groups: pl.DataFrame | pl.LazyFrame,
        iteration: int,
    ) -> pl.DataFrame:
        """Compute weighted mean plan utility by home transport zone."""
        current_plans = self._saved_iteration_state(run, iteration).current_plans.lazy()
        current_plans = self._join_home_zone(current_plans, demand_groups)
        required_columns = {"home_zone_id", "utility", "n_persons"}
        missing_columns = required_columns.difference(current_plans.collect_schema().names())
        if missing_columns:
            missing = ", ".join(sorted(missing_columns))
            raise ValueError(f"Saved iteration current plans are missing: {missing}.")

        return (
            current_plans
            .group_by("home_zone_id")
            .agg(
                weighted_utility=(pl.col("utility") * pl.col("n_persons")).sum(),
                n_persons=pl.col("n_persons").sum(),
            )
            .with_columns(
                value=pl.when(pl.col("n_persons") > 0.0)
                .then(pl.col("weighted_utility") / pl.col("n_persons"))
                .otherwise(0.0)
            )
            .rename({"home_zone_id": "transport_zone_id"})
            .select(["transport_zone_id", "value"])
            .collect(engine="streaming")
        )

    def _ghg_emissions_by_zone_for_saved_iteration(
        self,
        *,
        run: Any,
        demand_groups: pl.DataFrame | pl.LazyFrame,
        costs: pl.DataFrame | pl.LazyFrame,
        iteration: int,
        zone: Literal["home", "origin", "destination"],
    ) -> pl.DataFrame:
        """Compute total weighted GHG emissions by transport zone."""
        plan_steps = self._saved_iteration_plan_steps(run, iteration).lazy()
        if zone == "home":
            plan_steps = self._join_home_zone(plan_steps, demand_groups)
            zone_column = "home_zone_id"
        elif zone == "origin":
            zone_column = "from"
        else:
            zone_column = "to"

        required_columns = {"activity_seq_id", zone_column, "from", "to", "mode", "n_persons"}
        missing_columns = required_columns.difference(plan_steps.collect_schema().names())
        if missing_columns:
            missing = ", ".join(sorted(missing_columns))
            raise ValueError(f"Saved iteration plan steps are missing: {missing}.")

        costs_lazy = costs.lazy() if isinstance(costs, pl.DataFrame) else costs
        cost_columns = set(costs_lazy.collect_schema().names())
        missing_cost_columns = {"from", "to", "mode", "ghg_emissions_per_trip"}.difference(cost_columns)
        if missing_cost_columns:
            missing = ", ".join(sorted(missing_cost_columns))
            raise ValueError(f"Costs are missing: {missing}.")

        emissions_by_zone = (
            plan_steps
            .filter(pl.col("activity_seq_id") != 0)
            .with_columns(pl.col("mode").cast(pl.String))
            .join(
                costs_lazy
                .select(["from", "to", "mode", "ghg_emissions_per_trip"])
                .with_columns(pl.col("mode").cast(pl.String)),
                on=["from", "to", "mode"],
                how="left",
            )
            .with_columns(pl.col("ghg_emissions_per_trip").fill_null(0.0))
            .group_by(zone_column)
            .agg(total_emissions=(pl.col("ghg_emissions_per_trip") * pl.col("n_persons")).sum())
            .rename({zone_column: "transport_zone_id"})
        )
        population_by_zone = self._population_by_home_zone(demand_groups)
        return (
            emissions_by_zone
            .join(population_by_zone, on="transport_zone_id", how="left")
            .with_columns(pl.col("population").fill_null(0.0))
            .with_columns(
                value=pl.when(pl.col("population") > 0.0)
                .then(pl.col("total_emissions") / pl.col("population"))
                .otherwise(0.0)
            )
            .select(["transport_zone_id", "value"])
            .collect(engine="streaming")
        )

    def _population_by_home_zone(
        self,
        demand_groups: pl.DataFrame | pl.LazyFrame,
    ) -> pl.LazyFrame:
        """Return total resident population by home transport zone."""
        demand_groups_lazy = demand_groups.lazy() if isinstance(demand_groups, pl.DataFrame) else demand_groups
        required_columns = {"home_zone_id", "n_persons"}
        missing_columns = required_columns.difference(demand_groups_lazy.collect_schema().names())
        if missing_columns:
            missing = ", ".join(sorted(missing_columns))
            raise ValueError(f"Demand groups are missing: {missing}.")

        return (
            demand_groups_lazy
            .group_by("home_zone_id")
            .agg(population=pl.col("n_persons").sum())
            .rename({"home_zone_id": "transport_zone_id"})
        )

    def _global_plot_delta_by_iteration(
        self,
        *,
        reference_run: Any,
        reference_demand_groups: pl.DataFrame | pl.LazyFrame,
        current_costs: pl.DataFrame | pl.LazyFrame,
        reference_costs: pl.DataFrame | pl.LazyFrame,
        iteration: int,
        value: Literal["modal_share", "n_trips", "mean_utility", "ghg_emissions"],
        mode: str,
        zone: Literal["home", "origin", "destination"],
        inner_zones_only: bool,
        relative_delta: bool,
    ) -> float:
        """Return the global delta shown on a generic delta map."""
        return self._global_plot_delta_between_runs_by_iteration(
            current_run=self.results.run,
            current_demand_groups=self.results.demand_groups,
            reference_run=reference_run,
            reference_demand_groups=reference_demand_groups,
            current_costs=current_costs,
            reference_costs=reference_costs,
            iteration=iteration,
            value=value,
            mode=mode,
            zone=zone,
            inner_zones_only=inner_zones_only,
            relative_delta=relative_delta,
        )

    def _global_plot_delta_between_runs_by_iteration(
        self,
        *,
        current_run: Any,
        current_demand_groups: pl.DataFrame | pl.LazyFrame,
        reference_run: Any,
        reference_demand_groups: pl.DataFrame | pl.LazyFrame,
        current_costs: pl.DataFrame | pl.LazyFrame,
        reference_costs: pl.DataFrame | pl.LazyFrame,
        iteration: int,
        value: Literal["modal_share", "n_trips", "mean_utility", "ghg_emissions"],
        mode: str,
        zone: Literal["home", "origin", "destination"],
        inner_zones_only: bool,
        relative_delta: bool,
    ) -> float:
        """Return a global delta between two run contexts."""
        if value in {"modal_share", "n_trips"}:
            value_type = "modal_share_delta" if value == "modal_share" else "n_trips_delta"
            absolute_delta = self._global_delta_between_runs_by_iteration(
                current_run=current_run,
                current_demand_groups=current_demand_groups,
                reference_run=reference_run,
                reference_demand_groups=reference_demand_groups,
                iteration=iteration,
                mode=mode,
                zone=zone,
                inner_zones_only=inner_zones_only,
                value_type=value_type,
            )
            if not relative_delta:
                return absolute_delta
            reference_value = self._global_mode_trip_count_for_saved_iteration(
                run=reference_run,
                demand_groups=reference_demand_groups,
                iteration=iteration,
                mode=mode,
                zone=zone,
                inner_zones_only=inner_zones_only,
            )
            return absolute_delta / reference_value if reference_value != 0.0 else 0.0

        current_value = self._global_value_for_saved_iteration(
            run=current_run,
            demand_groups=current_demand_groups,
            costs=current_costs,
            iteration=iteration,
            value=value,
            zone=zone,
            inner_zones_only=inner_zones_only,
        )
        reference_value = self._global_value_for_saved_iteration(
            run=reference_run,
            demand_groups=reference_demand_groups,
            costs=reference_costs,
            iteration=iteration,
            value=value,
            zone=zone,
            inner_zones_only=inner_zones_only,
        )
        absolute_delta = current_value - reference_value
        if relative_delta:
            return absolute_delta / reference_value if reference_value != 0.0 else 0.0
        return absolute_delta

    def _global_value_for_saved_iteration(
        self,
        *,
        run: Any,
        demand_groups: pl.DataFrame | pl.LazyFrame,
        costs: pl.DataFrame | pl.LazyFrame,
        iteration: int,
        value: Literal["mean_utility", "ghg_emissions"],
        zone: Literal["home", "origin", "destination"],
        inner_zones_only: bool,
    ) -> float:
        """Compute a global value for one saved iteration."""
        if value == "mean_utility":
            current_plans = self._saved_iteration_state(run, iteration).current_plans.lazy()
            current_plans = self._join_home_zone(current_plans, demand_groups)
            required_columns = {"home_zone_id", "utility", "n_persons"}
            missing_columns = required_columns.difference(current_plans.collect_schema().names())
            if missing_columns:
                missing = ", ".join(sorted(missing_columns))
                raise ValueError(f"Saved iteration current plans are missing: {missing}.")
            if inner_zones_only:
                current_plans = current_plans.filter(
                    pl.col("home_zone_id").is_in(self._inner_transport_zone_ids())
                )
            summary = (
                current_plans
                .select(
                    weighted_utility=(pl.col("utility") * pl.col("n_persons")).sum(),
                    n_persons=pl.col("n_persons").sum(),
                )
                .collect(engine="streaming")
            )
            n_persons = float(summary["n_persons"][0] or 0.0)
            if n_persons == 0.0:
                return 0.0
            return float(summary["weighted_utility"][0] or 0.0) / n_persons

        emissions_by_zone = self._ghg_emissions_by_zone_for_saved_iteration(
            run=run,
            demand_groups=demand_groups,
            costs=costs,
            iteration=iteration,
            zone=zone,
        )
        if inner_zones_only:
            emissions_by_zone = emissions_by_zone.filter(
                pl.col("transport_zone_id").is_in(self._inner_transport_zone_ids())
            )
        population_by_zone = self._population_by_home_zone(demand_groups).collect(engine="streaming")
        if inner_zones_only:
            population_by_zone = population_by_zone.filter(
                pl.col("transport_zone_id").is_in(self._inner_transport_zone_ids())
            )
        total_population = float(population_by_zone["population"].sum() or 0.0)
        if total_population == 0.0:
            return 0.0
        total_emissions = (
            emissions_by_zone
            .join(population_by_zone, on="transport_zone_id", how="left")
            .select((pl.col("value") * pl.col("population").fill_null(0.0)).sum())
            .item()
            or 0.0
        )
        return float(total_emissions) / total_population

    @staticmethod
    def _plot_delta_value_settings(
        *,
        value: Literal["modal_share", "n_trips", "mean_utility", "ghg_emissions"],
        mode: str,
        relative_delta: bool,
    ) -> dict[str, str | None]:
        """Return map settings for a generic delta value."""
        if value == "modal_share":
            return {
                "save_name": f"{RunMetrics._safe_mode_name(mode)}-modal-share-delta",
                "legend_label": f"{RunMetrics._mode_label(mode)} modal share difference",
                "colorbar_tickformat": ".0%",
            }
        if value == "n_trips":
            return {
                "save_name": f"{RunMetrics._safe_mode_name(mode)}-n-trips"
                f"{'-relative' if relative_delta else ''}-delta",
                "legend_label": f"{RunMetrics._mode_label(mode)} "
                f"{'relative ' if relative_delta else ''}trip count difference",
                "colorbar_tickformat": ".0%" if relative_delta else None,
            }
        if value == "mean_utility":
            return {
                "save_name": f"mean-utility{'-relative' if relative_delta else ''}-delta",
                "legend_label": "Relative mean utility difference"
                if relative_delta
                else "Mean utility difference",
                "colorbar_tickformat": ".0%" if relative_delta else None,
            }
        return {
            "save_name": f"ghg-emissions{'-relative' if relative_delta else ''}-delta",
            "legend_label": (
                "Relative GHG emissions difference (kgCO2e/person)"
                if relative_delta
                else "GHG emissions difference (kgCO2e/person)"
            ),
            "colorbar_tickformat": ".0%" if relative_delta else None,
        }

    def _plot_metric_settings(
        self,
        *,
        quantity: Literal["trip_count", "distance", "travel_time", "modal_share"],
        mode: str,
    ) -> dict[str, Any]:
        """Return map settings for a one-scenario metric."""
        if quantity == "modal_share":
            safe_mode_name = self._safe_mode_name(mode)
            mode_label = self._mode_label(mode)
            return {
                "save_name": f"{safe_mode_name}-modal-share",
                "legend_label": f"{mode_label} modal share",
                "hover_columns": ["n_trips", "mode"],
                "colorbar_tickformat": ".0%",
                "color_continuous_scale": [
                    [0.0, "#FFFFFF"],
                    [1.0, self._mode_color(mode)],
                ],
            }
        if quantity == "trip_count":
            return {
                "save_name": "average-trip-count",
                "legend_label": "Average trip count (trips/pers.)",
                "hover_columns": ["total", "population", "quantity"],
                "colorbar_tickformat": None,
                "color_continuous_scale": self._ylorrd_color_scale(),
            }
        if quantity == "distance":
            return {
                "save_name": "average-travel-distance",
                "legend_label": "Average travel distance (km/pers.)",
                "hover_columns": ["total", "population", "quantity"],
                "colorbar_tickformat": None,
                "color_continuous_scale": self._ylorrd_color_scale(),
            }
        return {
            "save_name": "average-travel-time",
            "legend_label": "Average travel time (h/pers.)",
            "hover_columns": ["total", "population", "quantity"],
            "colorbar_tickformat": None,
            "color_continuous_scale": self._ylorrd_color_scale(),
        }

    def _plot_delta_frame_title(
        self,
        *,
        iteration: int,
        global_delta: float,
        value: Literal["modal_share", "n_trips", "mean_utility", "ghg_emissions"],
        relative_delta: bool,
    ) -> str:
        """Return the top-left text shown on generic delta maps."""
        scope_label = self._behavior_change_scope_label(iteration)
        if relative_delta:
            global_delta_label = f"Global relative delta: {global_delta:+.2%}"
        elif value == "modal_share":
            global_delta_label = f"Global delta: {global_delta:+.2%}"
        elif value == "n_trips":
            global_delta_label = f"Global delta: {global_delta:+,.0f} trips"
        elif value == "ghg_emissions":
            global_delta_label = f"Global delta: {global_delta:+,.2f} kgCO2e/person"
        else:
            global_delta_label = f"Global delta: {global_delta:+.3f}"
        return f"Iteration {iteration}\n{scope_label}\n{global_delta_label}"

    def _plot_metric_frame_title(
        self,
        *,
        iteration: int,
        global_value: float,
        quantity: Literal["trip_count", "distance", "travel_time", "modal_share"],
    ) -> str:
        """Return the top-left text shown on one-scenario metric maps."""
        scope_label = self._behavior_change_scope_label(iteration)
        if quantity == "modal_share":
            value_label = f"Global share: {global_value:.2%}"
        elif quantity == "trip_count":
            value_label = f"Global average: {global_value:.2f} trips/pers."
        elif quantity == "travel_time":
            value_label = f"Global average: {global_value:.2f} h/pers."
        else:
            value_label = f"Global average: {global_value:.2f} km/pers."
        return f"Iteration {iteration}\n{scope_label}\n{value_label}"

    def plot_metric_by_iteration(
        self,
        iteration: int,
        quantity: Literal[
            "trip_count",
            "distance",
            "travel_time",
            "modal_share",
            "mode_share",
        ],
        mode: str = "car",
        zone: Literal["home", "origin", "destination"] = "home",
        plot: bool = True,
        plot_method: str = "browser",
        save_to_file: bool = False,
        inner_zones_only: bool = False,
        color_range: float | tuple[float, float] | None = None,
        clamp_outliers: bool = False,
        outlier_quantile: float = 0.98,
        labels: bool = True,
        width: int = 1200,
        height: int = 850,
        max_labels: int = 30,
        simplify_tolerance: float | None = 50.0,
        return_figure: bool = False,
    ) -> pl.DataFrame | tuple[pl.DataFrame, go.Figure]:
        """Plot one saved-iteration metric by transport zone for this run."""
        if iteration < 1:
            raise ValueError("iteration must be greater than or equal to 1.")
        if quantity == "mode_share":
            quantity = "modal_share"
        if quantity not in {"trip_count", "distance", "travel_time", "modal_share"}:
            raise ValueError(
                "quantity should be one of: trip_count, distance, travel_time, modal_share."
            )
        if zone not in {"home", "origin", "destination"}:
            raise ValueError("zone should be one of: home, origin, destination.")
        if quantity != "modal_share" and zone != "home":
            raise ValueError(
                "trip_count, distance and travel_time are averaged by home zone only."
            )
        if not 0.5 < outlier_quantile <= 1.0:
            raise ValueError("outlier_quantile should be greater than 0.5 and at most 1.")

        metric = self._metric_by_zone_for_saved_iteration(
            run=self.results.run,
            demand_groups=self.results.demand_groups,
            iteration=iteration,
            quantity=quantity,
            mode=mode,
            zone=zone,
        )
        settings = self._plot_metric_settings(quantity=quantity, mode=mode)

        frame_title = None
        if plot or save_to_file or return_figure:
            global_value = self._global_metric_by_iteration(
                run=self.results.run,
                demand_groups=self.results.demand_groups,
                iteration=iteration,
                quantity=quantity,
                mode=mode,
                zone=zone,
                inner_zones_only=inner_zones_only,
            )
            frame_title = self._plot_metric_frame_title(
                iteration=iteration,
                global_value=global_value,
                quantity=quantity,
            )

        fig = None
        if plot or save_to_file or return_figure:
            fig = TransportZoneMaps(
                self.results.transport_zones,
                population=getattr(self.results.run, "population", None),
                max_labels=max_labels,
                simplify_tolerance=simplify_tolerance,
            ).metric(
                metric,
                value_column="value",
                save_name=(
                    f"{self.results.period}-{settings['save_name']}"
                    f"-iteration-{iteration}-map"
                ),
                save_to_file=save_to_file,
                inner_zones_only=inner_zones_only,
                labels=labels,
                width=width,
                height=height,
                hover_columns=settings["hover_columns"],
                legend_label=settings["legend_label"],
                frame_title=frame_title,
                classify=False,
                color_continuous_scale=settings["color_continuous_scale"],
                range_color=self._positive_metric_color_range(
                    self._map_metric_for_color_range(
                        metric,
                        inner_zones_only=inner_zones_only,
                    ),
                    color_range,
                    value_column="value",
                    clamp_outliers=clamp_outliers,
                    outlier_quantile=outlier_quantile,
                ),
                colorbar_tickformat=settings["colorbar_tickformat"],
            )
            if plot:
                fig.show(plot_method)

        if return_figure:
            return metric, fig
        return metric

    def modal_share_by_iteration(
        self,
        iteration: int,
        mode: str = "car",
        zone: Literal["home", "origin", "destination"] = "home",
        plot: bool = True,
        plot_method: str = "browser",
        save_to_file: bool = False,
        inner_zones_only: bool = False,
        color_range: float | tuple[float, float] | None = None,
        labels: bool = True,
        width: int = 1200,
        height: int = 850,
        max_labels: int = 30,
        simplify_tolerance: float | None = 50.0,
        return_figure: bool = False,
    ) -> pl.DataFrame | tuple[pl.DataFrame, go.Figure]:
        """Plot one mode modal share by transport zone for one saved iteration."""
        if iteration < 1:
            raise ValueError("iteration must be greater than or equal to 1.")
        if zone not in {"home", "origin", "destination"}:
            raise ValueError("zone should be one of: home, origin, destination.")

        modal_share = (
            self._transport_zone_ids()
            .join(
                self._modal_share_for_saved_iteration(
                    run=self.results.run,
                    demand_groups=self.results.demand_groups,
                    iteration=iteration,
                    zone=zone,
                    mode=mode,
                ),
                on="transport_zone_id",
                how="left",
            )
            .with_columns(
                pl.col("modal_share").fill_null(0.0),
                pl.col("n_trips").fill_null(0.0),
                mode=pl.lit(mode),
            )
            .sort("transport_zone_id")
        )

        safe_mode_name = self._safe_mode_name(mode)
        mode_label = self._mode_label(mode)
        frame_title = None
        if plot or save_to_file or return_figure:
            global_share = self._global_modal_share_for_saved_iteration(
                run=self.results.run,
                demand_groups=self.results.demand_groups,
                iteration=iteration,
                mode=mode,
                zone=zone,
                inner_zones_only=inner_zones_only,
            )
            frame_title = self._modal_share_frame_title(
                iteration=iteration,
                global_share=global_share,
            )

        fig = None
        if plot or save_to_file or return_figure:
            fig = TransportZoneMaps(
                self.results.transport_zones,
                population=getattr(self.results.run, "population", None),
                max_labels=max_labels,
                simplify_tolerance=simplify_tolerance,
            ).metric(
                modal_share,
                value_column="modal_share",
                save_name=(
                    f"{self.results.period}-{safe_mode_name}"
                    f"-modal-share-iteration-{iteration}-map"
                ),
                save_to_file=save_to_file,
                inner_zones_only=inner_zones_only,
                labels=labels,
                width=width,
                height=height,
                hover_columns=["n_trips", "mode"],
                legend_label=f"{mode_label} modal share",
                frame_title=frame_title,
                classify=False,
                color_continuous_scale=[
                    [0.0, "#FFFFFF"],
                    [1.0, self._mode_color(mode)],
                ],
                range_color=self._modal_share_color_range(color_range),
                colorbar_tickformat=".0%",
            )
            if plot:
                fig.show(plot_method)

        if return_figure:
            return modal_share, fig
        return modal_share

    def _paired_delta_by_iteration(
        self,
        *,
        paired_runs: list[tuple[Any, Any]],
        iteration: int,
        value: Literal["modal_share", "n_trips", "mean_utility", "ghg_emissions"],
        mode: str,
        zone: Literal["home", "origin", "destination"],
        inner_zones_only: bool,
        relative_delta: bool,
    ) -> tuple[pl.DataFrame, float]:
        """Return the average of paired run deltas for one saved iteration."""
        if not paired_runs:
            raise ValueError("paired_runs should contain at least one scenario/reference pair.")

        pair_deltas = []
        global_deltas = []
        for pair_id, (current_like, reference_like) in enumerate(paired_runs):
            current_context = self._resolve_run_context(
                current_like,
                include_costs=value == "ghg_emissions",
            )
            reference_context = self._resolve_run_context(
                reference_like,
                include_costs=value == "ghg_emissions",
            )
            current_costs = (
                current_context["costs"]
                if value == "ghg_emissions"
                else pl.DataFrame()
            )
            reference_costs = (
                reference_context["costs"]
                if value == "ghg_emissions"
                else pl.DataFrame()
            )
            pair_deltas.append(
                self._delta_value_between_runs_by_iteration(
                    current_run=current_context["run"],
                    current_demand_groups=current_context["demand_groups"],
                    reference_run=reference_context["run"],
                    reference_demand_groups=reference_context["demand_groups"],
                    current_costs=current_costs,
                    reference_costs=reference_costs,
                    iteration=iteration,
                    value=value,
                    mode=mode,
                    zone=zone,
                    relative_delta=relative_delta,
                ).with_columns(pair_id=pl.lit(pair_id))
            )
            global_deltas.append(
                self._global_plot_delta_between_runs_by_iteration(
                    current_run=current_context["run"],
                    current_demand_groups=current_context["demand_groups"],
                    reference_run=reference_context["run"],
                    reference_demand_groups=reference_context["demand_groups"],
                    current_costs=current_costs,
                    reference_costs=reference_costs,
                    iteration=iteration,
                    value=value,
                    mode=mode,
                    zone=zone,
                    inner_zones_only=inner_zones_only,
                    relative_delta=relative_delta,
                )
            )

        paired_delta = (
            pl.concat(pair_deltas, how="vertical_relaxed")
            .group_by("transport_zone_id")
            .agg(
                value=pl.col("value").mean(),
                value_reference=pl.col("value_reference").mean(),
                value_delta=pl.col("value_delta").mean(),
                value_delta_std=pl.col("value_delta").std(),
                n_pairs=pl.col("pair_id").n_unique(),
                value_name=pl.col("value_name").first(),
            )
            .with_columns(pl.col("value_delta_std").fill_null(0.0))
            .sort("transport_zone_id")
        )
        return paired_delta, float(sum(global_deltas) / len(global_deltas))

    def modal_share_delta_by_iteration(
        self,
        compare_with: Any,
        iteration: int,
        mode: str = "car",
        zone: Literal["home", "origin", "destination"] = "home",
        value_type: Literal["modal_share_delta", "n_trips_delta"] = "modal_share_delta",
        plot: bool = True,
        plot_method: str = "browser",
        save_to_file: bool = False,
        inner_zones_only: bool = False,
        color_range: float | tuple[float, float] | None = None,
        labels: bool = True,
        width: int = 1200,
        height: int = 850,
        max_labels: int = 30,
        simplify_tolerance: float | None = 50.0,
        return_figure: bool = False,
    ) -> pl.DataFrame | tuple[pl.DataFrame, go.Figure]:
        """Compare one mode with another scenario for one saved iteration.

        The returned ``modal_share_delta`` is this run minus ``compare_with``.
        If ``value_type`` is ``n_trips_delta``, the map shows the signed
        difference in weighted trips instead of the modal-share difference.
        By default the share is computed by home transport zone, matching the
        saved iteration state.
        ``color_range`` can be a positive number for a symmetric scale or a
        ``(min, max)`` tuple to share one scale across several iterations.
        """
        if iteration < 1:
            raise ValueError("iteration must be greater than or equal to 1.")
        if zone not in {"home", "origin", "destination"}:
            raise ValueError("zone should be one of: home, origin, destination.")
        if value_type not in {"modal_share_delta", "n_trips_delta"}:
            raise ValueError("value_type should be one of: modal_share_delta, n_trips_delta.")

        reference_run = self._resolve_comparison_run(compare_with)
        reference_demand_groups = self._resolve_comparison_demand_groups(
            compare_with=compare_with,
            reference_run=reference_run,
        )
        current_share = self._modal_share_for_saved_iteration(
            run=self.results.run,
            demand_groups=self.results.demand_groups,
            iteration=iteration,
            zone=zone,
            mode=mode,
        )
        reference_share = self._modal_share_for_saved_iteration(
            run=reference_run,
            demand_groups=reference_demand_groups,
            iteration=iteration,
            zone=zone,
            mode=mode,
        )

        modal_share_delta = (
            self._transport_zone_ids()
            .join(
                current_share,
                on="transport_zone_id",
                how="left",
            )
            .join(
                reference_share.rename(
                    {
                        "modal_share": "modal_share_reference",
                        "n_trips": "n_trips_reference",
                    }
                ),
                on="transport_zone_id",
                how="left",
            )
            .with_columns(
                pl.col("modal_share").fill_null(0.0),
                pl.col("modal_share_reference").fill_null(0.0),
                pl.col("n_trips").fill_null(0.0),
                pl.col("n_trips_reference").fill_null(0.0),
            )
            .with_columns(
                modal_share_delta=pl.col("modal_share") - pl.col("modal_share_reference"),
                n_trips_delta=pl.col("n_trips") - pl.col("n_trips_reference"),
                mode=pl.lit(mode),
            )
            .sort("transport_zone_id")
        )
        safe_mode_name = self._safe_mode_name(mode)
        mode_label = self._mode_label(mode)
        value_settings = self._modal_share_delta_value_settings(
            value_type=value_type,
            mode_label=mode_label,
        )
        frame_title = None
        if plot or save_to_file or return_figure:
            global_delta = self._global_delta_by_iteration(
                reference_run=reference_run,
                reference_demand_groups=reference_demand_groups,
                iteration=iteration,
                mode=mode,
                zone=zone,
                inner_zones_only=inner_zones_only,
                value_type=value_type,
            )
            frame_title = self._modal_share_delta_frame_title(
                iteration=iteration,
                global_delta=global_delta,
                value_type=value_type,
            )

        fig = None
        if plot or save_to_file:
            range_color = self._modal_share_delta_color_range(
                modal_share_delta,
                color_range,
                value_column=value_settings["value_column"],
            )
            fig = TransportZoneMaps(
                self.results.transport_zones,
                population=getattr(self.results.run, "population", None),
                max_labels=max_labels,
                simplify_tolerance=simplify_tolerance,
            ).metric(
                modal_share_delta,
                value_column=value_settings["value_column"],
                save_name=(
                    f"{self.results.period}-{safe_mode_name}-"
                    f"{value_settings['save_name']}-iteration-{iteration}-map"
                ),
                save_to_file=save_to_file,
                inner_zones_only=inner_zones_only,
                labels=labels,
                width=width,
                height=height,
                hover_columns=[
                    "modal_share",
                    "modal_share_reference",
                    "n_trips",
                    "n_trips_reference",
                ],
                legend_label=value_settings["legend_label"],
                frame_title=frame_title,
                classify=False,
                color_continuous_scale=[
                    [0.0, "#44546A"],
                    [0.5, "#FFFFFF"],
                    [1.0, "#D71A1C"],
                ],
                color_continuous_midpoint=0.0,
                range_color=range_color,
                colorbar_tickformat=value_settings["colorbar_tickformat"],
            )
            if plot:
                fig.show(plot_method)

        if return_figure:
            if fig is None:
                range_color = self._modal_share_delta_color_range(
                    modal_share_delta,
                    color_range,
                    value_column=value_settings["value_column"],
                )
                fig = TransportZoneMaps(
                    self.results.transport_zones,
                    population=getattr(self.results.run, "population", None),
                    max_labels=max_labels,
                    simplify_tolerance=simplify_tolerance,
                ).metric(
                    modal_share_delta,
                    value_column=value_settings["value_column"],
                    save_name=(
                        f"{self.results.period}-{safe_mode_name}-"
                        f"{value_settings['save_name']}-iteration-{iteration}-map"
                    ),
                    save_to_file=save_to_file,
                    inner_zones_only=inner_zones_only,
                    labels=labels,
                    width=width,
                    height=height,
                    hover_columns=[
                        "modal_share",
                        "modal_share_reference",
                        "n_trips",
                        "n_trips_reference",
                    ],
                    legend_label=value_settings["legend_label"],
                    frame_title=frame_title,
                    classify=False,
                    color_continuous_scale=[
                        [0.0, "#44546A"],
                        [0.5, "#FFFFFF"],
                        [1.0, "#D71A1C"],
                    ],
                    color_continuous_midpoint=0.0,
                    range_color=range_color,
                    colorbar_tickformat=value_settings["colorbar_tickformat"],
                )
            return modal_share_delta, fig

        return modal_share_delta

    def car_modal_share_delta_by_iteration(
        self,
        compare_with: Any,
        iteration: int,
        **kwargs,
    ) -> pl.DataFrame | tuple[pl.DataFrame, go.Figure]:
        """Compare car modal share with another scenario for one saved iteration."""
        result = self.modal_share_delta_by_iteration(
            compare_with,
            iteration,
            mode="car",
            **kwargs,
        )
        if isinstance(result, tuple):
            modal_share_delta, fig = result
            return self._car_modal_share_delta_columns(modal_share_delta), fig
        return self._car_modal_share_delta_columns(result)

    def modal_share_delta_gifs_by_iteration(
        self,
        compare_with: Any,
        modes: list[str],
        iterations: list[int] | range,
        zone: Literal["home", "origin", "destination"] = "home",
        value_type: Literal["modal_share_delta", "n_trips_delta"] = "modal_share_delta",
        inner_zones_only: bool = False,
        color_range: float | tuple[float, float] | None = None,
        labels: bool = True,
        width: int = 1200,
        height: int = 850,
        max_labels: int = 30,
        simplify_tolerance: float | None = 50.0,
        duration_ms: int = 500,
        output_folder: str | Path | None = None,
    ) -> dict[str, Path]:
        """Create one modal-share-delta GIF per mode across saved iterations.

        Temporary PNG frames are written in a temporary folder and removed after
        the GIF is created. The returned dictionary maps each mode to its GIF
        file path.
        """
        if not modes:
            raise ValueError("modes should contain at least one mode.")
        iterations = [int(iteration) for iteration in iterations]
        if not iterations:
            raise ValueError("iterations should contain at least one iteration.")
        if any(iteration < 1 for iteration in iterations):
            raise ValueError("iterations should be greater than or equal to 1.")
        if duration_ms <= 0:
            raise ValueError("duration_ms should be positive.")
        if value_type not in {"modal_share_delta", "n_trips_delta"}:
            raise ValueError("value_type should be one of: modal_share_delta, n_trips_delta.")

        reference_run = self._resolve_comparison_run(compare_with)
        reference_demand_groups = self._resolve_comparison_demand_groups(
            compare_with=compare_with,
            reference_run=reference_run,
        )
        output_folder = Path(output_folder) if output_folder is not None else self._report_folder()
        output_folder.mkdir(parents=True, exist_ok=True)

        gif_paths = {}
        for mode in modes:
            safe_mode_name = self._safe_mode_name(mode)
            value_settings = self._modal_share_delta_value_settings(
                value_type=value_type,
                mode_label=self._mode_label(mode),
            )
            gif_path = output_folder / (
                f"{self.results.inputs_hash}-{self.results.period}-"
                f"{safe_mode_name}-{value_settings['save_name']}-iterations-"
                f"{min(iterations)}-{max(iterations)}-map.gif"
            )
            with tempfile.TemporaryDirectory(prefix=f"{safe_mode_name}-modal-share-delta-") as temporary_folder:
                frame_paths = []
                for frame_index, iteration in enumerate(iterations):
                    modal_share_delta = self.modal_share_delta_by_iteration(
                        compare_with,
                        iteration=iteration,
                        mode=mode,
                        zone=zone,
                        value_type=value_type,
                        plot=False,
                    )
                    global_delta = self._global_delta_by_iteration(
                        reference_run=reference_run,
                        reference_demand_groups=reference_demand_groups,
                        iteration=iteration,
                        mode=mode,
                        zone=zone,
                        inner_zones_only=inner_zones_only,
                        value_type=value_type,
                    )
                    frame_path = Path(temporary_folder) / f"frame-{frame_index:04d}.png"
                    self._save_modal_share_delta_frame(
                        modal_share_delta=modal_share_delta,
                        mode=mode,
                        iteration=iteration,
                        value_type=value_type,
                        global_delta=global_delta,
                        output_path=frame_path,
                        inner_zones_only=inner_zones_only,
                        color_range=color_range,
                        labels=labels,
                        width=width,
                        height=height,
                        max_labels=max_labels,
                        simplify_tolerance=simplify_tolerance,
                    )
                    frame_paths.append(frame_path)
                self._write_gif(frame_paths, gif_path, duration_ms=duration_ms)
            gif_paths[mode] = gif_path

        return gif_paths

    def _save_modal_share_delta_frame(
        self,
        *,
        modal_share_delta: pl.DataFrame,
        mode: str,
        iteration: int,
        value_type: Literal["modal_share_delta", "n_trips_delta"],
        global_delta: float,
        output_path: Path,
        inner_zones_only: bool,
        color_range: float | tuple[float, float] | None,
        labels: bool,
        width: int,
        height: int,
        max_labels: int,
        simplify_tolerance: float | None,
    ) -> None:
        """Write one temporary image frame for a modal-share-delta GIF."""
        safe_mode_name = self._safe_mode_name(mode)
        mode_label = self._mode_label(mode)
        value_settings = self._modal_share_delta_value_settings(
            value_type=value_type,
            mode_label=mode_label,
        )
        range_color = self._modal_share_delta_color_range(
            modal_share_delta,
            color_range,
            value_column=value_settings["value_column"],
        )
        TransportZoneMaps(
            self.results.transport_zones,
            population=getattr(self.results.run, "population", None),
            max_labels=max_labels,
            simplify_tolerance=simplify_tolerance,
        ).metric(
            modal_share_delta,
            value_column=value_settings["value_column"],
            save_name=(
                f"{self.results.period}-{safe_mode_name}-"
                f"{value_settings['save_name']}-iteration-{iteration}-map"
            ),
            output_path=output_path,
            inner_zones_only=inner_zones_only,
            labels=labels,
            width=width,
            height=height,
            hover_columns=[
                "modal_share",
                "modal_share_reference",
                "n_trips",
                "n_trips_reference",
            ],
            legend_label=value_settings["legend_label"],
            frame_title=self._modal_share_delta_frame_title(
                iteration=iteration,
                global_delta=global_delta,
                value_type=value_type,
            ),
            classify=False,
            color_continuous_scale=[
                [0.0, "#44546A"],
                [0.5, "#FFFFFF"],
                [1.0, "#D71A1C"],
            ],
            color_continuous_midpoint=0.0,
            range_color=range_color,
            colorbar_tickformat=value_settings["colorbar_tickformat"],
        )

    @staticmethod
    def _write_gif(frame_paths: list[Path], output_path: Path, duration_ms: int) -> None:
        """Combine PNG frames into one GIF."""
        from PIL import Image

        images = [Image.open(frame_path).convert("P", palette=Image.ADAPTIVE) for frame_path in frame_paths]
        try:
            images[0].save(
                output_path,
                save_all=True,
                append_images=images[1:],
                duration=duration_ms,
                loop=0,
            )
        finally:
            for image in images:
                image.close()

    def _modal_share_delta_frame_title(
        self,
        *,
        iteration: int,
        global_delta: float,
        value_type: Literal["modal_share_delta", "n_trips_delta"],
    ) -> str:
        """Return the top-left text shown on modal-delta maps."""
        scope_label = self._behavior_change_scope_label(iteration)
        global_delta_label = self._global_delta_label(global_delta, value_type=value_type)
        return f"Iteration {iteration}\n{scope_label}\n{global_delta_label}"

    def _modal_share_frame_title(self, *, iteration: int, global_share: float) -> str:
        """Return the top-left text shown on modal-share maps."""
        scope_label = self._behavior_change_scope_label(iteration)
        return f"Iteration {iteration}\n{scope_label}\nGlobal share: {global_share:.2%}"

    def _behavior_change_scope_label(self, iteration: int) -> str:
        """Return the behavior-change scope label shown on GIF frames."""
        behavior_change = getattr(self.results.parameters, "behavior_change", None)
        if not callable(getattr(behavior_change, "scope_at", None)):
            return "Full replanning"
        scope = behavior_change.scope_at(iteration)
        scope_value = getattr(scope, "value", str(scope))
        return scope_value.replace("_", " ").capitalize()

    def _global_modal_share_delta_by_iteration(
        self,
        *,
        reference_run: Any,
        reference_demand_groups: pl.DataFrame | pl.LazyFrame,
        iteration: int,
        mode: str,
        zone: Literal["home", "origin", "destination"],
        inner_zones_only: bool,
    ) -> float:
        """Return the global modal-share delta for one mode."""
        return self._global_modal_share_delta_between_runs_by_iteration(
            current_run=self.results.run,
            current_demand_groups=self.results.demand_groups,
            reference_run=reference_run,
            reference_demand_groups=reference_demand_groups,
            iteration=iteration,
            mode=mode,
            zone=zone,
            inner_zones_only=inner_zones_only,
        )

    def _global_modal_share_delta_between_runs_by_iteration(
        self,
        *,
        current_run: Any,
        current_demand_groups: pl.DataFrame | pl.LazyFrame,
        reference_run: Any,
        reference_demand_groups: pl.DataFrame | pl.LazyFrame,
        iteration: int,
        mode: str,
        zone: Literal["home", "origin", "destination"],
        inner_zones_only: bool,
    ) -> float:
        """Return a global modal-share delta between two run contexts."""
        current_share = self._global_modal_share_for_saved_iteration(
            run=current_run,
            demand_groups=current_demand_groups,
            iteration=iteration,
            mode=mode,
            zone=zone,
            inner_zones_only=inner_zones_only,
        )
        reference_share = self._global_modal_share_for_saved_iteration(
            run=reference_run,
            demand_groups=reference_demand_groups,
            iteration=iteration,
            mode=mode,
            zone=zone,
            inner_zones_only=inner_zones_only,
        )
        return current_share - reference_share

    def _global_delta_by_iteration(
        self,
        *,
        reference_run: Any,
        reference_demand_groups: pl.DataFrame | pl.LazyFrame,
        iteration: int,
        mode: str,
        zone: Literal["home", "origin", "destination"],
        inner_zones_only: bool,
        value_type: Literal["modal_share_delta", "n_trips_delta"],
    ) -> float:
        """Return the global delta shown on one GIF frame."""
        return self._global_delta_between_runs_by_iteration(
            current_run=self.results.run,
            current_demand_groups=self.results.demand_groups,
            reference_run=reference_run,
            reference_demand_groups=reference_demand_groups,
            iteration=iteration,
            mode=mode,
            zone=zone,
            inner_zones_only=inner_zones_only,
            value_type=value_type,
        )

    def _global_delta_between_runs_by_iteration(
        self,
        *,
        current_run: Any,
        current_demand_groups: pl.DataFrame | pl.LazyFrame,
        reference_run: Any,
        reference_demand_groups: pl.DataFrame | pl.LazyFrame,
        iteration: int,
        mode: str,
        zone: Literal["home", "origin", "destination"],
        inner_zones_only: bool,
        value_type: Literal["modal_share_delta", "n_trips_delta"],
    ) -> float:
        """Return a modal-share or trip-count delta between two run contexts."""
        if value_type == "modal_share_delta":
            return self._global_modal_share_delta_between_runs_by_iteration(
                current_run=current_run,
                current_demand_groups=current_demand_groups,
                reference_run=reference_run,
                reference_demand_groups=reference_demand_groups,
                iteration=iteration,
                mode=mode,
                zone=zone,
                inner_zones_only=inner_zones_only,
            )

        current_n_trips = self._global_mode_trip_count_for_saved_iteration(
            run=current_run,
            demand_groups=current_demand_groups,
            iteration=iteration,
            mode=mode,
            zone=zone,
            inner_zones_only=inner_zones_only,
        )
        reference_n_trips = self._global_mode_trip_count_for_saved_iteration(
            run=reference_run,
            demand_groups=reference_demand_groups,
            iteration=iteration,
            mode=mode,
            zone=zone,
            inner_zones_only=inner_zones_only,
        )
        return current_n_trips - reference_n_trips

    def _global_metric_by_iteration(
        self,
        *,
        run: Any,
        demand_groups: pl.DataFrame | pl.LazyFrame,
        iteration: int,
        quantity: Literal["trip_count", "distance", "travel_time", "modal_share"],
        mode: str,
        zone: Literal["home", "origin", "destination"],
        inner_zones_only: bool,
    ) -> float:
        """Return the global value shown on one-scenario metric maps."""
        if quantity == "modal_share":
            return self._global_modal_share_for_saved_iteration(
                run=run,
                demand_groups=demand_groups,
                iteration=iteration,
                mode=mode,
                zone=zone,
                inner_zones_only=inner_zones_only,
            )

        metric_by_zone = self._average_trip_metric_by_home_zone_for_saved_iteration(
            run=run,
            demand_groups=demand_groups,
            iteration=iteration,
            quantity=quantity,
        )
        if inner_zones_only:
            metric_by_zone = metric_by_zone.filter(
                pl.col("transport_zone_id").is_in(self._inner_transport_zone_ids())
            )

        population = float(metric_by_zone["population"].sum() or 0.0)
        if population == 0.0:
            return 0.0
        return float(metric_by_zone["total"].sum() or 0.0) / population

    def _global_modal_share_for_saved_iteration(
        self,
        *,
        run: Any,
        demand_groups: pl.DataFrame | pl.LazyFrame,
        iteration: int,
        mode: str,
        zone: Literal["home", "origin", "destination"],
        inner_zones_only: bool,
    ) -> float:
        """Compute one mode share over the selected trip geography."""
        mobile_steps = self._global_delta_mobile_steps(
            run=run,
            demand_groups=demand_groups,
            iteration=iteration,
            zone=zone,
            inner_zones_only=inner_zones_only,
        )

        summary = (
            mobile_steps.select(
                total_trips=pl.col("n_persons").sum(),
                mode_trips=pl.col("n_persons").filter(pl.col("mode") == mode).sum(),
            )
            .collect(engine="streaming")
        )
        total_trips = float(summary["total_trips"][0] or 0.0)
        if total_trips == 0.0:
            return 0.0
        return float(summary["mode_trips"][0] or 0.0) / total_trips

    def _global_mode_trip_count_for_saved_iteration(
        self,
        *,
        run: Any,
        demand_groups: pl.DataFrame | pl.LazyFrame,
        iteration: int,
        mode: str,
        zone: Literal["home", "origin", "destination"],
        inner_zones_only: bool,
    ) -> float:
        """Compute the weighted trip count for one mode in the selected geography."""
        mobile_steps = self._global_delta_mobile_steps(
            run=run,
            demand_groups=demand_groups,
            iteration=iteration,
            zone=zone,
            inner_zones_only=inner_zones_only,
        )

        return float(
            mobile_steps
            .filter(pl.col("mode") == mode)
            .select(pl.col("n_persons").sum())
            .collect(engine="streaming")
            .item()
            or 0.0
        )

    def _global_delta_mobile_steps(
        self,
        *,
        run: Any,
        demand_groups: pl.DataFrame | pl.LazyFrame,
        iteration: int,
        zone: Literal["home", "origin", "destination"],
        inner_zones_only: bool,
    ) -> pl.LazyFrame:
        """Return mobile steps used by the GIF global delta."""
        plan_steps = self._saved_iteration_plan_steps(run, iteration).lazy()
        zone_column = None
        if inner_zones_only:
            if zone == "home":
                plan_steps = self._join_home_zone(plan_steps, demand_groups)
                zone_column = "home_zone_id"
            elif zone == "origin":
                zone_column = "from"
            else:
                zone_column = "to"

        required_columns = {"activity_seq_id", "mode", "n_persons"}
        if zone_column is not None:
            required_columns.add(zone_column)
        missing_columns = required_columns.difference(plan_steps.collect_schema().names())
        if missing_columns:
            missing = ", ".join(sorted(missing_columns))
            raise ValueError(f"Saved iteration plan steps are missing: {missing}.")

        mobile_steps = (
            plan_steps
            .filter(pl.col("activity_seq_id") != 0)
            .with_columns(pl.col("mode").cast(pl.String))
        )
        if zone_column is None:
            return mobile_steps

        return mobile_steps.filter(pl.col(zone_column).is_in(self._inner_transport_zone_ids()))

    @staticmethod
    def _global_delta_label(
        delta: float,
        *,
        value_type: Literal["modal_share_delta", "n_trips_delta"],
    ) -> str:
        """Return the GIF label for the global delta."""
        if value_type == "modal_share_delta":
            return f"Global delta: {delta:+.2%}"
        return f"Global delta: {delta:+,.0f} trips"

    @staticmethod
    def _car_modal_share_delta_columns(modal_share_delta: pl.DataFrame) -> pl.DataFrame:
        """Return the legacy car-specific column names."""
        return modal_share_delta.rename(
            {
                "modal_share": "car_modal_share",
                "modal_share_reference": "car_modal_share_reference",
                "modal_share_delta": "car_modal_share_delta",
            }
        ).select(
            [
                "transport_zone_id",
                "car_modal_share",
                "car_modal_share_reference",
                "car_modal_share_delta",
            ]
        )

    @staticmethod
    def _modal_share_delta_color_range(
        modal_share_delta: pl.DataFrame,
        color_range: float | tuple[float, float] | None,
        *,
        value_column: str = "modal_share_delta",
    ) -> tuple[float, float]:
        """Return the color scale range for a modal-share-delta map."""
        if isinstance(color_range, tuple):
            if len(color_range) != 2:
                raise ValueError("color_range tuple should have two values: (min, max).")
            lower, upper = float(color_range[0]), float(color_range[1])
            if lower >= upper:
                raise ValueError("color_range minimum should be smaller than maximum.")
            return lower, upper

        if color_range is not None:
            color_range = float(color_range)
            if color_range <= 0.0:
                raise ValueError("color_range should be positive.")
            return -color_range, color_range

        max_abs_delta = modal_share_delta[value_column].abs().max()
        automatic_range = max(float(max_abs_delta or 0.0), 0.01)
        return -automatic_range, automatic_range

    @staticmethod
    def _modal_share_color_range(
        color_range: float | tuple[float, float] | None,
    ) -> tuple[float, float]:
        """Return the color scale range for a modal-share map."""
        if isinstance(color_range, tuple):
            if len(color_range) != 2:
                raise ValueError("color_range tuple should have two values: (min, max).")
            lower, upper = float(color_range[0]), float(color_range[1])
            if lower >= upper:
                raise ValueError("color_range minimum should be smaller than maximum.")
            return lower, upper

        if color_range is not None:
            color_range = float(color_range)
            if color_range <= 0.0:
                raise ValueError("color_range should be positive.")
            return 0.0, color_range

        return 0.0, 1.0

    @staticmethod
    def _positive_metric_color_range(
        metric: pl.DataFrame,
        color_range: float | tuple[float, float] | None,
        *,
        value_column: str,
        clamp_outliers: bool = False,
        outlier_quantile: float = 0.98,
    ) -> tuple[float, float]:
        """Return the color scale range for a positive metric map."""
        if isinstance(color_range, tuple):
            if len(color_range) != 2:
                raise ValueError("color_range tuple should have two values: (min, max).")
            lower, upper = float(color_range[0]), float(color_range[1])
            if lower >= upper:
                raise ValueError("color_range minimum should be smaller than maximum.")
            return lower, upper

        if color_range is not None:
            color_range = float(color_range)
            if color_range <= 0.0:
                raise ValueError("color_range should be positive.")
            return 0.0, color_range

        if clamp_outliers:
            lower_quantile = 1.0 - outlier_quantile
            lower_value, max_value = metric.select(
                pl.col(value_column).quantile(lower_quantile).alias("lower"),
                pl.col(value_column).quantile(outlier_quantile).alias("upper"),
            ).row(0)
            min_value = lower_value
        else:
            min_value = metric[value_column].min()
            max_value = metric[value_column].max()

        lower = float(min_value or 0.0)
        upper = float(max_value or 0.0)
        if lower < upper:
            return lower, upper
        if upper == 0.0:
            return 0.0, 0.01
        margin = max(abs(upper) * 0.05, 0.01)
        return upper - margin, upper + margin

    def _map_metric_for_color_range(
        self,
        metric: pl.DataFrame,
        *,
        inner_zones_only: bool,
    ) -> pl.DataFrame:
        """Return the metric rows visible on the map color scale."""
        if not inner_zones_only:
            return metric
        return metric.filter(
            pl.col("transport_zone_id").is_in(self._inner_transport_zone_ids())
        )

    @staticmethod
    def _modal_share_delta_value_settings(
        *,
        value_type: Literal["modal_share_delta", "n_trips_delta"],
        mode_label: str,
    ) -> dict[str, str | None]:
        """Return plot settings for the selected delta value."""
        if value_type == "modal_share_delta":
            return {
                "value_column": "modal_share_delta",
                "save_name": "modal-share-delta",
                "legend_label": f"{mode_label} modal share difference",
                "colorbar_tickformat": ".0%",
            }
        if value_type == "n_trips_delta":
            return {
                "value_column": "n_trips_delta",
                "save_name": "n-trips-delta",
                "legend_label": f"{mode_label} trip count difference",
                "colorbar_tickformat": None,
            }
        raise ValueError("value_type should be one of: modal_share_delta, n_trips_delta.")

    @staticmethod
    def _safe_mode_name(mode: str) -> str:
        """Return a file-name-safe label for one transport mode."""
        safe_name = re.sub(r"[^A-Za-z0-9]+", "-", mode.lower()).strip("-")
        return safe_name or "mode"

    @staticmethod
    def _mode_label(mode: str) -> str:
        """Return a readable transport mode label."""
        if mode == "car":
            return "Car"
        if mode == "public_transport":
            return "Public transport"
        return mode.replace("_", " ").replace("/", " / ").capitalize()

    @classmethod
    def _mode_color(cls, mode: str) -> str:
        """Return the report color used for one transport mode."""
        return cls._mode_label_color_map([cls._mode_label(mode)])[cls._mode_label(mode)]

    @staticmethod
    def _ylorrd_color_scale() -> list[list[float | str]]:
        """Return a Plotly-style YlOrRd scale that also works for SVG export."""
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

    @staticmethod
    def _modal_share_mode_key(
        mode: str,
        *,
        aggregate_public_transport: bool,
    ) -> str:
        """Return the mode key used by modal-share evolution charts."""
        if aggregate_public_transport and "public_transport" in mode:
            return "public_transport"
        return mode

    @staticmethod
    def _ordered_modes(modes: list[str]) -> list[str]:
        """Return modes in a stable report order."""
        preferred_modes = [
            "walk",
            "bicycle",
            "public_transport",
            "walk/public_transport/walk",
            "car",
            "other",
        ]
        unique_modes = list(dict.fromkeys(modes))
        ordered_modes = [mode for mode in preferred_modes if mode in unique_modes]
        ordered_modes.extend(sorted(mode for mode in unique_modes if mode not in preferred_modes))
        return ordered_modes

    @staticmethod
    def _mode_label_color_map(mode_labels: list[str]) -> dict[str, str]:
        """Return stable report colors for readable mode labels."""
        fixed_colors = {
            "Walk": "#5F7F73",
            "Bicycle": "#0B5A66",
            "Public transport": "#EF4B3E",
            "Walk / public transport / walk": "#F06A5A",
            "Car / public transport / walk": "#EF4B3E",
            "Bicycle / public transport / walk": "#D7191C",
            "Car": "#4D4D4D",
            "Carpool": "#8C8C8C",
            "Other": "#7E6F9A",
        }
        fallback_palette = (
            px.colors.qualitative.Safe
            + px.colors.qualitative.Bold
            + px.colors.qualitative.Set3
        )

        color_map: dict[str, str] = {}
        fallback_index = 0
        for mode_label in mode_labels:
            if mode_label in fixed_colors:
                color_map[mode_label] = fixed_colors[mode_label]
                continue

            while fallback_index < len(fallback_palette) and fallback_palette[fallback_index] in color_map.values():
                fallback_index += 1
            if fallback_index >= len(fallback_palette):
                fallback_index = 0
            color_map[mode_label] = fallback_palette[fallback_index]
            fallback_index += 1

        return color_map

    def _modal_share_for_saved_iteration(
        self,
        *,
        run: Any,
        demand_groups: pl.DataFrame | pl.LazyFrame,
        iteration: int,
        zone: Literal["home", "origin", "destination"],
        mode: str,
    ) -> pl.DataFrame:
        """Compute one mode share by zone from a saved iteration state."""
        plan_steps = self._saved_iteration_plan_steps(run, iteration).lazy()
        if zone == "home":
            plan_steps = self._join_home_zone(plan_steps, demand_groups)
            zone_column = "home_zone_id"
        elif zone == "origin":
            zone_column = "from"
        else:
            zone_column = "to"

        required_columns = {"activity_seq_id", zone_column, "mode", "n_persons"}
        missing_columns = required_columns.difference(plan_steps.collect_schema().names())
        if missing_columns:
            missing = ", ".join(sorted(missing_columns))
            raise ValueError(f"Saved iteration plan steps are missing: {missing}.")

        mobile_steps = (
            plan_steps
            .filter(pl.col("activity_seq_id") != 0)
            .with_columns(pl.col("mode").cast(pl.String))
        )
        total_by_zone = mobile_steps.group_by(zone_column).agg(
            total_trips=pl.col("n_persons").sum()
        )
        mode_by_zone = (
            mobile_steps
            .filter(pl.col("mode") == mode)
            .group_by(zone_column)
            .agg(mode_trips=pl.col("n_persons").sum())
        )

        return (
            total_by_zone
            .join(mode_by_zone, on=zone_column, how="left")
            .with_columns(pl.col("mode_trips").fill_null(0.0))
            .with_columns(
                modal_share=pl.when(pl.col("total_trips") > 0.0)
                .then(pl.col("mode_trips") / pl.col("total_trips"))
                .otherwise(0.0)
            )
            .rename({zone_column: "transport_zone_id"})
            .rename({"mode_trips": "n_trips"})
            .select(["transport_zone_id", "modal_share", "n_trips"])
            .collect(engine="streaming")
        )

    def _saved_iteration_plan_steps(self, run: Any, iteration: int) -> pl.DataFrame:
        """Load current plan steps saved for one run iteration."""
        return self._saved_iteration_state(run, iteration).current_plan_steps

    def _saved_iteration_state(self, run: Any, iteration: int) -> Any:
        """Load the full saved state for one run iteration."""
        if not hasattr(run, "cache_path") or "plan_steps" not in run.cache_path:
            raise TypeError("Iteration metrics need a group-day-trips run.")
        if not hasattr(run, "inputs_hash"):
            raise TypeError("Iteration metrics need a run with an input hash.")

        return Iterations(
            run_inputs_hash=run.inputs_hash,
            is_weekday=getattr(run, "is_weekday", self.results.is_weekday),
            base_folder=Path(run.cache_path["plan_steps"]).parent,
        ).iteration(iteration).load_state()

    def _join_home_zone(
        self,
        plan_steps: pl.LazyFrame,
        demand_groups: pl.DataFrame | pl.LazyFrame,
    ) -> pl.LazyFrame:
        """Attach the home transport zone when saved steps only have demand group ids."""
        plan_columns = set(plan_steps.collect_schema().names())
        if "home_zone_id" in plan_columns:
            return plan_steps
        if "demand_group_id" not in plan_columns:
            raise ValueError(
                "Saved iteration plan steps need `home_zone_id` or `demand_group_id` "
                "to compute shares by home zone."
            )

        demand_groups_lazy = demand_groups.lazy() if isinstance(demand_groups, pl.DataFrame) else demand_groups
        demand_group_columns = set(demand_groups_lazy.collect_schema().names())
        missing_columns = {"demand_group_id", "home_zone_id"}.difference(demand_group_columns)
        if missing_columns:
            missing = ", ".join(sorted(missing_columns))
            raise ValueError(f"Demand groups are missing: {missing}.")

        return plan_steps.join(
            demand_groups_lazy.select(["demand_group_id", "home_zone_id"]),
            on="demand_group_id",
            how="left",
        )

    def _resolve_run_context(self, run_like: Any, *, include_costs: bool) -> dict[str, Any]:
        """Return run, demand groups, and costs for one run-like object."""
        run = self._resolve_comparison_run(run_like)
        demand_groups = self._resolve_comparison_demand_groups(
            compare_with=run_like,
            reference_run=run,
        )
        costs = (
            self._resolve_comparison_costs(
                compare_with=run_like,
                reference_run=run,
            )
            if include_costs
            else pl.DataFrame()
        )
        return {
            "run": run,
            "demand_groups": demand_groups,
            "costs": costs,
        }

    def _resolve_comparison_run(self, compare_with: Any) -> Any:
        """Return the day-type run used as the reference scenario."""
        if hasattr(compare_with, "run") and hasattr(compare_with.run, "cache_path"):
            reference_run = compare_with.run
        elif callable(getattr(compare_with, "run", None)):
            day_type = "weekday" if self.results.is_weekday else "weekend"
            reference_run = compare_with.run(day_type)
        else:
            reference_run = compare_with

        if not hasattr(reference_run, "cache_path"):
            raise TypeError(
                "compare_with should be a group-day-trips run, results object, "
                "or PopulationGroupDayTrips setup."
            )
        if getattr(reference_run, "is_weekday", self.results.is_weekday) != self.results.is_weekday:
            raise ValueError("compare_with should use the same day type as these results.")

        return reference_run

    def _resolve_comparison_demand_groups(
        self,
        *,
        compare_with: Any,
        reference_run: Any,
    ) -> pl.DataFrame | pl.LazyFrame:
        """Return demand groups for the reference scenario."""
        if hasattr(compare_with, "demand_groups"):
            return compare_with.demand_groups

        demand_groups_path = reference_run.cache_path.get("demand_groups")
        if demand_groups_path is None:
            raise TypeError("compare_with run needs `cache_path['demand_groups']`.")
        demand_groups_path = Path(demand_groups_path)
        if demand_groups_path.exists() is False and callable(getattr(reference_run, "get", None)):
            reference_run.get()
        if demand_groups_path.exists() is False:
            raise FileNotFoundError(f"Could not find reference demand groups: {demand_groups_path}.")
        return pl.scan_parquet(demand_groups_path)

    def _resolve_comparison_costs(
        self,
        *,
        compare_with: Any,
        reference_run: Any,
    ) -> pl.DataFrame | pl.LazyFrame:
        """Return costs for the reference scenario."""
        if hasattr(compare_with, "costs"):
            return compare_with.costs

        costs_path = reference_run.cache_path.get("costs")
        if costs_path is None:
            prefix = "weekday" if self.results.is_weekday else "weekend"
            costs_path = reference_run.cache_path.get(f"{prefix}_costs")
        if costs_path is None:
            raise TypeError("compare_with run needs `cache_path['costs']`.")

        costs_path = Path(costs_path)
        if costs_path.exists() is False and callable(getattr(reference_run, "get", None)):
            reference_run.get()
        if costs_path.exists() is False:
            raise FileNotFoundError(f"Could not find reference costs: {costs_path}.")
        return pl.scan_parquet(costs_path)

    def _transport_zone_ids(self) -> pl.DataFrame:
        """Return all transport-zone ids from the results geography."""
        zones = self.results.transport_zones.get()
        if "transport_zone_id" not in zones.columns:
            raise ValueError("Transport zones need a `transport_zone_id` column.")
        return pl.DataFrame({"transport_zone_id": zones["transport_zone_id"].to_list()})

    def _inner_transport_zone_ids(self) -> list[Any]:
        """Return ids of transport zones marked as inner zones."""
        zones = self.results.transport_zones.get()
        missing_columns = {"transport_zone_id", "is_inner_zone"}.difference(zones.columns)
        if missing_columns:
            missing = ", ".join(sorted(missing_columns))
            raise ValueError(f"Transport zones are missing: {missing}.")

        return zones.loc[zones["is_inner_zone"], "transport_zone_id"].to_list()

    def plot_map(
        self,
        tz,
        value: str = None,
        activity: str = None,
        plot_method: str = "browser",
        color_continuous_scale="Viridis",
        color_continuous_midpoint=None,
    ):
        """Render a Plotly choropleth for a transport-zone metric."""
        logging.getLogger("kaleido").setLevel(logging.WARNING)
        fig = px.choropleth(
            tz.drop(columns="geometry"),
            geojson=json.loads(tz.to_json()),
            locations="transport_zone_id",
            featureidkey="properties.transport_zone_id",
            color=value,
            hover_data=["transport_zone_id", value],
            color_continuous_scale=color_continuous_scale,
            color_continuous_midpoint=color_continuous_midpoint,
            projection="mercator",
            title=activity,
            subtitle=activity,
        )
        fig.update_geos(fitbounds="geojson", visible=False)
        fig.update_layout(margin=dict(l=0, r=0, t=0, b=0))
        fig.show(plot_method)

    def plot_modal_share(self, zone="origin", mode="car", labels=None, labels_size=[10, 6, 4], labels_color="black"):
        """Plot modal share for a selected mode by origin or destination zone."""
        logging.info(f"Plotting {mode} modal share for {zone} zones during {self.results.period}")
        population_df = self.results.plan_steps.collect().to_pandas()

        left_column = "from" if zone == "origin" else "to"
        mode_share = (
            population_df
            .groupby([left_column, "mode"], as_index=False)["n_persons"]
            .sum()
            .set_index([left_column])
        )
        mode_share["total"] = mode_share.groupby([left_column])["n_persons"].sum()
        mode_share["modal_share"] = mode_share["n_persons"] / mode_share["total"]

        if mode == "public_transport":
            mode_name = "Public transport"
            mode_share["mode"] = mode_share["mode"].replace(r"\S+/public_transport/\S+", "public_transport", regex=True)
        else:
            mode_name = mode.capitalize()
        mode_share = mode_share[mode_share["mode"] == mode]

        transport_zones_df = self.results.transport_zones.get()
        gc = gpd.GeoDataFrame(
            transport_zones_df.merge(mode_share, how="left", right_on=left_column, left_on="transport_zone_id", suffixes=('', '_z'))
        ).fillna(0)
        gcp = gc.plot("modal_share", legend=True)
        gcp.set_axis_off()
        plt.title(f"{mode_name} share per {zone} transport zone ({self.results.period})")

        if isinstance(labels, gpd.GeoDataFrame):
            self._show_labels(labels, labels_size, labels_color)

        plt.show()
        return mode_share

    def plot_od_flows(
        self,
        mode="all",
        activity="all",
        level_of_detail=1,
        n_largest=2000,
        color="blue",
        transparency=0.2,
        zones_color="xkcd:light grey",
        labels=None,
        labels_size=[10, 6, 4],
        labels_color="black",
    ):
        """Plot OD flows for a selected mode and this run's period."""
        if level_of_detail == 0:
            logging.info("OD between communes not implemented yet")
            return NotImplemented
        if level_of_detail != 1:
            logging.info("Level of detail should be 0 or 1")
            return NotImplemented

        logging.info(f"Plotting {mode} origin-destination flows during {self.results.period}")
        if activity != "all":
            logging.info("Speficic activities not implemented yet")
            return NotImplemented

        population_df = self.results.plan_steps.collect().to_pandas()
        mode_name = mode.capitalize()

        if mode != "all":
            if mode == "count":
                population_df["mode"] = population_df["mode"].fillna("unknown_mode")
                count_modes = population_df.groupby("mode")[["mode"]].count()
                return count_modes
            if mode == "public_transport":
                mode_name = "Public transport"
                population_df = population_df[population_df["mode"].fillna("unknown_mode").str.contains("public_transport")]
            else:
                population_df = population_df[population_df["mode"] == mode]

        biggest_flows = (
            population_df
            .groupby(["from", "to"], as_index=False)["n_persons"]
            .sum()
        )
        biggest_flows = biggest_flows.where(biggest_flows["from"] != biggest_flows["to"]).nlargest(n_largest, "n_persons")
        transport_zones_df = self.results.transport_zones.get()
        biggest_flows = biggest_flows.merge(
            transport_zones_df, left_on="from", right_on="transport_zone_id", suffixes=('', '_from')
        )
        biggest_flows = biggest_flows.merge(
            transport_zones_df, left_on="to", right_on="transport_zone_id", suffixes=('', '_to')
        )

        gc = gpd.GeoDataFrame(transport_zones_df)
        gcp = gc.plot(color=zones_color)
        gcp.set_axis_off()

        x_min = float(biggest_flows[["x"]].min().iloc[0])
        y_min = float(biggest_flows[["y"]].min().iloc[0])
        plt.plot([x_min, x_min + 4000], [y_min, y_min], linewidth=2, color=color)
        plt.text(x_min + 6000, y_min - 1000, "1 000", color=color)
        plt.title(f"{mode_name} flows between transport zones on {self.results.period}")

        for _, row in biggest_flows.iterrows():
            plt.plot(
                [row["x"], row["x_to"]],
                [row["y"], row["y_to"]],
                linewidth=row["n_persons"] / 500,
                color=color,
                alpha=transparency,
            )

        if isinstance(labels, gpd.GeoDataFrame):
            self._show_labels(labels, labels_size, labels_color)

        plt.show()
        return biggest_flows

    def get_prominent_cities(self, n_cities=20, n_levels=3, distance_km=2):
        """Get the most prominent cities for labeling maps."""
        population_df = self.results.plan_steps.collect().to_pandas()
        study_area_df = self.results.transport_zones.study_area.get()
        tzdf = self.results.transport_zones.get()

        flows_per_commune = population_df.merge(tzdf, left_on="from", right_on="transport_zone_id")
        flows_per_commune = flows_per_commune.groupby("local_admin_unit_id")["n_persons"].sum().reset_index()
        flows_per_commune = flows_per_commune.merge(study_area_df)
        flows_per_commune = flows_per_commune.sort_values(by="n_persons", ascending=False).head(n_cities * 2).reset_index()
        flows_per_commune.loc[0, "prominence"] = 1
        flows_per_commune.loc[1 : n_cities // 2, "prominence"] = 2
        flows_per_commune.loc[n_cities // 2 + 1 : n_cities, "prominence"] = 3
        flows_per_commune.loc[n_cities + 1 : n_cities * 2, "prominence"] = 3

        geoflows = gpd.GeoDataFrame(flows_per_commune)

        for i in range(n_cities // 2):
            coords = flows_per_commune.loc[i, "geometry"]
            geoflows["dists"] = geoflows["geometry"].distance(coords)
            geoflows.loc[
                ((geoflows["dists"] < distance_km * 1000) & (geoflows.index > i)), "prominence"
            ] = geoflows["prominence"] + 2
            geoflows = geoflows.sort_values(by="prominence").reset_index(drop=True)

        geoflows = geoflows[geoflows["prominence"] <= n_levels]
        xy_coords = geoflows["geometry"].centroid.get_coordinates()
        return geoflows.merge(xy_coords, left_index=True, right_index=True)

    @staticmethod
    def _show_labels(labels, size, color):
        """Annotate a matplotlib axes with place labels."""
        for _, row in labels.iterrows():
            if row["prominence"] == 1:
                plt.annotate(row["local_admin_unit_name"], (row["x"], row["y"]), size=size[0], ha="center", va="center", color=color)
            elif row["prominence"] < 3:
                plt.annotate(row["local_admin_unit_name"], (row["x"], row["y"]), size=size[1], ha="center", va="center", color=color)
            else:
                plt.annotate(row["local_admin_unit_name"], (row["x"], row["y"]), size=size[2], ha="center", va="center", color=color)

    def mask_outliers(self, series):
        """Mask outliers in a numeric pandas/Series-like array."""
        s = series.copy()
        q25 = s.quantile(0.25)
        q75 = s.quantile(0.75)
        iqr = q75 - q25
        lower, upper = q25 - 1.5 * iqr, q75 + 1.5 * iqr
        return s.mask((s < lower) | (s > upper), np.nan)

    def car_traffic(self, *args, **kwargs):
        return CarTrafficEvaluation(self.results).get(*args, **kwargs)

    def travel_costs(self, *args, **kwargs):
        return TravelCostsEvaluation(self.results).get(*args, **kwargs)

    def routing(self, *args, **kwargs):
        return RoutingEvaluation(self.results).get(*args, **kwargs)

    def public_transport_network(self, *args, **kwargs):
        return PublicTransportNetworkEvaluation(self.results).get(*args, **kwargs)
