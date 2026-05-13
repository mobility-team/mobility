from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Literal

import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import polars as pl
from plotly.subplots import make_subplots

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

        if plot:
            comparison_plot_df = (
                comparison.select(["variable", variable, "value", "value_ref"])
                .unpivot(
                    index=["variable", variable],
                    on=["value", "value_ref"],
                    variable_name="value_type",
                    value_name="value",
                )
                .sort(variable)
            )

            fig = px.bar(
                comparison_plot_df,
                x=variable,
                y="value",
                color="value_type",
                facet_col="variable",
                barmode="group",
                facet_col_spacing=0.05,
            )
            fig.update_yaxes(matches=None, showticklabels=True)
            fig.show("browser")

        return comparison

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
            self._plot_activity_time_series(time_series)

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
    ):
        """Plot a stacked bar chart of average occupancy by activity and transit mode."""
        time_series = self.activity_time_series(
            interval_minutes=interval_minutes,
            inner_zone_residents_only=inner_zone_residents_only,
        )
        return self._plot_activity_time_series(time_series)

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
    ):
        """Render stacked occupancy charts, with survey on the left when available."""
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
                        marker_color=color_map[label],
                        showlegend=(col_index == 1),
                    ),
                    row=1,
                    col=col_index,
                )

        fig.update_layout(
            barmode="stack",
            title=f"Average occupancy by activity on {self.results.period}",
            xaxis_title="Time of day",
            yaxis_title="Average persons in bin",
        )
        for axis_name in [f"xaxis{suffix}" for suffix in ([""] + [str(index) for index in range(2, len(panel_series) + 1)])]:
            fig.layout[axis_name].update(categoryorder="array", categoryarray=category_orders["time_label"])
        fig.show("browser")
        return fig

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
            "in_transit:car": "#00C389",
            "in_transit:walk": "#FFA15A",
            "in_transit:bicycle": "#EF553B",
            "in_transit:other": "#AB63FA",
            "in_transit:walk/public_transport/walk": "#19D3F3",
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
            compare_with.get()
            prefix = "weekday" if self.results.is_weekday else "weekend"
            plan_steps_comp = pl.scan_parquet(compare_with.cache_path[f"{prefix}_plan_steps"])
            costs_comp = pl.scan_parquet(compare_with.cache_path[f"{prefix}_costs"])

            metric_comp = metric + "_comp"
            metric_per_person_comp = metric + "_per_person_comp"
            metric_per_groups_and_transport_zones_comp = (
                plan_steps_comp.filter(pl.col("activity_seq_id") != 0)
                .rename({"home_zone_id": "transport_zone_id"})
                .join(costs_comp, on=["from", "to", "mode"])
                .group_by(["transport_zone_id", "csp", "n_cars"])
                .agg(metric=(pl.col(metric) * pl.col("n_persons")).sum())
                .join(transport_zones_df.select(["transport_zone_id", "local_admin_unit_id"]), on=["transport_zone_id"])
                .join(self.results.demand_groups.rename({"home_zone_id": "transport_zone_id"}), on=["transport_zone_id", "csp", "n_cars"])
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
