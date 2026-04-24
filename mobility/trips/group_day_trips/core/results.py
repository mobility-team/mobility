import json
import logging

import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import plotly.express as px
import polars as pl

from typing import Literal

from ..evaluation.car_traffic_evaluation import CarTrafficEvaluation
from ..evaluation.public_transport_network_evaluation import (
    PublicTransportNetworkEvaluation,
)
from ..evaluation.routing_evaluation import RoutingEvaluation
from ..evaluation.travel_costs_evaluation import TravelCostsEvaluation
from ..transitions.transition_metrics import state_waterfall as _state_waterfall


class RunResults:
    """Run-scoped analysis helper for one day-type output set."""

    def __init__(
        self,
        *,
        inputs_hash,
        is_weekday: bool,
        transport_zones,
        demand_groups,
        plan_steps,
        opportunities,
        costs,
        chains,
        transitions,
        surveys,
        modes,
        parameters,
        run,
    ):
        self.inputs_hash = inputs_hash
        self.is_weekday = is_weekday
        self.transport_zones = transport_zones
        self.demand_groups = demand_groups
        self.plan_steps = plan_steps
        self.opportunities = opportunities
        self.costs = costs
        self.chains = chains
        self.transitions = transitions
        self.surveys = surveys
        self.modes = modes
        self.parameters = parameters
        self.run = run

        self.metrics_methods = {
            "global_metrics": self.global_metrics,
            "metrics_by_variable": self.metrics_by_variable,
            "opportunity_occupation": self.opportunity_occupation,
            "sink_occupation": self.opportunity_occupation,
            "state_waterfall": self.state_waterfall,
            "trip_count_by_demand_group": self.trip_count_by_demand_group,
            "distance_per_person": self.distance_per_person,
            "ghg_per_person": self.ghg_per_person,
            "time_per_person": self.time_per_person,
            "cost_per_person": self.cost_per_person,
            "immobility": self.immobility,
            "car_traffic": self.car_traffic,
            "travel_costs": self.travel_costs,
            "routing": self.routing,
            "public_transport_network": self.public_transport_network,
        }

    @property
    def period(self) -> str:
        """Return the string period label expected by plotting methods."""
        return "weekdays" if self.is_weekday else "weekends"

    def global_metrics(self, normalize: bool = True):
        """Compute high-level trip, time, and distance metrics for this run."""
        ref_plan_steps = (
            self.chains.rename({"travel_time": "time"})
            .with_columns(country=pl.col("country").cast(pl.String()))
        )

        transport_zones_df = (
            pl.DataFrame(self.transport_zones.get().drop("geometry", axis=1))
            .filter(pl.col("is_inner_zone"))
            .lazy()
        )
        study_area_df = pl.DataFrame(self.transport_zones.study_area.get().drop("geometry", axis=1)).lazy()

        n_persons = (
            self.demand_groups.rename({"home_zone_id": "transport_zone_id"})
            .join(transport_zones_df.select(["transport_zone_id", "local_admin_unit_id"]), on=["transport_zone_id"])
            .join(study_area_df.select(["local_admin_unit_id", "country"]), on=["local_admin_unit_id"])
            .group_by("country")
            .agg(pl.col("n_persons").sum())
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

        trip_count = aggregate(self.plan_steps)
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

    def metrics_by_variable(
        self,
        variable: Literal["mode", "activity", "time_bin", "distance_bin"] = None,
        normalize: bool = True,
        plot: bool = False,
    ):
        """Compare model outputs and reference chains by one categorical variable."""
        ref_plan_steps = (
            self.chains.rename({"travel_time": "time"})
            .with_columns(mode=pl.col("mode").cast(pl.String()))
        )

        transport_zones_df = (
            pl.DataFrame(self.transport_zones.get().drop("geometry", axis=1))
            .filter(pl.col("is_inner_zone"))
            .lazy()
        )

        n_persons = (
            self.demand_groups.rename({"home_zone_id": "transport_zone_id"})
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
            trip_count = aggregate(self.plan_steps)
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
            for s in self.surveys
        ]
        column_name = "immobility_weekday" if self.is_weekday else "immobility_weekend"
        surveys_immobility = (
            pl.concat(surveys_immobility)
            .with_columns(p_immobility=pl.col(column_name))
            .select(["country", "csp", "p_immobility"])
        )

        transport_zones_df = (
            pl.DataFrame(self.transport_zones.get().drop("geometry", axis=1))
            .filter(pl.col("is_inner_zone"))
            .lazy()
        )
        study_area_df = (
            pl.DataFrame(self.transport_zones.study_area.get().drop("geometry", axis=1)[["local_admin_unit_id", "country"]]).lazy()
        )

        immobility = (
            self.plan_steps.filter(pl.col("activity_seq_id") == 0)
            .rename({"home_zone_id": "transport_zone_id"})
            .join(transport_zones_df.select(["transport_zone_id", "local_admin_unit_id"]), on=["transport_zone_id"])
            .with_columns(pl.col("csp").cast(pl.String()))
            .join(
                self.demand_groups.rename({"n_persons": "n_persons_dem_grp", "home_zone_id": "transport_zone_id"})
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
            .with_columns(p_immobility=pl.col("n_persons_imm") / pl.col("n_persons_dem_grp"))
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

    def opportunity_occupation(self, plot_activity: str = None, mask_outliers: bool = False):
        """Compute opportunity occupation per zone and activity for this run."""
        transport_zones_df = (
            pl.DataFrame(self.transport_zones.get().drop("geometry", axis=1))
            .filter(pl.col("is_inner_zone"))
            .lazy()
        )

        opportunity_occupation = (
            self.plan_steps.filter(pl.col("activity_seq_id") != 0)
            .rename({"home_zone_id": "transport_zone_id"})
            .join(transport_zones_df.select(["transport_zone_id", "local_admin_unit_id"]), on=["transport_zone_id"])
            .with_columns(pl.col("activity").cast(pl.String()))
            .group_by(["to", "activity"])
            .agg(pl.col("duration").sum())
            .join(
                self.opportunities.select(["to", "activity", "opportunity_capacity"]).with_columns(
                    pl.col("activity").cast(pl.String())
                ),
                on=["to", "activity"],
            )
            .with_columns(opportunity_occupation=pl.col("duration") / pl.col("opportunity_capacity"))
            .rename({"to": "transport_zone_id"})
            .collect(engine="streaming")
        )

        if plot_activity:
            tz = self.transport_zones.get().to_crs(4326)
            tz = tz.merge(transport_zones_df.collect().to_pandas(), on="transport_zone_id")
            tz = tz.merge(
                opportunity_occupation.filter(pl.col("activity") == plot_activity).to_pandas(),
                on="transport_zone_id",
                how="left",
            )
            tz["opportunity_occupation"] = tz["opportunity_occupation"].fillna(0.0)
            if mask_outliers:
                tz["opportunity_occupation"] = self.mask_outliers(tz["opportunity_occupation"])
            self.plot_map(tz, "opportunity_occupation", plot_activity)

        return opportunity_occupation

    def state_waterfall(
        self,
        quantity: Literal["distance", "utility", "travel_time", "trip_count"],
        plot: bool = True,
        top_n: int = 5,
        demand_group_ids: list[int] | None = None,
    ) -> tuple[pl.DataFrame, pl.DataFrame]:
        """Run one state-pair waterfall metric for this run."""
        return _state_waterfall(
            transitions=self.transitions,
            quantity=quantity,
            demand_groups=self.demand_groups,
            transport_zones=self.transport_zones,
            plot=plot,
            top_n=top_n,
            demand_group_ids=demand_group_ids,
        )

    def trip_count_by_demand_group(self, plot: bool = False, mask_outliers: bool = False):
        """Count trips and trips per person by demand group for this run."""
        transport_zones_df = (
            pl.DataFrame(self.transport_zones.get().drop("geometry", axis=1))
            .filter(pl.col("is_inner_zone"))
            .lazy()
        )

        trip_count = (
            self.plan_steps.filter(pl.col("activity_seq_id") != 0)
            .rename({"home_zone_id": "transport_zone_id"})
            .join(transport_zones_df.select(["transport_zone_id", "local_admin_unit_id"]), on=["transport_zone_id"])
            .group_by(["transport_zone_id", "csp", "n_cars"])
            .agg(n_trips=pl.col("n_persons").sum())
            .join(self.demand_groups.rename({"home_zone_id": "transport_zone_id"}), on=["transport_zone_id", "csp", "n_cars"])
            .with_columns(n_trips_per_person=pl.col("n_trips") / pl.col("n_persons"))
            .collect(engine="streaming")
        )

        if plot:
            tz = self.transport_zones.get().to_crs(4326)
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
            pl.DataFrame(self.transport_zones.get().drop("geometry", axis=1))
            .filter(pl.col("is_inner_zone"))
            .lazy()
        )

        metric_per_person = metric + "_per_person"

        metric_per_groups_and_transport_zones = (
            self.plan_steps.filter(pl.col("activity_seq_id") != 0)
            .rename({"home_zone_id": "transport_zone_id"})
            .join(self.costs, on=["from", "to", "mode"])
            .group_by(["transport_zone_id", "csp", "n_cars"])
            .agg(metric=(pl.col(metric) * pl.col("n_persons")).sum())
            .join(transport_zones_df.select(["transport_zone_id", "local_admin_unit_id"]), on=["transport_zone_id"])
            .join(self.demand_groups.rename({"home_zone_id": "transport_zone_id"}), on=["transport_zone_id", "csp", "n_cars"])
            .with_columns(metric_per_person=pl.col("metric") / pl.col("n_persons"))
            .rename({"metric": metric, "metric_per_person": metric_per_person})
            .collect(engine="streaming")
        )

        if compare_with is not None:
            compare_with.get()
            prefix = "weekday" if self.is_weekday else "weekend"
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
                .join(self.demand_groups.rename({"home_zone_id": "transport_zone_id"}), on=["transport_zone_id", "csp", "n_cars"])
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
            tz = self.transport_zones.get().to_crs(4326)
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

    def plot_map(self, tz, value: str = None, activity: str = None, plot_method: str = "browser",
                 color_continuous_scale="Viridis", color_continuous_midpoint=None):
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
        logging.info(f"Plotting {mode} modal share for {zone} zones during {self.period}")
        population_df = self.plan_steps.collect().to_pandas()

        left_column = "from" if zone == "origin" else "to"
        mode_share = population_df.groupby([left_column, "mode"]).sum("n_persons")
        mode_share = mode_share.reset_index().set_index([left_column])
        mode_share["total"] = mode_share.groupby([left_column])["n_persons"].sum()
        mode_share["modal_share"] = mode_share["n_persons"] / mode_share["total"]

        if mode == "public_transport":
            mode_name = "Public transport"
            mode_share["mode"] = mode_share["mode"].replace(r"\S+/public_transport/\S+", "public_transport", regex=True)
        else:
            mode_name = mode.capitalize()
        mode_share = mode_share[mode_share["mode"] == mode]

        transport_zones_df = self.transport_zones.get()
        gc = gpd.GeoDataFrame(
            transport_zones_df.merge(mode_share, how="left", right_on=left_column, left_on="transport_zone_id", suffixes=('', '_z'))
        ).fillna(0)
        gcp = gc.plot("modal_share", legend=True)
        gcp.set_axis_off()
        plt.title(f"{mode_name} share per {zone} transport zone ({self.period})")

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

        logging.info(f"Plotting {mode} origin-destination flows during {self.period}")
        if activity != "all":
            logging.info("Speficic activities not implemented yet")
            return NotImplemented

        population_df = self.plan_steps.collect().to_pandas()
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

        biggest_flows = population_df.groupby(["from", "to"]).sum("n_persons").reset_index()
        biggest_flows = biggest_flows.where(biggest_flows["from"] != biggest_flows["to"]).nlargest(n_largest, "n_persons")
        transport_zones_df = self.transport_zones.get()
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
        plt.title(f"{mode_name} flows between transport zones on {self.period}")

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
        population_df = self.plan_steps.collect().to_pandas()
        study_area_df = self.transport_zones.study_area.get()
        tzdf = self.transport_zones.get()

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
        return CarTrafficEvaluation(self).get(*args, **kwargs)

    def travel_costs(self, *args, **kwargs):
        return TravelCostsEvaluation(self).get(*args, **kwargs)

    def routing(self, *args, **kwargs):
        return RoutingEvaluation(self).get(*args, **kwargs)

    def public_transport_network(self, *args, **kwargs):
        return PublicTransportNetworkEvaluation(self).get(*args, **kwargs)
