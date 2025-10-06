import json
import polars as pl
import numpy as np
import plotly.express as px

from typing import Literal

class Results:
    
    def __init__(
            self,
            transport_zones,
            demand_groups,
            weekday_states_steps,
            weekend_states_steps,
            weekday_sinks,
            weekend_sinks,
            weekday_costs,
            weekend_costs,
            weekday_chains,
            weekend_chains
        ):
        
        self.transport_zones = transport_zones
        self.demand_groups = demand_groups
        self.weekday_states_steps = weekday_states_steps
        self.weekend_states_steps = weekend_states_steps
        self.weekday_sinks = weekday_sinks
        self.weekend_sinks = weekend_sinks
        self.weekday_costs = weekday_costs
        self.weekend_costs = weekend_costs
        self.weekday_chains = weekday_chains
        self.weekend_chains = weekend_chains
        
        self.metrics_methods = {
            "global_metrics": self.global_metrics,
            "sink_occupation": self.sink_occupation,
            "trip_count_by_demand_group": self.trip_count_by_demand_group,
            "distance_per_person": self.distance_per_person,
            "time_per_person": self.time_per_person
        }
        
        
        
    def global_metrics(
            self,
            weekday: bool = True,
            normalize: bool = True
        ):
    
        states_steps = self.weekday_states_steps if weekday else self.weekend_states_steps
        ref_states_steps = self.weekday_chains if weekday else self.weekend_chains
        
        n_persons = self.demand_groups.collect()["n_persons"].sum()
        
        trip_count = (
            states_steps 
            .filter(pl.col("motive_seq_id") != 0)
            .select(
                n_trips=pl.col("n_persons").sum(),
                time=pl.col("time").sum(),
                distance=pl.col("distance").sum()
            )
            .melt()
            .collect(engine="streaming")
        )
        
        trip_count_ref = (
            ref_states_steps 
            .filter(pl.col("motive_seq_id") != 0)
            .select(
                n_trips=pl.col("n_persons").sum(),
                time=pl.col("travel_time").sum(),
                distance=pl.col("distance").sum()
            )
            .melt()
            .collect(engine="streaming")
        )
        
        comparison = (
            trip_count
            .join(
                trip_count_ref,
                on="variable",
                suffix="_ref"
            )
        )
        
        if normalize:
            comparison = (
                comparison 
                .with_columns(
                    value=pl.col("value")/n_persons,
                    value_ref=pl.col("value_ref")/n_persons
                )
            )
           
        comparison = (
            comparison
            .with_columns(
                delta=pl.col("value") - pl.col("value_ref")
            )
            .with_columns(
                delta_relative=pl.col("delta")/pl.col("value_ref")
            )
            .select(["variable", "value", "value_ref", "delta", "delta_relative"])
        )
        
        return comparison
    
    
            
    def metrics_by_variable(
            self,
            variable: Literal["mode", "time_bin", "distance_bin"] = None,
            weekday: bool = True,
            normalize: bool = True,
            plot: bool = False
        ):
    
        states_steps = self.weekday_states_steps if weekday else self.weekend_states_steps
        ref_states_steps = self.weekday_chains if weekday else self.weekend_chains
        
        n_persons = self.demand_groups.collect()["n_persons"].sum()
        
        with pl.StringCache():
            
            trip_count = (
                states_steps 
                .filter(pl.col("motive_seq_id") != 0)
                .with_columns(
                    time_bin=(pl.col("time")*60.0).cut([0.0, 5.0, 10, 20, 30.0, 45.0, 60.0, 1e6], left_closed=True),
                    distance_bin=pl.col("distance").cut([0.0, 1.0, 5.0, 10.0, 20.0, 40.0, 80.0, 1e6], left_closed=True)
                )
                .group_by(variable)
                .agg(
                    n_trips=pl.col("n_persons").sum(),
                    time=pl.col("time").sum(),
                    distance=pl.col("distance").sum()
                )
                .melt(variable)
                .collect(engine="streaming")
            )
            
            trip_count_ref = (
                ref_states_steps 
                .filter(pl.col("motive_seq_id") != 0)
                .with_columns(
                    mode=pl.col("mode").cast(pl.String()),
                    time_bin=(pl.col("travel_time")*60.0).cut([0.0, 5.0, 10.0, 20.0, 30.0, 45.0, 60.0, 1e6], left_closed=True),
                    distance_bin=pl.col("distance").cut([0.0, 1.0, 5.0, 10.0, 20.0, 40.0, 80.0, 1e6], left_closed=True)
                )
                .group_by(variable)
                .agg(
                    n_trips=pl.col("n_persons").sum(),
                    time=pl.col("travel_time").sum(),
                    distance=pl.col("distance").sum()
                )
                .melt(variable)
                .collect(engine="streaming")
            )
            
        comparison = (
            trip_count
            .join(
                trip_count_ref,
                on=["variable", variable],
                suffix="_ref",
                how="full",
                coalesce=True
            )
        )
        
        if normalize:
            comparison = (
                comparison 
                .with_columns(
                    value=pl.col("value")/n_persons,
                    value_ref=pl.col("value_ref")/n_persons
                )
            )
           
        comparison = (
            comparison
            .with_columns(
                delta=pl.col("value") - pl.col("value_ref")
            )
            .with_columns(
                delta_relative=pl.col("delta")/pl.col("value_ref")
            )
            .select(["variable", variable, "value", "value_ref", "delta", "delta_relative"])
        )
        
        if plot:
            
            comparison_plot_df = (
                comparison
                .select(["variable", variable, "value", "value_ref"])
                .melt(["variable", variable], variable_name="value_type")
                .sort(variable)
            )
            
            
            fig = px.bar(
                comparison_plot_df,
                x=variable,
                y="value",
                color="value_type",
                facet_col="variable",
                barmode="group",
                facet_col_spacing=0.05
            )
            fig.update_yaxes(matches=None, showticklabels=True)
            fig.show("browser")
        
        return comparison
        
        
    def sink_occupation(
            self,
            weekday: bool = True,
            plot_motive: str = None
        ):
        """
        Compute sink occupation per (zone, motive), optionally map a single motive.
        
        Parameters
        ----------
        weekday : bool, default True
            Use weekday (True) or weekend (False) flows/sinks.
        plot_motive : str, optional
            If provided, renders a choropleth of occupation for that motive.
        
        Returns
        -------
        pl.DataFrame
            Columns: ['transport_zone_id', 'motive', 'duration', 'sink_capacity', 'sink_occupation'].
            'sink_occupation' = total occupied duration / capacity.
        """
        
        states_steps = self.weekday_states_steps if weekday else self.weekend_states_steps
        sinks = self.weekday_sinks if weekday else self.weekend_sinks

        sink_occupation = (
            states_steps 
            .filter(pl.col("motive_seq_id") != 0)
            .group_by(["to", "motive"])
            .agg(
                pl.col("duration").sum()
            )
            .join(
                sinks.select(["to", "motive", "sink_capacity"]),
                on=["to", "motive"]
            )
            .with_columns(
                sink_occupation=pl.col("duration")/pl.col("sink_capacity")
            )
            .rename({"to": "transport_zone_id"})
            .collect(engine="streaming")
        )

        if plot_motive:
            
            tz = self.transport_zones.get()
            tz = tz.to_crs(4326)
            
            tz = tz.merge(
                sink_occupation.filter(pl.col("motive") == plot_motive).to_pandas(),
                on="transport_zone_id",
                how="left"
            )
            
            tz["sink_occupation"] = tz["sink_occupation"].fillna(0.0)
            tz["sink_occupation"] = self.replace_outliers(tz["sink_occupation"])

            self.plot_map(tz, "sink_occupation")
        
        return sink_occupation
    
    
    def trip_count_by_demand_group(
            self,
            weekday: bool = True,
            plot: bool = False
        ):
        """
        Count trips and trips per person by demand group; optional map at home-zone level.
        
        Parameters
        ----------
        weekday : bool, default True
            Use weekday (True) or weekend (False) states.
        plot : bool, default False
            When True, shows a choropleth of average trips per person by home zone.
        
        Returns
        -------
        pl.DataFrame
            Grouped by ['home_zone_id', 'csp', 'n_cars'] with:
            - n_trips: total trips
            - n_persons: group size
            - n_trips_per_person: n_trips / n_persons
        """
        
        states_steps = self.weekday_states_steps if weekday else self.weekend_states_steps
        
        trip_count = (
            states_steps
            .filter(pl.col("motive_seq_id") != 0)
            .group_by(["home_zone_id", "csp", "n_cars"])
            .agg(
                n_trips=pl.col("n_persons").sum()
            )
            .join(self.demand_groups, on=["home_zone_id", "csp", "n_cars"])
            .with_columns(
                n_trips_per_person=pl.col("n_trips")/pl.col("n_persons")
            )
            .collect(engine="streaming")
        )
        
        if plot:
            
            tz = self.transport_zones.get()
            tz = tz.to_crs(4326)
            
            tz = tz.merge(
                (
                    trip_count
                    .group_by(["home_zone_id"])
                    .agg(
                        n_trips_per_person=pl.col("n_trips").sum()/pl.col("n_persons").sum()
                    )
                    .rename({"home_zone_id": "transport_zone_id"})
                    .to_pandas()
                ),
                on="transport_zone_id",
                how="left"
            )
            
            tz["n_trips_per_person"] = tz["n_trips_per_person"].fillna(0.0)
            tz["n_trips_per_person"] = self.replace_outliers(tz["n_trips_per_person"])

            self.plot_map(tz, "n_trips_per_person")
        
        return trip_count
        
    
    def distance_per_person(
            self,
            weekday: bool = True,
            plot: bool = False
        ):
        """
        Aggregate total travel distance and distance per person by demand group.
    
        Parameters
        ----------
        weekday : bool, default True
            Use weekday (True) or weekend (False) data.
        plot : bool, default False
            When True, shows a choropleth of average distance per person by home zone.
    
        Returns
        -------
        pl.DataFrame
            Grouped by ['home_zone_id', 'csp', 'n_cars'] with:
            - distance: sum(distance * n_persons)
            - n_persons: group size
            - distance_per_person: distance / n_persons
        """
        
        states_steps = self.weekday_states_steps if weekday else self.weekend_states_steps
        costs = self.weekday_costs if weekday else self.weekend_costs
        
        distance = (
            states_steps
            .filter(pl.col("motive_seq_id") != 0)
            .join(costs, on=["from", "to", "mode"])
            .group_by(["home_zone_id", "csp", "n_cars"])
            .agg(
                distance=(pl.col("distance")*pl.col("n_persons")).sum()
            )
            .join(self.demand_groups, on=["home_zone_id", "csp", "n_cars"])
            .with_columns(
                distance_per_person=pl.col("distance")/pl.col("n_persons")
            )
            .collect(engine="streaming")
        )
        
        if plot:
            
            tz = self.transport_zones.get()
            tz = tz.to_crs(4326)
            
            tz = tz.merge(
                (
                    distance
                    .group_by(["home_zone_id"])
                    .agg(
                        distance_per_person=pl.col("distance").sum()/pl.col("n_persons").sum()
                    )
                    .rename({"home_zone_id": "transport_zone_id"})
                    .to_pandas()
                ),
                on="transport_zone_id",
                how="left"
            )
            
            tz["distance_per_person"] = tz["distance_per_person"].fillna(0.0)
            tz["distance_per_person"] = self.replace_outliers(tz["distance_per_person"])

            self.plot_map(tz, "distance_per_person")
        
        return distance
             
    
    def time_per_person(
            self,
            weekday: bool = True,
            plot: bool = False
        ):
        """
        Aggregate total travel time and time per person by demand group.
        
        Parameters
        ----------
        weekday : bool, default True
            Use weekday (True) or weekend (False) data.
        plot : bool, default False
            When True, shows a choropleth of average time per person by home zone.
        
        Returns
        -------
        pl.DataFrame
            Grouped by ['home_zone_id', 'csp', 'n_cars'] with:
            - time: sum(time * n_persons) * 60.0 (minutes)
            - n_persons: group size
            - time_per_person: time / n_persons
        """
        
        states_steps = self.weekday_states_steps if weekday else self.weekend_states_steps
        costs = self.weekday_costs if weekday else self.weekend_costs
        
        time = (
            states_steps
            .filter(pl.col("motive_seq_id") != 0)
            .join(costs, on=["from", "to", "mode"])
            .group_by(["home_zone_id", "csp", "n_cars"])
            .agg(
                time=(pl.col("time")*pl.col("n_persons")).sum()*60.0
            )
            .join(self.demand_groups, on=["home_zone_id", "csp", "n_cars"])
            .with_columns(
                time_per_person=pl.col("time")/pl.col("n_persons")
            )
            .collect(engine="streaming")
        )
        
        if plot:
            
            tz = self.transport_zones.get()
            tz = tz.to_crs(4326)
            
            tz = tz.merge(
                (
                    time
                    .group_by(["home_zone_id"])
                    .agg(
                        time_per_person=pl.col("time").sum()/pl.col("n_persons").sum()
                    )
                    .rename({"home_zone_id": "transport_zone_id"})
                    .to_pandas()
                ),
                on="transport_zone_id",
                how="left"
            )
            
            tz["time_per_person"] = tz["time_per_person"].fillna(0.0)
            tz["time_per_person"] = self.replace_outliers(tz["time_per_person"])
            
            self.plot_map(tz, "time_per_person")

        
        return time
    
    
    def plot_map(self, tz, value: str = None):
        """
        Render a Plotly choropleth for a transport-zone metric.
        
        Parameters
        ----------
        tz : geopandas.GeoDataFrame
            Zones GeoDataFrame in EPSG:4326 with columns:
            ['transport_zone_id', value, 'geometry'].
        value : str
            Column name to color by (e.g., 'sink_occupation').
        
        Returns
        -------
        None
            Displays an interactive map in the browser.
        """
        
        fig = px.choropleth(
            tz.drop(columns="geometry"),
            geojson=json.loads(tz.to_json()),
            locations="transport_zone_id",
            featureidkey="properties.transport_zone_id",
            color=value,
            hover_data=["transport_zone_id", value],
            color_continuous_scale="Viridis",
            projection="mercator"
        )
        fig.update_geos(fitbounds="geojson", visible=False)
        fig.update_layout(margin=dict(l=0,r=0,t=0,b=0))
        fig.show("browser")
    
    
    def replace_outliers(self, series):
        """
        Mask outliers in a numeric pandas/Series-like array.
        
        Parameters
        ----------
        series : array-like
            Numeric series to clean.
        
        Returns
        -------
        array-like
            Series with outliers replaced by NaN (bounds: Q1 - 1.5*IQR, Q3 + 1.5*IQR).
        """
        
        s = series.copy()
        q25 = s.quantile(0.25)
        q75 = s.quantile(0.75)
        iqr = q75 - q25
        lower, upper = q25 - 1.5 * iqr, q75 + 1.5 * iqr
        
        return s.mask((s < lower) | (s > upper), np.nan)
    