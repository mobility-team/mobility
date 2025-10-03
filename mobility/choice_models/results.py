import json
import polars as pl
import numpy as np
import plotly.express as px

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
            weekend_costs
        ):
        
        self.transport_zones = transport_zones
        self.demand_groups = demand_groups
        self.weekday_states_steps = weekday_states_steps
        self.weekend_states_steps = weekend_states_steps
        self.weekday_sinks = weekday_sinks
        self.weekend_sinks = weekend_sinks
        self.weekday_costs = weekday_costs
        self.weekend_costs = weekend_costs
        
        self.metrics_methods = {
            "sink_occupation": self.sink_occupation,
            "trip_count_by_demand_group": self.trip_count_by_demand_group,
            "distance_per_person": self.distance_per_person,
            "time_per_person": self.time_per_person
        }
            
        
    def sink_occupation(
            self,
            weekday: bool = True,
            plot_motive: str = None
        ):
        
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
    
    
    def replace_outliers(self, series, method="iqr", z_thresh=3.0):
        
        s = series.copy()
        q25 = s.quantile(0.25)
        q75 = s.quantile(0.75)
        iqr = q75 - q25
        lower, upper = q25 - 1.5 * iqr, q75 + 1.5 * iqr
        
        return s.mask((s < lower) | (s > upper), np.nan)
    