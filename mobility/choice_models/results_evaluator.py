import json
import polars as pl
import pandas as pd
import numpy as np
import plotly.express as px

class ResultsEvaluator:
    
    def sink_occupation(
            self,
            transport_zones,
            states_steps,
            sinks,
            plot_motive: str = None
        ):

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
            
            tz = transport_zones.get()
            tz = tz.to_crs(4326)
            
            tz = tz.merge(
                sink_occupation.filter(pl.col("motive") == plot_motive).to_pandas(),
                on="transport_zone_id",
                how="left"
            )
            
            tz["sink_occupation"] = tz["sink_occupation"].fillna(0.0)
            tz["sink_occupation"] = self.replace_outliers(tz["sink_occupation"])

            fig = px.choropleth(
                tz.drop(columns="geometry"),
                geojson=json.loads(tz.to_json()),
                locations="transport_zone_id",
                featureidkey="properties.transport_zone_id",
                color="sink_occupation",
                hover_data=["transport_zone_id", "motive", "sink_occupation"],
                color_continuous_scale="Viridis",
                projection="mercator"
            )
            fig.update_geos(fitbounds="geojson", visible=False)
            fig.show("browser")
        
        return sink_occupation
    
    
    def trip_count_by_demand_group(self, transport_zones, states_steps, demand_groups, plot):
        
        trip_count = (
            states_steps
            .filter(pl.col("motive_seq_id") != 0)
            .group_by(["home_zone_id", "csp", "n_cars"])
            .agg(
                n_trips=pl.col("n_persons").sum()
            )
            .join(demand_groups, on=["home_zone_id", "csp", "n_cars"])
            .with_columns(
                n_trips_per_person=pl.col("n_trips")/pl.col("n_persons")
            )
            .collect(engine="streaming")
        )
        
        if plot:
            
            tz = transport_zones.get()
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

            fig = px.choropleth(
                tz.drop(columns="geometry"),
                geojson=json.loads(tz.to_json()),
                locations="transport_zone_id",
                featureidkey="properties.transport_zone_id",
                color="n_trips_per_person",
                hover_data=["transport_zone_id", "n_trips_per_person"],
                color_continuous_scale="Viridis",
                projection="mercator"
            )
            fig.update_geos(fitbounds="geojson", visible=False)
            fig.show("browser")
        
        return trip_count
        
    
    def distance_per_person(self, transport_zones, states_steps, costs, demand_groups, plot):
        
        distance = (
            states_steps
            .filter(pl.col("motive_seq_id") != 0)
            .join(costs, on=["from", "to", "mode"])
            .group_by(["home_zone_id", "csp", "n_cars"])
            .agg(
                distance=pl.col("distance").sum()
            )
            .join(demand_groups, on=["home_zone_id", "csp", "n_cars"])
            .with_columns(
                distance_per_person=pl.col("distance")/pl.col("n_persons")
            )
            .collect(engine="streaming")
        )
        
        if plot:
            
            tz = transport_zones.get()
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

            fig = px.choropleth(
                tz.drop(columns="geometry"),
                geojson=json.loads(tz.to_json()),
                locations="transport_zone_id",
                featureidkey="properties.transport_zone_id",
                color="distance_per_person",
                hover_data=["transport_zone_id", "distance_per_person"],
                color_continuous_scale="Viridis",
                projection="mercator"
            )
            fig.update_geos(fitbounds="geojson", visible=False)
            fig.show("browser")
        
        return distance
             
    
    def time_per_person(self, transport_zones, states_steps, costs, demand_groups, plot):
        
        distance = (
            states_steps
            .filter(pl.col("motive_seq_id") != 0)
            .join(costs, on=["from", "to", "mode"])
            .group_by(["home_zone_id", "csp", "n_cars"])
            .agg(
                time=pl.col("time").sum()*60.0
            )
            .join(demand_groups, on=["home_zone_id", "csp", "n_cars"])
            .with_columns(
                time_per_person=pl.col("time")/pl.col("n_persons")
            )
            .collect(engine="streaming")
        )
        
        if plot:
            
            tz = transport_zones.get()
            tz = tz.to_crs(4326)
            
            tz = tz.merge(
                (
                    distance
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
            
            fig = px.choropleth(
                tz.drop(columns="geometry"),
                geojson=json.loads(tz.to_json()),
                locations="transport_zone_id",
                featureidkey="properties.transport_zone_id",
                color="time_per_person",
                hover_data=["time_per_person", "time_per_person"],
                color_continuous_scale="Viridis",
                projection="mercator"
            )
            fig.update_geos(fitbounds="geojson", visible=False)
            fig.show("browser")
        
        return distance
    
    
    def replace_outliers(self, series, method="iqr", z_thresh=3.0):
        
        s = series.copy()
        q25 = s.quantile(0.25)
        q75 = s.quantile(0.75)
        iqr = q75 - q25
        lower, upper = q25 - 1.5 * iqr, q75 + 1.5 * iqr
        
        return s.mask((s < lower) | (s > upper), np.nan)
    