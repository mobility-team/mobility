import os
import pathlib
import logging
import shutil
import random
import warnings

import geopandas as gpd
import matplotlib.pyplot as plt
import polars as pl

import plotly.express as px
import plotly.colors as pc


from typing import List

from mobility.population import Population
from mobility.choice_models.population_trips import PopulationTrips
from mobility.choice_models.population_trips_parameters import PopulationTripsParameters
from mobility.choice_models.results import Results
from mobility.motives import Motive
from mobility.transport_modes.transport_mode import TransportMode
from mobility.parsers.mobility_survey import MobilitySurvey

#New library
from PIL import Image

# Script ones
import dotenv
import mobility

class MultiSeedPopulationTrips:
    """Class created to manage multi-seed PopulationTrips. Parameters are the same than for PopulationTrips. """
    
    def __init__(
            
            self,
            population: Population,
            modes: List[TransportMode] = None,
            motives: List[Motive] = None,
            surveys: List[MobilitySurvey] = None,
            parameters: PopulationTripsParameters = None,
            seeds: List[int] = [0]
            
        ):
        
        list_population_trips = []
        
        for seed in seeds:

            print("init seed", seed)
            parameters_seed = parameters
            parameters_seed.seed = seed
            pop_seed = PopulationTrips(
                population = population,
                modes = modes,
                motives = motives,
                surveys=surveys,
                parameters = parameters_seed
                )
            list_population_trips.append(pop_seed)
        
        self.trips = list_population_trips
        
    def get(self):
        return self.trips
    
    def evaluate(self, metric, plot_diff=False, plot=True, compare_with=None, variable=None, **kwargs):
        if metric in ["distance_per_person", "time_per_person", "cost_per_person"]: #todo: , "ghg_per_person"
            metric_per_person = metric
            metric = metric.replace("_per_person", "")
            
            metric_all = self.trips[0].evaluate(metric_per_person).select("transport_zone_id", metric, "n_persons").group_by("transport_zone_id").agg(pl.col(metric).sum(), pl.col("n_persons").sum())
            i = 1
            for pop in self.trips[1:]:
                metric_add = pop.evaluate(metric_per_person).select("transport_zone_id", metric, "n_persons").group_by("transport_zone_id").agg(pl.col(metric).sum(), pl.col("n_persons").sum())
                suffix = "_" + str(i)
                metric_all = metric_all.join(metric_add, on="transport_zone_id", suffix=suffix)
                i += 1
            metric_all = metric_all.sort(by=metric)
            regex = "^" + metric + ".*$"
            metric_all = metric_all.with_columns(metric_mean=pl.mean_horizontal(regex),
                                                 n_persons_mean=pl.mean_horizontal("^n_persons.*$"))
            metric_all = metric_all.with_columns(metric_per_person=pl.col("metric_mean")/pl.col("n_persons_mean"))
            
            if plot_diff:
                logging.info(f"Plotting {metric}, each curve being a different seed. Curves should be close.")
                pandas_metric=metric_all.select(pl.col(regex)).to_pandas()
                pandas_metric.plot()
                
            if plot:
                pass #todo : add the mean and median map for the metric
                
            if compare_with is not None:
                metric_all = metric_all.join(compare_with.select("transport_zone_id", "metric_per_person"), on="transport_zone_id", suffix="_base")
                metric_all = metric_all.with_columns(delta=pl.col("metric_per_person")-pl.col("metric_per_person_base"))

            return metric_all
        
        elif metric == "metrics_by_variable":
            print(variable)
            metric_all = self.trips[0].evaluate("metrics_by_variable", variable=variable, plot=False)
            i = 1
            for pop in self.trips[1:]:
                metric_add = pop.evaluate("metrics_by_variable", variable=variable, plot=False).select("variable", variable, "value", "delta", "delta_relative")
                suffix = "_" + str(i)
                metric_all = metric_all.join(metric_add, on=("variable", variable), suffix=suffix)
                i += 1
                
            if plot:              
                comparison_plot_df = (
                    metric_all
                    .rename({"value":"value_0"})
                    .select(["variable", variable, pl.col("^value.*$")])
                    .melt(["variable", variable], variable_name="value_type")
                    .sort(variable)
                )
                
                value_types = comparison_plot_df["value_type"].unique()
                value_types = [v for v in value_types if v != "value_ref"] + ["value_ref"]

                
                blues = pc.sequential.Blues_r
                blue_values = [v for v in value_types if v != "value_ref"]
                
                color_map = {
                    vt: blues[i % len(blues)]
                    for i, vt in enumerate(blue_values)
                }
                color_map["value_ref"] = "red"
                
                fig = px.bar(
                    comparison_plot_df,
                    x=variable,
                    y="value",
                    color="value_type",
                    facet_col="variable",
                    barmode="group",
                    facet_col_spacing=0.05,
                    color_discrete_map=color_map,
                    category_orders={"value_type": value_types}
                )
                fig.update_yaxes(matches=None, showticklabels=True)
                fig.show("browser")
            
        else:
            return NotImplemented
    
            
    def plot_modal_share(self, plot_mode="all", **kwargs):
        if plot_mode == "all":
            logging.info("Plotting all modal shares for the given seeds")
            for t in self.trips:
                t.plot_modal_share(**kwargs)
        else:
            return NotImplemented
    
    def plot_od_flows(self, plot_mode="all", **kwargs):
        """
        Plots flows for the given seeds. Can show them all or create a GIF.

        Parameters
        ----------
        plot_mode : TYPE, optional
            Plot mode : "all" shows OD flows for all seeds. "gif" creates a gif. The default is "all".
        **kwargs : TYPE
            Same parameters than the function plot_od_flows from PopulationTrips
        """
        if plot_mode == "all":
            logging.info("Plotting all OD flows for the given seeds")
            for t in self.trips:
                t.plot_od_flows(**kwargs)
        elif plot_mode == "gif":
            frames = []
            for t in self.trips:
                t.plot_od_flows(save=True, **kwargs)
                text = "plot-" + str(t.parameters["seed"]) + ".png"
                frames.append(Image.open(text))
            
        frames[0].save(
            "output.gif",
            format="GIF",
            append_images=frames[1:],
            save_all=True,
            duration=400,  # milliseconds per frame
            loop=0         # 0 = infinite loop
        )    
        
    def get_prominent_cities(self, **kwargs):
        """Returning the cities of the first simulation as it is consistent over simulations"""
        return self.trips[0].get_prominent_cities(**kwargs)
        
if __name__ == "__main__":
    dotenv.load_dotenv()
    os.environ["MOBILITY_GTFS_DOWNLOAD_DATE"] = "2025/12/01"
    mobility.set_params(
            package_data_folder_path="D:/mobility-data",
            project_data_folder_path="D:/test-09",
            debug=False
    )

    transport_zones = mobility.TransportZones("fr-29019", radius = 40, level_of_detail=1)
    emp = mobility.EMPMobilitySurvey()
    pop = mobility.Population(transport_zones, sample_size = 1000)
    modes = [mobility.CarMode(transport_zones), mobility.WalkMode(transport_zones), mobility.BicycleMode(transport_zones), mobility.PublicTransportMode(transport_zones)]
    surveys = [emp]
    motives = [mobility.HomeMotive(), mobility.WorkMotive(), mobility.OtherMotive(population=pop)]

    # Simulating the trips for this population for three modes : car, walk and bicyle, and only home and work motives (OtherMotive is mandatory)
    pop_trips = MultiSeedPopulationTrips(
        pop,
        modes,
        motives,
        surveys,
        parameters=mobility.PopulationTripsParameters(n_iterations=4, k_mode_sequences=6),
        seeds = [0, 42, 69, 99, 123]
        )
        
    # for t in pop_trips.get():
    #     g = t.get()
    
    #labels = pop_trips.get_prominent_cities()
    #pop_trips.plot_modal_share(mode="walk")
    #pop_trips.plot_od_flows(mode="gif", labels=labels)
    d = pop_trips.evaluate("distance_per_person")
    t = pop_trips.evaluate("time_per_person")
    c = pop_trips.evaluate("cost_per_person")
    g = pop_trips.evaluate("ghg_per_person")
    
    v = pop_trips.evaluate("metrics_by_variable", variable="mode", plot=True)
    v2 = pop_trips.evaluate("metrics_by_variable", variable="motive", plot=True)
    v3 = pop_trips.evaluate("metrics_by_variable", variable="time_bin", plot=True)
    v4 = pop_trips.evaluate("metrics_by_variable", variable="distance_bin", plot=True)
    
