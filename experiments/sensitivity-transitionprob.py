import os
import dotenv

import mobility
from mobility import CarMode, WalkMode, BicycleMode, PublicTransportMode, GeneralizedCostParameters, CostOfTimeParameters, WorkParameters, OtherParameters


import polars as pl
import matplotlib.pyplot as plt

from mobility.trips.group_day_trips import Parameters


dotenv.load_dotenv()

os.environ["MOBILITY_GTFS_DOWNLOAD_DATE"] = "2026/04/30"

mobility.set_params(
        package_data_folder_path="D:/mobility-data",
        project_data_folder_path="D:/sensitivity/2026",
        debug=False,
        # r_timeout_seconds=600,
        # r_max_retries=2,
        # r_retry_delay_seconds=10,
        # r_heartbeat_interval_seconds=30,
)

global_metrics = pl.DataFrame()
modal_shares = pl.DataFrame()
ssis = pl.DataFrame()
ssis200 = pl.DataFrame()


# VARIABLES
congestion_flows_scaling_factors = [0.16, 0.18, 0.20, 0.25]
radiations = [0.999, 0.9999, 0.99999, 0.999999, 0.9999999, 0.99999999]
car_constant_costs = [0.0, 2.0, 3.0, 3.5, 4.0, 4.5, 5.0, 6.0]
car_distance_costs = [0.0, 0.05, 0.1, 0.15, 0.2, 0.3]
walk_constant_costs = [0.0, 0.5, 1.0, 2.0]
walk_distance_costs = [0.0, 0.01, 0.02, 0.05, 0.1, 0.2]
bicycle_constant_costs = [0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 8.0, 10.0]
bicycle_distance_costs = walk_distance_costs
pt_constant_costs = [0.0, 0.5, 1.0, 1.5, 2.0, 3.0, 4.0]
pt_distance_costs = [0.0, 0.02, 0.04, 0.06, 0.1, 0.2]
costs_of_time = [i * 2 for i in range (13)] # 0 to 24
values_of_time = costs_of_time
values_of_time_home = [i * 2 for i in range(5)] # 0 to 8
n_iterations = range(14, 15)

# 1. Choose just below which variable to investigate
# 2. Change the name of the legend just below
# 3. Then change the line where it is used by using the parameter 'factor' (do not forget to remove it where it was used elsewhere)
factors = n_iterations
legend = "n_iterations (congestion at every iteration)"
# legend = "radiation_lambda for both work and other motives"
# legend = "radiation_lambda for work (other=0.9999)"
# legend = "radiation_lambda for other (work=0.99999)"

radius_exceptions = []

#Rennes 20-
# radiuses = range(32, 60, 12)
# insee = "fr-35238"
# radius_exceptions = []

# Bordeaux 20- | blocage edge 68
insee = "fr-33063"
city_name = "Bordeaux"

#Toulouse 20-
# insee = "fr-31555"
# city_name = "Toulouse"

#Bayonne 20-
# insee = "fr-64102"
# city_name = "Bayonne"

radius = 20
transitions = [0.05, 0.1, 0.25, 0.5, 0.8, 1.0]
# speed things up when n_iterations > 10:
transitions = [0.1, 0.5, 0.8, 1.0]
#tmp
# transitions = [0.8, 1.0]


for transition in transitions:
    for i, factor in enumerate(factors):
        if radius not in radius_exceptions:
        # if i >= 1:
            # factor_other = factors[i-1]
            print(f"\n\n\nRADIUS: {radius}, TRANSITION PROBABILITY: {transition}, FACTOR: {factor}\n")
            transport_zones = mobility.TransportZones(insee, radius = radius, level_of_detail=1)

            emp = mobility.EMPMobilitySurvey()
            pop = mobility.Population(transport_zones, sample_size = 1000)
            car_mode = CarMode(transport_zones,
                             congestion=True,
                             congestion_flows_scaling_factor=0.2, #0.2 by default
                             generalized_cost_parameters=GeneralizedCostParameters(cost_constant=4.0, #4.0 by default
                                                                                   cost_of_distance=0.15, #0.15 by default
                                                                                   cost_of_time=CostOfTimeParameters(intercept=20.0))) #20.0 by default
            walk_mode = WalkMode(transport_zones,
                     generalized_cost_parameters=GeneralizedCostParameters(cost_constant=0.0, #0.0 by default
                                                                           cost_of_distance=0.05, #0.05 by default
                                                                           cost_of_time=CostOfTimeParameters(intercept=20.0))) #20.0 by default
            bicycle_mode = BicycleMode(transport_zones,
                        generalized_cost_parameters=GeneralizedCostParameters(cost_constant=5.0, #5.0 by default
                                                                              cost_of_distance=0.04, #0.04 by default
                                                                              cost_of_time=CostOfTimeParameters(intercept=20.0))) #20.0 by default
            pt_mode = PublicTransportMode(transport_zones,
                                          first_leg_mode= walk_mode, last_leg_mode= walk_mode,
                                generalized_cost_parameters=GeneralizedCostParameters(cost_constant=1.5, #1.5 by default
                                                                                      cost_of_distance=0.06, #0.06 by default
                                                                                      cost_of_time=CostOfTimeParameters(intercept=8.0))) #8.0 by default
            modes = [car_mode, walk_mode, bicycle_mode, pt_mode]
            surveys = [emp]
            activities = [mobility.HomeActivity(value_of_time=3.0, #3.0 by default 
                                                value_of_time_stay_home=2.0), #2.0 by default 
                          mobility.WorkActivity(parameters=WorkParameters(radiation_lambda=0.99999, #0.99999 by default
                                                                          value_of_time=14.0)), #14.0 by default
                          mobility.OtherActivity(population=pop, parameters=OtherParameters(radiation_lambda=0.9999, #0.9999 by default
                                                                                            value_of_time=12.0))] #12.0 by default 
            
            # Simulating the trips for this population for three modes : car, walk and bicyle, and only home and work motives (OtherMotive is mandatory)
            population_trips = mobility.PopulationGroupDayTrips(
                pop,
                modes,
                activities,
                surveys,
                parameters=Parameters(
                    n_iterations=factor, # 4 by default
                    n_iter_per_cost_update=5, #3 by default
                    mode_sequence_search_parallel=False,
                    transition_revision_probability=transition, # 1 b   y default
                    use_rust_mode_sequence_search=True,
                    persist_iteration_artifacts=False,
                    
                ),
                )
        
            # get it back
            results = population_trips.weekday_run.results()
            labels=results.get_prominent_cities()
            
            # You can get weekday plan steps to inspect them
            weekday_plan_steps = population_trips.get()["weekday_plan_steps"].collect()        
            

            
            if global_metrics.is_empty():
                rad = pl.DataFrame({"transition": [transition, transition],
                                    "factor": [factor, factor],
                                    "type": ["value", "value_ref"]})
                global_metrics = pl.concat([rad, population_trips.weekday_run.evaluate("global_metrics").select(["value", "value_ref"]).transpose()], how="horizontal")
            else:
                rad = pl.DataFrame({"transition": [transition, transition],
                                    "factor": [factor, factor],
                                    "type": ["value", "value_ref"]})
                global_metrics = pl.concat([global_metrics, pl.concat([rad, population_trips.weekday_run.evaluate("global_metrics").select(["value", "value_ref"]).transpose()], how="horizontal")])
                #global_metrics = global_metrics.join(population_trips.weekday_run.evaluate("global_metrics"), on =["country", "variable"], suffix=suffix)
            
            if modal_shares.is_empty():
                rad = pl.DataFrame({"transition": [transition for i in range(16)],
                                    "factor": [factor for i in range(16)]})
                mp = population_trips.weekday_run.evaluate("metrics_by_variable", variable="mode", plot=False)
                modal_shares = pl.concat([rad, population_trips.weekday_run.evaluate("metrics_by_variable", variable="mode", plot=False).select(["variable", "mode", "value", "value_ref"])], how="horizontal")
            else:
                rad = pl.DataFrame({"transition": [transition for i in range(16)],
                                    "factor": [factor for i in range(16)]})
                local_modal_shares = pl.concat([rad, population_trips.weekday_run.evaluate("metrics_by_variable", variable="mode", plot=False).select(["variable", "mode", "value", "value_ref"])], how="horizontal")
                modal_shares = pl.concat([modal_shares, local_modal_shares])
    
            
            if radius % 50 == 0:            
                metrics_by_mode = population_trips.weekday_run.evaluate("metrics_by_variable", variable="mode", plot=True)
                metrics_by_motive = population_trips.weekday_run.evaluate("metrics_by_variable", variable="motive", plot=True)
            
            # # OD flows between transport zones and modal shares
            if radius % 12 == 0:
                results.plot_od_flows(mode="car", level_of_detail=1, labels=labels)
                results.plot_od_flows(mode="walk", level_of_detail=1, labels=labels)
                results.plot_od_flows(mode="bicycle", level_of_detail=1, labels=labels)
                results.plot_od_flows(mode="public_transport", labels=labels)
                results.plot_modal_share(mode="public_transport", labels=labels)
                results.plot_modal_share(mode="bicycle", labels=labels)
                results.plot_modal_share(mode="walk", labels=labels)
                cms = results.plot_modal_share(mode="car", labels=labels)
                
            if radius % 20 == 0:
                # Congestion (visualization possible in QGIS)
                population_trips.weekday_run.evaluate("car_traffic")
                
    
            if ssis.is_empty():
    
                rad = pl.DataFrame({"transition": transition, "factor": factor, "type": "value"})
                ssis = pl.concat([rad, population_trips.weekday_run.evaluate("ssi")], how="horizontal")
                ssis200 = pl.concat([rad, population_trips.weekday_run.evaluate("ssi", threshold=200)], how="horizontal")
            else:
                rad = pl.DataFrame({"transition": transition, "factor": factor, "type": "value"})
                ssis = pl.concat([ssis, pl.concat([rad, population_trips.weekday_run.evaluate("ssi")], how="horizontal")])
                ssis200 = pl.concat([ssis200, pl.concat([rad, population_trips.weekday_run.evaluate("ssi", threshold=200)], how="horizontal")])
            

            
global_metrics = global_metrics.rename({"column_0": "n_trips",
                                        "column_1": "time",
                                        "column_2": "distance"})

print(global_metrics)
print(ssis)
print(ssis200)
  

def plot_df(df, metrics, city_name, legend, plot_ref_values=True):
    """Plot a graph with the result of the sensitivity analysis"""

    fig, axes = plt.subplots(
        nrows=len(metrics),
        figsize=(10, 4 * len(metrics)),
        sharex=True,
    )
    
    cmap = plt.get_cmap("tab10")
    
    for ax, metric in zip(axes, metrics):
    
        for i, transition in enumerate(transitions):
    
            color = cmap(i)
    
            # values varying with factor
            values = (
                df.filter(
                    (pl.col("transition") == transition)
                    & (pl.col("type") == "value")
                )
                .sort("factor")
            )
    
            x = values["factor"].to_numpy()
            y = values[metric].to_numpy()
    
            ax.plot(
                x,
                y,
                marker="o",
                color=color,
                label=f"transition_prob={transition}",
            )
            
            if plot_ref_values:    
                # reference values
                values_ref = (
                    df.filter(
                        (pl.col("transition") == transition)
                        & (pl.col("type") == "value_ref")
                    )
                    .sort("factor")
                )
        
                x_ref = values_ref["factor"].to_numpy()
                y_ref = values_ref[metric].to_numpy()
        
                ax.plot(
                    x_ref,
                    y_ref,
                    marker="o",
                    color=color,
                    linestyle="--",
                    alpha=0.7,
                    label=f"r={transition} (ref)",
                )
        
    
        ax.set_title(metric)
        ax.set_ylabel("value")
        ax.grid(alpha=0.3)
    
    axes[-1].set_xlabel(legend)
    #axes[-1].set_xscale("logit") # only for radiation_lambda 
    
    # single legend
    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(
        handles,
        labels,
        loc="upper right",
        ncol=min(len(transitions), 6),
    )
    
    plt.suptitle(f"Sensitivity around {city_name}", x=0.10, size='xx-large', ha='right', va='center')
    # plt.tight_layout(rect=[0, 0, 1, 0.95])
    plt.tight_layout(rect=[0, 0, 1, 1])
    plt.show()

def plot_mode(df, mode, metrics, city_name, legend, plot_ref_values=True, mode_name=None):
    """Plot a graph with the result of the sensitivity analysis"""

    if mode_name is None:
        mode_name = mode

    fig, axes = plt.subplots(
        nrows=len(metrics),
        figsize=(10, 4 * len(metrics)),
        sharex=True,
    )
    
    cmap = plt.get_cmap("tab10")
    
    for ax, metric in zip(axes, metrics):
    
        for i, radius in enumerate(transitions):
    
            color = cmap(i)
    
            # values varying with factor
            values = (
                df.filter(
                    (pl.col("transition") == transition)
                    & (pl.col("mode") == mode)
                    & (pl.col("variable") == metric)
                )
                .sort("factor")
            )
    
            x = values["factor"].to_numpy()
            y = values["value"].to_numpy()
    
            ax.plot(
                x,
                y,
                marker="o",
                color=color,
                label=f"t_prob={transition}",
            )
            
            if plot_ref_values:    
                # reference values
        
                x_ref = values["factor"].to_numpy()
                y_ref = values["value_ref"].to_numpy()
        
                ax.plot(
                    x_ref,
                    y_ref,
                    marker="o",
                    color=color,
                    linestyle="--",
                    alpha=0.7,
                    label=f"t_prob={transition} (ref)",
                )
        
    
        ax.set_title(metric)
        ax.set_ylabel("value")
        ax.grid(alpha=0.3)
    
    axes[-1].set_xlabel(legend)
    #axes[-1].set_xscale("logit") # only for radiation_lambda 
    
    # single legend
    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(
        handles,
        labels,
        loc="upper right",
        ncol=min(len(transitions), 6),
    )
    
    plt.suptitle(f"{city_name}. {mode_name}", x=0.10, size='xx-large', ha='right', va='center')
    # plt.tight_layout(rect=[0, 0, 1, 0.95])
    plt.tight_layout(rect=[0, 0, 1, 1])
    plt.show()

def plot_all_modes(df, modes, metrics, city_name, legend, plot_ref_values=True):
    """Plot a graph with the result of the sensitivity analysis"""

    fig, axess = plt.subplots(
        nrows=len(modes),
        ncols=len(metrics),
        figsize=(20, 4 * len(metrics)),
        sharex=True,
    )
    
    cmap = plt.get_cmap("tab10")
    
    
    for axes, mode in zip(axess, modes):
        for ax, metric in zip(axes, metrics):
        
            for i, transition in enumerate(transitions):
        
                color = cmap(i)
        
                # values varying with factor
                values = (
                    df.filter(
                        (pl.col("transition") == transition)
                        & (pl.col("mode") == mode)
                        & (pl.col("variable") == metric)
                    )
                    .sort("factor")
                )
        
                x = values["factor"].to_numpy()
                y = values["value"].to_numpy()
        
                ax.plot(
                    x,
                    y,
                    marker="o",
                    color=color,
                    label=f"t_prob={transition}",
                )
                
                if plot_ref_values:    
                    # reference values
            
                    x_ref = values["factor"].to_numpy()
                    y_ref = values["value_ref"].to_numpy()
            
                    ax.plot(
                        x_ref,
                        y_ref,
                        marker="o",
                        color=color,
                        linestyle="--",
                        alpha=0.7,
                        label=f"t_prob={transition} (ref)",
                    )
            
        
            ax.set_title(metric)
            ax.set_ylabel("value")
            ax.grid(alpha=0.3)
        
        axes[-1].set_xlabel(legend)
        #axes[-1].set_xscale("logit") # only for radiation_lambda 
    
    # single legend
    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(
        handles,
        labels,
        loc="upper right",
        ncol=min(len(transitions), 6),
    )
    
    plt.suptitle(f"{city_name}", x=0.10, size='xx-large', ha='right', va='center')
    # plt.tight_layout(rect=[0, 0, 1, 0.95])
    plt.tight_layout(rect=[0, 0, 1, 1])
    plt.show()


plot_df(global_metrics, ["n_trips", "time", "distance"], city_name, legend)
plot_df(ssis, ["ssi-20", "ssi_liu_yan_2020", "ssi_yan_2014"], city_name, legend, plot_ref_values=False)
# plot_mode(modal_shares, "car", ["n_trips", "time", "distance"], city_name, legend, mode_name="🚗 Car")
# plot_mode(modal_shares, "walk", ["n_trips", "time", "distance"], city_name, legend, mode_name="🚶 Walk")
# plot_mode(modal_shares, "bicycle", ["n_trips", "time", "distance"], city_name, legend, mode_name="🚲 Bicyle")
# plot_mode(modal_shares, "walk/public_transport/walk", ["n_trips", "time", "distance"], city_name, legend, mode_name="🚎 Public transport")
plot_all_modes(modal_shares, ["car", "walk", "bicycle", "walk/public_transport/walk"], ["n_trips", "time", "distance"], city_name, legend)
# plot_df(ssis200, ["ssi-200", "ssi_liu_yan_2020", "ssi_yan_2014"], city_name, legend, plot_ref_values=False)
