import os
import dotenv
import logging

import mobility

import polars as pl

dotenv.load_dotenv()


mobility.set_params(
    # package_data_folder_path=os.environ["MOBILITY_PACKAGE_DATA_FOLDER"],
    # project_data_folder_path=os.environ["MOBILITY_PROJECT_DATA_FOLDER"]
        package_data_folder_path="D:/mobility-data",
        project_data_folder_path="D:/test-09",
        debug=False
)

# Using Foix (a small town) and a limited radius for quick results
transport_zones = mobility.TransportZones("fr-09122", radius = 10)

# Using EMP, the latest national mobility survey for France
emp = mobility.EMPMobilitySurvey()

# Creating a synthetic population of 1000 for the area
pop = mobility.Population(transport_zones, sample_size = 1000)

modes = [mobility.CarMode(transport_zones), mobility.WalkMode(transport_zones), mobility.BicycleMode(transport_zones)]
surveys = [emp]
motives = [mobility.HomeMotive(), mobility.WorkMotive(), mobility.OtherMotive(population=pop)]


# Simulating the trips for this population for three modes : car, walk and bicyle, and only home and work motives (OtherMotive is mandatory)
pop_trips = mobility.PopulationTrips(
    pop,
    modes,
    motives,
    surveys,
    parameters=mobility.PopulationTripsParameters(n_iterations=4, k_mode_sequences=42)
    )

pop_trips_base = mobility.PopulationTrips(
    pop,
    modes,
    motives,
    surveys,
    parameters=mobility.PopulationTripsParameters(n_iterations=4, seed=49283092, k_mode_sequences=42)
    )

tz = transport_zones.get()


# You can get the weekday trips to inspect them
weekday_flows = pop_trips.get()["weekday_flows"].collect()
weekday_flows_base = pop_trips_base.get()["weekday_flows"].collect()

# You can also plot the flows, with labels for the cities that are bigger than their neighbours
labels=pop_trips.get_prominent_cities()

# You can can also get global metrics that will be compared to the theoetical values for this population 
cost_per_person = pop_trips.evaluate("cost_per_person", plot_delta=True, compare_with=pop_trips_base, labels=labels, plot=True)
dist_per_person = pop_trips.evaluate("distance_per_person", plot_delta=True, compare_with=pop_trips_base, plot=True)
time_per_person = pop_trips.evaluate("time_per_person", plot_delta=True, compare_with=pop_trips_base, plot=True)
ghg_per_person = pop_trips.evaluate("ghg_per_person", plot_delta=True, compare_with=pop_trips_base, plot=True)
global_metrics = pop_trips.evaluate("global_metrics")


pop_trips.plot_modal_share(mode="public_transport", labels=labels)
pop_trips.plot_modal_share(mode="bicycle")
pop_trips.plot_modal_share(mode="walk")
cms = pop_trips.plot_modal_share(mode="car")

# OD flows between transport zones
pop_trips.plot_od_flows(mode="car", level_of_detail=1, labels=labels)
pop_trips.plot_od_flows(mode="walk", level_of_detail=1)
pop_trips.plot_od_flows(mode="bicycle", level_of_detail=1)
pop_trips.plot_od_flows(mode="public_transport")