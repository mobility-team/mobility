import os
import dotenv

import mobility

dotenv.load_dotenv()

mobility.set_params(
    package_data_folder_path=os.environ["MOBILITY_PACKAGE_DATA_FOLDER"],
    project_data_folder_path=os.environ["MOBILITY_PROJECT_DATA_FOLDER"]
)

# Using Foix (a small town) and a limited radius for quick results
transport_zones = mobility.TransportZones("fr-09122", radius = 10)

# Using EMP, the latest national mobility survey for France
emp = mobility.EMPMobilitySurvey()

# Creating a synthetic population of 1000 for the area
pop = mobility.Population(transport_zones, sample_size = 1000)

car_mode = mobility.CarMode(transport_zones)
walk_mode = mobility.WalkMode(transport_zones)
bicycle_mode = mobility.BicycleMode(transport_zones)
mode_registry = mobility.ModeRegistry(
    [car_mode, walk_mode, bicycle_mode],
    pt_access_mode_id="walk",
    pt_egress_mode_id="walk"
)
public_transport_mode = mobility.PublicTransportMode(
    transport_zones,
    mode_registry=mode_registry
)

# Simulating the trips for this population for three modes : car, walk, bicyle and public transport, and only home and work motives (OtherMotive is mandatory)
pop_trips = mobility.PopulationTrips(
    pop,
    [car_mode, walk_mode, bicycle_mode, public_transport_mode],
    [mobility.HomeMotive(), mobility.WorkMotive(), mobility.OtherMotive(population=pop)],
    [emp]
    )

# You can get the weekday trips to inspect them
weekday_flows = pop_trips.get()["weekday_flows"].collect()

# You can can also get global metrics that will be compared to the theoetical values for this population 
global_metrics = pop_trips.evaluate("global_metrics")

# You can also plot the flows, with labels for the cities that are bigger than their neighbours
labels = pop_trips.get_prominent_cities()
pop_trips.plot_od_flows(labels=labels)

# You can get a report of the parameters used in the model (for now not all of them, but eventually all of them)
report = pop_trips.parameters_dataframe()