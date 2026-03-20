import os
import dotenv
import mobility

dotenv.load_dotenv()

mobility.set_params(
    package_data_folder_path=os.environ["MOBILITY_PACKAGE_DATA_FOLDER"],
    project_data_folder_path=os.environ["MOBILITY_PROJECT_DATA_FOLDER"]
)

# Using Foix (a small town) and a limited radius for quick results
transport_zones = mobility.TransportZones("fr-09122", radius=10.0)

# Using EMP, the latest national mobility survey for France
survey = mobility.EMPMobilitySurvey()

# Creating a synthetic population of 1000 for the area
population = mobility.Population(transport_zones, sample_size=1000)

# Simulating trips for this population for car, walk, bicycle
population_trips = mobility.PopulationTrips(
    population,
    [
        mobility.CarMode(transport_zones),
        mobility.WalkMode(transport_zones),
        mobility.BicycleMode(transport_zones),
    ],
    [
        mobility.HomeMotive(),
        mobility.WorkMotive(),
        mobility.OtherMotive(population=population),
    ],
    [survey],
    n_iterations=1,
)

# You can get weekday trips to inspect them
weekday_flows = population_trips.get()["weekday_flows"].collect()

# You can compute global metrics for weekday trips
global_metrics = population_trips.weekday_run.evaluate("global_metrics")

# You can plot weekday OD flows, with labels for prominent cities
weekday_results = population_trips.weekday_run.results()
labels = weekday_results.get_prominent_cities()
weekday_results.plot_od_flows(labels=labels)

# You can get a report of the parameters used in the model
report = population_trips.parameters_dataframe()
