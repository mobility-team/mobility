import os
import dotenv
import mobility
from mobility.trips.group_day_trips import Parameters

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
population_trips = mobility.GroupDayTrips(
    population=population,
    modes=[
        mobility.Car(transport_zones),
        mobility.Walk(transport_zones),
        mobility.Bicycle(transport_zones),
    ],
    activities=[
        mobility.Home(),
        mobility.Work(),
        mobility.Other(population=population),
    ],
    surveys=[survey],
    parameters=Parameters(
        n_iterations=1,
        mode_sequence_search_parallel=False,
    ),
)

# You can get weekday plan steps to inspect them
weekday_plan_steps = population_trips.get()["weekday_plan_steps"].collect()

# You can compute global metrics for weekday trips
global_metrics = population_trips.weekday_run.evaluate("global_metrics")

# You can plot weekday OD flows, with labels for prominent cities
weekday_results = population_trips.weekday_run.results()
labels = weekday_results.get_prominent_cities()
weekday_results.plot_od_flows(labels=labels)

# You can get a report of the parameters used in the model
report = population_trips.parameters_dataframe()
