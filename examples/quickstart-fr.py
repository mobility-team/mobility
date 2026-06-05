import os
import dotenv
import mobility

# Dear Mobility users, this quickstart should always work, please open an issue if this is not the case (and sorry for that)

# Note to Mobility developers : thank you for doing that! <3
# If you bring changes to this quickstart, please also update quickstart-fr-ci (in the same folder)
# If those changes are substantial, please consider updating test_901_quickstart_drift_guard.py in the unit folder test 

dotenv.load_dotenv()

mobility.set_params(
)

# Using Limoges and a limited radius to reuse the smaller Limousin OSM extract
transport_zones = mobility.TransportZones("fr-87085", radius=10.0)

# Using EMP, the latest national mobility survey for France
survey = mobility.EMPMobilitySurvey()

# Creating a synthetic population of 1000 for the area
population = mobility.Population(transport_zones, sample_size=1000)

# Simulating trips for this population for car, walk, bicycle
population_trips = mobility.PopulationGroupDayTrips(
    population=population,
    modes=[
        mobility.CarMode(transport_zones),
        mobility.WalkMode(transport_zones),
        mobility.BicycleMode(transport_zones),
    ],
    activities=[
        mobility.HomeActivity(),
        mobility.WorkActivity(),
        mobility.OtherActivity(population=population),
    ],
    surveys=[survey],
    parameters=mobility.GroupDayTripsParameters(
        run=mobility.GroupDayTripsRunParameters(n_iterations=1),
        mode_sequences=mobility.GroupDayTripsModeSequenceParameters(
            mode_sequence_search_parallel=False,
        ),
    ),
)

# You can get weekday plan steps to inspect them
weekday_run = population_trips.run("weekday")
weekday_plan_steps = weekday_run.get()["plan_steps"].collect()

# You can compute global metrics for weekday trips
global_metrics = weekday_run.results().metrics.aggregate()

# You can plot weekday OD flows, with labels for prominent cities
weekday_results = weekday_run.results()
labels = weekday_results.metrics.get_prominent_cities()
weekday_results.metrics.plot_od_flows(labels=labels)

# You can get a report of the parameters used in the model
parameters_report = weekday_run.parameters_dataframe()
