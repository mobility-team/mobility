import dotenv
import mobility

dotenv.load_dotenv()

mobility.set_params()

# Use Limoges and a limited radius to reuse the smaller Limousin OSM extract.
transport_zones = mobility.TransportZones("fr-87085", radius=10.0)

# Use EMP 2018-2019, the current Mobility survey source for French examples.
survey = mobility.EMPMobilitySurvey()

# Create a synthetic population of 1000 people for the area.
population = mobility.Population(transport_zones, sample_size=1000)

# Simulate trips for this population with car, walk, and bicycle.
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

# Run the weekday model.
weekday_run = population_trips.run("weekday")
weekday_plan_steps = weekday_run.get()["plan_steps"].collect()

# Use population_trips.results(...) as the main entry point for indicators.
weekday_results = population_trips.results("weekday")
trip_count_by_mode = weekday_results.metrics.trip_count(
    by_variable="mode",
    iterations="last",
    output="table",
)

# Plot origin-destination flows between transport zones.
od_flow_plot = weekday_results.metrics.trip_count(
    by_zone=["origin_zone", "destination_zone"],
    iterations="last",
    output="plot",
)

# Get a report of the parameters used by the model.
parameters_report = weekday_run.parameters_dataframe()
