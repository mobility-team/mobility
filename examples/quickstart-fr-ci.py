import mobility
from mobility.trips.group_day_trips import Parameters

# This file is not the actual quickstart. It is used by our CI system to ensure that the quickstart always work!

def run_quickstart_ci():
    # In CI/integration tests, test setup configures Mobility paths in conftest.py.

    # Using Foix (a small town) and a limited radius for quick results
    transport_zones = mobility.TransportZones("fr-09122", radius=10.0)

    # Using EMP, the latest national mobility survey for France
    survey = mobility.EMPMobilitySurvey()

    # Creating a smaller synthetic population for faster CI runs
    population = mobility.Population(transport_zones, sample_size=100)

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
        parameters=Parameters(
            n_iterations=1,
            mode_sequence_search_parallel=False,
        ),
    )

    # You can get weekday plan steps to inspect them
    weekday_plan_steps = population_trips.get()["weekday_plan_steps"].collect()

    # You can compute global metrics for this population
    global_metrics = population_trips.weekday_run.evaluate("global_metrics")
    
    # You can plot weekday OD flows, with labels for prominent cities
    weekday_results = population_trips.weekday_run.results()
    labels = weekday_results.get_prominent_cities()
    #weekday_results.plot_od_flows(labels=labels) #Not plotting on CI to avoid crashes

    # You can get a report of the parameters used in the model
    parameters_report = population_trips.weekday_run.parameters_dataframe()

    return {
        "weekday_plan_steps": weekday_plan_steps,
        "global_metrics": global_metrics,
    }


if __name__ == "__main__":
    run_quickstart_ci()
