import dotenv
import mobility
from mobility.trips.group_day_trips.core.parameters import GroupDayTripsActivitySequenceParameters, GroupDayTripsDestinationSequenceParameters, GroupDayTripsModeSequenceParameters, GroupDayTripsPlanUpdateParameters

dotenv.load_dotenv()

mobility.set_params(package_data_folder_path="./data", project_data_folder_path="./data", )

# de-083155012074 = Müllheim im Markgräflerland
transport_zones = mobility.TransportZones("de-083155012074", radius=100.0, backend="python")
transport_zones.get().to_file("./data/test.geojson")

# TODO
survey = mobility.EMPMobilitySurvey()

# TODO
# # Create a synthetic population of 1000 people for the area.
population = mobility.Population(transport_zones, sample_size=1000)
population.create_and_get_asset()

# # TODO
population_trips = mobility.PopulationGroupDayTrips(
    population=population,
    modes=[
        mobility.CarMode(transport_zones),
        mobility.WalkMode(transport_zones),
        mobility.BicycleMode(transport_zones),
    ],
    activities=[
        mobility.OtherActivity(population=population),
        mobility.HomeActivity(),
        mobility.WorkActivity(),
    ],
    surveys=[survey],
    parameters=mobility.GroupDayTripsParameters(
        run=mobility.GroupDayTripsRunParameters(n_iterations=5),
    ),
    # activity_sequences=GroupDayTripsActivitySequenceParameters(
    #     k_activity_sequences=3,
    # ),
    destination_sequences=GroupDayTripsDestinationSequenceParameters(
        k_destination_sequences=3,
        alpha=0.25,
        dest_prob_cutoff=0.9,
        cost_uncertainty_sd=1.0,
        refresh_active_mode_alternatives=True,  
    ),
    mode_sequences=GroupDayTripsModeSequenceParameters(
        k_mode_sequences=3,
        mode_sequence_search_parallel=True,
        use_rust_mode_sequence_search=True,
    ),
    plan_update=GroupDayTripsPlanUpdateParameters(
        transition_logit_scale=0.25,
        update_plan_timings_from_modeled_travel_times=True,
        transition_revision_probability=0.4,
        n_warmup_iterations=0,
        max_inactive_age=1,
        transition_utility_pruning_delta=1.0,
        plan_probability_pruning_retained_share=0.95,
        plan_probability_pruning_min_iteration=2,
        min_transition_utility_gain=0.1,
        use_destination_shadow_prices=True,
        min_activity_time_constant=2.0,
    ),
)

# # TODO
# # Run the weekday model.
# weekday_run = population_trips.run("weekday")
# weekday_plan_steps = weekday_run.get()["plan_steps"].collect()

# # TODO
# # Use population_trips.results(...) as the main entry point for indicators.
# weekday_results = population_trips.results("weekday")
# trip_count_by_mode = weekday_results.metrics.trip_count(
#     by_variable="mode",
#     iterations="last",
#     output="table",
# )

# # TODO
# # Plot origin-destination flows between transport zones.
# od_flow_plot = weekday_results.metrics.trip_count(
#     by_zone=["origin_zone", "destination_zone"],
#     iterations="last",
#     output="plot",
# )

# # TODO
# # Get a report of the parameters used by the model.
# parameters_report = weekday_run.parameters_dataframe()
