import polars as pl
import pytest

import mobility
from mobility.activities import HomeActivity, OtherActivity, WorkActivity
from mobility.surveys.france import EMPMobilitySurvey
from mobility.trips.group_day_trips import (
    BehaviorChangePhase,
    BehaviorChangeScope,
    GroupDayTripsBehaviorChangeParameters,
    GroupDayTripsDestinationSequenceParameters,
    GroupDayTripsModeSequenceParameters,
    GroupDayTripsOutputParameters,
    GroupDayTripsParameters,
    GroupDayTripsRunParameters,
    PopulationGroupDayTrips,
)


@pytest.mark.dependency(
    depends=[
        "tests/back/integration/test_008_group_day_trips_can_be_computed.py::test_008_group_day_trips_can_be_computed"
    ],
    scope="session",
)
def test_011_population_trips_behavior_change_phases_can_be_computed(test_data):
    transport_zones = mobility.TransportZones(
        local_admin_unit_id=test_data["transport_zones_local_admin_unit_id"],
        radius=test_data["transport_zones_radius"],
    )
    emp = EMPMobilitySurvey()

    pop = mobility.Population(
        transport_zones,
        sample_size=test_data["population_sample_size"],
    )

    car_mode = mobility.CarMode(transport_zones)
    walk_mode = mobility.WalkMode(transport_zones)

    pop_trips = PopulationGroupDayTrips(
        population=pop,
        modes=[car_mode, walk_mode],
        activities=[HomeActivity(), WorkActivity(), OtherActivity(population=pop)],
        surveys=[emp],
        parameters=GroupDayTripsParameters(
            run=GroupDayTripsRunParameters(
                n_iterations=3,
                n_iter_per_cost_update=0,
                seed=0,
            ),
            outputs=GroupDayTripsOutputParameters(
                cache_iteration_events=True,
            ),
            behavior_change=GroupDayTripsBehaviorChangeParameters(
                phases=[
                    BehaviorChangePhase(start_iteration=1, scope=BehaviorChangeScope.FULL_REPLANNING),
                    BehaviorChangePhase(start_iteration=2, scope=BehaviorChangeScope.MODE_REPLANNING),
                    BehaviorChangePhase(start_iteration=3, scope=BehaviorChangeScope.DESTINATION_REPLANNING),
                ],
            ),
            destination_sequences=GroupDayTripsDestinationSequenceParameters(
                dest_prob_cutoff=0.9,
                cost_uncertainty_sd=1.0,
            ),
            mode_sequences=GroupDayTripsModeSequenceParameters(
                k_mode_sequences=3,
                mode_sequence_search_parallel=False,
            ),
        ),
    )

    weekday_run = pop_trips.run("weekday")
    result = weekday_run.get()
    weekday_plan_steps = result["plan_steps"].collect()
    weekday_transitions = result["transitions"].collect()
    destination_sequences_1 = weekday_run.iteration_state_assets[0].destination_sequences.get_cached_asset()
    destination_sequences_2 = weekday_run.iteration_state_assets[1].destination_sequences.get_cached_asset()
    destination_sequences_3 = weekday_run.iteration_state_assets[2].destination_sequences.get_cached_asset()
    destination_sequence_index_3 = weekday_run.iteration_state_assets[2].destination_sequences.get_index()
    mode_sequence_index_3 = weekday_run.iteration_state_assets[2].mode_sequences.get_index()
    plan_id_index_3 = weekday_run.final_iteration_state.get_cached_asset().plan_id_index

    assert weekday_plan_steps.height > 0
    assert weekday_transitions.height > 0
    assert weekday_transitions["iteration"].unique().sort().to_list() == [1, 2, 3]
    assert destination_sequences_1.height > 0
    assert destination_sequences_2.height > 0
    assert destination_sequences_3.height > 0
    assert destination_sequence_index_3.height > 0
    assert mode_sequence_index_3.height > 0
    assert plan_id_index_3.height > 0
    assert weekday_plan_steps.filter(pl.col("activity_seq_id") != 0)["mode_seq_id"].min() > 0

    bad_mode = weekday_transitions.filter(
        (pl.col("iteration") == 2)
        & (
            (pl.col("activity_seq_id") != pl.col("activity_seq_id_trans"))
            | (pl.col("dest_seq_id") != pl.col("dest_seq_id_trans"))
        )
    )
    bad_destination = weekday_transitions.filter(
        (pl.col("iteration") == 3)
        & (pl.col("activity_seq_id") != pl.col("activity_seq_id_trans"))
    )

    mode_replanning_dest_keys = (
        destination_sequences_2
        .select(["demand_group_id", "activity_seq_id", "dest_seq_id"])
        .unique()
    )
    initial_dest_keys = (
        destination_sequences_1
        .select(["demand_group_id", "activity_seq_id", "dest_seq_id"])
        .unique()
    )
    destination_replanning_activity_keys = (
        destination_sequences_3
        .select(["demand_group_id", "activity_seq_id"])
        .unique()
    )
    mode_replanning_activity_keys = (
        destination_sequences_2
        .select(["demand_group_id", "activity_seq_id"])
        .unique()
    )

    new_dest_keys_in_mode_replanning = mode_replanning_dest_keys.join(
        initial_dest_keys,
        on=["demand_group_id", "activity_seq_id", "dest_seq_id"],
        how="anti",
    )
    new_activity_keys_in_destination_replanning = destination_replanning_activity_keys.join(
        mode_replanning_activity_keys,
        on=["demand_group_id", "activity_seq_id"],
        how="anti",
    )

    assert new_dest_keys_in_mode_replanning.height == 0
    assert new_activity_keys_in_destination_replanning.height == 0
    assert bad_mode.height == 0
    assert bad_destination.height == 0
