import polars as pl
import pytest

import mobility
from mobility.activities import Home, Other, Work
from mobility.surveys.france import EMPMobilitySurvey
from mobility.trips.group_day_trips import BehaviorChangePhase, BehaviorChangeScope, GroupDayTrips, Parameters


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

    car_mode = mobility.Car(transport_zones)
    walk_mode = mobility.Walk(transport_zones)

    pop_trips = GroupDayTrips(
        population=pop,
        modes=[car_mode, walk_mode],
        activities=[Home(), Work(), Other(population=pop)],
        surveys=[emp],
        parameters=Parameters(
            n_iterations=3,
            n_iter_per_cost_update=0,
            alpha=0.01,
            dest_prob_cutoff=0.9,
            k_mode_sequences=3,
            cost_uncertainty_sd=1.0,
            mode_sequence_search_parallel=False,
            seed=0,
            behavior_change_phases=[
                BehaviorChangePhase(start_iteration=1, scope=BehaviorChangeScope.FULL_REPLANNING),
                BehaviorChangePhase(start_iteration=2, scope=BehaviorChangeScope.MODE_REPLANNING),
                BehaviorChangePhase(start_iteration=3, scope=BehaviorChangeScope.DESTINATION_REPLANNING),
            ],
        ),
    )

    result = pop_trips.get()
    weekday_plan_steps = result["weekday_plan_steps"].collect()
    weekday_transitions = result["weekday_transitions"].collect()
    cache_parent = pop_trips.weekday_run.cache_path["plan_steps"].parent
    inputs_hash = pop_trips.weekday_run.inputs_hash
    destination_sequences_dir = cache_parent / f"{inputs_hash}-destination-sequences"
    destination_sequences_1 = pl.read_parquet(next(destination_sequences_dir.glob("*destination_sequences_1.parquet")))
    destination_sequences_2 = pl.read_parquet(next(destination_sequences_dir.glob("*destination_sequences_2.parquet")))
    destination_sequences_3 = pl.read_parquet(next(destination_sequences_dir.glob("*destination_sequences_3.parquet")))

    assert weekday_plan_steps.height > 0
    assert weekday_transitions.height > 0
    assert weekday_transitions["iteration"].unique().sort().to_list() == [1, 2, 3]
    assert destination_sequences_1.height > 0
    assert destination_sequences_2.height > 0
    assert destination_sequences_3.height > 0

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
