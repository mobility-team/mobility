import polars as pl

from mobility.trips.group_day_trips import BehaviorChangePhase, BehaviorChangeScope, Parameters
from mobility.trips.group_day_trips.plans.destination_sequences import DestinationSequences
from mobility.trips.group_day_trips.plans.plan_updater import PlanUpdater
from mobility.trips.group_day_trips.population_trips_candidates import get_spatialized_chains


def test_parameters_returns_default_behavior_change_scope():
    parameters = Parameters()

    assert parameters.get_behavior_change_scope(1) == BehaviorChangeScope.FULL_REPLANNING


def test_parameters_resolves_active_behavior_change_scope():
    parameters = Parameters(
        behavior_change_phases=[
            BehaviorChangePhase(start_iteration=3, scope=BehaviorChangeScope.MODE_REPLANNING),
            BehaviorChangePhase(start_iteration=5, scope=BehaviorChangeScope.DESTINATION_REPLANNING),
        ]
    )

    assert parameters.get_behavior_change_scope(1) == BehaviorChangeScope.FULL_REPLANNING
    assert parameters.get_behavior_change_scope(3) == BehaviorChangeScope.MODE_REPLANNING
    assert parameters.get_behavior_change_scope(6) == BehaviorChangeScope.DESTINATION_REPLANNING


def test_sample_active_destination_sequences_keeps_only_active_activity_sequences(tmp_path):
    destination_sequences = DestinationSequences(
        run_key="run",
        is_weekday=True,
        iteration=3,
        base_folder=tmp_path,
        current_plans=pl.DataFrame(
            {
                "demand_group_id": [1],
                "activity_seq_id": [10],
                "dest_seq_id": [100],
                "mode_seq_id": [1000],
            },
            schema={
                "demand_group_id": pl.UInt32,
                "activity_seq_id": pl.UInt32,
                "dest_seq_id": pl.UInt32,
                "mode_seq_id": pl.UInt32,
            },
        ),
        chains=pl.DataFrame(
            {
                "demand_group_id": [1, 1],
                "activity_seq_id": [10, 20],
                "seq_step_index": [0, 0],
                "activity": ["work", "other"],
            },
            schema={
                "demand_group_id": pl.UInt32,
                "activity_seq_id": pl.UInt32,
                "seq_step_index": pl.UInt32,
                "activity": pl.Utf8,
            },
        ),
        activities=[],
        transport_zones=None,
        remaining_opportunities=pl.DataFrame(),
        demand_groups=pl.DataFrame(),
        costs=pl.DataFrame(),
        parameters=Parameters(),
        seed=123,
    )

    seen = {}

    def fake_run(activities, transport_zones, remaining_opportunities, chains, demand_groups, costs, parameters, seed):
        seen["chains"] = chains
        return pl.DataFrame(
            {
                "demand_group_id": [1],
                "activity_seq_id": [10],
                "dest_seq_id": [100],
                "seq_step_index": [0],
                "from": [1],
                "to": [2],
                "iteration": [3],
            },
            schema={
                "demand_group_id": pl.UInt32,
                "activity_seq_id": pl.UInt32,
                "dest_seq_id": pl.UInt32,
                "seq_step_index": pl.UInt32,
                "from": pl.Int32,
                "to": pl.Int32,
                "iteration": pl.UInt32,
            },
        )

    destination_sequences.run = fake_run
    get_spatialized_chains(
        behavior_change_scope=BehaviorChangeScope.DESTINATION_REPLANNING,
        current_plans=destination_sequences.current_plans,
        current_plan_steps=destination_sequences.current_plan_steps,
        destination_sequence_sampler=destination_sequences,
        activities=destination_sequences.activities,
        transport_zones=destination_sequences.transport_zones,
        remaining_opportunities=destination_sequences.remaining_opportunities,
        iteration=destination_sequences.iteration,
        chains_by_activity=destination_sequences.chains,
        demand_groups=destination_sequences.demand_groups,
        costs=destination_sequences.costs,
        parameters=destination_sequences.parameters,
        seed=destination_sequences.seed,
    )

    assert seen["chains"].select("activity_seq_id").to_series().to_list() == [10]


def test_reuse_current_destination_sequences_reuses_current_plan_steps(tmp_path):
    destination_sequences = DestinationSequences(
        run_key="run",
        is_weekday=True,
        iteration=4,
        base_folder=tmp_path,
        current_plans=pl.DataFrame(
            {
                "demand_group_id": [1],
                "activity_seq_id": [10],
                "dest_seq_id": [100],
                "mode_seq_id": [1000],
            },
            schema={
                "demand_group_id": pl.UInt32,
                "activity_seq_id": pl.UInt32,
                "dest_seq_id": pl.UInt32,
                "mode_seq_id": pl.UInt32,
            },
        ),
        current_plan_steps=pl.DataFrame(
            {
                "demand_group_id": [1, 1],
                "activity_seq_id": [10, 10],
                "dest_seq_id": [100, 100],
                "mode_seq_id": [1000, 1001],
                "seq_step_index": [0, 1],
                "from": [21, 22],
                "to": [22, 23],
            },
            schema={
                "demand_group_id": pl.UInt32,
                "activity_seq_id": pl.UInt32,
                "dest_seq_id": pl.UInt32,
                "mode_seq_id": pl.UInt32,
                "seq_step_index": pl.UInt32,
                "from": pl.Int32,
                "to": pl.Int32,
            },
        ),
    )

    result = get_spatialized_chains(
        behavior_change_scope=BehaviorChangeScope.MODE_REPLANNING,
        current_plans=destination_sequences.current_plans,
        current_plan_steps=destination_sequences.current_plan_steps,
        destination_sequence_sampler=destination_sequences,
        activities=[],
        transport_zones=None,
        remaining_opportunities=pl.DataFrame(),
        iteration=destination_sequences.iteration,
        chains_by_activity=pl.DataFrame(),
        demand_groups=pl.DataFrame(),
        costs=pl.DataFrame(),
        parameters=Parameters(),
        seed=123,
    )

    assert result["iteration"].unique().to_list() == [4]
    assert result["seq_step_index"].sort().to_list() == [0, 1]


def test_get_transition_probabilities_blocks_stay_home_in_mode_replanning():
    updater = PlanUpdater()
    current_plans = pl.DataFrame(
        {
            "demand_group_id": [1, 1],
            "activity_seq_id": [0, 10],
            "dest_seq_id": [0, 100],
            "mode_seq_id": [0, 1000],
            "utility": [0.0, 1.0],
            "n_persons": [5.0, 5.0],
        },
        schema={
            "demand_group_id": pl.UInt32,
            "activity_seq_id": pl.UInt32,
            "dest_seq_id": pl.UInt32,
            "mode_seq_id": pl.UInt32,
            "utility": pl.Float64,
            "n_persons": pl.Float64,
        },
    )
    possible_plan_utility = pl.DataFrame(
        {
            "demand_group_id": [1, 1, 1, 1],
            "activity_seq_id": [0, 10, 10, 11],
            "dest_seq_id": [0, 100, 100, 101],
            "mode_seq_id": [0, 1001, 1002, 1003],
            "utility": [10.0, 2.0, 3.0, 4.0],
        },
        schema={
            "demand_group_id": pl.UInt32,
            "activity_seq_id": pl.UInt32,
            "dest_seq_id": pl.UInt32,
            "mode_seq_id": pl.UInt32,
            "utility": pl.Float64,
        },
    ).lazy()

    result = updater.get_transition_probabilities(
        current_plans=current_plans,
        possible_plan_utility=possible_plan_utility,
        behavior_change_scope=BehaviorChangeScope.MODE_REPLANNING,
    )

    assert result.filter(pl.col("activity_seq_id") != 10).height == 0
    assert result.filter(pl.col("dest_seq_id") != 100).height == 0
    assert result.filter(pl.col("activity_seq_id_trans") != 10).height == 0
    assert result.filter(pl.col("dest_seq_id_trans") != 100).height == 0
    assert result.filter(pl.col("activity_seq_id_trans") == 0).height == 0


def test_get_transition_probabilities_limits_destination_replanning_to_same_activity():
    updater = PlanUpdater()
    current_plans = pl.DataFrame(
        {
            "demand_group_id": [1],
            "activity_seq_id": [10],
            "dest_seq_id": [100],
            "mode_seq_id": [1000],
            "utility": [1.0],
            "n_persons": [5.0],
        },
        schema={
            "demand_group_id": pl.UInt32,
            "activity_seq_id": pl.UInt32,
            "dest_seq_id": pl.UInt32,
            "mode_seq_id": pl.UInt32,
            "utility": pl.Float64,
            "n_persons": pl.Float64,
        },
    )
    possible_plan_utility = pl.DataFrame(
        {
            "demand_group_id": [1, 1, 1],
            "activity_seq_id": [10, 10, 11],
            "dest_seq_id": [100, 101, 100],
            "mode_seq_id": [1000, 1001, 1002],
            "utility": [1.0, 2.0, 3.0],
        },
        schema={
            "demand_group_id": pl.UInt32,
            "activity_seq_id": pl.UInt32,
            "dest_seq_id": pl.UInt32,
            "mode_seq_id": pl.UInt32,
            "utility": pl.Float64,
        },
    ).lazy()

    result = updater.get_transition_probabilities(
        current_plans=current_plans,
        possible_plan_utility=possible_plan_utility,
        behavior_change_scope=BehaviorChangeScope.DESTINATION_REPLANNING,
    )

    assert result.select("activity_seq_id_trans").unique().to_series().to_list() == [10]
