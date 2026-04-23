import polars as pl

from mobility.trips.group_day_trips import BehaviorChangePhase, BehaviorChangeScope, Parameters
from mobility.trips.group_day_trips.plans.plan_updater import PlanUpdater
from mobility.trips.group_day_trips.population_trips_candidates import (
    empty_mode_sequences,
    empty_spatialized_chains,
    get_active_activity_chains,
    get_active_destination_sequences,
    get_mode_sequences,
    get_spatialized_chains,
)


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


def test_get_active_activity_chains_keeps_only_non_stay_home_sequences():
    chains_by_activity = pl.DataFrame(
        {
            "demand_group_id": [1, 1, 2],
            "activity_seq_id": [10, 20, 30],
            "seq_step_index": [0, 0, 0],
            "activity": ["work", "other", "work"],
        },
        schema={"demand_group_id": pl.UInt32, "activity_seq_id": pl.UInt32, "seq_step_index": pl.UInt32, "activity": pl.Utf8},
    )
    current_plans = pl.DataFrame(
        {
            "demand_group_id": [1, 1, 2],
            "activity_seq_id": [10, 0, 0],
            "dest_seq_id": [100, 0, 0],
            "mode_seq_id": [1000, 0, 0],
        },
        schema={
            "demand_group_id": pl.UInt32,
            "activity_seq_id": pl.UInt32,
            "dest_seq_id": pl.UInt32,
            "mode_seq_id": pl.UInt32,
        },
    )

    result = get_active_activity_chains(chains_by_activity, current_plans)

    assert result.select(["demand_group_id", "activity_seq_id"]).to_dicts() == [
        {"demand_group_id": 1, "activity_seq_id": 10}
    ]


def test_get_active_activity_chains_returns_empty_when_no_active_non_stay_home_sequences():
    chains_by_activity = pl.DataFrame(
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
    )
    current_plans = pl.DataFrame(
        {
            "demand_group_id": [1],
            "activity_seq_id": [0],
            "dest_seq_id": [0],
            "mode_seq_id": [0],
        },
        schema={
            "demand_group_id": pl.UInt32,
            "activity_seq_id": pl.UInt32,
            "dest_seq_id": pl.UInt32,
            "mode_seq_id": pl.UInt32,
        },
    )

    result = get_active_activity_chains(chains_by_activity, current_plans)

    assert result.height == 0
    assert result.schema == chains_by_activity.schema


def test_get_active_destination_sequences_reuses_current_plan_steps():
    current_plans = pl.DataFrame(
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
    )
    current_plan_steps = pl.DataFrame(
        {
            "demand_group_id": [1, 1],
            "activity_seq_id": [10, 10],
            "dest_seq_id": [100, 100],
            "mode_seq_id": [1000, 1000],
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
    )

    result = get_active_destination_sequences(
        current_plans=current_plans,
        current_plan_steps=current_plan_steps,
        iteration=4,
    )

    assert result["iteration"].unique().to_list() == [4]
    assert result["seq_step_index"].sort().to_list() == [0, 1]


def test_get_active_destination_sequences_returns_empty_when_no_active_non_stay_home_sequences():
    current_plans = pl.DataFrame(
        {
            "demand_group_id": [1],
            "activity_seq_id": [0],
            "dest_seq_id": [0],
            "mode_seq_id": [0],
        },
        schema={
            "demand_group_id": pl.UInt32,
            "activity_seq_id": pl.UInt32,
            "dest_seq_id": pl.UInt32,
            "mode_seq_id": pl.UInt32,
        },
    )

    result = get_active_destination_sequences(
        current_plans=current_plans,
        current_plan_steps=None,
        iteration=4,
    )

    assert result.height == 0
    assert result.schema == empty_spatialized_chains().schema


def test_get_active_destination_sequences_raises_when_current_plan_steps_missing():
    current_plans = pl.DataFrame(
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
    )

    try:
        get_active_destination_sequences(
            current_plans=current_plans,
            current_plan_steps=None,
            iteration=4,
        )
    except ValueError as exc:
        assert "No current plan steps available" in str(exc)
    else:
        raise AssertionError("Expected ValueError when current_plan_steps is missing")


def test_get_active_destination_sequences_raises_when_active_states_cannot_be_reused():
    current_plans = pl.DataFrame(
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
    )
    current_plan_steps = pl.DataFrame(
        {
            "demand_group_id": [1],
            "activity_seq_id": [10],
            "dest_seq_id": [999],
            "mode_seq_id": [1000],
            "seq_step_index": [0],
            "from": [21],
            "to": [22],
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
    )

    try:
        get_active_destination_sequences(
            current_plans=current_plans,
            current_plan_steps=current_plan_steps,
            iteration=4,
        )
    except ValueError as exc:
        assert "could not be matched to reusable destination chains" in str(exc)
    else:
        raise AssertionError("Expected ValueError when active destination sequences cannot be reused")


def test_get_spatialized_chains_limits_destination_resampling_to_active_activities():
    class DummySampler:
        def __init__(self):
            self.seen_chains = None

        def run(self, activities, transport_zones, remaining_opportunities, chains, demand_groups, costs, parameters, seed):
            self.seen_chains = chains
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

    destination_sequence_sampler = DummySampler()

    chains_by_activity = pl.DataFrame(
        {
            "demand_group_id": [1, 1],
            "activity_seq_id": [10, 20],
            "seq_step_index": [0, 0],
            "activity": ["work", "other"],
        },
        schema={"demand_group_id": pl.UInt32, "activity_seq_id": pl.UInt32, "seq_step_index": pl.UInt32, "activity": pl.Utf8},
    )
    current_plans = pl.DataFrame(
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
    )

    get_spatialized_chains(
        behavior_change_scope=BehaviorChangeScope.DESTINATION_REPLANNING,
        current_plans=current_plans,
        current_plan_steps=None,
        destination_sequence_sampler=destination_sequence_sampler,
        activities=[],
        transport_zones=None,
        remaining_opportunities=pl.DataFrame(),
        iteration=3,
        chains_by_activity=chains_by_activity,
        demand_groups=pl.DataFrame(),
        costs=pl.DataFrame(),
        parameters=Parameters(),
        seed=123,
    )

    assert destination_sequence_sampler.seen_chains.select("activity_seq_id").to_series().to_list() == [10]


def test_get_spatialized_chains_returns_empty_when_no_active_non_stay_home_plans():
    class DummySampler:
        def run(self, *args, **kwargs):
            raise AssertionError("Sampler should not run when there are no active non-stay-home plans")

    current_plans = pl.DataFrame(
        {
            "demand_group_id": [1],
            "activity_seq_id": [0],
            "dest_seq_id": [0],
            "mode_seq_id": [0],
        },
        schema={
            "demand_group_id": pl.UInt32,
            "activity_seq_id": pl.UInt32,
            "dest_seq_id": pl.UInt32,
            "mode_seq_id": pl.UInt32,
        },
    )
    chains_by_activity = pl.DataFrame(
        {
            "demand_group_id": [1],
            "activity_seq_id": [10],
            "seq_step_index": [0],
            "activity": ["work"],
        },
        schema={
            "demand_group_id": pl.UInt32,
            "activity_seq_id": pl.UInt32,
            "seq_step_index": pl.UInt32,
            "activity": pl.Utf8,
        },
    )

    result = get_spatialized_chains(
        behavior_change_scope=BehaviorChangeScope.DESTINATION_REPLANNING,
        current_plans=current_plans,
        current_plan_steps=None,
        destination_sequence_sampler=DummySampler(),
        activities=[],
        transport_zones=None,
        remaining_opportunities=pl.DataFrame(),
        iteration=3,
        chains_by_activity=chains_by_activity,
        demand_groups=pl.DataFrame(),
        costs=pl.DataFrame(),
        parameters=Parameters(),
        seed=123,
    )

    assert result.height == 0
    assert result.schema == empty_spatialized_chains().schema


def test_get_spatialized_chains_raises_when_active_non_stay_home_plans_have_no_available_chains():
    class DummySampler:
        def run(self, *args, **kwargs):
            raise AssertionError("Sampler should not run when no active chains are available")

    current_plans = pl.DataFrame(
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
    )
    chains_by_activity = pl.DataFrame(
        schema={
            "demand_group_id": pl.UInt32,
            "activity_seq_id": pl.UInt32,
            "seq_step_index": pl.UInt32,
            "activity": pl.Utf8,
        }
    )

    try:
        get_spatialized_chains(
            behavior_change_scope=BehaviorChangeScope.DESTINATION_REPLANNING,
            current_plans=current_plans,
            current_plan_steps=None,
            destination_sequence_sampler=DummySampler(),
            activities=[],
            transport_zones=None,
            remaining_opportunities=pl.DataFrame(),
            iteration=3,
            chains_by_activity=chains_by_activity,
            demand_groups=pl.DataFrame(),
            costs=pl.DataFrame(),
            parameters=Parameters(),
            seed=123,
        )
    except ValueError as exc:
        assert "No chains available for active non-stay-home states" in str(exc)
    else:
        raise AssertionError("Expected ValueError when active non-stay-home states have no chains")


def test_get_mode_sequences_returns_empty_when_no_spatialized_chains():
    class DummySearcher:
        def run(self, *args, **kwargs):
            raise AssertionError("Searcher should not run when there are no spatialized chains")

    result = get_mode_sequences(
        spatialized_chains=empty_spatialized_chains(),
        top_k_mode_sequence_search=DummySearcher(),
        iteration=2,
        costs_aggregator=None,
        tmp_folders={},
        parameters=Parameters(),
    )

    assert result.height == 0
    assert result.schema == empty_mode_sequences().schema


def test_get_mode_sequences_delegates_to_searcher():
    class DummySearcher:
        def __init__(self):
            self.calls = []

        def run(self, iteration, costs_aggregator, tmp_folders, parameters):
            self.calls.append((iteration, costs_aggregator, tmp_folders, parameters))
            return pl.DataFrame(
                {
                    "demand_group_id": [1],
                    "activity_seq_id": [10],
                    "dest_seq_id": [100],
                    "mode_seq_id": [1000],
                    "seq_step_index": [0],
                    "mode": ["car"],
                    "iteration": [2],
                },
                schema={
                    "demand_group_id": pl.UInt32,
                    "activity_seq_id": pl.UInt32,
                    "dest_seq_id": pl.UInt32,
                    "mode_seq_id": pl.UInt32,
                    "seq_step_index": pl.UInt32,
                    "mode": pl.Utf8,
                    "iteration": pl.UInt32,
                },
            )

    searcher = DummySearcher()
    spatialized_chains = pl.DataFrame(
        {
            "demand_group_id": [1],
            "activity_seq_id": [10],
            "dest_seq_id": [100],
            "seq_step_index": [0],
            "from": [1],
            "to": [2],
            "iteration": [2],
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

    parameters = Parameters()

    result = get_mode_sequences(
        spatialized_chains=spatialized_chains,
        top_k_mode_sequence_search=searcher,
        iteration=2,
        costs_aggregator="costs",
        tmp_folders={"tmp": "folder"},
        parameters=parameters,
    )

    assert searcher.calls == [(2, "costs", {"tmp": "folder"}, parameters)]
    assert result["mode"].to_list() == ["car"]


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
