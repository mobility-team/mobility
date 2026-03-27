import shutil
import uuid
from pathlib import Path

import polars as pl

from mobility.choice_models.population_trips_candidates import (
    get_active_destination_sequences,
    get_active_motive_chains,
    get_spatialized_chains,
)
from mobility.choice_models.population_trips_parameters import (
    BehaviorChangePhase,
    BehaviorChangeScope,
    PopulationTripsParameters,
)
from mobility.choice_models.state_updater import StateUpdater


def test_population_trips_parameters_returns_default_behavior_change_scope():
    parameters = PopulationTripsParameters()

    assert parameters.get_behavior_change_scope(1) == BehaviorChangeScope.FULL_REPLANNING


def test_population_trips_parameters_resolves_active_behavior_change_scope():
    parameters = PopulationTripsParameters(
        behavior_change_phases=[
            BehaviorChangePhase(start_iteration=3, scope=BehaviorChangeScope.MODE_REPLANNING),
            BehaviorChangePhase(start_iteration=5, scope=BehaviorChangeScope.DESTINATION_REPLANNING),
        ]
    )

    assert parameters.get_behavior_change_scope(1) == BehaviorChangeScope.FULL_REPLANNING
    assert parameters.get_behavior_change_scope(3) == BehaviorChangeScope.MODE_REPLANNING
    assert parameters.get_behavior_change_scope(6) == BehaviorChangeScope.DESTINATION_REPLANNING


def test_get_active_motive_chains_keeps_only_non_stay_home_sequences():
    chains_by_motive = pl.DataFrame(
        {
            "demand_group_id": [1, 1, 2],
            "motive_seq_id": [10, 20, 30],
            "seq_step_index": [0, 0, 0],
            "motive": ["work", "other", "work"],
        },
        schema={"demand_group_id": pl.UInt32, "motive_seq_id": pl.UInt32, "seq_step_index": pl.UInt32, "motive": pl.Utf8},
    )
    current_states = pl.DataFrame(
        {
            "demand_group_id": [1, 1, 2],
            "motive_seq_id": [10, 0, 0],
            "dest_seq_id": [100, 0, 0],
            "mode_seq_id": [1000, 0, 0],
        },
        schema={
            "demand_group_id": pl.UInt32,
            "motive_seq_id": pl.UInt32,
            "dest_seq_id": pl.UInt32,
            "mode_seq_id": pl.UInt32,
        },
    )

    result = get_active_motive_chains(chains_by_motive, current_states)

    assert result.select(["demand_group_id", "motive_seq_id"]).to_dicts() == [
        {"demand_group_id": 1, "motive_seq_id": 10}
    ]


def test_get_active_motive_chains_deduplicates_active_motive_keys():
    chains_by_motive = pl.DataFrame(
        {
            "demand_group_id": [1, 1],
            "motive_seq_id": [10, 10],
            "seq_step_index": [0, 1],
            "motive": ["work", "home"],
        },
        schema={"demand_group_id": pl.UInt32, "motive_seq_id": pl.UInt32, "seq_step_index": pl.UInt32, "motive": pl.Utf8},
    )
    current_states = pl.DataFrame(
        {
            "demand_group_id": [1, 1],
            "motive_seq_id": [10, 10],
            "dest_seq_id": [100, 101],
            "mode_seq_id": [1000, 1001],
        },
        schema={
            "demand_group_id": pl.UInt32,
            "motive_seq_id": pl.UInt32,
            "dest_seq_id": pl.UInt32,
            "mode_seq_id": pl.UInt32,
        },
    )

    result = get_active_motive_chains(chains_by_motive, current_states)

    assert result.height == 2
    assert result["seq_step_index"].to_list() == [0, 1]


def test_get_active_destination_sequences_uses_latest_matching_chain():
    spatialized_dir = Path("tests/back/unit/choice_models/.tmp") / str(uuid.uuid4()) / "spatialized-chains"
    spatialized_dir.mkdir(parents=True, exist_ok=True)

    try:
        pl.DataFrame(
            {
                "demand_group_id": [1],
                "motive_seq_id": [10],
                "dest_seq_id": [100],
                "seq_step_index": [0],
                "from": [11],
                "to": [12],
                "iteration": [1],
            },
            schema={
                "demand_group_id": pl.UInt32,
                "motive_seq_id": pl.UInt32,
                "dest_seq_id": pl.UInt32,
                "seq_step_index": pl.UInt32,
                "from": pl.Int32,
                "to": pl.Int32,
                "iteration": pl.UInt32,
            },
        ).write_parquet(spatialized_dir / "spatialized_chains_1.parquet")
        pl.DataFrame(
            {
                "demand_group_id": [1],
                "motive_seq_id": [10],
                "dest_seq_id": [100],
                "seq_step_index": [0],
                "from": [21],
                "to": [22],
                "iteration": [2],
            },
            schema={
                "demand_group_id": pl.UInt32,
                "motive_seq_id": pl.UInt32,
                "dest_seq_id": pl.UInt32,
                "seq_step_index": pl.UInt32,
                "from": pl.Int32,
                "to": pl.Int32,
                "iteration": pl.UInt32,
            },
        ).write_parquet(spatialized_dir / "spatialized_chains_2.parquet")

        current_states = pl.DataFrame(
            {
                "demand_group_id": [1],
                "motive_seq_id": [10],
                "dest_seq_id": [100],
                "mode_seq_id": [1000],
            },
            schema={
                "demand_group_id": pl.UInt32,
                "motive_seq_id": pl.UInt32,
                "dest_seq_id": pl.UInt32,
                "mode_seq_id": pl.UInt32,
            },
        )

        result = get_active_destination_sequences(
            current_states=current_states,
            iteration=4,
            tmp_folders={"spatialized-chains": spatialized_dir},
        )

        assert result.to_dicts() == [
            {
                "demand_group_id": 1,
                "motive_seq_id": 10,
                "dest_seq_id": 100,
                "seq_step_index": 0,
                "from": 21,
                "to": 22,
                "iteration": 4,
            }
        ]
    finally:
        shutil.rmtree(spatialized_dir.parent, ignore_errors=True)


def test_get_active_destination_sequences_deduplicates_active_destination_keys():
    spatialized_dir = Path("tests/back/unit/choice_models/.tmp") / str(uuid.uuid4()) / "spatialized-chains"
    spatialized_dir.mkdir(parents=True, exist_ok=True)

    try:
        pl.DataFrame(
            {
                "demand_group_id": [1, 1],
                "motive_seq_id": [10, 10],
                "dest_seq_id": [100, 100],
                "seq_step_index": [0, 1],
                "from": [21, 22],
                "to": [22, 23],
                "iteration": [2, 2],
            },
            schema={
                "demand_group_id": pl.UInt32,
                "motive_seq_id": pl.UInt32,
                "dest_seq_id": pl.UInt32,
                "seq_step_index": pl.UInt32,
                "from": pl.Int32,
                "to": pl.Int32,
                "iteration": pl.UInt32,
            },
        ).write_parquet(spatialized_dir / "spatialized_chains_2.parquet")

        current_states = pl.DataFrame(
            {
                "demand_group_id": [1, 1],
                "motive_seq_id": [10, 10],
                "dest_seq_id": [100, 100],
                "mode_seq_id": [1000, 1001],
            },
            schema={
                "demand_group_id": pl.UInt32,
                "motive_seq_id": pl.UInt32,
                "dest_seq_id": pl.UInt32,
                "mode_seq_id": pl.UInt32,
            },
        )

        result = get_active_destination_sequences(
            current_states=current_states,
            iteration=4,
            tmp_folders={"spatialized-chains": spatialized_dir},
        )

        assert result.height == 2
        assert result["seq_step_index"].sort().to_list() == [0, 1]
    finally:
        shutil.rmtree(spatialized_dir.parent, ignore_errors=True)


def test_get_spatialized_chains_limits_destination_resampling_to_active_motives():
    class DummySampler:
        def __init__(self):
            self.seen_chains = None

        def run(self, motives, transport_zones, remaining_sinks, iteration, chains, demand_groups, costs, tmp_folders, parameters, seed):
            self.seen_chains = chains
            return pl.DataFrame(
                {
                    "demand_group_id": [1],
                    "motive_seq_id": [10],
                    "dest_seq_id": [100],
                    "seq_step_index": [0],
                    "from": [1],
                    "to": [2],
                    "iteration": [iteration],
                },
                schema={
                    "demand_group_id": pl.UInt32,
                    "motive_seq_id": pl.UInt32,
                    "dest_seq_id": pl.UInt32,
                    "seq_step_index": pl.UInt32,
                    "from": pl.Int32,
                    "to": pl.Int32,
                    "iteration": pl.UInt32,
                },
            )

    destination_sequence_sampler = DummySampler()

    chains_by_motive = pl.DataFrame(
        {
            "demand_group_id": [1, 1],
            "motive_seq_id": [10, 20],
            "seq_step_index": [0, 0],
            "motive": ["work", "other"],
        },
        schema={"demand_group_id": pl.UInt32, "motive_seq_id": pl.UInt32, "seq_step_index": pl.UInt32, "motive": pl.Utf8},
    )
    current_states = pl.DataFrame(
        {
            "demand_group_id": [1],
            "motive_seq_id": [10],
            "dest_seq_id": [100],
            "mode_seq_id": [1000],
        },
        schema={
            "demand_group_id": pl.UInt32,
            "motive_seq_id": pl.UInt32,
            "dest_seq_id": pl.UInt32,
            "mode_seq_id": pl.UInt32,
        },
    )

    get_spatialized_chains(
        behavior_change_scope=BehaviorChangeScope.DESTINATION_REPLANNING,
        current_states=current_states,
        destination_sequence_sampler=destination_sequence_sampler,
        motives=[],
        transport_zones=None,
        remaining_sinks=pl.DataFrame(),
        iteration=3,
        chains_by_motive=chains_by_motive,
        demand_groups=pl.DataFrame(),
        costs=pl.DataFrame(),
        tmp_folders={"spatialized-chains": None},
        parameters=PopulationTripsParameters(),
        seed=123,
    )

    assert destination_sequence_sampler.seen_chains.select("motive_seq_id").to_series().to_list() == [10]


def test_filter_reachable_possible_states_steps_limits_mode_replanning_to_active_destinations():
    updater = StateUpdater()
    possible_states_steps = pl.DataFrame(
        {
            "demand_group_id": [1, 1, 1],
            "motive_seq_id": [10, 10, 11],
            "dest_seq_id": [100, 101, 100],
            "mode_seq_id": [1000, 1001, 1002],
            "seq_step_index": [0, 0, 0],
        },
        schema={
            "demand_group_id": pl.UInt32,
            "motive_seq_id": pl.UInt32,
            "dest_seq_id": pl.UInt32,
            "mode_seq_id": pl.UInt32,
            "seq_step_index": pl.UInt32,
        },
    ).lazy()
    current_states = pl.DataFrame(
        {
            "demand_group_id": [1],
            "motive_seq_id": [10],
            "dest_seq_id": [100],
            "mode_seq_id": [999],
        },
        schema={
            "demand_group_id": pl.UInt32,
            "motive_seq_id": pl.UInt32,
            "dest_seq_id": pl.UInt32,
            "mode_seq_id": pl.UInt32,
        },
    )

    result = updater.filter_reachable_possible_states_steps(
        possible_states_steps=possible_states_steps,
        current_states=current_states,
        behavior_change_scope=BehaviorChangeScope.MODE_REPLANNING,
    ).collect()

    assert result.select(["motive_seq_id", "dest_seq_id"]).unique().to_dicts() == [
        {"motive_seq_id": 10, "dest_seq_id": 100}
    ]


def test_filter_reachable_possible_states_steps_limits_destination_replanning_to_active_motives():
    updater = StateUpdater()
    possible_states_steps = pl.DataFrame(
        {
            "demand_group_id": [1, 1, 1],
            "motive_seq_id": [10, 10, 11],
            "dest_seq_id": [100, 101, 100],
            "mode_seq_id": [1000, 1001, 1002],
            "seq_step_index": [0, 0, 0],
        },
        schema={
            "demand_group_id": pl.UInt32,
            "motive_seq_id": pl.UInt32,
            "dest_seq_id": pl.UInt32,
            "mode_seq_id": pl.UInt32,
            "seq_step_index": pl.UInt32,
        },
    ).lazy()
    current_states = pl.DataFrame(
        {
            "demand_group_id": [1],
            "motive_seq_id": [10],
            "dest_seq_id": [100],
            "mode_seq_id": [999],
        },
        schema={
            "demand_group_id": pl.UInt32,
            "motive_seq_id": pl.UInt32,
            "dest_seq_id": pl.UInt32,
            "mode_seq_id": pl.UInt32,
        },
    )

    result = updater.filter_reachable_possible_states_steps(
        possible_states_steps=possible_states_steps,
        current_states=current_states,
        behavior_change_scope=BehaviorChangeScope.DESTINATION_REPLANNING,
    ).collect()

    assert result.select("motive_seq_id").unique().to_series().to_list() == [10]


def test_filter_reachable_possible_states_steps_keeps_only_stay_home_when_no_active_states():
    updater = StateUpdater()
    possible_states_steps = pl.DataFrame(
        {
            "demand_group_id": [1],
            "motive_seq_id": [10],
            "dest_seq_id": [100],
            "mode_seq_id": [1000],
            "seq_step_index": [0],
        },
        schema={
            "demand_group_id": pl.UInt32,
            "motive_seq_id": pl.UInt32,
            "dest_seq_id": pl.UInt32,
            "mode_seq_id": pl.UInt32,
            "seq_step_index": pl.UInt32,
        },
    ).lazy()
    current_states = pl.DataFrame(
        {
            "demand_group_id": [1],
            "motive_seq_id": [0],
            "dest_seq_id": [0],
            "mode_seq_id": [0],
        },
        schema={
            "demand_group_id": pl.UInt32,
            "motive_seq_id": pl.UInt32,
            "dest_seq_id": pl.UInt32,
            "mode_seq_id": pl.UInt32,
        },
    )

    result = updater.filter_reachable_possible_states_steps(
        possible_states_steps=possible_states_steps,
        current_states=current_states,
        behavior_change_scope=BehaviorChangeScope.MODE_REPLANNING,
    ).collect()

    assert result.height == 0


def test_get_transition_probabilities_blocks_stay_home_in_mode_replanning():
    updater = StateUpdater()
    current_states = pl.DataFrame(
        {
            "demand_group_id": [1, 1],
            "motive_seq_id": [0, 10],
            "dest_seq_id": [0, 100],
            "mode_seq_id": [0, 1000],
            "utility": [0.0, 1.0],
            "n_persons": [5.0, 5.0],
        },
        schema={
            "demand_group_id": pl.UInt32,
            "motive_seq_id": pl.UInt32,
            "dest_seq_id": pl.UInt32,
            "mode_seq_id": pl.UInt32,
            "utility": pl.Float64,
            "n_persons": pl.Float64,
        },
    )
    possible_states_utility = pl.DataFrame(
        {
            "demand_group_id": [1, 1, 1, 1],
            "motive_seq_id": [0, 10, 10, 11],
            "dest_seq_id": [0, 100, 101, 100],
            "mode_seq_id": [0, 1001, 1002, 1003],
            "utility": [10.0, 2.0, 3.0, 4.0],
        },
        schema={
            "demand_group_id": pl.UInt32,
            "motive_seq_id": pl.UInt32,
            "dest_seq_id": pl.UInt32,
            "mode_seq_id": pl.UInt32,
            "utility": pl.Float64,
        },
    ).lazy()

    result = updater.get_transition_probabilities(
        current_states=current_states,
        possible_states_utility=possible_states_utility,
        behavior_change_scope=BehaviorChangeScope.MODE_REPLANNING,
    )

    assert result.filter(pl.col("motive_seq_id") != 10).height == 0
    assert result.filter(pl.col("dest_seq_id") != 100).height == 0
    assert result.filter(pl.col("motive_seq_id_trans") != 10).height == 0
    assert result.filter(pl.col("dest_seq_id_trans") != 100).height == 0
    assert result.filter(pl.col("motive_seq_id_trans") == 0).height == 0


def test_get_transition_probabilities_limits_destination_replanning_to_same_motive():
    updater = StateUpdater()
    current_states = pl.DataFrame(
        {
            "demand_group_id": [1],
            "motive_seq_id": [10],
            "dest_seq_id": [100],
            "mode_seq_id": [1000],
            "utility": [1.0],
            "n_persons": [5.0],
        },
        schema={
            "demand_group_id": pl.UInt32,
            "motive_seq_id": pl.UInt32,
            "dest_seq_id": pl.UInt32,
            "mode_seq_id": pl.UInt32,
            "utility": pl.Float64,
            "n_persons": pl.Float64,
        },
    )
    possible_states_utility = pl.DataFrame(
        {
            "demand_group_id": [1, 1, 1],
            "motive_seq_id": [10, 10, 11],
            "dest_seq_id": [100, 101, 100],
            "mode_seq_id": [1000, 1001, 1002],
            "utility": [1.0, 2.0, 3.0],
        },
        schema={
            "demand_group_id": pl.UInt32,
            "motive_seq_id": pl.UInt32,
            "dest_seq_id": pl.UInt32,
            "mode_seq_id": pl.UInt32,
            "utility": pl.Float64,
        },
    ).lazy()

    result = updater.get_transition_probabilities(
        current_states=current_states,
        possible_states_utility=possible_states_utility,
        behavior_change_scope=BehaviorChangeScope.DESTINATION_REPLANNING,
    )

    assert result.select("motive_seq_id_trans").unique().to_series().to_list() == [10]
