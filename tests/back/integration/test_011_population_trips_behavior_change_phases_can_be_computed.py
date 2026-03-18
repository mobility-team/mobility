import polars as pl
import pytest

import mobility
from mobility.choice_models.population_trips import PopulationTrips
from mobility.choice_models.population_trips_parameters import (
    BehaviorChangePhase,
    BehaviorChangeScope,
    PopulationTripsParameters,
)
from mobility.motives import HomeMotive, OtherMotive, WorkMotive
from mobility.parsers.mobility_survey.france import EMPMobilitySurvey

@pytest.mark.dependency(
    depends=[
        "tests/back/integration/test_008_population_trips_can_be_computed.py::test_008_population_trips_can_be_computed"
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

    pop_trips = PopulationTrips(
        population=pop,
        modes=[mobility.CarMode(transport_zones), mobility.WalkMode(transport_zones)],
        motives=[HomeMotive(), WorkMotive(), OtherMotive(population=pop)],
        surveys=[emp],
        parameters=PopulationTripsParameters(
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

    pop_trips.remove()
    result = pop_trips.get()
    weekday_flows = result["weekday_flows"].collect()
    weekday_transitions = result["weekday_transitions"].collect()
    cache_path = pop_trips.cache_path["weekday_flows"]
    inputs_hash = str(cache_path.stem).split("-")[0]
    spatialized_chains_dir = cache_path.parent / f"{inputs_hash}-spatialized-chains"
    spatialized_chains_1 = pl.read_parquet(spatialized_chains_dir / "spatialized_chains_1.parquet")
    spatialized_chains_2 = pl.read_parquet(spatialized_chains_dir / "spatialized_chains_2.parquet")
    spatialized_chains_3 = pl.read_parquet(spatialized_chains_dir / "spatialized_chains_3.parquet")

    assert weekday_flows.height > 0
    assert weekday_transitions.height > 0
    assert weekday_transitions["iteration"].unique().sort().to_list() == [1, 2, 3]
    assert spatialized_chains_1.height > 0
    assert spatialized_chains_2.height > 0
    assert spatialized_chains_3.height > 0

    bad_mode = weekday_transitions.filter(
        (pl.col("iteration") == 2)
        & (
            (pl.col("motive_seq_id") != pl.col("motive_seq_id_trans"))
            | (pl.col("dest_seq_id") != pl.col("dest_seq_id_trans"))
        )
    )
    bad_destination = weekday_transitions.filter(
        (pl.col("iteration") == 3)
        & (pl.col("motive_seq_id") != pl.col("motive_seq_id_trans"))
    )

    mode_replanning_dest_keys = (
        spatialized_chains_2
        .select(["demand_group_id", "motive_seq_id", "dest_seq_id"])
        .unique()
    )
    initial_dest_keys = (
        spatialized_chains_1
        .select(["demand_group_id", "motive_seq_id", "dest_seq_id"])
        .unique()
    )
    destination_replanning_motive_keys = (
        spatialized_chains_3
        .select(["demand_group_id", "motive_seq_id"])
        .unique()
    )
    mode_replanning_motive_keys = (
        spatialized_chains_2
        .select(["demand_group_id", "motive_seq_id"])
        .unique()
    )

    new_dest_keys_in_mode_replanning = (
        mode_replanning_dest_keys
        .join(
            initial_dest_keys,
            on=["demand_group_id", "motive_seq_id", "dest_seq_id"],
            how="anti",
        )
    )
    new_motive_keys_in_destination_replanning = (
        destination_replanning_motive_keys
        .join(
            mode_replanning_motive_keys,
            on=["demand_group_id", "motive_seq_id"],
            how="anti",
        )
    )

    assert new_dest_keys_in_mode_replanning.height == 0
    assert new_motive_keys_in_destination_replanning.height == 0
    assert bad_mode.height == 0
    assert bad_destination.height == 0
