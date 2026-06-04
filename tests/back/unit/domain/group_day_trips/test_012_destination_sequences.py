from pathlib import Path
from types import SimpleNamespace

import polars as pl

from mobility.trips.group_day_trips import (
    GroupDayTripsDestinationSequenceParameters,
    GroupDayTripsParameters,
    GroupDayTripsPlanUpdateParameters,
)
from mobility.trips.group_day_trips.plans.destination_sequences import DestinationSequences


def _make_local_tmp_path(tmp_path: Path, name: str) -> Path:
    path = tmp_path / "group_day_trips" / name
    path.mkdir(parents=True, exist_ok=True)
    return path


def test_sample_active_destination_sequences_keeps_only_active_activity_sequences(tmp_path):
    class _StubAsset:
        def __init__(self, df):
            self._df = df

        def get_cached_asset(self):
            return self._df

    destination_sequences = DestinationSequences(
        run_key="run",
        is_weekday=True,
        iteration=3,
        base_folder=_make_local_tmp_path(tmp_path, "active_destination_sequences"),
        current_plans=pl.DataFrame(
            {
                "demand_group_id": [1],
                "activity_seq_id": [10],
                "time_seq_id": [1],
                "dest_seq_id": [100],
                "mode_seq_id": [1000],
            },
            schema={
                "demand_group_id": pl.UInt32,
                "activity_seq_id": pl.UInt32,
                "time_seq_id": pl.UInt32,
                "dest_seq_id": pl.UInt32,
                "mode_seq_id": pl.UInt32,
            },
        ),
        activity_sequences=_StubAsset(
            pl.DataFrame(
                {
                    "demand_group_id": [1],
                    "activity_seq_id": [10],
                    "time_seq_id": [0],
                    "seq_step_index": [0],
                    "activity": ["work"],
                },
                schema={
                    "demand_group_id": pl.UInt32,
                    "activity_seq_id": pl.UInt32,
                    "time_seq_id": pl.UInt32,
                    "seq_step_index": pl.UInt32,
                    "activity": pl.Utf8,
                },
            )
        ),
        activities=[],
        transport_zones=None,
        destination_saturation=pl.DataFrame(),
        demand_groups=pl.DataFrame(),
        costs=pl.DataFrame(),
        parameters=GroupDayTripsParameters(),
        seed=123,
    )

    seen = {}

    def fake_run(
        activities,
        transport_zones,
        destination_saturation,
        chains,
        demand_groups,
        costs,
        parameters,
        seed,
    ):
        seen["chains"] = chains
        return pl.DataFrame(
            {
                "demand_group_id": [1],
                "activity_seq_id": [10],
                "time_seq_id": [0],
                "dest_seq_id": [100],
                "seq_step_index": [0],
                "from": [1],
                "to": [2],
                "departure_time": [8.0],
                "arrival_time": [9.0],
                "next_departure_time": [17.0],
                "iteration": [3],
            },
            schema={
                "demand_group_id": pl.UInt32,
                "activity_seq_id": pl.UInt32,
                "time_seq_id": pl.UInt32,
                "dest_seq_id": pl.UInt32,
                "seq_step_index": pl.UInt32,
                "from": pl.Int32,
                "to": pl.Int32,
                "departure_time": pl.Float64,
                "arrival_time": pl.Float64,
                "next_departure_time": pl.Float64,
                "iteration": pl.UInt32,
            },
        )

    destination_sequences.run = fake_run
    destination_sequences._sample_active_destination_sequences()

    assert seen["chains"].select("activity_seq_id").to_series().to_list() == [10]


def test_refresh_active_mode_alternatives_appends_active_destination_chains(tmp_path):
    destination_sequences = DestinationSequences(
        run_key="run",
        is_weekday=True,
        iteration=5,
        base_folder=_make_local_tmp_path(tmp_path, "refresh_active_destinations"),
        current_plans=pl.DataFrame(
            {
                "demand_group_id": [1],
                "activity_seq_id": [10],
                "time_seq_id": [20],
                "dest_seq_id": [30],
                "mode_seq_id": [40],
            },
            schema={
                "demand_group_id": pl.UInt32,
                "activity_seq_id": pl.UInt32,
                "time_seq_id": pl.UInt32,
                "dest_seq_id": pl.UInt32,
                "mode_seq_id": pl.UInt32,
            },
        ),
        current_plan_steps=pl.DataFrame(
            {
                "demand_group_id": [1],
                "activity_seq_id": [10],
                "time_seq_id": [20],
                "dest_seq_id": [30],
                "seq_step_index": [1],
                "from": [100],
                "to": [200],
                "departure_time": [8.0],
                "arrival_time": [9.0],
                "next_departure_time": [17.0],
            },
            schema={
                "demand_group_id": pl.UInt32,
                "activity_seq_id": pl.UInt32,
                "time_seq_id": pl.UInt32,
                "dest_seq_id": pl.UInt32,
                "seq_step_index": pl.UInt8,
                "from": pl.UInt16,
                "to": pl.UInt16,
                "departure_time": pl.Float32,
                "arrival_time": pl.Float32,
                "next_departure_time": pl.Float32,
            },
        ),
        activities=[],
        transport_zones=None,
        destination_saturation=pl.DataFrame(),
        demand_groups=pl.DataFrame(),
        costs=pl.DataFrame(),
        parameters=GroupDayTripsParameters(
            destination_sequences=GroupDayTripsDestinationSequenceParameters(
                refresh_active_mode_alternatives=True,
            ),
        ),
        seed=123,
        resolved_activity_parameters={},
    )
    sampled = pl.DataFrame(
        {
            "demand_group_id": [1],
            "activity_seq_id": [11],
            "time_seq_id": [21],
            "dest_seq_id": [31],
            "seq_step_index": [1],
            "from": [101],
            "to": [201],
            "departure_time": [8.0],
            "arrival_time": [9.0],
            "next_departure_time": [17.0],
            "anchor_to": [201],
            "iteration": [5],
        },
        schema={
            "demand_group_id": pl.UInt32,
            "activity_seq_id": pl.UInt32,
            "time_seq_id": pl.UInt32,
            "dest_seq_id": pl.UInt32,
            "seq_step_index": pl.UInt8,
            "from": pl.UInt16,
            "to": pl.UInt16,
            "departure_time": pl.Float32,
            "arrival_time": pl.Float32,
            "next_departure_time": pl.Float32,
            "anchor_to": pl.UInt16,
            "iteration": pl.UInt16,
        },
    )

    result = destination_sequences._with_refreshed_active_destination_sequences(sampled)

    assert result.select("dest_seq_id").sort("dest_seq_id").to_series().to_list() == [30, 31]
    assert result.filter(pl.col("dest_seq_id") == 30).select("iteration").item() == 5
    assert result.columns == DestinationSequences.OUTPUT_COLUMNS


def test_refresh_active_mode_alternatives_default_keeps_sampled_destinations_only(tmp_path):
    destination_sequences = DestinationSequences(
        run_key="run",
        is_weekday=True,
        iteration=5,
        base_folder=_make_local_tmp_path(tmp_path, "refresh_active_destinations_default"),
        current_plans=pl.DataFrame(),
        current_plan_steps=pl.DataFrame(),
        activities=[],
        transport_zones=None,
        destination_saturation=pl.DataFrame(),
        demand_groups=pl.DataFrame(),
        costs=pl.DataFrame(),
        parameters=GroupDayTripsParameters(),
        seed=123,
        resolved_activity_parameters={},
    )
    sampled = pl.DataFrame(
        {
            "demand_group_id": [1],
            "activity_seq_id": [11],
            "time_seq_id": [21],
            "dest_seq_id": [31],
            "seq_step_index": [1],
            "from": [101],
            "to": [201],
            "departure_time": [8.0],
            "arrival_time": [9.0],
            "next_departure_time": [17.0],
            "iteration": [5],
        }
    )

    result = destination_sequences._with_refreshed_active_destination_sequences(sampled)

    assert result is sampled


def test_destination_probability_inputs_use_cost_and_sink_even_without_destination_utilities(tmp_path):
    destination_sequences = DestinationSequences(
        run_key="run",
        is_weekday=True,
        iteration=1,
        base_folder=_make_local_tmp_path(tmp_path, "destination_utilities_default_zero"),
        activities=[],
        transport_zones=None,
        destination_saturation=pl.DataFrame(),
        demand_groups=pl.DataFrame(),
        costs=pl.DataFrame(),
        parameters=GroupDayTripsParameters(),
        seed=123,
        resolved_activity_parameters={},
        current_plans=pl.DataFrame(),
    )

    class _TransportCosts:
        def __init__(self):
            self.modes = [SimpleNamespace(inputs={"parameters": SimpleNamespace(name="car")})]

        def get_costs_by_od_and_mode(self, columns, detail_distances=False):
            return pl.DataFrame(
                {
                    "from": [1],
                    "to": [10],
                    "mode": ["car"],
                    "cost": [3.0],
                    "distance": [10.0],
                    "time": [1.0],
                }
            )

    opportunities = pl.DataFrame(
        {
            "to": [10],
            "activity": ["work"],
            "opportunity_capacity": [100.0],
            "k_saturation_utility": [1.0],
        }
    ).with_columns(activity=pl.col("activity").cast(pl.Enum(["work"])))

    costs_by_bin, cost_bin_to_destination = destination_sequences._get_destination_probability_inputs(
        opportunities=opportunities,
        costs=_TransportCosts().get_costs_by_od_and_mode(["cost", "distance", "time"]),
        cost_uncertainty_sd=1.0,
    )

    assert costs_by_bin.collect().height > 0
    assert cost_bin_to_destination.collect().height > 0


def test_destination_probability_inputs_use_shadow_attraction_when_enabled(tmp_path):
    destination_sequences = DestinationSequences(
        run_key="run",
        is_weekday=True,
        iteration=1,
        base_folder=_make_local_tmp_path(tmp_path, "destination_shadow_inputs"),
        activities=[],
        transport_zones=None,
        destination_saturation=pl.DataFrame(),
        demand_groups=pl.DataFrame(),
        costs=pl.DataFrame(),
        parameters=GroupDayTripsParameters(
            plan_update=GroupDayTripsPlanUpdateParameters(use_destination_shadow_prices=True),
        ),
        seed=123,
        resolved_activity_parameters={},
        current_plans=pl.DataFrame(),
    )
    opportunities = pl.DataFrame(
        {
            "to": [10, 20],
            "activity": ["work", "work"],
            "opportunity_capacity": [100.0, 100.0],
            "k_saturation_utility": [1.0, 1.0],
            "destination_sampling_attraction_factor": [1.0, 0.5],
        }
    ).with_columns(activity=pl.col("activity").cast(pl.Enum(["work"])))
    costs = pl.DataFrame(
        {
            "from": [1, 1],
            "to": [10, 20],
            "cost": [3.0, 3.0],
        }
    )

    _, cost_bin_to_destination = destination_sequences._get_destination_probability_inputs(
        opportunities=opportunities,
        costs=costs,
        cost_uncertainty_sd=1.0,
    )

    probabilities = (
        cost_bin_to_destination
        .group_by("to")
        .agg(p_to=pl.col("p_to").mean())
        .sort("to")
    ).collect()

    assert probabilities["p_to"].to_list() == [2.0 / 3.0, 1.0 / 3.0]


def test_spatialize_trip_chain_step_uses_chain_cost_to_reweight_non_anchor_candidates(tmp_path):
    destination_sequences = DestinationSequences(
        run_key="run",
        is_weekday=True,
        iteration=1,
        base_folder=_make_local_tmp_path(tmp_path, "non_anchor_chain_cost_weighting"),
        activities=[],
        transport_zones=None,
        destination_saturation=pl.DataFrame(),
        demand_groups=pl.DataFrame(),
        costs=pl.DataFrame(),
        parameters=GroupDayTripsParameters(),
        seed=123,
        resolved_activity_parameters={},
        current_plans=pl.DataFrame(),
    )

    seed = 123
    candidate_destinations = [20, 30, 40, 50]
    anchor_destination = 99
    base_probability = 1.0 / len(candidate_destinations)

    candidate_noise = (
        pl.DataFrame(
            {
                "demand_group_id": [1] * len(candidate_destinations),
                "activity_seq_id": [10] * len(candidate_destinations),
                "time_seq_id": [1] * len(candidate_destinations),
                "dest_draw_id": [1] * len(candidate_destinations),
                "to": candidate_destinations,
            },
            schema={
                "demand_group_id": pl.UInt32,
                "activity_seq_id": pl.UInt32,
                "time_seq_id": pl.UInt32,
                "dest_draw_id": pl.UInt32,
                "to": pl.UInt16,
            },
        )
        .with_columns(
            noise=(
                pl.struct(["demand_group_id", "activity_seq_id", "time_seq_id", "dest_draw_id", "to"])
                .hash(seed=seed)
                .cast(pl.Float64)
                .truediv(pl.lit(18446744073709551616.0))
                .log()
                .neg()
            )
        )
        .sort("noise")
    )
    lowest_noise_candidate = int(candidate_noise["to"][0])
    highest_noise_candidate = int(candidate_noise["to"][-1])

    chains_step = pl.DataFrame(
        {
            "demand_group_id": [1],
            "home_zone_id": [1],
            "activity_seq_id": [10],
            "time_seq_id": [1],
            "dest_draw_id": [1],
            "activity": ["shop"],
            "is_anchor": [False],
            "seq_step_index": [1],
            "step_count": [1],
            "anchor_to": [anchor_destination],
            "from": [1],
            "departure_time": [8.0],
            "arrival_time": [8.5],
            "next_departure_time": [9.0],
        },
        schema={
            "demand_group_id": pl.UInt32,
            "home_zone_id": pl.UInt16,
            "activity_seq_id": pl.UInt32,
            "time_seq_id": pl.UInt32,
            "dest_draw_id": pl.UInt32,
                "activity": pl.Utf8,
                "is_anchor": pl.Boolean,
                "seq_step_index": pl.UInt8,
                "step_count": pl.UInt8,
                "anchor_to": pl.UInt16,
                "from": pl.UInt16,
                "departure_time": pl.Float64,
            "arrival_time": pl.Float64,
            "next_departure_time": pl.Float64,
        },
    )
    destination_probability = pl.DataFrame(
        {
            "activity": ["shop"] * len(candidate_destinations),
            "from": [1] * len(candidate_destinations),
            "to": candidate_destinations,
            "p_ij": [base_probability] * len(candidate_destinations),
        },
        schema={
            "activity": pl.Utf8,
            "from": pl.UInt16,
            "to": pl.UInt16,
            "p_ij": pl.Float64,
        },
    )
    costs = pl.DataFrame(
        {
            "from": [1] * len(candidate_destinations) + candidate_destinations,
            "to": candidate_destinations + [anchor_destination] * len(candidate_destinations),
            "cost": [1.0] * len(candidate_destinations) + [
                0.0 if destination == highest_noise_candidate else 50.0
                for destination in candidate_destinations
            ],
        },
        schema={
            "from": pl.UInt16,
            "to": pl.UInt16,
            "cost": pl.Float64,
        },
    )

    result_without_chain_penalty = destination_sequences._spatialize_trip_chain_step(
        seq_step_index=1,
        chains_step=chains_step,
        destination_probability=destination_probability,
        costs=costs,
        alpha=0.0,
        seed=seed,
    )
    result_with_chain_penalty = destination_sequences._spatialize_trip_chain_step(
        seq_step_index=1,
        chains_step=chains_step,
        destination_probability=destination_probability,
        costs=costs,
        alpha=1.0,
        seed=seed,
    )

    assert result_without_chain_penalty["to"].to_list() == [lowest_noise_candidate]
    assert result_with_chain_penalty["to"].to_list() == [highest_noise_candidate]
    assert result_with_chain_penalty["to"].to_list() != result_without_chain_penalty["to"].to_list()


def test_spatialize_anchor_activities_samples_from_current_anchor_location(tmp_path):
    destination_sequences = DestinationSequences(
        run_key="run",
        is_weekday=True,
        iteration=1,
        base_folder=_make_local_tmp_path(tmp_path, "anchor_current_location"),
        activities=[],
        transport_zones=None,
        destination_saturation=pl.DataFrame(),
        demand_groups=pl.DataFrame(),
        costs=pl.DataFrame(),
        parameters=GroupDayTripsParameters(
            destination_sequences=GroupDayTripsDestinationSequenceParameters(k_destination_sequences=1),
        ),
        seed=123,
        resolved_activity_parameters={},
        current_plans=pl.DataFrame(),
    )

    chains = pl.DataFrame(
        {
            "demand_group_id": [1, 1, 1],
            "home_zone_id": [10, 10, 10],
            "activity_seq_id": [20, 20, 20],
            "time_seq_id": [1, 1, 1],
            "activity": ["studies", "work", "home"],
            "is_anchor": [True, True, True],
            "seq_step_index": [1, 2, 3],
            "step_count": [3, 3, 3],
            "departure_time": [8.0, 12.0, 18.0],
            "arrival_time": [8.5, 12.5, 18.5],
            "next_departure_time": [12.0, 18.0, 18.5],
        },
        schema={
            "demand_group_id": pl.UInt32,
            "home_zone_id": pl.UInt16,
            "activity_seq_id": pl.UInt32,
            "time_seq_id": pl.UInt32,
            "activity": pl.Utf8,
            "is_anchor": pl.Boolean,
            "seq_step_index": pl.UInt8,
            "step_count": pl.UInt8,
            "departure_time": pl.Float64,
            "arrival_time": pl.Float64,
            "next_departure_time": pl.Float64,
        },
    )
    destination_probability = pl.DataFrame(
        {
            "activity": ["studies", "work", "work"],
            "from": [10, 94, 94],
            "to": [94, 331, 332],
            "p_ij": [1.0, 0.9, 0.1],
        },
        schema={
            "activity": pl.Utf8,
            "from": pl.UInt16,
            "to": pl.UInt16,
            "p_ij": pl.Float64,
        },
    )
    costs = pl.DataFrame(
        {
            "from": [10, 94, 94, 332],
            "to": [94, 10, 332, 10],
            "cost": [1.0, 1.0, 1.0, 1.0],
        },
        schema={
            "from": pl.UInt16,
            "to": pl.UInt16,
            "cost": pl.Float64,
        },
    )

    result = destination_sequences._spatialize_anchor_activities(
        chains,
        destination_probability,
        costs,
        alpha=0.0,
        seed=123,
    )

    sampled_anchors = (
        result
        .filter(pl.col("is_anchor"))
        .sort("seq_step_index")
        .select(["activity", "anchor_to"])
        .to_dicts()
    )
    assert sampled_anchors == [
        {"activity": "studies", "anchor_to": 94},
        {"activity": "work", "anchor_to": 332},
        {"activity": "home", "anchor_to": 10},
    ]


def test_spatialize_anchor_activities_uses_chain_cost_to_reweight_candidates(tmp_path):
    destination_sequences = DestinationSequences(
        run_key="run",
        is_weekday=True,
        iteration=1,
        base_folder=_make_local_tmp_path(tmp_path, "anchor_chain_cost_weighting"),
        activities=[],
        transport_zones=None,
        destination_saturation=pl.DataFrame(),
        demand_groups=pl.DataFrame(),
        costs=pl.DataFrame(),
        parameters=GroupDayTripsParameters(
            destination_sequences=GroupDayTripsDestinationSequenceParameters(k_destination_sequences=1),
        ),
        seed=123,
        resolved_activity_parameters={},
        current_plans=pl.DataFrame(),
    )

    seed = 123
    candidate_destinations = [20, 30, 40, 50]
    home_zone = 1
    base_probability = 1.0 / len(candidate_destinations)
    candidate_noise = (
        pl.DataFrame(
            {
                "demand_group_id": [1] * len(candidate_destinations),
                "activity_seq_id": [10] * len(candidate_destinations),
                "time_seq_id": [1] * len(candidate_destinations),
                "dest_draw_id": [1] * len(candidate_destinations),
                "to": candidate_destinations,
            },
            schema={
                "demand_group_id": pl.UInt32,
                "activity_seq_id": pl.UInt32,
                "time_seq_id": pl.UInt32,
                "dest_draw_id": pl.UInt32,
                "to": pl.UInt16,
            },
        )
        .with_columns(
            noise=(
                pl.struct(["demand_group_id", "activity_seq_id", "time_seq_id", "dest_draw_id", "to"])
                .hash(seed=seed)
                .cast(pl.Float64)
                .truediv(pl.lit(18446744073709551616.0))
                .log()
                .neg()
            )
        )
        .sort("noise")
    )
    lowest_noise_candidate = int(candidate_noise["to"][0])
    highest_noise_candidate = int(candidate_noise["to"][-1])

    chains = pl.DataFrame(
        {
            "demand_group_id": [1, 1],
            "home_zone_id": [home_zone, home_zone],
            "activity_seq_id": [10, 10],
            "time_seq_id": [1, 1],
            "activity": ["work", "home"],
            "is_anchor": [True, True],
            "seq_step_index": [1, 2],
            "step_count": [2, 2],
            "departure_time": [8.0, 17.0],
            "arrival_time": [8.5, 17.5],
            "next_departure_time": [17.0, 17.5],
        },
        schema={
            "demand_group_id": pl.UInt32,
            "home_zone_id": pl.UInt16,
            "activity_seq_id": pl.UInt32,
            "time_seq_id": pl.UInt32,
            "activity": pl.Utf8,
            "is_anchor": pl.Boolean,
            "seq_step_index": pl.UInt8,
            "step_count": pl.UInt8,
            "departure_time": pl.Float64,
            "arrival_time": pl.Float64,
            "next_departure_time": pl.Float64,
        },
    )
    destination_probability = pl.DataFrame(
        {
            "activity": ["work"] * len(candidate_destinations),
            "from": [home_zone] * len(candidate_destinations),
            "to": candidate_destinations,
            "p_ij": [base_probability] * len(candidate_destinations),
        },
        schema={
            "activity": pl.Utf8,
            "from": pl.UInt16,
            "to": pl.UInt16,
            "p_ij": pl.Float64,
        },
    )
    costs = pl.DataFrame(
        {
            "from": [home_zone] * len(candidate_destinations) + candidate_destinations,
            "to": candidate_destinations + [home_zone] * len(candidate_destinations),
            "cost": [1.0] * len(candidate_destinations) + [
                0.0 if destination == highest_noise_candidate else 50.0
                for destination in candidate_destinations
            ],
        },
        schema={
            "from": pl.UInt16,
            "to": pl.UInt16,
            "cost": pl.Float64,
        },
    )

    result_without_chain_penalty = destination_sequences._spatialize_anchor_activities(
        chains,
        destination_probability,
        costs,
        alpha=0.0,
        seed=seed,
    )
    result_with_chain_penalty = destination_sequences._spatialize_anchor_activities(
        chains,
        destination_probability,
        costs,
        alpha=1.0,
        seed=seed,
    )

    assert result_without_chain_penalty.filter(pl.col("activity") == "work")["anchor_to"].to_list() == [
        lowest_noise_candidate
    ]
    assert result_with_chain_penalty.filter(pl.col("activity") == "work")["anchor_to"].to_list() == [
        highest_noise_candidate
    ]


def test_spatialize_trip_chain_step_drops_anchor_without_leg_cost(tmp_path):
    destination_sequences = DestinationSequences(
        run_key="run",
        is_weekday=True,
        iteration=1,
        base_folder=_make_local_tmp_path(tmp_path, "missing_anchor_leg_cost"),
        activities=[],
        transport_zones=None,
        destination_saturation=pl.DataFrame(),
        demand_groups=pl.DataFrame(),
        costs=pl.DataFrame(),
        parameters=GroupDayTripsParameters(),
        seed=123,
        resolved_activity_parameters={},
        current_plans=pl.DataFrame(),
    )

    chains_step = pl.DataFrame(
        {
            "demand_group_id": [1],
            "home_zone_id": [1],
            "activity_seq_id": [10],
            "time_seq_id": [1],
            "dest_draw_id": [1],
            "activity": ["work"],
            "is_anchor": [True],
            "seq_step_index": [1],
            "step_count": [1],
            "anchor_to": [99],
            "from": [1],
            "departure_time": [8.0],
            "arrival_time": [8.5],
            "next_departure_time": [17.0],
        },
        schema={
            "demand_group_id": pl.UInt32,
            "home_zone_id": pl.UInt16,
            "activity_seq_id": pl.UInt32,
            "time_seq_id": pl.UInt32,
            "dest_draw_id": pl.UInt32,
            "activity": pl.Utf8,
            "is_anchor": pl.Boolean,
            "seq_step_index": pl.UInt8,
            "step_count": pl.UInt8,
            "anchor_to": pl.UInt16,
            "from": pl.UInt16,
            "departure_time": pl.Float64,
            "arrival_time": pl.Float64,
            "next_departure_time": pl.Float64,
        },
    )
    destination_probability = pl.DataFrame(
        schema={
            "activity": pl.Utf8,
            "from": pl.UInt16,
            "to": pl.UInt16,
            "p_ij": pl.Float64,
        },
    )
    costs = pl.DataFrame(
        {
            "from": [1],
            "to": [88],
            "cost": [1.0],
        },
        schema={
            "from": pl.UInt16,
            "to": pl.UInt16,
            "cost": pl.Float64,
        },
    )

    result = destination_sequences._spatialize_trip_chain_step(
        seq_step_index=1,
        chains_step=chains_step,
        destination_probability=destination_probability,
        costs=costs,
        alpha=0.0,
        seed=123,
    )

    assert result.is_empty()


def test_drop_incomplete_destination_draws_removes_partial_draws():
    activity_sequences = pl.DataFrame(
        {
            "demand_group_id": [1, 1, 2, 2],
            "home_zone_id": [100, 100, 200, 200],
            "activity_seq_id": [10, 10, 20, 20],
            "time_seq_id": [1, 1, 2, 2],
            "dest_draw_id": [1, 1, 1, 1],
            "activity": ["shop", "study", "work", "home"],
            "anchor_to": [100, 100, 200, 200],
            "from": [100, 110, 200, 210],
            "to": [110, 120, 210, 200],
            "departure_time": [8.0, 9.0, 8.5, 17.0],
            "arrival_time": [8.5, 9.5, 9.0, 17.5],
            "next_departure_time": [9.0, 10.0, 17.0, 17.5],
            "seq_step_index": [1, 2, 1, 2],
            "step_count": [3, 3, 2, 2],
        },
        schema={
            "demand_group_id": pl.UInt32,
            "home_zone_id": pl.UInt16,
            "activity_seq_id": pl.UInt32,
            "time_seq_id": pl.UInt32,
            "dest_draw_id": pl.UInt32,
            "activity": pl.Utf8,
            "anchor_to": pl.UInt16,
            "from": pl.UInt16,
            "to": pl.UInt16,
            "departure_time": pl.Float64,
            "arrival_time": pl.Float64,
            "next_departure_time": pl.Float64,
            "seq_step_index": pl.UInt8,
            "step_count": pl.UInt8,
        },
    )

    result = DestinationSequences._drop_incomplete_destination_draws(
        activity_sequences=activity_sequences,
        iteration=4,
    )

    assert result.select(["demand_group_id", "activity_seq_id", "time_seq_id", "dest_draw_id"]).unique().to_dicts() == [
        {
            "demand_group_id": 2,
            "activity_seq_id": 20,
            "time_seq_id": 2,
            "dest_draw_id": 1,
        }
    ]
    assert result["seq_step_index"].to_list() == [1, 2]


def test_reuse_current_destination_sequences_reuses_current_plan_steps(tmp_path):
    destination_sequences = DestinationSequences(
        run_key="run",
        is_weekday=True,
        iteration=4,
        base_folder=_make_local_tmp_path(tmp_path, "reuse_current_destination_sequences"),
        current_plans=pl.DataFrame(
            {
                "demand_group_id": [1],
                "activity_seq_id": [10],
                "time_seq_id": [1],
                "dest_seq_id": [100],
                "mode_seq_id": [1000],
            },
            schema={
                "demand_group_id": pl.UInt32,
                "activity_seq_id": pl.UInt32,
                "time_seq_id": pl.UInt32,
                "dest_seq_id": pl.UInt32,
                "mode_seq_id": pl.UInt32,
            },
        ),
        current_plan_steps=pl.DataFrame(
            {
                "demand_group_id": [1, 1],
                "activity_seq_id": [10, 10],
                "time_seq_id": [1, 1],
                "dest_seq_id": [100, 100],
                "mode_seq_id": [1000, 1000],
                "seq_step_index": [0, 1],
                "from": [21, 22],
                "to": [22, 23],
                "departure_time": [8.0, 9.0],
                "arrival_time": [8.5, 9.5],
                "next_departure_time": [9.0, 17.0],
            },
            schema={
                "demand_group_id": pl.UInt32,
                "activity_seq_id": pl.UInt32,
                "time_seq_id": pl.UInt32,
                "dest_seq_id": pl.UInt32,
                "mode_seq_id": pl.UInt32,
                "seq_step_index": pl.UInt32,
                "from": pl.Int32,
                "to": pl.Int32,
                "departure_time": pl.Float64,
                "arrival_time": pl.Float64,
                "next_departure_time": pl.Float64,
            },
        ),
    )

    result = destination_sequences._reuse_current_destination_sequences()

    assert result["iteration"].unique().to_list() == [4]
    assert result["seq_step_index"].sort().to_list() == [0, 1]
