from types import SimpleNamespace
import math

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from mobility.trips.group_day_trips import (
    GroupDayTripsDestinationSequenceParameters,
    GroupDayTripsParameters,
    GroupDayTripsPlanUpdateParameters,
)
from mobility.runtime.assets.in_memory_asset import InMemoryAsset
from mobility.trips.group_day_trips.plans.destination_sequences import DestinationSequences
from mobility.trips.group_day_trips.plans.plan_schedule_updater import PlanScheduleUpdater


class _StubAsset(InMemoryAsset):
    def __init__(self, value: pl.DataFrame):
        super().__init__({"name": "destination-sequence-test-asset"})
        self.value = value

    def create_and_get_asset(self) -> pl.DataFrame:
        return self.value


def test_destination_probabilities_are_reused_only_within_the_same_profile(monkeypatch):
    """A new profile or outer-step table must recompute its probabilities."""
    parameters = GroupDayTripsParameters()
    sequences = pl.DataFrame({
        "home_zone_id": [1], "activity_seq_id": [1], "time_seq_id": [1],
        "activity": ["work"], "is_anchor": [True], "seq_step_index": [1],
        "step_count": [1], "departure_time": [8.0], "arrival_time": [8.5],
        "next_departure_time": [17.0],
    })
    calls = []
    probabilities = pl.DataFrame({"p_ij": [1.0]})

    class StopAfterProbability(Exception):
        pass

    def build_probability(*args):
        calls.append(True)
        return probabilities

    def stop_after_probability(*args):
        raise StopAfterProbability

    def sample(asset, profile):
        pytest.raises(
            StopAfterProbability, asset._run_for_utility_profile,
            [], None, pl.DataFrame(), sequences, pl.DataFrame(), parameters, 0,
            profile_id=profile,
        )

    asset = DestinationSequences(
        is_weekday=True, iteration=1, activities=[], resolved_activity_parameters={},
        parameters=parameters, sampling_probabilities={},
    )
    monkeypatch.setattr(asset, "_get_destination_probability_inputs", lambda *args: None)
    monkeypatch.setattr(asset, "_get_destination_probability", build_probability)
    monkeypatch.setattr(asset, "_spatialization_cost_views", stop_after_probability)
    sample(asset, 0)
    sample(asset, 0)
    sample(asset, 1)
    assert len(calls) == 2
    asset.sampling_probabilities = {}  # A new outer step has its own tables.
    sample(asset, 0)
    assert len(calls) == 3


@pytest.mark.parametrize("seed", [0, 1, 2])
def test_grouped_destination_choices_match_per_request_races(seed):
    # Several schedule draws share a distribution, while another has a
    # different onward anchor. Every draw must keep its own seeded race.
    steps = pl.DataFrame({
        "home_zone_id": [1, 1, 1, 1], "from": [1, 1, 1, 1],
        "activity": ["shopping"] * 4, "anchor_to": [4, 4, 4, 5],
        "activity_draw_id": [0, 1, 2, 3], "activity_seq_id": [1, 1, 2, 2],
        "time_seq_id": [1, 1, 2, 2], "dest_draw_id": [0, 0, 1, 0],
    })
    # More than eight draws per shared context exercises multiple column batches.
    steps = pl.concat([
        steps.with_columns(activity_draw_id=pl.col("activity_draw_id") + 4 * index)
        for index in range(4)
    ])
    # The same draw key can appear in more than one choice context. Its race
    # must still choose the minimum across all of those contexts.
    steps = pl.concat([steps, steps.head(1).with_columns(anchor_to=pl.lit(5, dtype=pl.Int64))])
    probabilities = pl.DataFrame({
        "from": [1, 1, 1], "activity": ["shopping"] * 3,
        "to": [2, 3, 6], "p_ij": [0.5, 0.3, 0.2],
    })
    origin_costs = pl.DataFrame({
        "candidate_from": [1, 1, 1], "candidate_to": [2, 3, 6],
        "cost_to_candidate": [1.0, 2.0, 3.0],
    })
    onward_costs = pl.DataFrame({
        "anchor_from": [2, 3, 6, 2, 3], "anchor_to_cost": [4, 4, 4, 5, 5],
        "cost_to_anchor": [2.0, 1.0, 3.0, 4.0, 1.0],
    })
    draw_keys = ["home_zone_id", "activity_draw_id", "activity_seq_id", "time_seq_id", "dest_draw_id"]
    noise_hash = pl.struct(draw_keys + ["to"]).hash(seed=seed)
    _, _, joined, sampled = DestinationSequences._sample_sequence_aware_destinations(
        steps_lf=steps.lazy(), destination_probability_lf=probabilities.lazy(),
        origin_cost_lf=origin_costs.lazy(), onward_cost_lf=onward_costs.lazy(),
        onward_left_on=["to", "anchor_to"], onward_right_on=["anchor_from", "anchor_to_cost"],
        onward_cost_col="cost_to_anchor", alpha=0.25,
        sequence_context_columns=["home_zone_id"], seed=seed,
        output_cols=draw_keys + ["to"],
    )
    reference = (
        joined.with_columns(
            weight=(pl.col("p_ij").clip(1e-18).log() - 0.25 * (
                pl.col("cost_to_candidate") + pl.col("cost_to_anchor")
            )).exp(),
            noise=noise_hash.cast(pl.Float64).truediv(18446744073709551616.0).log().neg(),
        )
        .with_columns(score=pl.col("noise") / pl.col("weight").clip(1e-18) + pl.col("to") * 1e-18)
        .filter(pl.col("score") == pl.col("score").min().over(draw_keys))
        .select(draw_keys + ["to"])
        .collect().sort(draw_keys)
    )
    assert_frame_equal(sampled.collect().sort(draw_keys), reference)


def test_batched_destination_race_handles_no_reachable_candidates():
    steps = pl.DataFrame({
        "home_zone_id": [1], "from": [1], "activity": ["shopping"], "anchor_to": [4],
        "activity_draw_id": [0], "activity_seq_id": [1], "time_seq_id": [1], "dest_draw_id": [0],
    })
    probabilities = pl.DataFrame({"from": [1], "activity": ["shopping"], "to": [2], "p_ij": [1.0]})
    origin = pl.DataFrame({"candidate_from": [1], "candidate_to": [2], "cost_to_candidate": [1.0]})
    onward = pl.DataFrame({"anchor_from": [3], "anchor_to_cost": [4], "cost_to_anchor": [1.0]})
    _, _, _, sampled = DestinationSequences._sample_sequence_aware_destinations(
        steps_lf=steps.lazy(), destination_probability_lf=probabilities.lazy(),
        origin_cost_lf=origin.lazy(), onward_cost_lf=onward.lazy(),
        onward_left_on=["to", "anchor_to"], onward_right_on=["anchor_from", "anchor_to_cost"],
        onward_cost_col="cost_to_anchor", alpha=0.25,
        sequence_context_columns=["home_zone_id"], seed=0,
        output_cols=["home_zone_id", "to"],
    )
    assert sampled.collect().is_empty()


def test_utility_sampling_trades_travel_cost_against_activity_time():
    """A short activity can favor a fast trip even at a larger destination."""
    steps = pl.DataFrame({
        "home_zone_id": [1] * 100, "from": [1] * 100, "activity": ["shopping"] * 100,
        "anchor_to": [4] * 100, "activity_draw_id": list(range(100)),
        "activity_seq_id": [1] * 100, "time_seq_id": [1] * 100, "dest_draw_id": [0] * 100,
        "departure_time": [8.0] * 100, "arrival_time": [8.5] * 100,
        "next_departure_time": [9.0] * 100,
    })
    probabilities = pl.DataFrame({
        "from": [1, 1], "activity": ["shopping", "shopping"], "to": [2, 3],
        "p_ij": [1.0, 1000000.0], "activity_saturation": [1.0, 1.0], "activity_shadow_price": [0.0, 0.0],
        "country_value_coefficient": [1.0, 1.0],
    })
    origin = pl.DataFrame({
        "candidate_from": [1, 1], "candidate_to": [2, 3], "cost_to_candidate": [1.0, 1.0],
        "candidate_fastest_cost": [2.0, 10.0], "candidate_fastest_time": [0.1, 0.9],
        "candidate_cheapest_cost": [1.0, 1.0], "candidate_cheapest_time": [2.0, 2.0],
    })
    onward = pl.DataFrame({
        "anchor_from": [2, 3], "anchor_to_cost": [4, 4],
        "cost_to_anchor": [0.0, 0.0], "onward_cheapest_cost": [0.0, 0.0],
    })
    args = dict(
        steps_lf=steps.lazy(), destination_probability_lf=probabilities.lazy(),
        origin_cost_lf=origin.lazy(), onward_cost_lf=onward.lazy(),
        onward_left_on=["to", "anchor_to"], onward_right_on=["anchor_from", "anchor_to_cost"],
        onward_cost_col="cost_to_anchor", alpha=0.5, sequence_context_columns=["home_zone_id"],
        seed=3, output_cols=["activity_draw_id", "to"], utility_values={"shopping": 20.0},
        min_activity_time_constant=2.0, logit_scale=1.0,
    )
    sampled = DestinationSequences._sample_sequence_aware_destinations(**args)[3].collect()
    assert sampled.height == 100
    assert sampled.filter(pl.col("to") == 2).height > 95
    repeated = DestinationSequences._sample_sequence_aware_destinations(**args)[3].collect()
    assert_frame_equal(sampled.sort("activity_draw_id"), repeated.sort("activity_draw_id"))


def test_utility_sampling_keeps_each_modes_cost_and_time_together():
    modes = pl.DataFrame({
        "from": [1, 1, 1, 1], "to": [2, 2, 2, 2], "mode": ["fast", "cheap", "invalid", "unknown"],
        "time": [0.5, 2.0, 0.01, None], "cost": [10.0, 1.0, float("inf"), 2.0],
    })
    options = DestinationSequences._travel_options(modes).row(0, named=True)
    assert (options["fastest_cost"], options["fastest_time"]) == (10.0, 0.5)
    assert (options["cheapest_cost"], options["cheapest_time"]) == (1.0, 2.0)
    assert options["has_unknown_time"] is True
    assert_frame_equal(DestinationSequences._travel_options(modes), DestinationSequences._travel_options(modes.reverse()))


def test_utility_sampling_values_the_next_anchor_and_its_timing():
    steps = pl.DataFrame({
        "home_zone_id": [1] * 50, "from": [1] * 50, "activity": ["shopping"] * 50,
        "anchor_to": [4] * 50, "activity_draw_id": list(range(50)), "activity_seq_id": [1] * 50,
        "time_seq_id": [1] * 50, "dest_draw_id": [0] * 50,
        "departure_time": [8.0] * 50, "arrival_time": [8.5] * 50, "next_departure_time": [9.0] * 50,
        "_next_activity": ["work"] * 50, "_next_departure": [9.0] * 50, "_next_arrival": [9.5] * 50,
        "_next_next_departure": [17.0] * 50, "_next_is_direct": [True] * 50,
    })
    probabilities = pl.DataFrame({
        "from": [1, 1, 2], "activity": ["shopping", "shopping", "work"], "to": [2, 3, 4],
        "p_ij": [1.0, 1.0, 1.0], "activity_saturation": [1.0, 1.0, 1.0],
        "activity_shadow_price": [0.0, 0.0, 0.0], "country_value_coefficient": [1.0, 1.0, 1.0],
    })
    origin = pl.DataFrame({
        "candidate_from": [1, 1], "candidate_to": [2, 3], "cost_to_candidate": [1.0, 1.0],
        "candidate_fastest_cost": [1.0, 1.0], "candidate_fastest_time": [0.5, 0.5],
        "candidate_cheapest_cost": [1.0, 1.0], "candidate_cheapest_time": [0.5, 0.5],
    })
    onward = pl.DataFrame({
        "anchor_from": [2, 3], "anchor_to_cost": [4, 4], "cost_to_anchor": [1.0, 1.0],
        "onward_cheapest_cost": [1.0, 1.0], "onward_cheapest_time": [0.25, 2.0],
        "onward_fastest_cost": [1.0, 1.0], "onward_fastest_time": [0.25, 2.0],
    })
    args = dict(
        steps_lf=steps.lazy(), destination_probability_lf=probabilities.lazy(),
        origin_cost_lf=origin.lazy(), onward_cost_lf=onward.lazy(),
        onward_left_on=["to", "anchor_to"], onward_right_on=["anchor_from", "anchor_to_cost"],
        onward_cost_col="cost_to_anchor", alpha=0.5, sequence_context_columns=["home_zone_id"],
        seed=3, output_cols=["activity_draw_id", "to"], utility_values={"shopping": 1.0, "work": 100.0},
        mean_durations={"shopping": 1.0, "work": 8.0}, activity_rigidity={"shopping": 0.0, "work": 0.0},
        min_activity_time_constant=2.0, logit_scale=1.0, use_next_timing=True,
    )
    sampled = DestinationSequences._sample_sequence_aware_destinations(**args)[3].collect()
    assert sampled.height == 50
    assert sampled["to"].to_list() == [2] * 50
    # If the following activity is not direct, its timing is irrelevant and
    # evaluating only the cheapest onward trip must keep the seeded choices.
    args["steps_lf"] = steps.with_columns(pl.lit(False).alias("_next_is_direct")).lazy()
    args["onward_cost_lf"] = onward.with_columns(pl.lit(2.0).alias("onward_fastest_cost")).lazy()
    reference = DestinationSequences._sample_sequence_aware_destinations(**args)[3].collect()
    args["use_next_timing"] = False
    simplified = DestinationSequences._sample_sequence_aware_destinations(**args)[3].collect()
    assert_frame_equal(reference.sort("activity_draw_id"), simplified.sort("activity_draw_id"))


def test_utility_sampling_filters_only_proven_timing_conflicts():
    steps = pl.DataFrame({
        "home_zone_id": [1], "from": [1], "activity": ["shopping"], "anchor_to": [4],
        "activity_draw_id": [0], "activity_seq_id": [1], "time_seq_id": [1], "dest_draw_id": [0],
        "departure_time": [8.0], "arrival_time": [8.5], "next_departure_time": [8.75],
        "_next_activity": ["work"], "_next_departure": [8.75], "_next_arrival": [9.25],
        "_next_next_departure": [17.0], "_next_is_direct": [True],
    })
    probabilities = pl.DataFrame({
        "from": [1, 1], "activity": ["shopping", "shopping"], "to": [2, 3],
        "p_ij": [1.0, 1.0], "activity_saturation": [1.0, 1.0],
        "activity_shadow_price": [0.0, 0.0], "country_value_coefficient": [1.0, 1.0],
    })
    origin = pl.DataFrame({
        "candidate_from": [1, 1], "candidate_to": [2, 3], "cost_to_candidate": [1.0, 1.0],
        "candidate_fastest_cost": [1.0, 1.0], "candidate_fastest_time": [0.5, 1.25],
        "candidate_cheapest_cost": [1.0, 1.0], "candidate_cheapest_time": [0.5, 1.25],
        "candidate_has_unknown_time": [False, False],
    })
    onward = pl.DataFrame({
        "anchor_from": [2, 3], "anchor_to_cost": [4, 4], "cost_to_anchor": [1.0, 1.0],
        "onward_cheapest_cost": [1.0, 1.0], "onward_cheapest_time": [0.5, 0.5],
        "onward_fastest_cost": [1.0, 1.0], "onward_fastest_time": [0.5, 0.5],
        "onward_has_unknown_time": [False, False],
    })
    args = dict(
        steps_lf=steps.lazy(), destination_probability_lf=probabilities.lazy(),
        origin_cost_lf=origin.lazy(), onward_cost_lf=onward.lazy(),
        onward_left_on=["to", "anchor_to"], onward_right_on=["anchor_from", "anchor_to_cost"],
        onward_cost_col="cost_to_anchor", alpha=0.5, sequence_context_columns=["home_zone_id"],
        seed=3, output_cols=["to"], utility_values={"shopping": 1.0, "work": 1.0},
        activity_rigidity={"shopping": 0.0, "work": 0.0}, min_activity_time_constant=1.0,
        use_next_timing=True, filter_infeasible=True,
    )
    assert DestinationSequences._sample_sequence_aware_destinations(**args)[3].collect()["to"].to_list() == [2]
    args["destination_probability_lf"] = probabilities.filter(pl.col("to") == 3).lazy()
    assert DestinationSequences._sample_sequence_aware_destinations(**args)[3].collect().is_empty()
    args["origin_cost_lf"] = origin.with_columns(candidate_has_unknown_time=pl.lit(True)).lazy()
    assert DestinationSequences._sample_sequence_aware_destinations(**args)[3].collect()["to"].to_list() == [3]
    args["origin_cost_lf"] = origin.lazy()
    args["steps_lf"] = steps.with_columns(_next_is_direct=pl.lit(False)).lazy()
    assert DestinationSequences._sample_sequence_aware_destinations(**args)[3].collect()["to"].to_list() == [3]
    args["steps_lf"] = steps.lazy()
    args["activity_rigidity"] = {"shopping": 1.0, "work": 0.0}
    assert DestinationSequences._sample_sequence_aware_destinations(**args)[3].collect()["to"].to_list() == [3]
    # A rounding-sized overlap must be left to the final second-level check.
    args["activity_rigidity"] = {"shopping": 0.0, "work": 0.0}
    args["origin_cost_lf"] = origin.with_columns(
        pl.when(pl.col("candidate_to") == 3).then(1.0003).otherwise(pl.col("candidate_fastest_time"))
        .alias("candidate_fastest_time")
    ).lazy()
    assert DestinationSequences._sample_sequence_aware_destinations(**args)[3].collect()["to"].to_list() == [3]


def test_utility_sampling_accounts_for_previous_activity_and_home_time():
    steps = pl.DataFrame({
        "home_zone_id": [1] * 50, "from": [1] * 50, "activity": ["work"] * 50,
        "anchor_to": [4] * 50, "activity_draw_id": list(range(50)), "activity_seq_id": [1] * 50,
        "time_seq_id": [1] * 50, "dest_draw_id": [0] * 50,
        "departure_time": [8.0] * 50, "arrival_time": [8.5] * 50, "next_departure_time": [17.0] * 50,
        "_previous_activity": ["shopping"] * 50, "_previous_arrival": [6.0] * 50,
        "_previous_is_anchor": [False] * 50, "_previous_is_first": [False] * 50,
        "_home_night_reference": [8.0] * 50,
    })
    probabilities = pl.DataFrame({
        "from": [1, 1, 4], "activity": ["work", "work", "shopping"], "to": [2, 3, 1],
        "p_ij": [1.0] * 3, "activity_saturation": [1.0] * 3,
        "activity_shadow_price": [0.0] * 3, "country_value_coefficient": [1.0] * 3,
    })
    origin = pl.DataFrame({
        "candidate_from": [1, 1], "candidate_to": [2, 3], "cost_to_candidate": [1.0, 1.0],
        "candidate_fastest_cost": [1.0, 1.0], "candidate_fastest_time": [0.5, 2.0],
        "candidate_cheapest_cost": [1.0, 1.0], "candidate_cheapest_time": [0.5, 2.0],
    })
    onward = pl.DataFrame({
        "anchor_from": [2, 3], "anchor_to_cost": [4, 4], "cost_to_anchor": [1.0, 1.0],
        "onward_cheapest_cost": [1.0, 1.0],
    })
    args = dict(
        steps_lf=steps.lazy(), destination_probability_lf=probabilities.lazy(),
        origin_cost_lf=origin.lazy(), onward_cost_lf=onward.lazy(),
        onward_left_on=["to", "anchor_to"], onward_right_on=["anchor_from", "anchor_to_cost"],
        onward_cost_col="cost_to_anchor", alpha=0.5, sequence_context_columns=["home_zone_id"],
        seed=3, output_cols=["activity_draw_id", "to"], utility_values={"shopping": 20.0, "work": 1.0},
        mean_durations={"shopping": 2.0, "work": 8.0}, activity_rigidity={"work": 1.0},
        min_activity_time_constant=2.0, logit_scale=1.0, use_previous_timing=True,
        mean_home_duration=8.0, home_value=20.0,
    )
    sampled = DestinationSequences._sample_sequence_aware_destinations(**args)[3].collect()
    assert sampled.height == 50 and sampled["to"].to_list() == [2] * 50
    args["steps_lf"] = steps.with_columns(_previous_is_first=pl.lit(True)).lazy()
    sampled = DestinationSequences._sample_sequence_aware_destinations(**args)[3].collect()
    assert sampled.height == 50 and sampled["to"].to_list() == [2] * 50
    args["update_timings"] = False
    sampled = DestinationSequences._sample_sequence_aware_destinations(**args)[3].collect()
    assert sampled["to"].n_unique() == 2


def test_utility_sampling_uses_known_preceding_trip_for_previous_activity():
    steps = pl.DataFrame({
        "home_zone_id": [1] * 300, "from": [1] * 300, "activity": ["work"] * 300,
        "anchor_to": [4] * 300, "activity_draw_id": list(range(300)), "activity_seq_id": [1] * 300,
        "time_seq_id": [1] * 300, "dest_draw_id": [0] * 300,
        "departure_time": [8.0] * 300, "arrival_time": [8.5] * 300, "next_departure_time": [17.0] * 300,
        "_previous_activity": ["shopping"] * 300, "_previous_arrival": [6.0] * 300,
        "_previous_departure": [5.5] * 300, "_previous_origin": [0] * 300,
        "_previous_origin_known": [True] * 300, "_previous_is_anchor": [False] * 300,
        "_previous_is_first": [False] * 300, "_home_night_reference": [8.0] * 300,
    })
    probabilities = pl.DataFrame({
        "from": [1, 1, 4], "activity": ["work", "work", "shopping"], "to": [2, 3, 1],
        "p_ij": [1.0] * 3, "activity_saturation": [1.0] * 3,
        "activity_shadow_price": [0.0] * 3, "country_value_coefficient": [1.0] * 3,
    })
    origin = pl.DataFrame({
        "candidate_from": [1, 1, 0], "candidate_to": [2, 3, 1], "cost_to_candidate": [1.0, 1.0, 0.0],
        "candidate_fastest_cost": [1.0, 1.0, 0.0], "candidate_fastest_time": [0.5, 2.0, 0.75],
        "candidate_cheapest_cost": [1.0, 1.0, 0.0], "candidate_cheapest_time": [0.5, 2.0, 0.75],
        "candidate_has_unknown_time": [False, False, False],
    })
    onward = pl.DataFrame({
        "anchor_from": [2, 3], "anchor_to_cost": [4, 4], "cost_to_anchor": [1.0, 1.0],
        "onward_cheapest_cost": [1.0, 1.0],
    })
    args = dict(
        steps_lf=steps.lazy(), destination_probability_lf=probabilities.lazy(),
        origin_cost_lf=origin.lazy(), onward_cost_lf=onward.lazy(),
        onward_left_on=["to", "anchor_to"], onward_right_on=["anchor_from", "anchor_to_cost"],
        onward_cost_col="cost_to_anchor", alpha=0.5, sequence_context_columns=["home_zone_id"],
        seed=3, output_cols=["activity_draw_id", "to"], utility_values={"shopping": 2.0, "work": 1.0},
        mean_durations={"shopping": 2.0, "work": 8.0}, activity_rigidity={"shopping": 0.0, "work": 1.0},
        min_activity_time_constant=2.0, logit_scale=0.5, use_previous_timing=True,
    )
    reference = DestinationSequences._sample_sequence_aware_destinations(**args)[3].collect()
    args["model_previous_arrival"] = True
    modeled = DestinationSequences._sample_sequence_aware_destinations(**args)[3].collect()
    assert modeled.filter(pl.col("to") == 2).height > reference.filter(pl.col("to") == 2).height
    args["origin_cost_lf"] = origin.with_columns(
        pl.when(pl.col("candidate_from") == 0).then(0.5).otherwise(pl.col("candidate_fastest_time"))
        .alias("candidate_fastest_time"),
        pl.when(pl.col("candidate_from") == 0).then(100.0).otherwise(pl.col("candidate_fastest_cost"))
        .alias("candidate_fastest_cost"),
    ).lazy()
    expensive_fast = DestinationSequences._sample_sequence_aware_destinations(**args)[3].collect()
    assert_frame_equal(modeled.sort("activity_draw_id"), expensive_fast.sort("activity_draw_id"))
    args["origin_cost_lf"] = origin.lazy()
    args["steps_lf"] = steps.with_columns(_previous_origin_known=pl.lit(False)).lazy()
    fallback = DestinationSequences._sample_sequence_aware_destinations(**args)[3].collect()
    assert_frame_equal(reference.sort("activity_draw_id"), fallback.sort("activity_draw_id"))
    args["steps_lf"] = steps.lazy()
    args["origin_cost_lf"] = origin.with_columns(
        (pl.col("candidate_from") == 0).alias("candidate_has_unknown_time")
    ).lazy()
    fallback = DestinationSequences._sample_sequence_aware_destinations(**args)[3].collect()
    assert_frame_equal(reference.sort("activity_draw_id"), fallback.sort("activity_draw_id"))


def test_anchor_sampling_uses_last_known_location_for_an_intervening_stop(monkeypatch):
    parameters = GroupDayTripsParameters(
        destination_sequences={"sampling_method": "utility", "k_destination_sequences": 1},
        plan_update={"update_plan_timings_from_modeled_travel_times": True},
    )
    activities = [SimpleNamespace(name="work", is_anchor=True), SimpleNamespace(name="studies", is_anchor=True)]
    asset = DestinationSequences(
        is_weekday=True, iteration=1, activities=[], parameters=parameters, resolved_activity_parameters={},
    )
    asset.activities = activities
    asset.resolved_activity_parameters = {"work": SimpleNamespace(value_of_time=1.0),
                                          "studies": SimpleNamespace(value_of_time=1.0)}
    sequences = pl.DataFrame({
        "home_zone_id": [1] * 4, "activity_seq_id": [1] * 4, "time_seq_id": [1] * 4,
        "activity": ["work", "shopping", "studies", "home"], "is_anchor": [True, False, True, True],
        "seq_step_index": [1, 2, 3, 4], "step_count": [4] * 4,
        "departure_time": [8.0, 10.0, 11.0, 17.0], "arrival_time": [8.5, 10.25, 11.5, 17.5],
        "next_departure_time": [10.0, 11.0, 17.0, 24.0],
    }).with_columns(pl.col("seq_step_index", "step_count").cast(pl.UInt8))
    observed = []
    observed_timing = []

    def sample(**kwargs):
        steps = kwargs["steps_lf"].collect()
        observed_timing.append(kwargs["use_next_timing"])
        observed.extend(steps.select("activity", "from", "_previous_origin", "_previous_is_anchor").to_dicts())
        result = steps.with_columns(
            pl.when(pl.col("activity") == "work").then(pl.lit(2)).otherwise(pl.lit(3))
            .cast(pl.UInt16).alias("to")
        ).select(kwargs["output_cols"])
        return None, None, None, result.lazy()

    monkeypatch.setattr(asset, "_sample_sequence_aware_destinations", sample)
    asset._spatialize_anchor_activities(
        sequences, pl.DataFrame(), 0.5, 0, {"to_candidate": pl.DataFrame().lazy(), "to_home": pl.DataFrame().lazy()},
    )
    assert observed == [
        {"activity": "work", "from": 1, "_previous_origin": 1, "_previous_is_anchor": False},
        {"activity": "studies", "from": 2, "_previous_origin": 2, "_previous_is_anchor": False},
    ]
    assert observed_timing == [False, True]
    # Even with an explicit first home activity, the first trip leaves home.
    observed.clear()
    home_first = pl.DataFrame({
        "home_zone_id": [1] * 3, "activity_seq_id": [2] * 3, "time_seq_id": [2] * 3,
        "activity": ["home", "work", "home"], "is_anchor": [True] * 3,
        "seq_step_index": [1, 2, 3], "step_count": [3] * 3,
        "departure_time": [4.0, 8.0, 17.0], "arrival_time": [4.25, 8.5, 17.5],
        "next_departure_time": [8.0, 17.0, 24.0],
    }).with_columns(pl.col("seq_step_index", "step_count").cast(pl.UInt8))
    asset._spatialize_anchor_activities(
        home_first, pl.DataFrame(), 0.5, 0, {"to_candidate": pl.DataFrame().lazy(), "to_home": pl.DataFrame().lazy()},
    )
    assert observed == [{"activity": "work", "from": 1, "_previous_origin": 1, "_previous_is_anchor": True}]


def test_other_sampling_carries_actual_previous_origin(monkeypatch):
    parameters = GroupDayTripsParameters(destination_sequences={"sampling_method": "utility"})
    asset = DestinationSequences(is_weekday=True, iteration=1, parameters=parameters)
    sequences = pl.DataFrame({
        "home_zone_id": [1] * 3, "activity_draw_id": [0] * 3, "activity_seq_id": [1] * 3,
        "time_seq_id": [1] * 3, "dest_draw_id": [0] * 3, "seq_step_index": [1, 2, 3],
        "is_anchor": [True, False, False],
    })
    observed = []
    keys = ["home_zone_id", "activity_draw_id", "activity_seq_id", "time_seq_id", "dest_draw_id"]

    def spatialize(step, current, *args, **kwargs):
        observed.append(current.select("from", "_previous_origin", "_previous_origin_known").row(0))
        return current.select(keys + ["from"]).with_columns(to=pl.lit(step + 1, dtype=pl.Int64))

    monkeypatch.setattr(asset, "_spatialize_sequence_step", spatialize)
    asset._spatialize_other_activities(sequences, pl.DataFrame(), pl.DataFrame(), 0.5, 0, {})
    assert observed == [(1, 1, False), (2, 1, True), (3, 2, True)]


@pytest.mark.parametrize("rigidity", [0.0, 0.5, 1.0])
def test_previous_activity_utility_change_matches_full_schedule_timing(rigidity):
    """With the previous arrival fixed, the guide matches plan scoring."""
    steps = pl.DataFrame({
        "demand_group_id": [1, 1], "demand_subgroup_id": [0, 0], "activity_seq_id": [1, 1],
        "time_seq_id": [1, 1], "dest_seq_id": [1, 1], "mode_seq_id": [1, 1],
        "seq_step_index": [1, 2], "activity": ["shopping", "work"],
        "departure_time": [6.5, 8.0], "arrival_time": [7.0, 8.5],
        "next_departure_time": [8.0, 17.0], "duration_per_pers": [1.0, 8.5], "time": [0.5, 1.0],
    })
    modeled = PlanScheduleUpdater().update_plan_timings(
        steps.lazy(), activity_time_rigidity_by_activity={"shopping": 0.0, "work": rigidity}, enabled=True,
    ).collect()
    actual_duration = modeled.filter(pl.col("activity") == "shopping")["duration_per_pers"].item()
    actual_delta = 20.0 * 2.0 * (
        max(0.0, math.log(actual_duration / (2.0 * math.exp(-2.0))))
        - max(0.0, math.log(1.0 / (2.0 * math.exp(-2.0))))
    )
    predicted = pl.select(DestinationSequences._activity_utility_delta(
        pl.lit(1.0), pl.lit(1.0 - (1.0 + rigidity) / 2.0 * 0.5), pl.lit(2.0), pl.lit(20.0), 2.0,
    )).item()
    assert predicted == pytest.approx(actual_delta)


@pytest.mark.parametrize("rigidity", [0.0, 0.5, 1.0])
def test_first_departure_home_utility_change_matches_full_plan(rigidity):
    steps = pl.DataFrame({
        "demand_group_id": [1, 1], "demand_subgroup_id": [0, 0], "activity_seq_id": [1, 1],
        "time_seq_id": [1, 1], "dest_seq_id": [1, 1], "mode_seq_id": [1, 1],
        "seq_step_index": [1, 2], "activity": ["work", "home"],
        "departure_time": [8.0, 17.0], "arrival_time": [9.0, 18.0],
        "next_departure_time": [17.0, 24.0], "duration_per_pers": [8.0, 6.0], "time": [1.5, 1.0],
    })
    modeled = PlanScheduleUpdater().update_plan_timings(
        steps.lazy(), activity_time_rigidity_by_activity={"work": rigidity, "home": 0.0}, enabled=True,
    ).collect()
    actual_home = 24.0 - modeled["duration_per_pers"].sum() - modeled["time"].sum()
    actual_delta = 2.0 * 8.0 * (
        max(0.0, math.log(actual_home / (8.0 * math.exp(-1.0))))
        - max(0.0, math.log(8.0 / (8.0 * math.exp(-1.0))))
    )
    predicted = pl.select(DestinationSequences._activity_utility_delta(
        pl.lit(8.0), pl.lit(8.0 - (1.0 + rigidity) / 2.0 * 0.5), pl.lit(8.0), pl.lit(2.0), 1.0,
    )).item()
    assert predicted == pytest.approx(actual_delta)


def test_utility_sampling_cache_includes_utility_settings():
    parameters = GroupDayTripsParameters(destination_sequences={"sampling_method": "utility"})
    asset = DestinationSequences(is_weekday=True, iteration=1, parameters=parameters)
    assert asset.inputs["destination_sequence_parameters"].sampling_method == "utility"
    assert asset.inputs["utility_sampling_parameters"] == (
        parameters.plan_update.min_activity_time_constant, parameters.plan_update.transition_logit_scale,
        parameters.plan_update.update_plan_timings_from_modeled_travel_times,
    )


def test_destination_alpha_is_part_of_cache_key():
    parameters = GroupDayTripsParameters(
        destination_sequences=GroupDayTripsDestinationSequenceParameters(alpha=0.25),
    )
    destination_sequences = DestinationSequences(
        is_weekday=True,
        iteration=1,
        activities=[],
        resolved_activity_parameters={},
        parameters=parameters,
    )

    assert destination_sequences.inputs["destination_sequence_parameters"].alpha == 0.25


def test_sample_active_destination_sequences_keeps_only_active_activity_sequences():
    destination_sequences = DestinationSequences(
        is_weekday=True,
        iteration=3,
        current_plans=pl.DataFrame(
            {
                "demand_group_id": [1],
                "demand_subgroup_id": [0],
                "activity_seq_id": [10],
                "time_seq_id": [1],
                "dest_seq_id": [100],
                "mode_seq_id": [1000],
            },
            schema={
                "demand_group_id": pl.UInt32,
                "demand_subgroup_id": pl.UInt32,
                "activity_seq_id": pl.UInt32,
                "time_seq_id": pl.UInt32,
                "dest_seq_id": pl.UInt32,
                "mode_seq_id": pl.UInt32,
            },
        ),
        activity_sequences=_StubAsset(
            pl.DataFrame(
                {
                    "home_zone_id": [1],
                    "activity_seq_id": [10],
                    "time_seq_id": [0],
                    "seq_step_index": [0],
                    "activity": ["work"],
                },
                schema={
                    "home_zone_id": pl.Int32,
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
        activity_sequences,
        costs,
        parameters,
        seed,
    ):
        seen["activity_sequences"] = activity_sequences
        return pl.DataFrame(
            {
                "home_zone_id": [1],
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
                "home_zone_id": pl.Int32,
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

    assert seen["activity_sequences"].select("activity_seq_id").to_series().to_list() == [10]


def test_refresh_active_mode_alternatives_appends_active_destination_sequences():
    destination_sequences = DestinationSequences(
        is_weekday=True,
        iteration=5,
        current_plans=pl.DataFrame(
            {
                "demand_group_id": [1],
                "demand_subgroup_id": [0],
                "activity_seq_id": [10],
                "time_seq_id": [20],
                "dest_seq_id": [30],
                "mode_seq_id": [40],
            },
            schema={
                "demand_group_id": pl.UInt32,
                "demand_subgroup_id": pl.UInt32,
                "activity_seq_id": pl.UInt32,
                "time_seq_id": pl.UInt32,
                "dest_seq_id": pl.UInt32,
                "mode_seq_id": pl.UInt32,
            },
        ),
        current_plan_steps=pl.DataFrame(
            {
                "demand_group_id": [1],
                "demand_subgroup_id": [0],
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
                "demand_subgroup_id": pl.UInt32,
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
        demand_groups=pl.DataFrame(
            {
                "demand_group_id": [1],
                "demand_subgroup_id": [0],
                "home_zone_id": [1],
            },
            schema={
                "demand_group_id": pl.UInt32,
                "demand_subgroup_id": pl.UInt32,
                "home_zone_id": pl.UInt16,
            },
        ),
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
            "home_zone_id": [1],
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
        },
        schema={
            "home_zone_id": pl.Int32,
            "activity_seq_id": pl.UInt32,
            "time_seq_id": pl.UInt32,
            "dest_seq_id": pl.UInt32,
            "seq_step_index": pl.UInt8,
            "from": pl.UInt16,
            "to": pl.UInt16,
            "departure_time": pl.Float32,
            "arrival_time": pl.Float32,
            "next_departure_time": pl.Float32,
            "iteration": pl.UInt16,
        },
    )

    result = destination_sequences._with_current_active_destination_sequences(sampled)

    assert result.select("dest_seq_id").sort("dest_seq_id").to_series().to_list() == [30, 31]
    assert result.filter(pl.col("dest_seq_id") == 30).select("iteration").item() == 5
    assert result.columns == DestinationSequences.OUTPUT_COLUMNS


def test_refresh_active_mode_alternatives_deduplicates_shared_zone_steps():
    destination_sequences = DestinationSequences(
        is_weekday=True,
        iteration=5,
        current_plans=pl.DataFrame(
            {
                "demand_group_id": [1, 1],
                "demand_subgroup_id": [0, 1],
                "activity_seq_id": [10, 10],
                "time_seq_id": [20, 20],
                "dest_seq_id": [30, 30],
                "mode_seq_id": [40, 40],
            },
            schema={
                "demand_group_id": pl.UInt32,
                "demand_subgroup_id": pl.UInt32,
                "activity_seq_id": pl.UInt32,
                "time_seq_id": pl.UInt32,
                "dest_seq_id": pl.UInt32,
                "mode_seq_id": pl.UInt32,
            },
        ),
        current_plan_steps=pl.DataFrame(
            {
                "demand_group_id": [1, 1, 1, 1],
                "demand_subgroup_id": [0, 0, 1, 1],
                "activity_seq_id": [10, 10, 10, 10],
                "time_seq_id": [20, 20, 20, 20],
                "dest_seq_id": [30, 30, 30, 30],
                "seq_step_index": [1, 2, 1, 2],
                "from": [100, 200, 101, 201],
                "to": [200, 100, 200, 100],
                "departure_time": [8.0, 17.0, 8.0, 17.0],
                "arrival_time": [9.0, 18.0, 9.0, 18.0],
                "next_departure_time": [17.0, 18.0, 17.0, 18.0],
            },
            schema={
                "demand_group_id": pl.UInt32,
                "demand_subgroup_id": pl.UInt32,
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
        demand_groups=pl.DataFrame(
            {
                "demand_group_id": [1, 1],
                "demand_subgroup_id": [0, 1],
                "home_zone_id": [1, 1],
            },
            schema={
                "demand_group_id": pl.UInt32,
                "demand_subgroup_id": pl.UInt32,
                "home_zone_id": pl.Int32,
            },
        ),
        costs=pl.DataFrame(),
        parameters=GroupDayTripsParameters(
            destination_sequences=GroupDayTripsDestinationSequenceParameters(
                refresh_active_mode_alternatives=True,
            ),
        ),
        seed=123,
        resolved_activity_parameters={},
    )
    sampled = pl.DataFrame(schema=DestinationSequences.OUTPUT_SCHEMA)

    result = destination_sequences._with_current_active_destination_sequences(sampled)

    assert result.height == 2
    assert result.select("home_zone_id", "dest_seq_id").unique().to_dicts() == [
        {"home_zone_id": 1, "dest_seq_id": 30}
    ]
    assert result.sort("seq_step_index").select("to").to_series().to_list() == [200, 100]


def test_refresh_active_mode_alternatives_default_keeps_sampled_destinations_only():
    destination_sequences = DestinationSequences(
        is_weekday=True,
        iteration=5,
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

    result = destination_sequences._with_current_active_destination_sequences(sampled)

    assert result is sampled


def test_destination_probability_inputs_use_cost_and_sink_even_without_destination_utilities():
    destination_sequences = DestinationSequences(
        is_weekday=True,
        iteration=1,
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


def test_destination_probability_inputs_use_shadow_attraction_when_enabled():
    destination_sequences = DestinationSequences(
        is_weekday=True,
        iteration=1,
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


def test_spatialize_sequence_step_uses_sequence_cost_to_reweight_non_anchor_candidates():
    destination_sequences = DestinationSequences(
        is_weekday=True,
        iteration=1,
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
                "home_zone_id": [1] * len(candidate_destinations),
                "activity_draw_id": [0] * len(candidate_destinations),
                "activity_seq_id": [10] * len(candidate_destinations),
                "time_seq_id": [1] * len(candidate_destinations),
                "dest_draw_id": [1] * len(candidate_destinations),
                "to": candidate_destinations,
            },
            schema={
                "home_zone_id": pl.UInt16,
                "activity_draw_id": pl.UInt32,
                "activity_seq_id": pl.UInt32,
                "time_seq_id": pl.UInt32,
                "dest_draw_id": pl.UInt32,
                "to": pl.UInt16,
            },
        )
        .with_columns(
            noise=(
                    pl.struct(
                        ["home_zone_id", "activity_draw_id", "activity_seq_id", "time_seq_id", "dest_draw_id", "to"]
                    ).hash(seed=seed)
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

    sequence_step = pl.DataFrame(
        {
            "demand_group_id": [1],
            "demand_subgroup_id": [0],
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
            "demand_subgroup_id": pl.UInt32,
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

    result_without_sequence_penalty = destination_sequences._spatialize_sequence_step(
        seq_step_index=1,
        sequence_step=sequence_step,
        destination_probability=destination_probability,
        costs=costs,
        alpha=0.0,
        seed=seed,
        cost_views=DestinationSequences._spatialization_cost_views(costs),
        non_anchor_count=1,
        anchor_count=0,
    )
    result_with_sequence_penalty = destination_sequences._spatialize_sequence_step(
        seq_step_index=1,
        sequence_step=sequence_step,
        destination_probability=destination_probability,
        costs=costs,
        alpha=1.0,
        seed=seed,
        cost_views=DestinationSequences._spatialization_cost_views(costs),
        non_anchor_count=1,
        anchor_count=0,
    )

    assert result_without_sequence_penalty["to"].to_list() == [lowest_noise_candidate]
    assert result_with_sequence_penalty["to"].to_list() == [highest_noise_candidate]
    assert result_with_sequence_penalty["to"].to_list() != result_without_sequence_penalty["to"].to_list()


def test_spatialize_anchor_activities_samples_from_current_anchor_location():
    destination_sequences = DestinationSequences(
        is_weekday=True,
        iteration=1,
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

    sequences = pl.DataFrame(
        {
            "demand_group_id": [1, 1, 1],
            "demand_subgroup_id": [0, 0, 0],
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
            "demand_subgroup_id": pl.UInt32,
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
        sequences,
        destination_probability,
        alpha=0.0,
        seed=123,
        cost_views=DestinationSequences._spatialization_cost_views(costs),
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


def test_spatialize_anchor_activities_uses_sequence_cost_to_reweight_candidates():
    destination_sequences = DestinationSequences(
        is_weekday=True,
        iteration=1,
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
                "home_zone_id": [home_zone] * len(candidate_destinations),
                "activity_draw_id": [0] * len(candidate_destinations),
                "activity_seq_id": [10] * len(candidate_destinations),
                "time_seq_id": [1] * len(candidate_destinations),
                "dest_draw_id": [0] * len(candidate_destinations),
                "to": candidate_destinations,
            },
            schema={
                "home_zone_id": pl.UInt16,
                "activity_draw_id": pl.UInt32,
                "activity_seq_id": pl.UInt32,
                "time_seq_id": pl.UInt32,
                "dest_draw_id": pl.UInt32,
                "to": pl.UInt16,
            },
        )
        .with_columns(
            noise=(
                    pl.struct(
                        ["home_zone_id", "activity_draw_id", "activity_seq_id", "time_seq_id", "dest_draw_id", "to"]
                    ).hash(seed=seed)
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

    sequences = pl.DataFrame(
        {
            "demand_group_id": [1, 1],
            "demand_subgroup_id": [0, 0],
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
            "demand_subgroup_id": pl.UInt32,
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

    result_without_sequence_penalty = destination_sequences._spatialize_anchor_activities(
        sequences,
        destination_probability,
        alpha=0.0,
        seed=seed,
        cost_views=DestinationSequences._spatialization_cost_views(costs),
    )
    result_with_sequence_penalty = destination_sequences._spatialize_anchor_activities(
        sequences,
        destination_probability,
        alpha=1.0,
        seed=seed,
        cost_views=DestinationSequences._spatialization_cost_views(costs),
    )

    assert result_without_sequence_penalty.filter(pl.col("activity") == "work")["anchor_to"].to_list() == [
        lowest_noise_candidate
    ]
    assert result_with_sequence_penalty.filter(pl.col("activity") == "work")["anchor_to"].to_list() == [
        highest_noise_candidate
    ]


def test_spatialize_sequence_step_drops_anchor_without_leg_cost():
    destination_sequences = DestinationSequences(
        is_weekday=True,
        iteration=1,
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

    sequence_step = pl.DataFrame(
        {
            "demand_group_id": [1],
            "demand_subgroup_id": [0],
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
            "demand_subgroup_id": pl.UInt32,
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

    result = destination_sequences._spatialize_sequence_step(
        seq_step_index=1,
        sequence_step=sequence_step,
        destination_probability=destination_probability,
        costs=costs,
        alpha=0.0,
        seed=123,
        cost_views=DestinationSequences._spatialization_cost_views(costs),
        non_anchor_count=0,
        anchor_count=1,
    )

    assert result.is_empty()


def test_drop_incomplete_destination_draws_removes_partial_draws():
    activity_sequences = pl.DataFrame(
        {
            "demand_group_id": [1, 1, 2, 2],
            "demand_subgroup_id": [0, 0, 0, 0],
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
            "demand_subgroup_id": pl.UInt32,
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
        sequence_context_columns=["home_zone_id"],
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


def test_reuse_current_destination_sequences_reuses_current_plan_steps():
    destination_sequences = DestinationSequences(
        is_weekday=True,
        iteration=4,
        current_plans=pl.DataFrame(
            {
                "demand_group_id": [1],
                "demand_subgroup_id": [0],
                "activity_seq_id": [10],
                "time_seq_id": [1],
                "dest_seq_id": [100],
                "mode_seq_id": [1000],
            },
            schema={
                "demand_group_id": pl.UInt32,
                "demand_subgroup_id": pl.UInt32,
                "activity_seq_id": pl.UInt32,
                "time_seq_id": pl.UInt32,
                "dest_seq_id": pl.UInt32,
                "mode_seq_id": pl.UInt32,
            },
        ),
        current_plan_steps=pl.DataFrame(
            {
                "demand_group_id": [1, 1],
                "demand_subgroup_id": [0, 0],
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
                "demand_subgroup_id": pl.UInt32,
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
        demand_groups=pl.DataFrame(
            {
                "demand_group_id": [1],
                "demand_subgroup_id": [0],
                "home_zone_id": [21],
            },
            schema={
                "demand_group_id": pl.UInt32,
                "demand_subgroup_id": pl.UInt32,
                "home_zone_id": pl.Int32,
            },
        ),
    )

    result = destination_sequences._reuse_current_destination_sequences()

    assert result["iteration"].unique().to_list() == [4]
    assert result["seq_step_index"].sort().to_list() == [0, 1]
