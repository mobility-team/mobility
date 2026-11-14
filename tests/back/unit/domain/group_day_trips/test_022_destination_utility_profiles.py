"""Sample complete destination chains with each group's resolved costs."""

from types import SimpleNamespace

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from mobility.trips.group_day_trips.core.parameters import GroupDayTripsParameters
from mobility.runtime.assets.in_memory_asset import InMemoryAsset
from mobility.trips.group_day_trips.plans.destination_sequences import DestinationSequences, TravelOptionsCache


@pytest.mark.parametrize("sampling_method", ["radiation", "utility"])
def test_profile_sampling_does_not_build_unused_aggregate_costs(sampling_method):
    demand = pl.DataFrame({"demand_group_id": [1]})
    state = SimpleNamespace(
        destination_sequence_index=None, current_plans=pl.DataFrame(), current_plan_steps=pl.DataFrame(),
        destination_saturation=pl.DataFrame(), demand_groups=demand, costs=pl.DataFrame({"cost": [1.0]}),
        activity_dur=pl.DataFrame(), home_night_dur=pl.DataFrame(),
    )
    sampler = DestinationSequences(
        is_weekday=True, iteration=1,
        parameters=GroupDayTripsParameters(destination_sequences={"sampling_method": sampling_method}),
    )
    sampler.previous_state = SimpleNamespace(get=lambda: state)
    sampler.transport_costs = SimpleNamespace(get_costs_by_od=lambda *_: pytest.fail("aggregate costs are unused"))
    sampler._load_missing_runtime_inputs_from_previous_state()
    assert sampler.demand_groups is demand
    assert sampler.costs is None

    sampler.transport_costs = None
    sampler._load_missing_runtime_inputs_from_previous_state()
    assert sampler.costs is state.costs


@pytest.mark.parametrize("shared_destination", [False, True])
@pytest.mark.parametrize("sampling_method", ["radiation", "utility"])
def test_destination_sampling_uses_profiles_present_in_each_zone(shared_destination, sampling_method, monkeypatch):
    demand = pl.DataFrame({
        "demand_group_id": [1, 2, 3], "demand_subgroup_id": [0, 0, 0],
        "home_zone_id": [1, 1, 4],
    }).with_columns(pl.col("demand_group_id", "demand_subgroup_id").cast(pl.UInt32))
    assignments = demand.select("demand_group_id", "demand_subgroup_id").with_columns(
        utility_profile_id=pl.Series([0, 1, 0], dtype=pl.UInt32)
    )
    calls = []

    def profile_costs(profiles, metrics):
        profile_id = next(iter(profiles))
        calls.append(profile_id)
        destination = 2 if profile_id == 0 or shared_destination else 3
        # Profile 0 can reach only destination 2; profile 1 only destination 3.
        # Two identical modes also exercise the expected-cost aggregation.
        return pl.DataFrame({
            "from": [1, destination, 4, destination] * 2,
            "to": [destination, 1, destination, 4] * 2,
            "cost": [1.0] * 4 + [2.0] * 4,
            "time": [1.0] * 4 + [0.5] * 4,
            "mode": ["walk"] * 4 + ["car"] * 4,
            "utility_profile_id": [profile_id] * 8,
        })

    transport_costs = SimpleNamespace(
        get_utility_profiles=lambda groups, segments: (assignments, {1: [], 0: []}),
        get_profile_costs_by_od_and_mode=profile_costs,
    )
    parameters = GroupDayTripsParameters(destination_sequences={"k_destination_sequences": 2, "sampling_method": sampling_method})
    sampler = DestinationSequences(
        is_weekday=True, iteration=1, activities=[], resolved_activity_parameters={},
        demand_groups=demand, parameters=parameters,
    )
    sampler.transport_costs = transport_costs
    draw_counts = []
    spatialize = sampler._spatialize_anchor_activities

    def record_draws(*args, **kwargs):
        result = spatialize(*args, **kwargs)
        draw_counts.append(result["dest_draw_id"].n_unique())
        return result

    monkeypatch.setattr(sampler, "_spatialize_anchor_activities", record_draws)
    schedules = pl.DataFrame({
        "home_zone_id": [1, 1, 4, 4],
        "activity_seq_id": [1] * 4, "time_seq_id": [10] * 4,
        "activity": ["work", "home"] * 2, "is_anchor": [True] * 4,
        "seq_step_index": [1, 2] * 2, "step_count": [2] * 4,
        "departure_time": [8.0, 17.0] * 2, "arrival_time": [9.0, 18.0] * 2,
        "next_departure_time": [17.0, 24.0] * 2,
    }).with_columns(
        pl.col("activity_seq_id", "time_seq_id").cast(pl.UInt32),
        pl.col("seq_step_index", "step_count").cast(pl.UInt8),
    )
    opportunities = pl.DataFrame({
        "to": [2, 3], "activity": ["work", "work"],
        "opportunity_capacity": [100.0, 100.0],
    })
    activities = [SimpleNamespace(name="work")]
    sampler.resolved_activity_parameters = {"work": SimpleNamespace(radiation_lambda=0.99, value_of_time=1.0)}
    result = sampler.run(
        activities, None, opportunities, schedules, pl.DataFrame(), parameters, 42,
    )
    assert calls == [0, 1]
    assert draw_counts == [2, 2]
    outbound = result.filter(pl.col("seq_step_index") == 1)
    expected = [2] if shared_destination else [2, 3]
    assert outbound.filter(pl.col("home_zone_id") == 1)["to"].sort().to_list() == expected
    assert outbound.filter(pl.col("home_zone_id") == 4)["to"].to_list() == [2]
    chain_count = 2 if shared_destination else 3
    assert result.group_by("home_zone_id", "dest_seq_id").len()["len"].to_list() == [2] * chain_count
    # The same chain receives one stable id when drawn in another iteration.
    index = sampler.sequence_index
    sampler.previous_sequence_index = index
    repeated = sampler.run(
        activities, None, opportunities, schedules.reverse(), pl.DataFrame(), parameters, 42,
    )
    assert_frame_equal(result, repeated)
    assert_frame_equal(index, sampler.sequence_index)


def test_travel_options_cache_reuses_only_matching_costs_and_profiles(monkeypatch):
    demand = pl.DataFrame({"demand_group_id": [1], "demand_subgroup_id": [0], "home_zone_id": [1]})
    assignments = demand.select("demand_group_id", "demand_subgroup_id").with_columns(
        utility_profile_id=pl.lit(0, dtype=pl.UInt32)
    )

    class Costs(InMemoryAsset):
        def __init__(self, name, profile):
            super().__init__({"name": name})
            self.profile = profile
            self.calls = 0

        def get_utility_profiles(self, *_):
            return assignments, {0: [self.profile]}

        def get_profile_costs_by_od_and_mode(self, *_):
            self.calls += 1
            return pl.DataFrame({"from": [1], "to": [2], "mode": ["walk"], "cost": [1.0], "time": [0.5]})

    cache = TravelOptionsCache(max_bytes=10000)
    parameters = GroupDayTripsParameters(destination_sequences={"sampling_method": "utility"})
    costs = Costs("costs-a", InMemoryAsset({"profile": "a"}))
    sampler = DestinationSequences(
        is_weekday=True, iteration=1, activities=[], resolved_activity_parameters={},
        demand_groups=demand, parameters=parameters, transport_costs=costs,
        travel_options_cache=cache, travel_options_run_key=1,
    )
    sequences = pl.DataFrame({"home_zone_id": [1], "activity_seq_id": [1]})
    sampled = pl.DataFrame({
        "home_zone_id": [1], "activity_seq_id": [1], "time_seq_id": [1], "dest_seq_id": [1], "seq_step_index": [1],
    })
    monkeypatch.setattr(sampler, "_run_for_utility_profile", lambda *args, **kwargs: sampled)

    def run():
        return sampler.run([], None, pl.DataFrame(), sequences, pl.DataFrame(), parameters, 0)

    assert_frame_equal(run(), run())
    assert costs.calls == 1
    sampler.travel_options_run_key = 2
    run()
    assert costs.calls == 2  # Another scenario cannot use this scenario's table.
    sampler.travel_options_run_key = 1
    costs.profile = InMemoryAsset({"profile": "b"})
    run()
    assert costs.calls == 3  # Profile definitions can change while IDs stay the same.
    new_costs = Costs("costs-b", costs.profile)
    sampler.transport_costs = new_costs
    run()
    assert new_costs.calls == 1  # A different cost asset cannot reuse old costs.
    cache.clear()
    run()
    assert new_costs.calls == 2

    sampler.travel_options_cache = TravelOptionsCache(max_bytes=0)
    run()
    run()
    assert new_costs.calls == 4  # A full cache affects speed, not results.
