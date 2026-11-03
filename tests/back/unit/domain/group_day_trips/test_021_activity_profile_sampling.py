"""Give every activity profile its configured schedule draws in each zone."""

import polars as pl
from polars.testing import assert_frame_equal

from mobility.trips.group_day_trips.core.parameters import GroupDayTripsParameters
from mobility.trips.group_day_trips.plans.activity_sequences import ActivitySequences


def test_each_profile_gets_k_draws_in_each_zone():
    # The two profiles have disjoint support, so a zone-wide sample cannot
    # accidentally satisfy this check by drawing one shared schedule.
    schedules = pl.DataFrame({
        "country": ["fr", "fr"],
        "activity_profile_id": ["work", "shop"],
        "activity_seq_id": [1, 2], "time_seq_id": [10, 20],
        "plan_weight_mass": [1000.0, 1.0],
    })
    zones = pl.DataFrame({
        "country": ["fr"] * 4, "home_zone_id": [1, 1, 2, 2],
        "activity_profile_id": ["work", "shop", "work", "shop"],
    })
    sampler = ActivitySequences(
        is_weekday=True, iteration=1, seed=42,
        parameters=GroupDayTripsParameters(activity_sequences={"k_activity_sequences": 3}),
    )
    sampled = sampler._sample_schedules_with_replacement(schedules, zones)
    assert sampled.height == 12
    assert sampled.group_by("home_zone_id", "time_seq_id").len()["len"].to_list() == [3] * 4
    for zone in [1, 2]:
        assert sampled.filter(pl.col("home_zone_id") == zone)["activity_draw_id"].to_list() == list(range(6))
    # Input order cannot change the seeded sample or draw identities.
    repeated = sampler._sample_schedules_with_replacement(schedules.reverse(), zones.reverse())
    assert_frame_equal(sampled, repeated)


def test_profile_draws_use_survey_weights():
    schedules = pl.DataFrame({
        "country": ["fr", "fr"], "activity_profile_id": ["work", "work"],
        "activity_seq_id": [1, 2], "time_seq_id": [10, 20],
        "plan_weight_mass": [0.0, 1.0],
    })
    zones = pl.DataFrame({
        "country": ["fr"], "home_zone_id": [1], "activity_profile_id": ["work"],
    })
    sampler = ActivitySequences(
        is_weekday=True, iteration=1, seed=42,
        parameters=GroupDayTripsParameters(activity_sequences={"k_activity_sequences": 5}),
    )
    sampled = sampler._sample_schedules_with_replacement(schedules, zones)
    assert sampled["time_seq_id"].to_list() == [20] * 5


def test_all_schedules_are_kept_once_when_k_is_none():
    schedules = pl.DataFrame({
        "country": ["fr", "fr", "fr"],
        "activity_profile_id": ["work", "work_shop", "work_shop"],
        "activity_seq_id": [1, 1, 2], "time_seq_id": [10, 10, 20],
        "plan_weight_mass": [1.0, 1.0, 1.0],
    })
    zones = pl.DataFrame({
        "country": ["fr", "fr"], "home_zone_id": [1, 1],
        "activity_profile_id": ["work", "work_shop"],
    })
    sampler = ActivitySequences(
        is_weekday=True, iteration=1, seed=42,
        parameters=GroupDayTripsParameters(activity_sequences={"k_activity_sequences": None}),
    )
    sampled = sampler._sample_schedules_with_replacement(schedules, zones)
    assert sorted(sampled["time_seq_id"].to_list()) == [10, 20]


def test_empty_zones_produce_no_draws():
    schedules = pl.DataFrame(schema={
        "country": pl.String, "activity_profile_id": pl.String,
        "activity_seq_id": pl.UInt32, "time_seq_id": pl.UInt32,
        "plan_weight_mass": pl.Float64,
    })
    zones = pl.DataFrame(schema={
        "country": pl.String, "home_zone_id": pl.Int32, "activity_profile_id": pl.String,
    })
    sampler = ActivitySequences(
        is_weekday=True, iteration=1, seed=42, parameters=GroupDayTripsParameters(),
    )
    sampled = sampler._sample_schedules_with_replacement(schedules, zones)
    assert sampled.is_empty()
    assert sampled.schema["activity_draw_id"] == pl.UInt32


def test_survey_pool_and_sampler_return_complete_profile_schedules():
    # A working profile and a shopping profile share a zone. Each must
    # receive two complete schedules, even with very different survey weights.
    steps = pl.DataFrame({
        "activity_seq_id": [1, 1, 2, 2], "time_seq_id": [10, 10, 20, 20],
        "activity": ["work", "home", "shop", "home"],
        "is_anchor": [True, True, False, True],
        "seq_step_index": [1, 2, 1, 2], "step_count": [2] * 4,
        "departure_time": [8.0, 17.0, 10.0, 11.0],
        "arrival_time": [9.0, 18.0, 10.5, 11.5],
        "next_departure_time": [17.0, 24.0, 11.0, 24.0],
    }).with_columns(pl.col("activity_seq_id", "time_seq_id").cast(pl.UInt32))
    surveys = pl.DataFrame({
        "country": ["fr", "fr"], "activity_seq_id": [1, 2],
        "time_seq_id": [10, 20], "plan_weight_mass": [1000.0, 1.0],
        "is_weekday": [True, True],
    }).with_columns(pl.col("activity_seq_id", "time_seq_id").cast(pl.UInt32))
    demand = pl.DataFrame({
        "country": ["fr", "fr"], "home_zone_id": [1, 1],
        "allowed_activities": [["work"], ["shop"]],
    })
    sampler = ActivitySequences(
        is_weekday=True, iteration=1, seed=42, current_plans=pl.DataFrame(),
        survey_plans=surveys, survey_plan_steps=steps, demand_groups=demand,
        parameters=GroupDayTripsParameters(activity_sequences={"k_activity_sequences": 2}),
    )
    sampled = sampler.get()
    assert sampled.height == 8
    assert sampled["activity_draw_id"].n_unique() == 4
    assert sampled.group_by("time_seq_id").len()["len"].to_list() == [4, 4]
    assert sampled.group_by("activity_draw_id").agg(
        pl.col("seq_step_index").sort()
    )["seq_step_index"].to_list() == [[1, 2]] * 4
