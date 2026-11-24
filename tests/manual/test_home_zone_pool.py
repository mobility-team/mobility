"""Small checks for the direct home-zone plan experiment."""
import polars as pl

from polars.testing import assert_frame_equal

from home_zone_pool import (build_factored_home_zone_candidates, build_home_zone_candidates,
                            build_home_zone_candidates_batch, prepare_home_zone_schedules)
from mobility.trips.group_day_trips.plans.compact_candidate_steps import PLAN_COLUMNS


def test_builds_all_compatible_schedules_with_each_mode_at_home():
    plans = pl.DataFrame({
        "country": ["fr", "fr", "fr", "fr", "ch"], "is_weekday": [True, True, True, False, True],
        "activity_seq_id": [1, 1, 2, 3, 4], "time_seq_id": [1, 1, 2, 3, 4],
        "plan_weight_mass": [2.0, 1.0, 4.0, 8.0, 8.0],
    })
    steps = pl.DataFrame({
        "activity_seq_id": [1, 1, 2, 2, 3, 3, 4, 4], "time_seq_id": [1, 1, 2, 2, 3, 3, 4, 4],
        "seq_step_index": [1, 2] * 4, "activity": ["shopping", "home", "work", "home"] * 2,
        "step_count": [2] * 8, "duration_per_pers": [1.0] * 8, "departure_time": [8.0] * 8,
        "arrival_time": [8.5] * 8, "next_departure_time": [10.0] * 8,
    }).with_columns(pl.col("activity_seq_id", "time_seq_id").cast(pl.UInt32),
                    pl.col("seq_step_index", "step_count").cast(pl.UInt8))
    context = pl.DataFrame({"demand_group_id": [7], "demand_subgroup_id": [0], "home_zone_id": [9],
                            "country": ["fr"], "csp": ["worker"], "allowed_activities": [["shopping", "work"]]})
    opportunities = pl.DataFrame({"to": [9, 9], "activity": ["shopping", "work"],
                                  "opportunity_capacity": [1.0, 0.0]})
    candidates = build_home_zone_candidates(
        survey_plans=plans, survey_steps=steps, context=context, opportunities=opportunities,
        modes=["walk", "car"], is_weekday=True,
    )
    assert candidates.height == 4
    assert candidates["activity_seq_id"].unique().to_list() == [1]
    assert set(candidates["mode"].to_list()) == {"walk", "car"}
    assert candidates["from"].unique().to_list() == [9]
    assert candidates["to"].unique().to_list() == [9]
    assert candidates["mode_seq_id"].n_unique() == 2
    prepared = prepare_home_zone_schedules(plans, steps, True)
    cached = build_home_zone_candidates(
        survey_plans=plans, survey_steps=steps, context=context, opportunities=opportunities,
        modes=["walk", "car"], is_weekday=True, prepared_schedules=prepared,
    )
    assert_frame_equal(candidates.sort("mode", "seq_step_index"), cached.sort("mode", "seq_step_index"))

    no_opportunities = opportunities.with_columns(opportunity_capacity=pl.lit(0.0))
    empty = build_home_zone_candidates(
        survey_plans=plans, survey_steps=steps, context=context, opportunities=no_opportunities,
        modes=["walk", "car"], is_weekday=True,
    )
    assert empty.is_empty()
    no_modes = build_home_zone_candidates(
        survey_plans=plans, survey_steps=steps, context=context, opportunities=opportunities,
        modes=[], is_weekday=True,
    )
    assert no_modes.is_empty()

    contexts = pl.concat([
        context.with_columns(utility_profile_id=pl.lit(0, dtype=pl.UInt32)),
        context.with_columns(demand_group_id=pl.lit(8, dtype=pl.Int64), home_zone_id=pl.lit(10, dtype=pl.Int64),
                             utility_profile_id=pl.lit(1, dtype=pl.UInt32)),
    ])
    local_opportunities = pl.concat([opportunities, opportunities.with_columns(to=pl.lit(10, dtype=pl.Int64))])
    available = pl.DataFrame({"home_zone_id": [9, 9, 10, 10], "utility_profile_id": [0, 0, 1, 1],
                              "mode": ["walk", "car", "walk", "car"], "_mode_index": [0, 1, 0, 1]})
    available = available.with_columns(pl.col("utility_profile_id", "_mode_index").cast(pl.UInt32))
    batched = build_home_zone_candidates_batch(
        survey_steps=steps, contexts=contexts, opportunities=local_opportunities,
        available_modes=available, prepared_schedules=prepared,
    )
    separate = pl.concat([
        build_home_zone_candidates(survey_plans=plans, survey_steps=steps, context=row,
                                   opportunities=local_opportunities, modes=["walk", "car"], is_weekday=True)
        .with_columns(utility_profile_id=pl.lit(profile, dtype=pl.UInt32))
        for row, profile in [(contexts.slice(0, 1), 0), (contexts.slice(1, 1), 1)]
    ])
    order = ["demand_group_id", "mode_seq_id", "seq_step_index"]
    assert_frame_equal(batched.sort(order), separate.sort(order))
    selected = batched.filter(pl.col("demand_group_id") == 7).select(
        "demand_group_id", "demand_subgroup_id", "activity_seq_id", "time_seq_id",
    ).unique()
    selected_batch = build_home_zone_candidates_batch(
        survey_steps=steps, contexts=contexts, opportunities=local_opportunities,
        available_modes=available, prepared_schedules=prepared, selected_schedules=selected,
    )
    assert_frame_equal(selected_batch.sort(order), batched.filter(pl.col("demand_group_id") == 7).sort(order))
    selected_all = batched.select("demand_group_id", "demand_subgroup_id", "activity_seq_id", "time_seq_id").unique()
    physical, membership = build_factored_home_zone_candidates(
        survey_steps=steps, contexts=contexts, opportunities=local_opportunities,
        available_modes=available, prepared_schedules=prepared, selected_schedules=selected_all,
    )
    rebuilt = membership.join(physical.with_columns(pl.col("from").alias("home_zone_id")),
                              on=PLAN_COLUMNS + ["home_zone_id"]).select(batched.columns)
    assert_frame_equal(rebuilt.sort(order), batched.sort(order))


def test_top_survey_schedules_are_selected_after_local_opportunity_filter():
    plans = pl.DataFrame({
        "country": ["fr", "fr", "fr"], "is_weekday": [True] * 3,
        "activity_seq_id": [1, 2, 3], "time_seq_id": [1, 2, 3], "plan_weight_mass": [3.0, 9.0, 2.0],
    })
    steps = pl.DataFrame({
        "activity_seq_id": [1, 1, 2, 2, 3, 3], "time_seq_id": [1, 1, 2, 2, 3, 3],
        "seq_step_index": [1, 2] * 3, "activity": ["shopping", "home", "work", "home", "shopping", "home"],
        "step_count": [2] * 6, "duration_per_pers": [1.0] * 6, "departure_time": [8.0] * 6,
        "arrival_time": [8.5] * 6, "next_departure_time": [10.0] * 6,
    }).with_columns(pl.col("activity_seq_id", "time_seq_id").cast(pl.UInt32),
                    pl.col("seq_step_index", "step_count").cast(pl.UInt8))
    context = pl.DataFrame({"demand_group_id": [7], "demand_subgroup_id": [0], "home_zone_id": [9],
                            "country": ["fr"], "csp": ["worker"], "allowed_activities": [["shopping", "work"]]})
    opportunities = pl.DataFrame({"to": [9], "activity": ["shopping"], "opportunity_capacity": [1.0]})
    candidates = build_home_zone_candidates(
        survey_plans=plans, survey_steps=steps, context=context, opportunities=opportunities,
        modes=["walk", "car"], is_weekday=True, max_schedules=1,
    )
    assert candidates["activity_seq_id"].unique().to_list() == [1]
    assert candidates["mode"].n_unique() == 2
