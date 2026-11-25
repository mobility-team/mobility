"""Timing shared physical plans must match the context-expanded evaluator."""
import polars as pl
import pytest
from dataclasses import replace
from polars.testing import assert_frame_equal

from mobility.trips.group_day_trips.plans.compact_candidate_steps import PLAN_COLUMNS
from mobility.trips.group_day_trips.plans.factored_plan_timing import time_factored_candidates
from mobility.trips.group_day_trips.plans.factored_plan_scoring import (
    SectionScoreCache, score_factored_sections,
)
from mobility.trips.group_day_trips.plans.plan_ids import PLAN_KEY_COLS
from mobility.trips.group_day_trips.plans.plan_schedule_updater import PlanScheduleUpdater
from mobility.trips.group_day_trips.plans.scoring_contexts import ScoringMemoryBudget
from mobility.trips.group_day_trips.plans.time_sequences import assign_time_sequence_ids


def test_factored_timing_matches_expanded_ids_and_steps_for_shared_and_distinct_times():
    schedule = pl.DataFrame({
        "activity_seq_id": [1, 1], "time_seq_id": [1, 1], "seq_step_index": [1, 2],
        "activity": ["work", "home"], "is_anchor": [True, True], "step_count": [2, 2],
        "departure_time": [8.0, 17.0], "arrival_time": [9.0, 18.0],
        "next_departure_time": [17.0, 24.0], "duration_per_pers": [8.0, 6.0],
    }).with_columns(pl.col("activity_seq_id", "time_seq_id").cast(pl.UInt32),
                    pl.col("seq_step_index", "step_count").cast(pl.UInt8),
                    pl.col("departure_time", "arrival_time", "next_departure_time", "duration_per_pers").cast(pl.Float32))
    physical = schedule.drop("is_anchor", "step_count").with_columns(
        pl.Series("from", [1, 2], dtype=pl.UInt16), pl.Series("to", [2, 1], dtype=pl.UInt16),
        dest_seq_id=pl.lit(1, dtype=pl.UInt32), mode_seq_id=pl.lit(1, dtype=pl.UInt32), mode=pl.lit("car"),
    )
    memberships = pl.DataFrame({"demand_group_id": [1, 2, 3], "demand_subgroup_id": [0, 0, 0]}).with_columns(
        pl.col("demand_group_id", "demand_subgroup_id").cast(pl.UInt32),
        activity_seq_id=pl.lit(1, dtype=pl.UInt32), time_seq_id=pl.lit(1, dtype=pl.UInt32),
        dest_seq_id=pl.lit(1, dtype=pl.UInt32), mode_seq_id=pl.lit(1, dtype=pl.UInt32),
    )
    profiles = memberships.select("demand_group_id", "demand_subgroup_id").with_columns(
        utility_profile_id=pl.Series([1, 2, 3], dtype=pl.UInt32))
    times = pl.DataFrame({"utility_profile_id": [1, 1, 2, 2, 3, 3],
                          "from": [1, 2, 1, 2, 1, 2], "to": [2, 1, 2, 1, 2, 1],
                          "mode": ["car"] * 6, "time": [1.5, 1.0, 1.5, 1.0, 2.0, 1.0]}).with_columns(
                              pl.col("utility_profile_id").cast(pl.UInt32), pl.col("from", "to").cast(pl.UInt16))
    rigidity = {"work": 1.0, "home": 0.0}
    factored = time_factored_candidates(physical, memberships, profiles, times, schedule,
                                        activity_time_rigidity_by_activity=rigidity, enabled=True)
    assert factored.steps["_timing_plan"].n_unique() == 2
    expanded = memberships.join(profiles, on=["demand_group_id", "demand_subgroup_id"]).join(
        physical, on=PLAN_COLUMNS).join(times, on=["utility_profile_id", "from", "to", "mode"])
    timed = PlanScheduleUpdater().update_plan_timings(
        expanded.lazy(), activity_time_rigidity_by_activity=rigidity, enabled=True).collect(engine="streaming")
    reference_steps, reference_catalogue, reference_mapping = assign_time_sequence_ids(timed, schedule)
    actual_mapping = factored.context_mapping.drop("_timing_plan")
    assert_frame_equal(actual_mapping.sort(PLAN_KEY_COLS[:-3] + ["previous_time_seq_id", "dest_seq_id", "mode_seq_id"]),
                       reference_mapping.select(actual_mapping.columns).sort(
                           PLAN_KEY_COLS[:-3] + ["previous_time_seq_id", "dest_seq_id", "mode_seq_id"]), check_exact=True)
    actual_steps = (factored.context_mapping.join(factored.steps, on="_timing_plan", suffix="_physical")
                    .select(PLAN_KEY_COLS + ["seq_step_index", "departure_time", "arrival_time",
                                             "next_departure_time", "duration_per_pers"]))
    cols = actual_steps.columns
    assert_frame_equal(actual_steps.sort(PLAN_KEY_COLS + ["seq_step_index"]),
                       reference_steps.select(cols).sort(PLAN_KEY_COLS + ["seq_step_index"]), check_exact=True)
    assert_frame_equal(factored.time_sequence_steps, reference_catalogue, check_exact=True)

    # Equal travel times can be shared without sharing the profiles' costs or
    # activity values. Score the timed physical plans once per real profile.
    contexts = profiles.with_columns(
        country=pl.lit("fr"), csp=pl.when(pl.col("utility_profile_id") == 3).then(pl.lit("retired"))
        .otherwise(pl.lit("worker")),
    )
    assert factored.context_links.height == 3 and factored.timing_plans.height == 2
    profile_columns = ["country", "csp", "utility_profile_id"]
    expected_profiles = factored.context_mapping.join(
        contexts.select(["demand_group_id", "demand_subgroup_id"] + profile_columns),
        on=["demand_group_id", "demand_subgroup_id"], how="left", maintain_order="left",
    )
    assert_frame_equal(factored.contexts_with_profiles(contexts, profile_columns), expected_profiles, check_exact=True)
    scores = (expanded.join(contexts.select("demand_group_id", "demand_subgroup_id", "country", "csp"),
                            on=["demand_group_id", "demand_subgroup_id"])
              .select("country", "csp", "utility_profile_id", "activity", "from", "to", "mode", "time").unique()
              .with_columns(cost=pl.col("utility_profile_id").cast(pl.Float64), distance=pl.lit(1.0),
                            is_feasible=pl.col("utility_profile_id") != 2,
                            _missing_cost_row=pl.lit(False), _invalid_cost_row=pl.col("utility_profile_id") == 2,
                            min_activity_time=pl.lit(1.0),
                            activity_utility_scale=pl.col("utility_profile_id").cast(pl.Float64)))
    home = pl.DataFrame({"country": ["fr", "fr"], "csp": ["worker", "retired"],
                         "mean_home_night_per_pers": [8.0, 9.0]})
    score_keys = ["country", "csp", "utility_profile_id", "activity", "from", "to", "mode", "time"]
    direct = (reference_steps.join(contexts.select("demand_group_id", "demand_subgroup_id", "country", "csp"),
                                   on=["demand_group_id", "demand_subgroup_id"])
              .join(scores, on=score_keys)
              .with_columns(is_feasible=pl.col("is_feasible") & (
                  (pl.col("seq_step_index") == pl.col("seq_step_index").max().over(PLAN_KEY_COLS))
                  | (pl.col("next_departure_time") >= pl.col("arrival_time"))),
                  utility=pl.col("activity_utility_scale")
                  * (pl.col("duration_per_pers") / pl.col("min_activity_time")).log().clip(0.0) - pl.col("cost"))
              .group_by(PLAN_KEY_COLS + ["country", "csp"]).agg(
                  utility=pl.col("utility").sort().implode().list.sum(), is_feasible=pl.col("is_feasible").all(),
                  missing_cost_row_count=pl.col("_missing_cost_row").sum(),
                  invalid_cost_row_count=pl.col("_invalid_cost_row").sum(),
                  home_night_per_pers=(24.0 - pl.col("duration_per_pers").cast(pl.Float64).sort().implode().list.sum()
                                       - pl.col("time").cast(pl.Float64).sort().implode().list.sum()).clip(0.0))
              .join(home, on=["country", "csp"])
              .with_columns(utility=pl.col("utility") + pl.col("mean_home_night_per_pers")
                            * (pl.col("home_night_per_pers") / (pl.col("mean_home_night_per_pers") * 0.36787944117144233))
                            .log().clip(0.0))
              .select(PLAN_KEY_COLS + ["utility", "is_feasible", "missing_cost_row_count", "invalid_cost_row_count"]))
    section_scores = score_factored_sections(factored, contexts, scores, home, rigid_activities={"work"},
                                              value_of_time_stay_home=1.0, min_activity_time_constant=1.0)
    assert_frame_equal(section_scores.utilities.sort(PLAN_KEY_COLS), direct.sort(PLAN_KEY_COLS),
                       check_exact=False, rel_tol=1e-14, abs_tol=1e-14)
    cache = SectionScoreCache()
    first = replace(factored, context_links=factored.context_links.filter(pl.col("demand_group_id") < 3))
    score_factored_sections(first, contexts, scores, home, rigid_activities={"work"},
                            value_of_time_stay_home=1.0, min_activity_time_constant=1.0, cache=cache)
    previous_scores = cache.scores
    extended = score_factored_sections(factored, contexts, scores, home, rigid_activities={"work"},
                                       value_of_time_stay_home=1.0, min_activity_time_constant=1.0, cache=cache)
    assert cache.scores.height > previous_scores.height
    assert_frame_equal(cache.scores.head(previous_scores.height), previous_scores, check_exact=True)
    assert_frame_equal(extended.utilities.sort(PLAN_KEY_COLS), section_scores.utilities.sort(PLAN_KEY_COLS),
                       check_exact=True)
    repeated = score_factored_sections(factored, contexts, scores, home, rigid_activities={"work"},
                                       value_of_time_stay_home=1.0, min_activity_time_constant=1.0, cache=cache)
    assert_frame_equal(repeated.utilities.sort(PLAN_KEY_COLS), section_scores.utilities.sort(PLAN_KEY_COLS),
                       check_exact=True)
    # A new outer step may reuse section identities, but must calculate its
    # scores again after costs or opportunity values change.
    next_step = SectionScoreCache(sections=cache.sections)
    changed_scores = scores.with_columns(cost=pl.col("cost") + 2.0,
                                         activity_utility_scale=pl.col("activity_utility_scale") * 0.8)
    reused = score_factored_sections(factored, contexts, changed_scores, home, rigid_activities={"work"},
                                     value_of_time_stay_home=1.0, min_activity_time_constant=1.0, cache=next_step)
    rebuilt = score_factored_sections(factored, contexts, changed_scores, home, rigid_activities={"work"},
                                      value_of_time_stay_home=1.0, min_activity_time_constant=1.0)
    assert_frame_equal(reused.utilities.sort(PLAN_KEY_COLS), rebuilt.utilities.sort(PLAN_KEY_COLS), check_exact=True)
    assert not reused.utilities.sort(PLAN_KEY_COLS).equals(section_scores.utilities.sort(PLAN_KEY_COLS))
    budget = ScoringMemoryBudget(max_bytes=1)
    limited = SectionScoreCache(budget=budget)
    limited_result = score_factored_sections(factored, contexts, scores, home, rigid_activities={"work"},
                                              value_of_time_stay_home=1.0, min_activity_time_constant=1.0,
                                              cache=limited)
    assert limited.sections is None and limited.scores is None and budget.used_bytes == 0
    assert_frame_equal(limited_result.utilities.sort(PLAN_KEY_COLS), section_scores.utilities.sort(PLAN_KEY_COLS),
                       check_exact=True)


def test_factored_timing_rejects_missing_context_profiles():
    physical = pl.DataFrame({
        "activity_seq_id": [1], "time_seq_id": [1], "dest_seq_id": [1], "mode_seq_id": [1],
        "seq_step_index": [0],
    })
    memberships = pl.DataFrame({
        "demand_group_id": [1], "demand_subgroup_id": [0], "activity_seq_id": [1],
        "time_seq_id": [1], "dest_seq_id": [1], "mode_seq_id": [1],
    })
    profiles = pl.DataFrame(schema={"demand_group_id": pl.Int64, "demand_subgroup_id": pl.Int64,
                                    "utility_profile_id": pl.Int64})
    times = pl.DataFrame(schema={"utility_profile_id": pl.Int64, "from": pl.Int64, "to": pl.Int64,
                                 "mode": pl.String, "time": pl.Float64})
    error = pytest.raises(ValueError, time_factored_candidates, physical, memberships, profiles, times, pl.DataFrame(),
                          activity_time_rigidity_by_activity={}, enabled=True)
    assert "missing its transport profile" in str(error.value)
