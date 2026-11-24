import math
from types import SimpleNamespace

import polars as pl
import pytest
from polars.testing import assert_frame_equal
import mobility.trips.group_day_trips.plans.plan_updater as plan_updater_module

from mobility.trips.group_day_trips import (
    BehaviorChangePhase,
    BehaviorChangeScope,
    GroupDayTripsBehaviorChangeParameters,
)
from mobility.trips.group_day_trips.plans.candidate_plan_steps import CandidatePlanStepsAsset
from mobility.trips.group_day_trips.plans.plan_ids import PLAN_KEY_COLS, add_plan_id
from mobility.trips.group_day_trips.plans.plan_updater import CandidatePlanEvaluation, PlanUpdater
from mobility.trips.group_day_trips.plans.scoring_contexts import ScoringContexts


@pytest.mark.parametrize("scope,expected_modes", [
    (BehaviorChangeScope.MODE_REPLANNING, [1, 2]),
    (BehaviorChangeScope.DESTINATION_REPLANNING, [1, 2, 3]),
    (BehaviorChangeScope.NO_TRANSITIONS, [1]),
])
def test_intermediate_scoring_keeps_reachable_candidates_and_forced_contexts(scope, expected_modes):
    candidates = pl.DataFrame({
        "demand_group_id": [1, 1, 1, 1, 2, 2, 3], "demand_subgroup_id": [0] * 7,
        "activity_seq_id": [1, 1, 1, 2, 1, 2, 1], "time_seq_id": [1, 2, 1, 1, 1, 1, 1],
        "dest_seq_id": [1, 1, 2, 1, 1, 2, 1], "mode_seq_id": [1, 2, 3, 4, 1, 4, 1],
        "seq_step_index": [1] * 7,
    })
    origins = candidates.filter(
        ((pl.col("demand_group_id") == 1) & (pl.col("mode_seq_id") == 1))
        | ((pl.col("demand_group_id") == 2) & (pl.col("mode_seq_id") == 1))
    ).select(PLAN_KEY_COLS)
    references = pl.DataFrame({"time_seq_id": [1, 2], "reference_time_seq_id": [1, 1]})
    forced = pl.DataFrame({"demand_group_id": [2], "demand_subgroup_id": [0]})
    selected = PlanUpdater._reachable_scoring_candidates(candidates, origins, references, scope, forced)
    assert selected.filter(pl.col("demand_group_id") == 1)["mode_seq_id"].sort().to_list() == expected_modes
    assert selected.filter(pl.col("demand_group_id") == 2)["mode_seq_id"].sort().to_list() == [1, 4]
    assert selected.filter(pl.col("demand_group_id") == 3).is_empty()


@pytest.mark.parametrize("scope", list(BehaviorChangeScope))
def test_shared_context_transitions_keep_unit_origins_and_history(scope, monkeypatch):
    units = pl.DataFrame({
        "demand_group_id": [1, 2, 3], "demand_subgroup_id": [0, 0, 0],
        "_score_context_id": [0, 0, 0],
    }).cast(pl.UInt32)
    contexts = ScoringContexts(pl.DataFrame(), units)
    choices = pl.DataFrame({
        "demand_group_id": [0, 0, 0, 0], "demand_subgroup_id": [0, 0, 0, 0],
        "activity_seq_id": [0, 1, 1, 1], "time_seq_id": [0, 1, 1, 1],
        "dest_seq_id": [0, 1, 1, 2], "mode_seq_id": [0, 1, 2, 1],
        "utility": [0.0, 1.0, 2.0, 3.0], "is_feasible": [True, True, False, True],
    }).with_columns(pl.col(PLAN_KEY_COLS).cast(pl.UInt32))
    choices, compact_index = add_plan_id(choices, previous_index=None)
    expanded_choices, index = add_plan_id(contexts.expand_step_rows(choices).drop("plan_id"), previous_index=None)
    expanded_choices = expanded_choices.collect()
    origins = expanded_choices.filter(
        ((pl.col("demand_group_id") == 1) & (pl.col("mode_seq_id") == 1) & (pl.col("dest_seq_id") == 1))
        | ((pl.col("demand_group_id") == 2) & (pl.col("mode_seq_id") == 2))
        | ((pl.col("demand_group_id") == 3) & (pl.col("mode_seq_id") == 0))
    ).select(PLAN_KEY_COLS + ["plan_id"]).with_columns(
        n_persons=pl.Series([10.0, 20.0, 30.0]), utility=pl.Series([-1.0, -2.0, -3.0]),
    )
    times = pl.DataFrame({"time_seq_id": [0, 1], "reference_time_seq_id": [0, 1]}).cast(pl.UInt32)
    compact = CandidatePlanEvaluation(
        candidate_plan_steps=pl.DataFrame(), possible_plan_steps=pl.DataFrame(),
        possible_plan_utility=choices, plan_id_index=compact_index,
        time_sequence_steps=times, scoring_contexts=contexts,
        time_mapping=choices.select(PLAN_KEY_COLS).with_columns(previous_time_seq_id=pl.col("time_seq_id")),
        current_plans=contexts.collapse_allocations(origins).join(compact_index, on=PLAN_KEY_COLS),
    )
    evaluation = CandidatePlanEvaluation(
        candidate_plan_steps=pl.DataFrame(), possible_plan_steps=pl.DataFrame(),
        possible_plan_utility=expanded_choices, plan_id_index=index, current_plans=origins,
        time_sequence_steps=times, compact_transition_evaluation=compact,
    )
    updater = PlanUpdater()
    settings = dict(transport_zones=None, transition_revision_probability=0.4, min_transition_utility_gain=0.1)
    expected = updater.get_transition_probabilities(
        origins, expanded_choices.join(times, on="time_seq_id").lazy(), pl.DataFrame(), scope, **settings,
    )
    actual = updater.get_context_transition_probabilities(evaluation, scope, **settings)
    keys = ["plan_id_from", "plan_id_trans"]
    assert_frame_equal(actual.select(expected.columns).sort(keys), expected.sort(keys))
    for min_persons, iteration in ((0.0, 3), (1.0, 3), (1.0, 1)):
        effective_floor = 0.0 if scope == BehaviorChangeScope.NO_TRANSITIONS else min_persons
        home = expanded_choices.filter(pl.col("mode_seq_id") == 0).select(["demand_group_id", "demand_subgroup_id", "plan_id", "utility"])
        expected_allocation, _ = updater.apply_transitions(origins, expected, iteration, min_plan_persons=effective_floor, stay_home_plans=home)
        actual_allocation, _ = updater.apply_transitions(origins, actual, iteration, min_plan_persons=effective_floor, stay_home_plans=home)
        assert_frame_equal(actual_allocation.sort(PLAN_KEY_COLS), expected_allocation.sort(PLAN_KEY_COLS))
        without_events, events = updater.apply_transitions(
            origins, actual, iteration, min_plan_persons=effective_floor, stay_home_plans=home, include_events=False,
        )
        assert events is None
        assert_frame_equal(without_events.sort(PLAN_KEY_COLS), expected_allocation.sort(PLAN_KEY_COLS))
        expected_context = contexts.collapse_allocations(expected_allocation).join(compact_index, on=PLAN_KEY_COLS)
        reachable = updater.estimate_reachable_allocation(
            compact, origins, iteration, scope, min_plan_persons=min_persons, **settings,
        )
        compare_columns = [column for column in expected_context.columns if column != "utility"]
        assert_frame_equal(reachable.select(compare_columns).sort(PLAN_KEY_COLS),
                           expected_context.select(compare_columns).sort(PLAN_KEY_COLS))
        if min_persons > 0 and iteration >= 2 and scope != BehaviorChangeScope.NO_TRANSITIONS:
            monkeypatch.setattr(plan_updater_module, "_REACHABLE_ALLOCATION_BATCH_ROWS", 1)
            batched = updater.estimate_reachable_allocation(
                compact, origins, iteration, scope, min_plan_persons=min_persons, **settings,
            )
            assert_frame_equal(batched.select(compare_columns).sort(PLAN_KEY_COLS),
                               expected_context.select(compare_columns).sort(PLAN_KEY_COLS), check_exact=True)


def test_small_plan_allocations_move_to_the_same_subgroup_home():
    keys = pl.DataFrame({
        "demand_group_id": [1, 1, 1, 1, 1, 1],
        "demand_subgroup_id": [0, 0, 0, 1, 1, 1],
        "activity_seq_id": [0, 1, 2, 0, 1, 2],
        "time_seq_id": [0, 1, 2, 0, 1, 2],
        "dest_seq_id": [0, 1, 2, 0, 1, 2],
        "mode_seq_id": [0, 1, 2, 0, 1, 2],
    }).cast(pl.UInt32)
    keys, _ = add_plan_id(keys, previous_index=None)
    homes = keys.filter(pl.col("mode_seq_id") == 0).with_columns(utility=pl.lit(-3.0))
    origins = keys.filter(pl.col("mode_seq_id") != 0).with_columns(
        n_persons=pl.Series([0.6, 0.6, 1.0, 0.9]), utility=pl.lit(1.0),
    )
    # Two origins contribute to the same target in subgroup 0; the floor
    # applies to their combined allocation, not to each transition separately.
    targets = origins.with_columns(
        plan_id_trans=pl.when(pl.col("demand_subgroup_id") == 0)
            .then(pl.lit(keys.filter((pl.col("demand_subgroup_id") == 0) & (pl.col("mode_seq_id") == 1))["plan_id"][0]))
            .otherwise(pl.col("plan_id")),
        **{col + "_trans": pl.when(pl.col("demand_subgroup_id") == 0).then(pl.lit(1)).otherwise(pl.col(col))
           for col in ("activity_seq_id", "time_seq_id", "dest_seq_id", "mode_seq_id")},
        p_transition=pl.lit(1.0), utility_trans=pl.lit(2.0), utility_from_updated=pl.lit(1.0),
        utility_prev_from=pl.lit(1.0), tau_transition=pl.lit(0.0),
        q_transition=pl.lit(1.0), adjustment_factor=pl.lit(1.0),
    ).drop("n_persons", "utility", "plan_id")
    result, events = PlanUpdater().apply_transitions(
        origins, targets, 2, min_plan_persons=1.0, stay_home_plans=homes,
    )
    assert result.filter((pl.col("demand_subgroup_id") == 0) & (pl.col("mode_seq_id") == 1))["n_persons"][0] == pytest.approx(1.2)
    assert result.filter((pl.col("demand_subgroup_id") == 1) & (pl.col("mode_seq_id") == 1))["n_persons"][0] == pytest.approx(1.0)
    home = result.filter((pl.col("demand_subgroup_id") == 1) & (pl.col("mode_seq_id") == 0))
    assert home["n_persons"][0] == pytest.approx(0.9)
    assert home["utility"][0] == -3.0
    assert result["n_persons"].sum() == pytest.approx(origins["n_persons"].sum())
    redirected = events.collect().filter((pl.col("demand_subgroup_id") == 1) & (pl.col("mode_seq_id_trans") == 0))
    assert redirected["chosen_mode_seq_id"].to_list() == [2]
    reordered, _ = PlanUpdater().apply_transitions(
        origins.reverse(), targets.reverse(), 2, min_plan_persons=1.0,
        stay_home_plans=homes.reverse(), include_events=False,
    )
    assert_frame_equal(result.sort(PLAN_KEY_COLS), reordered.sort(PLAN_KEY_COLS))


def test_no_transition_step_keeps_small_occupied_plans():
    """State-size pruning must not move people on a no-transition step."""
    choices = pl.DataFrame({
        "demand_group_id": [1, 1], "demand_subgroup_id": [0, 0],
        "activity_seq_id": [1, 1], "time_seq_id": [1, 1],
        "dest_seq_id": [1, 1], "mode_seq_id": [1, 2],
        "utility": [1.0, 2.0], "is_feasible": [True, True],
    }).with_columns(pl.col(PLAN_KEY_COLS).cast(pl.UInt32))
    choices, _ = add_plan_id(choices, previous_index=None)
    origins = choices.select(PLAN_KEY_COLS + ["plan_id", "utility"]).with_columns(
        n_persons=pl.Series([99.0, 1.0]),
    )
    updater = PlanUpdater()
    probabilities = updater.get_transition_probabilities(
        origins, choices.lazy(), pl.DataFrame(), BehaviorChangeScope.NO_TRANSITIONS,
        transport_zones=None,
    )
    result, _ = updater.apply_transitions(
        origins, probabilities, 5, min_plan_persons=2.0,
        behavior_change_scope=BehaviorChangeScope.NO_TRANSITIONS, include_events=False,
    )
    assert_frame_equal(
        result.select(PLAN_KEY_COLS + ["n_persons"]).sort(PLAN_KEY_COLS),
        origins.select(PLAN_KEY_COLS + ["n_persons"]).sort(PLAN_KEY_COLS),
    )
    contexts = ScoringContexts(
        pl.DataFrame(),
        pl.DataFrame({"demand_group_id": [1], "demand_subgroup_id": [0], "_score_context_id": [0]}).cast(pl.UInt32),
    )
    compact_choices, compact_index = add_plan_id(
        choices.drop("plan_id").with_columns(demand_group_id=pl.lit(0, dtype=pl.UInt32)), previous_index=None,
    )
    compact_origins = contexts.collapse_allocations(origins).join(compact_index, on=PLAN_KEY_COLS)
    times = pl.DataFrame({"time_seq_id": [1], "reference_time_seq_id": [1]}).cast(pl.UInt32)
    evaluation = CandidatePlanEvaluation(
        candidate_plan_steps=pl.DataFrame(), possible_plan_steps=pl.DataFrame(),
        possible_plan_utility=compact_choices, plan_id_index=compact_index,
        current_plans=compact_origins, time_sequence_steps=times, scoring_contexts=contexts,
    )
    estimated = updater.estimate_reachable_allocation(
        evaluation, origins, 5, BehaviorChangeScope.NO_TRANSITIONS,
        transport_zones=None, min_plan_persons=2.0,
    )
    assert_frame_equal(
        estimated.select(PLAN_KEY_COLS + ["n_persons"]).sort(PLAN_KEY_COLS),
        compact_origins.select(PLAN_KEY_COLS + ["n_persons"]).sort(PLAN_KEY_COLS),
    )


def test_transition_probabilities_are_stable_when_options_are_reordered():
    transitions = pl.DataFrame({
        "plan_id_from": [1] * 4, "plan_id_trans": [1, 2, 3, 4],
        "demand_group_id": [1] * 4, "demand_subgroup_id": [0] * 4,
        "activity_seq_id": [1] * 4, "time_seq_id": [1] * 4,
        "dest_seq_id": [1] * 4, "mode_seq_id": [1] * 4,
        "activity_seq_id_trans": [1] * 4, "time_seq_id_trans": [1] * 4,
        "dest_seq_id_trans": [1] * 4, "mode_seq_id_trans": [1, 2, 3, 4],
        "utility_prev_from": [0.0] * 4, "utility": [0.0] * 4,
        "utility_trans": [0.0, -12.350490091531594, -0.24164211866780505, -6.232632468659979],
    })
    updater = PlanUpdater()
    forward = updater.compute_transition_probabilities_from_utilities(transitions.lazy()).sort("plan_id_trans")
    reverse = updater.compute_transition_probabilities_from_utilities(transitions.reverse().lazy()).sort("plan_id_trans")
    assert forward.equals(reverse)


def test_full_replanning_keeps_self_and_all_feasible_for_forced_reassignment():
    """Pruning must not remove a self-transition or strand an infeasible origin."""
    choices = pl.DataFrame({
        "demand_group_id": [1, 1, 1, 1, 2, 2, 2, 3],
        "demand_subgroup_id": [0] * 8,
        "activity_seq_id": [1, 2, 3, 4, 1, 2, 3, 1],
        "time_seq_id": [1] * 8,
        "dest_seq_id": [1, 2, 3, 4, 1, 2, 3, 1],
        "mode_seq_id": [1] * 8,
        "plan_id": list(range(1, 9)),
        "utility": [0.0, 10.0, -2.0, 100.0, 10.0, -100.0, 0.0, 0.0],
        "is_feasible": [True, True, True, False, False, True, True, False],
    })
    current = choices.filter(pl.col("plan_id").is_in([1, 5, 8])).select(
        "demand_group_id", "demand_subgroup_id", "activity_seq_id", "time_seq_id",
        "dest_seq_id", "mode_seq_id", "utility",
    )
    result = PlanUpdater().get_transition_probabilities(
        current, choices.lazy(), pl.DataFrame(), BehaviorChangeScope.FULL_REPLANNING,
        transport_zones=None, transition_utility_pruning_delta=1.0,
        transition_logit_scale=1.0, min_transition_utility_gain=0.1,
        transition_revision_probability=0.4,
    )
    assert result.select("plan_id_from", "plan_id_trans").sort(
        ["plan_id_from", "plan_id_trans"]
    ).rows() == [(1, 1), (1, 2), (5, 6), (5, 7)]
    sums = result.group_by("plan_id_from").agg(pl.col("p_transition").sum()).sort("plan_id_from")
    assert sums["p_transition"].to_list() == pytest.approx([1.0, 1.0])


@pytest.mark.parametrize("scale,delta,gain", [(0.25, 1.0, 0.1), (1.0, math.inf, 0.0), (0.0, 1.0, 0.2)])
@pytest.mark.parametrize("scope", list(BehaviorChangeScope))
def test_replanning_matches_filtering_after_pair_generation(scale, delta, gain, scope):
    """Moving the filters into the join must preserve the old pair choices and probabilities."""
    choices = pl.DataFrame({
        "demand_group_id": [1, 1, 1, 1, 2, 2, 2, 1], "demand_subgroup_id": [0] * 8,
        "activity_seq_id": [1, 1, 1, 4, 1, 2, 3, 0], "time_seq_id": [1, 2, 1, 1, 1, 1, 1, 0],
        "reference_time_seq_id": [1, 1, 1, 1, 1, 1, 1, 0],
        "dest_seq_id": [1, 1, 2, 4, 1, 2, 3, 0], "mode_seq_id": [1, 2, 1, 1, 1, 1, 1, 0],
        "plan_id": list(range(1, 9)), "utility": [0.0, 4.0, 5.0, 9.0, 7.0, -2.0, 2.0, 8.0],
        "is_feasible": [True, True, True, False, False, True, True, True],
    }).lazy()
    current = choices.filter(pl.col("plan_id").is_in([1, 2, 5, 8])).select(PLAN_KEY_COLS + ["utility"]).collect()
    updater = PlanUpdater()
    actual = updater.build_allowed_plan_transitions(
        current, choices, scope,
        transition_utility_pruning_delta=delta, transition_logit_scale=scale,
        min_transition_utility_gain=gain,
    )
    # This is the previous calculation: enumerate the eligible pairs first,
    # then find the best target and filter within each origin plan.
    forced = ~pl.col("is_feasible").fill_null(True)
    same = (pl.col("plan_id_from") == pl.col("plan_id_trans"))
    scope_pair = pl.lit(True)
    if scope in (BehaviorChangeScope.DESTINATION_REPLANNING, BehaviorChangeScope.MODE_REPLANNING):
        scope_pair = (
            (pl.col("activity_seq_id") == pl.col("activity_seq_id_trans"))
            & (pl.col("reference_time_seq_id") == pl.col("reference_time_seq_id_trans"))
        )
        if scope == BehaviorChangeScope.MODE_REPLANNING:
            scope_pair = scope_pair & (pl.col("dest_seq_id") == pl.col("dest_seq_id_trans"))
        scope_pair = (scope_pair & (pl.col("mode_seq_id_trans") != 0)) | forced
        current = current.filter(pl.col("mode_seq_id") != 0)
    elif scope == BehaviorChangeScope.NO_TRANSITIONS:
        scope_pair = (
            (pl.col("activity_seq_id") == pl.col("activity_seq_id_trans"))
            & (pl.col("time_seq_id") == pl.col("time_seq_id_trans"))
            & (pl.col("dest_seq_id") == pl.col("dest_seq_id_trans"))
            & (pl.col("mode_seq_id") == pl.col("mode_seq_id_trans"))
        ) | forced
    legacy = (
        current.lazy().rename({"utility": "utility_prev_from"})
        .join(choices, on=PLAN_KEY_COLS).rename({"plan_id": "plan_id_from"})
        .join_where(
            choices.filter(pl.col("is_feasible")),
            (pl.col("demand_group_id") == pl.col("demand_group_id_trans"))
            & (pl.col("demand_subgroup_id") == pl.col("demand_subgroup_id_trans")) & scope_pair,
            suffix="_trans",
        )
        .drop("demand_group_id_trans", "demand_subgroup_id_trans")
        .rename({"plan_id": "plan_id_trans"})
        .with_columns(max_utility_trans=pl.col("utility_trans").max().over(PLAN_KEY_COLS))
    )
    threshold = updater._scale_utility_pruning_delta(delta, scale)
    if math.isfinite(threshold):
        legacy = legacy.filter(forced | same | (pl.col("utility_trans") >= pl.col("max_utility_trans") - threshold))
    legacy = legacy.filter(forced | same | (pl.col("utility_trans") >= pl.col("utility") + gain)).drop("max_utility_trans")
    expected = updater.compute_transition_probabilities_from_utilities(
        legacy, transition_logit_scale=scale, transition_revision_probability=0.4,
    ).sort(["plan_id_from", "plan_id_trans"])
    result = updater.compute_transition_probabilities_from_utilities(
        actual, transition_logit_scale=scale, transition_revision_probability=0.4,
    ).sort(["plan_id_from", "plan_id_trans"])
    assert result.equals(expected)

    if scope != BehaviorChangeScope.FULL_REPLANNING:
        units = current.select("demand_group_id", "demand_subgroup_id").unique().with_columns(
            _score_context_id=pl.col("demand_group_id")
        )
        time_steps = choices.select("time_seq_id", "reference_time_seq_id").unique().collect()
        evaluation = SimpleNamespace(
            scoring_contexts=ScoringContexts(pl.DataFrame(), units),
            possible_plan_utility=choices.drop("reference_time_seq_id").collect(),
            time_sequence_steps=time_steps,
        )
        narrowed = updater._restrict_transition_choices(
            choices.drop("reference_time_seq_id"), current, evaluation, scope,
        ).join(time_steps.lazy(), on="time_seq_id", how="left")
        assert narrowed.collect().height < choices.collect().height
        reduced = updater.get_transition_probabilities(
            current, narrowed, pl.DataFrame(), scope, transport_zones=None,
            transition_utility_pruning_delta=delta, transition_logit_scale=scale,
            min_transition_utility_gain=gain, transition_revision_probability=0.4,
        ).sort(["plan_id_from", "plan_id_trans"])
        assert reduced.equals(result)


def _make_possible_plan_steps(rows: dict[str, list]) -> pl.DataFrame:
    rows = {
        "demand_subgroup_id": rows.get("demand_subgroup_id", [0] * len(rows["demand_group_id"])),
        "country": rows.get("country", ["fr"] * len(rows["demand_group_id"])),
        "time_seq_id": rows.get("time_seq_id", [0] * len(rows["demand_group_id"])),
        "first_seen_iteration": rows.get("first_seen_iteration", [None] * len(rows["demand_group_id"])),
        "last_seen_iteration": rows.get("last_seen_iteration", [None] * len(rows["demand_group_id"])),
        "last_active_iteration": rows.get("last_active_iteration", [None] * len(rows["demand_group_id"])),
        **rows,
    }
    return pl.DataFrame(
        rows,
        schema={
            "demand_group_id": pl.UInt32,
            "demand_subgroup_id": pl.UInt32,
            "country": pl.Utf8,
            "activity_seq_id": pl.UInt32,
            "time_seq_id": pl.UInt32,
            "dest_seq_id": pl.UInt32,
            "mode_seq_id": pl.UInt32,
            "seq_step_index": pl.UInt32,
            "activity": pl.Utf8,
            "from": pl.Int32,
            "to": pl.Int32,
            "mode": pl.Utf8,
            "duration_per_pers": pl.Float64,
            "departure_time": pl.Float64,
            "arrival_time": pl.Float64,
            "next_departure_time": pl.Float64,
            "iteration": pl.UInt32,
            "csp": pl.Utf8,
            "first_seen_iteration": pl.UInt32,
            "last_seen_iteration": pl.UInt32,
            "last_active_iteration": pl.UInt32,
            "cost": pl.Float64,
            "distance": pl.Float64,
            "time": pl.Float64,
            "mean_duration_per_pers": pl.Float64,
            "value_of_time": pl.Float64,
            "k_saturation_utility": pl.Float64,
            "min_activity_time": pl.Float64,
            "utility": pl.Float64,
        },
    )


def _make_current_plans(rows: dict[str, list]) -> pl.DataFrame:
    rows = {
        "demand_subgroup_id": rows.get("demand_subgroup_id", [0] * len(rows["demand_group_id"])),
        "time_seq_id": rows.get("time_seq_id", [0] * len(rows["demand_group_id"])),
        **rows,
    }
    return pl.DataFrame(
        rows,
        schema={
            "demand_group_id": pl.UInt32,
            "demand_subgroup_id": pl.UInt32,
            "activity_seq_id": pl.UInt32,
            "time_seq_id": pl.UInt32,
            "dest_seq_id": pl.UInt32,
            "mode_seq_id": pl.UInt32,
            "utility": pl.Float64,
            "n_persons": pl.Float64,
        },
    )


def _make_possible_plan_utility(rows: dict[str, list]) -> pl.LazyFrame:
    rows = {
        "demand_subgroup_id": rows.get("demand_subgroup_id", [0] * len(rows["demand_group_id"])),
        "time_seq_id": rows.get("time_seq_id", [0] * len(rows["demand_group_id"])),
        **rows,
    }
    return pl.DataFrame(
        rows,
        schema={
            "demand_group_id": pl.UInt32,
            "demand_subgroup_id": pl.UInt32,
            "activity_seq_id": pl.UInt32,
            "time_seq_id": pl.UInt32,
            "dest_seq_id": pl.UInt32,
            "mode_seq_id": pl.UInt32,
            "utility": pl.Float64,
        },
    ).lazy()


def _with_plan_id(
    frame: pl.DataFrame | pl.LazyFrame,
    *,
    tmp_path,
    name: str,
) -> pl.DataFrame | pl.LazyFrame:
    indexed, _ = add_plan_id(frame, previous_index=None)
    return indexed
def test_parameters_resolve_behavior_change_scope_from_active_phases():
    parameters = GroupDayTripsBehaviorChangeParameters(
        phases=[
            BehaviorChangePhase(start_step=3, scope=BehaviorChangeScope.MODE_REPLANNING),
            BehaviorChangePhase(start_step=5, scope=BehaviorChangeScope.DESTINATION_REPLANNING),
        ]
    )

    assert GroupDayTripsBehaviorChangeParameters().scope_at(1) == BehaviorChangeScope.FULL_REPLANNING
    assert parameters.scope_at(3) == BehaviorChangeScope.MODE_REPLANNING
    assert parameters.scope_at(6) == BehaviorChangeScope.DESTINATION_REPLANNING


def test_get_transition_probabilities_blocks_stay_home_in_mode_replanning(tmp_path):
    updater = PlanUpdater()
    current_plans = _make_current_plans(
        {
            "demand_group_id": [1, 1],
            "demand_subgroup_id": [0, 0],
            "activity_seq_id": [0, 10],
            "dest_seq_id": [0, 100],
            "mode_seq_id": [0, 1000],
            "utility": [0.0, 1.0],
            "n_persons": [5.0, 5.0],
        }
    )
    possible_plan_utility = _make_possible_plan_utility(
        {
            "demand_group_id": [1, 1, 1, 1],
            "activity_seq_id": [0, 10, 10, 11],
            "dest_seq_id": [0, 100, 100, 101],
            "mode_seq_id": [0, 1001, 1002, 1003],
            "utility": [10.0, 2.0, 3.0, 4.0],
        }
    )

    possible_plan_steps = _make_possible_plan_steps(
        {
            "demand_group_id": [1, 1, 1, 1],
            "activity_seq_id": [0, 10, 10, 11],
            "dest_seq_id": [0, 100, 100, 101],
            "mode_seq_id": [0, 1001, 1002, 1003],
            "seq_step_index": [0, 0, 0, 0],
            "activity": ["home", "work", "work", "other"],
            "from": [1, 1, 1, 1],
            "to": [1, 2, 2, 3],
            "mode": ["stay_home", "car", "walk", "bike"],
            "duration_per_pers": [24.0, 8.0, 8.0, 6.0],
            "departure_time": [0.0, 8.0, 8.0, 9.0],
            "arrival_time": [0.0, 9.0, 9.0, 10.0],
            "next_departure_time": [24.0, 17.0, 17.0, 15.0],
            "iteration": [0, 1, 1, 1],
            "csp": ["x", "x", "x", "x"],
            "cost": [0.0, 1.0, 1.0, 1.0],
            "distance": [0.0, 10.0, 10.0, 12.0],
            "time": [0.0, 1.0, 1.0, 1.0],
            "mean_duration_per_pers": [24.0, 8.0, 8.0, 6.0],
            "value_of_time": [0.0, 1.0, 1.0, 1.0],
            "k_saturation_utility": [1.0, 1.0, 1.0, 1.0],
            "min_activity_time": [0.0, 1.0, 1.0, 1.0],
            "utility": [10.0, 2.0, 3.0, 4.0],
        }
    )

    result = updater.get_transition_probabilities(
        current_plans=current_plans,
        possible_plan_utility=_with_plan_id(
            possible_plan_utility,
            tmp_path=tmp_path,
            name="transition_probabilities_mode_replanning_utility",
        ),
        possible_plan_steps=_with_plan_id(
            possible_plan_steps,
            tmp_path=tmp_path,
            name="transition_probabilities_mode_replanning_steps",
        ),
        behavior_change_scope=BehaviorChangeScope.MODE_REPLANNING,
        transport_zones=None,
    )

    assert result.filter(pl.col("activity_seq_id") != 10).height == 0
    assert result.filter(pl.col("dest_seq_id") != 100).height == 0
    assert result.filter(pl.col("activity_seq_id_trans") != 10).height == 0
    assert result.filter(pl.col("dest_seq_id_trans") != 100).height == 0
    assert result.filter(pl.col("activity_seq_id_trans") == 0).height == 0


def test_get_transition_probabilities_no_transitions_keeps_current_plan(tmp_path):
    updater = PlanUpdater()
    current_plans = _make_current_plans(
        {
            "demand_group_id": [1, 1],
            "demand_subgroup_id": [0, 0],
            "activity_seq_id": [0, 10],
            "time_seq_id": [0, 20],
            "dest_seq_id": [0, 100],
            "mode_seq_id": [0, 1000],
            "utility": [0.0, 1.0],
            "n_persons": [3.0, 5.0],
        }
    )
    possible_plan_utility = _make_possible_plan_utility(
        {
            "demand_group_id": [1, 1, 1, 1],
            "activity_seq_id": [0, 10, 10, 10],
            "time_seq_id": [0, 20, 20, 20],
            "dest_seq_id": [0, 100, 100, 101],
            "mode_seq_id": [0, 1000, 1001, 1002],
            "utility": [2.0, 4.0, 20.0, 30.0],
        }
    )
    possible_plan_steps = _make_possible_plan_steps(
        {
            "demand_group_id": [1, 1, 1, 1],
            "activity_seq_id": [0, 10, 10, 10],
            "time_seq_id": [0, 20, 20, 20],
            "dest_seq_id": [0, 100, 100, 101],
            "mode_seq_id": [0, 1000, 1001, 1002],
            "seq_step_index": [0, 0, 0, 0],
            "activity": ["home", "work", "work", "work"],
            "from": [1, 1, 1, 1],
            "to": [1, 2, 2, 3],
            "mode": ["stay_home", "car", "walk", "bike"],
            "duration_per_pers": [24.0, 8.0, 8.0, 8.0],
            "departure_time": [0.0, 8.0, 8.0, 8.0],
            "arrival_time": [0.0, 9.0, 9.5, 9.4],
            "next_departure_time": [24.0, 17.0, 17.0, 17.0],
            "iteration": [1, 1, 1, 1],
            "csp": ["x", "x", "x", "x"],
            "cost": [0.0, 1.0, 0.5, 0.3],
            "distance": [0.0, 10.0, 2.0, 4.0],
            "time": [0.0, 1.0, 1.5, 1.4],
            "mean_duration_per_pers": [24.0, 8.0, 8.0, 8.0],
            "value_of_time": [0.0, 1.0, 1.0, 1.0],
            "k_saturation_utility": [1.0, 1.0, 1.0, 1.0],
            "min_activity_time": [0.0, 1.0, 1.0, 1.0],
            "utility": [2.0, 4.0, 20.0, 30.0],
        }
    )

    result = updater.get_transition_probabilities(
        current_plans=current_plans,
        possible_plan_utility=_with_plan_id(
            possible_plan_utility,
            tmp_path=tmp_path,
            name="transition_probabilities_no_transitions_utility",
        ),
        possible_plan_steps=_with_plan_id(
            possible_plan_steps,
            tmp_path=tmp_path,
            name="transition_probabilities_no_transitions_steps",
        ),
        behavior_change_scope=BehaviorChangeScope.NO_TRANSITIONS,
        transport_zones=None,
        transition_revision_probability=0.5,
    )

    result = result.sort(["activity_seq_id", "time_seq_id", "dest_seq_id", "mode_seq_id"])
    result_plan_keys = result.select(
        [
            "activity_seq_id_trans",
            "time_seq_id_trans",
            "dest_seq_id_trans",
            "mode_seq_id_trans",
        ]
    ).to_dicts()

    assert result_plan_keys == [
        {
            "activity_seq_id_trans": 0,
            "time_seq_id_trans": 0,
            "dest_seq_id_trans": 0,
            "mode_seq_id_trans": 0,
        },
        {
            "activity_seq_id_trans": 10,
            "time_seq_id_trans": 20,
            "dest_seq_id_trans": 100,
            "mode_seq_id_trans": 1000,
        }
    ]
    assert result.select("utility_trans").to_series().to_list() == pytest.approx([2.0, 4.0])
    assert result.select("q_transition").to_series().to_list() == pytest.approx([1.0, 1.0])
    assert result.select("p_transition").to_series().to_list() == pytest.approx([1.0, 1.0])


def test_add_plan_id_keeps_stay_home_and_non_stay_home_states_distinct():
    plans = pl.DataFrame(
        {
            "demand_group_id": [1, 1],
            "demand_subgroup_id": [0, 0],
            "activity_seq_id": [0, 10],
            "time_seq_id": [0, 0],
            "dest_seq_id": [0, 100],
            "mode_seq_id": [0, 1000],
        },
        schema={
            "demand_group_id": pl.UInt32,
            "demand_subgroup_id": pl.UInt32,
            "activity_seq_id": pl.UInt32,
            "time_seq_id": pl.UInt32,
            "dest_seq_id": pl.UInt32,
            "mode_seq_id": pl.UInt32,
        },
    )

    result, plan_id_index = add_plan_id(plans, previous_index=None)

    assert result["plan_id"].n_unique() == 2
    assert (
        result
        .filter(pl.col("activity_seq_id") == 0)
        .select("plan_id")
        .item()
        != result
        .filter(pl.col("activity_seq_id") == 10)
        .select("plan_id")
        .item()
    )

    repeated, repeated_index = add_plan_id(plans, previous_index=None)
    assert result["plan_id"].to_list() == repeated["plan_id"].to_list()
    assert plan_id_index.to_dicts() == repeated_index.to_dicts()


class _StubPlanAsset:
    def __init__(self, frame):
        self.frame = frame

    def get(self):
        return self.frame


def _candidate_memory_inputs(arrival_time=18.6):
    destination_sequences = _StubPlanAsset(
        pl.DataFrame(
            {
                "home_zone_id": [1],
                "activity_seq_id": [11],
                "time_seq_id": [1],
                "dest_seq_id": [101],
                "seq_step_index": [1],
                "from": [1],
                "to": [3],
                "departure_time": [18.0],
                "arrival_time": [arrival_time],
                "next_departure_time": [19.0],
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
    )
    mode_sequences = _StubPlanAsset(
        pl.DataFrame(
            {
                "home_zone_id": [1],
                "utility_profile_id": [0],
                "activity_seq_id": [11],
                "time_seq_id": [1],
                "dest_seq_id": [101],
                "mode_seq_id": [1001],
                "seq_step_index": [1],
                "mode": ["bike"],
                "iteration": [5],
            },
            schema={
                "home_zone_id": pl.Int32,
                "utility_profile_id": pl.UInt32,
                "activity_seq_id": pl.UInt32,
                "time_seq_id": pl.UInt32,
                "dest_seq_id": pl.UInt32,
                "mode_seq_id": pl.UInt32,
                "seq_step_index": pl.UInt8,
                "mode": pl.Utf8,
                "iteration": pl.UInt16,
            },
        )
    )
    survey_plan_steps = pl.DataFrame(
        {
            "activity_seq_id": [11],
            "time_seq_id": [1],
            "seq_step_index": [1],
            "activity": ["shop"],
            "duration_per_pers": [1.0],
            "departure_time": [18.0],
            "arrival_time": [arrival_time],
            "next_departure_time": [19.0],
        },
        schema={
            "activity_seq_id": pl.UInt32,
            "time_seq_id": pl.UInt32,
            "seq_step_index": pl.UInt8,
            "activity": pl.Utf8,
            "duration_per_pers": pl.Float64,
            "departure_time": pl.Float64,
            "arrival_time": pl.Float64,
            "next_departure_time": pl.Float64,
        },
    )
    demand_groups = pl.DataFrame(
        {
            "demand_group_id": [1],
            "demand_subgroup_id": [0],
            "country": ["fr"],
            "csp": ["x"],
            "home_zone_id": [1],
            "allowed_activities": [["shop"]],
        },
        schema={
            "demand_group_id": pl.UInt32,
            "demand_subgroup_id": pl.UInt32,
            "country": pl.Utf8,
            "csp": pl.Utf8,
            "home_zone_id": pl.Int32,
            "allowed_activities": pl.List(pl.String),
        },
    )
    return {
        "mode_sequences": mode_sequences,
        "destination_sequences": destination_sequences,
        "survey_plan_steps": survey_plan_steps,
        "demand_groups": demand_groups,
        "profile_assignments": pl.DataFrame(
            {
                "demand_group_id": [1],
                "demand_subgroup_id": [0],
                "utility_profile_id": [0],
            },
            schema={
                "demand_group_id": pl.UInt32,
                "demand_subgroup_id": pl.UInt32,
                "utility_profile_id": pl.UInt32,
            },
        ),
        "current_plans": pl.DataFrame(
            schema={
                "demand_group_id": pl.UInt32,
                "demand_subgroup_id": pl.UInt32,
                "activity_seq_id": pl.UInt32,
                "time_seq_id": pl.UInt32,
                "dest_seq_id": pl.UInt32,
                "mode_seq_id": pl.UInt32,
            }
        ),
    }


def _previous_candidate_plan(*, activity_seq_id, mode_seq_id, last_seen, arrival_time):
    return pl.DataFrame(
        {
            "demand_group_id": [1],
            "demand_subgroup_id": [0],
            "country": ["fr"],
            "activity_seq_id": [activity_seq_id],
            "time_seq_id": [1],
            "dest_seq_id": [101],
            "mode_seq_id": [mode_seq_id],
            "seq_step_index": [1],
            "activity": ["shop"],
            "from": [1],
            "to": [3],
            "mode": ["bike"],
            "duration_per_pers": [1.0],
            "departure_time": [18.0],
            "arrival_time": [arrival_time],
            "next_departure_time": [19.0],
            "iteration": [last_seen],
            "csp": ["x"],
            "first_seen_iteration": [1],
            "last_seen_iteration": [last_seen],
            "last_active_iteration": [None],
        },
        schema={
            "demand_group_id": pl.UInt32,
            "demand_subgroup_id": pl.UInt32,
            "country": pl.Utf8,
            "activity_seq_id": pl.UInt32,
            "time_seq_id": pl.UInt32,
            "dest_seq_id": pl.UInt32,
            "mode_seq_id": pl.UInt32,
            "seq_step_index": pl.UInt8,
            "activity": pl.Utf8,
            "from": pl.UInt16,
            "to": pl.UInt16,
            "mode": pl.Utf8,
            "duration_per_pers": pl.Float64,
            "departure_time": pl.Float64,
            "arrival_time": pl.Float64,
            "next_departure_time": pl.Float64,
            "iteration": pl.UInt16,
            "csp": pl.Utf8,
            "first_seen_iteration": pl.UInt16,
            "last_seen_iteration": pl.UInt16,
            "last_active_iteration": pl.UInt16,
        },
    )


def _build_candidate_memory(previous_candidates, *, iteration=5):
    inputs = _candidate_memory_inputs()
    return CandidatePlanStepsAsset.build_candidate_memory(
        **inputs,
        previous_candidate_plan_steps=previous_candidates,
        current_iteration=iteration,
        n_warmup_steps=1,
        max_inactive_age=2,
    ).collect()


def test_candidate_memory_ignores_persisted_stay_home_rows():
    persisted_stay_home = _previous_candidate_plan(
        activity_seq_id=0,
        mode_seq_id=0,
        last_seen=4,
        arrival_time=24.0,
    )

    result = _build_candidate_memory(persisted_stay_home)

    assert result.filter(pl.col("mode_seq_id") == 0).is_empty()
    assert result.select("activity_seq_id").to_series().to_list() == [11]


def test_candidate_memory_prunes_old_inactive_plans_after_warmup():
    old_plan = _previous_candidate_plan(
        activity_seq_id=12,
        mode_seq_id=1002,
        last_seen=1,
        arrival_time=18.5,
    )

    result = _build_candidate_memory(old_plan)

    assert result.select("activity_seq_id").to_series().to_list() == [11]


def test_candidate_memory_keeps_regenerated_inactive_plans_after_warmup():
    regenerated_plan = _previous_candidate_plan(
        activity_seq_id=11,
        mode_seq_id=1001,
        last_seen=1,
        arrival_time=18.5,
    )

    result = _build_candidate_memory(regenerated_plan)

    assert result.select("activity_seq_id").to_series().to_list() == [11]
    assert result.select("first_seen_iteration").item() == 1
    assert result.select("last_seen_iteration").item() == 5
    assert result.select("arrival_time").item() == pytest.approx(18.6)


def test_candidate_memory_keeps_occupied_and_recent_plans_at_age_boundary():
    inputs = _candidate_memory_inputs()
    previous = pl.concat([
        _previous_candidate_plan(activity_seq_id=12, mode_seq_id=1002, last_seen=2, arrival_time=18.5),
        _previous_candidate_plan(activity_seq_id=13, mode_seq_id=1003, last_seen=3, arrival_time=18.5),
        _previous_candidate_plan(activity_seq_id=14, mode_seq_id=1004, last_seen=2, arrival_time=18.5),
    ])
    inputs["current_plans"] = previous.filter(pl.col("activity_seq_id") == 14).select(
        "demand_group_id", "demand_subgroup_id", "activity_seq_id", "time_seq_id", "dest_seq_id", "mode_seq_id"
    )
    args = dict(
        **inputs, previous_candidate_plan_steps=previous, current_iteration=5,
        n_warmup_steps=1, max_inactive_age=2,
    )
    result = CandidatePlanStepsAsset.build_candidate_memory(**args).collect()
    assert set(result["activity_seq_id"]) == {11, 13, 14}
    assert result.filter(pl.col("activity_seq_id") == 14)["last_active_iteration"].item() == 5

    # Warmup keeps the same old, unoccupied plan until pruning starts.
    warmup = CandidatePlanStepsAsset.build_candidate_memory(**{**args, "n_warmup_steps": 5}).collect()
    assert set(warmup["activity_seq_id"]) == {11, 12, 13, 14}


def test_get_transition_probabilities_limits_destination_replanning_to_same_timing_profile(tmp_path):
    updater = PlanUpdater()
    current_plans = _make_current_plans(
        {
            "demand_group_id": [1],
            "activity_seq_id": [10],
            "dest_seq_id": [100],
            "mode_seq_id": [1000],
            "utility": [1.0],
            "n_persons": [5.0],
        }
    )
    possible_plan_utility = _make_possible_plan_utility(
        {
            "demand_group_id": [1, 1, 1],
            "activity_seq_id": [10, 10, 11],
            "dest_seq_id": [100, 101, 100],
            "mode_seq_id": [1000, 1001, 1002],
            "utility": [1.0, 2.0, 3.0],
        }
    )

    possible_plan_steps = _make_possible_plan_steps(
        {
            "demand_group_id": [1, 1, 1],
            "activity_seq_id": [10, 10, 11],
            "dest_seq_id": [100, 101, 100],
            "mode_seq_id": [1000, 1001, 1002],
            "seq_step_index": [0, 0, 0],
            "activity": ["work", "work", "other"],
            "from": [1, 1, 1],
            "to": [2, 3, 2],
            "mode": ["car", "car", "walk"],
            "duration_per_pers": [8.0, 8.0, 6.0],
            "departure_time": [8.0, 8.0, 9.0],
            "arrival_time": [9.0, 9.5, 10.0],
            "next_departure_time": [17.0, 17.0, 15.0],
            "iteration": [1, 1, 1],
            "csp": ["x", "x", "x"],
            "cost": [1.0, 1.0, 1.0],
            "distance": [10.0, 12.0, 8.0],
            "time": [1.0, 1.5, 1.0],
            "mean_duration_per_pers": [8.0, 8.0, 6.0],
            "value_of_time": [1.0, 1.0, 1.0],
            "k_saturation_utility": [1.0, 1.0, 1.0],
            "min_activity_time": [1.0, 1.0, 1.0],
            "utility": [1.0, 2.0, 3.0],
        }
    )

    result = updater.get_transition_probabilities(
        current_plans=current_plans,
        possible_plan_utility=_with_plan_id(
            possible_plan_utility,
            tmp_path=tmp_path,
            name="transition_probabilities_destination_replanning_utility",
        ),
        possible_plan_steps=_with_plan_id(
            possible_plan_steps,
            tmp_path=tmp_path,
            name="transition_probabilities_destination_replanning_steps",
        ),
        behavior_change_scope=BehaviorChangeScope.DESTINATION_REPLANNING,
        transport_zones=None,
    )

    assert result.select("time_seq_id_trans").unique().to_series().to_list() == [0]
    assert result.select("activity_seq_id_trans").unique().to_series().to_list() == [10]


def test_get_transition_probabilities_filters_candidates_by_distance_threshold(tmp_path):
    updater = PlanUpdater()
    updater.attach_transition_distances = lambda allowed_transitions, **kwargs: allowed_transitions.with_columns(
        distance=pl.when(pl.col("dest_seq_id_trans") == pl.col("dest_seq_id"))
        .then(0.0)
        .when(pl.col("dest_seq_id_trans") == 101)
        .then(0.05)
        .otherwise(1.0)
    )
    current_plans = _make_current_plans(
        {
            "demand_group_id": [1],
            "activity_seq_id": [10],
            "dest_seq_id": [100],
            "mode_seq_id": [1000],
            "utility": [1.0],
            "n_persons": [5.0],
        }
    )
    possible_plan_utility = _make_possible_plan_utility(
        {"demand_group_id": [1, 1, 1], "activity_seq_id": [10, 10, 10], "dest_seq_id": [100, 101, 102], "mode_seq_id": [1000, 1001, 1002], "utility": [1.0, 2.0, 3.0]},
    )

    possible_plan_steps = _make_possible_plan_steps(
        {
            "demand_group_id": [1, 1, 1],
            "activity_seq_id": [10, 10, 10],
            "dest_seq_id": [100, 101, 102],
            "mode_seq_id": [1000, 1001, 1002],
            "seq_step_index": [0, 0, 0],
            "activity": ["work", "work", "shop"],
            "from": [1, 1, 1],
            "to": [2, 3, 4],
            "mode": ["car", "bike", "walk"],
            "duration_per_pers": [8.0, 8.0, 1.0],
            "departure_time": [8.0, 8.0, 18.0],
            "arrival_time": [9.0, 9.0, 18.5],
            "next_departure_time": [17.0, 17.0, 19.0],
            "iteration": [1, 1, 1],
            "csp": ["x", "x", "x"],
            "cost": [1.0, 1.0, 1.0],
            "distance": [10.0, 12.0, 14.0],
            "time": [1.0, 1.0, 0.5],
            "mean_duration_per_pers": [8.0, 8.0, 1.0],
            "value_of_time": [1.0, 1.0, 1.0],
            "k_saturation_utility": [1.0, 1.0, 1.0],
            "min_activity_time": [1.0, 1.0, 1.0],
            "utility": [1.0, 2.0, 3.0],
        }
    )

    result = updater.get_transition_probabilities(
        current_plans=current_plans,
        possible_plan_utility=_with_plan_id(
            possible_plan_utility,
            tmp_path=tmp_path,
            name="transition_probabilities_distance_threshold_utility",
        ),
        possible_plan_steps=_with_plan_id(
            possible_plan_steps,
            tmp_path=tmp_path,
            name="transition_probabilities_distance_threshold_steps",
        ),
        behavior_change_scope=BehaviorChangeScope.FULL_REPLANNING,
        transport_zones=None,
        enable_transition_distance_model=True,
        transition_distance_threshold=0.1,
    )

    assert result["mode_seq_id_trans"].sort().to_list() == [1000, 1001]
    assert abs(float(result["p_transition"].sum()) - 1.0) < 1e-9


def test_get_transition_probabilities_uses_revision_probability_for_redistribution(tmp_path):
    updater = PlanUpdater()
    updater.attach_transition_distances = lambda allowed_transitions, **kwargs: allowed_transitions.with_columns(
        distance=pl.when(pl.col("dest_seq_id_trans") == pl.col("dest_seq_id"))
        .then(0.0)
        .otherwise(1.0)
    )
    current_plans = _make_current_plans(
        {
            "demand_group_id": [1],
            "activity_seq_id": [10],
            "dest_seq_id": [100],
            "mode_seq_id": [1000],
            "utility": [1.0],
            "n_persons": [5.0],
        }
    )
    possible_plan_utility = _make_possible_plan_utility(
        {
            "demand_group_id": [1, 1],
            "activity_seq_id": [10, 10],
            "dest_seq_id": [100, 101],
            "mode_seq_id": [1000, 1001],
            "utility": [1.0, 2.0],
        }
    )

    possible_plan_steps = _make_possible_plan_steps(
        {
            "demand_group_id": [1, 1],
            "activity_seq_id": [10, 10],
            "dest_seq_id": [100, 101],
            "mode_seq_id": [1000, 1001],
            "seq_step_index": [0, 0],
            "activity": ["work", "work"],
            "from": [1, 1],
            "to": [2, 3],
            "mode": ["car", "bike"],
            "duration_per_pers": [8.0, 8.0],
            "departure_time": [8.0, 8.0],
            "arrival_time": [9.0, 9.0],
            "next_departure_time": [17.0, 17.0],
            "iteration": [1, 1],
            "csp": ["x", "x"],
            "cost": [1.0, 1.0],
            "distance": [10.0, 12.0],
            "time": [1.0, 1.0],
            "mean_duration_per_pers": [8.0, 8.0],
            "value_of_time": [1.0, 1.0],
            "k_saturation_utility": [1.0, 1.0],
            "min_activity_time": [1.0, 1.0],
            "utility": [1.0, 2.0],
        }
    )

    result = updater.get_transition_probabilities(
        current_plans=current_plans,
        possible_plan_utility=_with_plan_id(
            possible_plan_utility,
            tmp_path=tmp_path,
            name="transition_probabilities_revision_probability_utility",
        ),
        possible_plan_steps=_with_plan_id(
            possible_plan_steps,
            tmp_path=tmp_path,
            name="transition_probabilities_revision_probability_steps",
        ),
        behavior_change_scope=BehaviorChangeScope.FULL_REPLANNING,
        transport_zones=None,
        enable_transition_distance_model=True,
        transition_distance_threshold=10.0,
        transition_revision_probability=0.25,
    )

    q_self = result.filter(pl.col("mode_seq_id_trans") == 1000)["q_transition"][0]
    q_switch = result.filter(pl.col("mode_seq_id_trans") == 1001)["q_transition"][0]
    p_self = result.filter(pl.col("mode_seq_id_trans") == 1000)["p_transition"][0]
    p_switch = result.filter(pl.col("mode_seq_id_trans") == 1001)["p_transition"][0]

    assert abs(float(q_self + q_switch) - 1.0) < 1e-9
    assert abs(float(p_switch) - float(0.25 * q_switch)) < 1e-9
    assert abs(float(p_self) - float(0.75 + 0.25 * q_self)) < 1e-9
    assert abs(float(p_self + p_switch) - 1.0) < 1e-9


def test_get_transition_probabilities_scales_pruning_window_with_transition_logit_scale(tmp_path):
    updater = PlanUpdater()
    current_plans = _make_current_plans(
        {
            "demand_group_id": [1],
            "activity_seq_id": [10],
            "dest_seq_id": [100],
            "mode_seq_id": [1000],
            "utility": [4.0],
            "n_persons": [5.0],
        }
    )
    possible_plan_utility = _make_possible_plan_utility(
        {
            "demand_group_id": [1, 1, 1],
            "activity_seq_id": [10, 10, 10],
            "dest_seq_id": [100, 101, 102],
            "mode_seq_id": [1000, 1001, 1002],
            "utility": [4.0, 4.0, 10.0],
        }
    )

    possible_plan_steps = _make_possible_plan_steps(
        {
            "demand_group_id": [1, 1, 1],
            "activity_seq_id": [10, 10, 10],
            "dest_seq_id": [100, 101, 102],
            "mode_seq_id": [1000, 1001, 1002],
            "seq_step_index": [0, 0, 0],
            "activity": ["work", "work", "work"],
            "from": [1, 1, 1],
            "to": [2, 3, 4],
            "mode": ["car", "bike", "walk"],
            "duration_per_pers": [8.0, 8.0, 8.0],
            "departure_time": [8.0, 8.0, 8.0],
            "arrival_time": [9.0, 9.0, 9.0],
            "next_departure_time": [17.0, 17.0, 17.0],
            "iteration": [1, 1, 1],
            "csp": ["x", "x", "x"],
            "cost": [1.0, 1.0, 1.0],
            "distance": [10.0, 12.0, 8.0],
            "time": [1.0, 1.0, 1.0],
            "mean_duration_per_pers": [8.0, 8.0, 8.0],
            "value_of_time": [1.0, 1.0, 1.0],
            "k_saturation_utility": [1.0, 1.0, 1.0],
            "min_activity_time": [1.0, 1.0, 1.0],
            "utility": [4.0, 4.0, 10.0],
        }
    )

    result_default = updater.get_transition_probabilities(
        current_plans=current_plans,
        possible_plan_utility=_with_plan_id(
            possible_plan_utility,
            tmp_path=tmp_path,
            name="transition_probabilities_pruning_default_utility",
        ),
        possible_plan_steps=_with_plan_id(
            possible_plan_steps,
            tmp_path=tmp_path,
            name="transition_probabilities_pruning_default_steps",
        ),
        behavior_change_scope=BehaviorChangeScope.FULL_REPLANNING,
        transport_zones=None,
    )
    result_scaled = updater.get_transition_probabilities(
        current_plans=current_plans,
        possible_plan_utility=_with_plan_id(
            possible_plan_utility,
            tmp_path=tmp_path,
            name="transition_probabilities_pruning_scaled_utility",
        ),
        possible_plan_steps=_with_plan_id(
            possible_plan_steps,
            tmp_path=tmp_path,
            name="transition_probabilities_pruning_scaled_steps",
        ),
        behavior_change_scope=BehaviorChangeScope.FULL_REPLANNING,
        transport_zones=None,
        transition_logit_scale=0.25,
    )

    assert result_default["mode_seq_id_trans"].sort().to_list() == [1000, 1002]
    assert result_scaled["mode_seq_id_trans"].sort().to_list() == [1000, 1001, 1002]


def test_get_transition_probabilities_filters_non_self_by_minimum_utility_gain(tmp_path):
    updater = PlanUpdater()
    current_plans = _make_current_plans(
        {
            "demand_group_id": [1],
            "activity_seq_id": [10],
            "dest_seq_id": [100],
            "mode_seq_id": [1000],
            "utility": [10.0],
            "n_persons": [5.0],
        }
    )
    possible_plan_utility = _make_possible_plan_utility(
        {
            "demand_group_id": [1, 1, 1, 1, 1],
            "activity_seq_id": [10, 10, 10, 10, 10],
            "dest_seq_id": [100, 101, 102, 103, 104],
            "mode_seq_id": [1000, 1001, 1002, 1003, 1004],
            "utility": [10.0, 9.9, 10.0, 10.05, 10.2],
        }
    )
    possible_plan_steps = _make_possible_plan_steps(
        {
            "demand_group_id": [1, 1, 1, 1, 1],
            "activity_seq_id": [10, 10, 10, 10, 10],
            "dest_seq_id": [100, 101, 102, 103, 104],
            "mode_seq_id": [1000, 1001, 1002, 1003, 1004],
            "seq_step_index": [0, 0, 0, 0, 0],
            "activity": ["work", "work", "work", "work", "work"],
            "from": [1, 1, 1, 1, 1],
            "to": [2, 3, 4, 5, 6],
            "mode": ["car", "bike", "walk", "car", "bike"],
            "duration_per_pers": [8.0, 8.0, 8.0, 8.0, 8.0],
            "departure_time": [8.0, 8.0, 8.0, 8.0, 8.0],
            "arrival_time": [9.0, 9.0, 9.0, 9.0, 9.0],
            "next_departure_time": [17.0, 17.0, 17.0, 17.0, 17.0],
            "iteration": [1, 1, 1, 1, 1],
            "csp": ["x", "x", "x", "x", "x"],
            "cost": [1.0, 1.0, 1.0, 1.0, 1.0],
            "distance": [10.0, 12.0, 8.0, 11.0, 9.0],
            "time": [1.0, 1.0, 1.0, 1.0, 1.0],
            "mean_duration_per_pers": [8.0, 8.0, 8.0, 8.0, 8.0],
            "value_of_time": [1.0, 1.0, 1.0, 1.0, 1.0],
            "k_saturation_utility": [1.0, 1.0, 1.0, 1.0, 1.0],
            "min_activity_time": [1.0, 1.0, 1.0, 1.0, 1.0],
            "utility": [10.0, 9.9, 10.0, 10.05, 10.2],
        }
    )

    result_default = updater.get_transition_probabilities(
        current_plans=current_plans,
        possible_plan_utility=_with_plan_id(
            possible_plan_utility,
            tmp_path=tmp_path,
            name="transition_probabilities_min_gain_default_utility",
        ),
        possible_plan_steps=_with_plan_id(
            possible_plan_steps,
            tmp_path=tmp_path,
            name="transition_probabilities_min_gain_default_steps",
        ),
        behavior_change_scope=BehaviorChangeScope.FULL_REPLANNING,
        transport_zones=None,
    )
    result_with_gain = updater.get_transition_probabilities(
        current_plans=current_plans,
        possible_plan_utility=_with_plan_id(
            possible_plan_utility,
            tmp_path=tmp_path,
            name="transition_probabilities_min_gain_threshold_utility",
        ),
        possible_plan_steps=_with_plan_id(
            possible_plan_steps,
            tmp_path=tmp_path,
            name="transition_probabilities_min_gain_threshold_steps",
        ),
        behavior_change_scope=BehaviorChangeScope.FULL_REPLANNING,
        transport_zones=None,
        min_transition_utility_gain=0.1,
    )

    assert result_default["mode_seq_id_trans"].sort().to_list() == [1000, 1002, 1003, 1004]
    assert result_with_gain["mode_seq_id_trans"].sort().to_list() == [1000, 1004]
    assert abs(float(result_with_gain["p_transition"].sum()) - 1.0) < 1e-9


def test_get_transition_probabilities_transition_distance_friction_penalizes_far_states(tmp_path):
    updater = PlanUpdater()
    updater.attach_transition_distances = lambda allowed_transitions, **kwargs: allowed_transitions.with_columns(
        distance=pl.when(pl.col("dest_seq_id_trans") == pl.col("dest_seq_id"))
        .then(0.0)
        .when(pl.col("dest_seq_id_trans") == 101)
        .then(0.1)
        .otherwise(5.0)
    )
    current_plans = _make_current_plans(
        {
            "demand_group_id": [1],
            "activity_seq_id": [10],
            "dest_seq_id": [100],
            "mode_seq_id": [1000],
            "utility": [1.0],
            "n_persons": [5.0],
        }
    )
    possible_plan_utility = _make_possible_plan_utility(
        {
            "demand_group_id": [1, 1, 1],
            "activity_seq_id": [10, 10, 10],
            "dest_seq_id": [100, 101, 102],
            "mode_seq_id": [1000, 1001, 1002],
            "utility": [1.0, 2.0, 2.0],
        }
    )

    possible_plan_steps = _make_possible_plan_steps(
        {
            "demand_group_id": [1, 1, 1],
            "activity_seq_id": [10, 10, 10],
            "dest_seq_id": [100, 101, 102],
            "mode_seq_id": [1000, 1001, 1002],
            "seq_step_index": [0, 0, 0],
            "activity": ["work", "work", "shop"],
            "from": [1, 1, 1],
            "to": [2, 3, 4],
            "mode": ["car", "bike", "walk"],
            "duration_per_pers": [8.0, 8.0, 1.0],
            "departure_time": [8.0, 8.0, 18.0],
            "arrival_time": [9.0, 9.0, 18.5],
            "next_departure_time": [17.0, 17.0, 19.0],
            "iteration": [1, 1, 1],
            "csp": ["x", "x", "x"],
            "cost": [1.0, 1.0, 1.0],
            "distance": [10.0, 12.0, 14.0],
            "time": [1.0, 1.0, 0.5],
            "mean_duration_per_pers": [8.0, 8.0, 1.0],
            "value_of_time": [1.0, 1.0, 1.0],
            "k_saturation_utility": [1.0, 1.0, 1.0],
            "min_activity_time": [1.0, 1.0, 1.0],
            "utility": [1.0, 2.0, 2.0],
        }
    )

    result = updater.get_transition_probabilities(
        current_plans=current_plans,
        possible_plan_utility=_with_plan_id(
            possible_plan_utility,
            tmp_path=tmp_path,
            name="transition_probabilities_distance_friction_utility",
        ),
        possible_plan_steps=_with_plan_id(
            possible_plan_steps,
            tmp_path=tmp_path,
            name="transition_probabilities_distance_friction_steps",
        ),
        behavior_change_scope=BehaviorChangeScope.FULL_REPLANNING,
        transport_zones=None,
        enable_transition_distance_model=True,
        transition_distance_friction=10.0,
        transition_distance_threshold=1e9,
    )

    q_near = result.filter(pl.col("mode_seq_id_trans") == 1001)["q_transition"][0]
    q_far = result.filter(pl.col("mode_seq_id_trans") == 1002)["q_transition"][0]
    tau_far = result.filter(pl.col("mode_seq_id_trans") == 1002)["tau_transition"][0]

    assert float(q_near) > float(q_far)
    assert float(tau_far) > 0.0


def test_apply_transitions_probability_pruning_keeps_default_behavior(tmp_path):
    updater = PlanUpdater()
    plan_keys = pl.DataFrame(
        {
            "demand_group_id": [1, 1],
            "demand_subgroup_id": [0, 0],
            "activity_seq_id": [10, 11],
            "time_seq_id": [100, 101],
            "dest_seq_id": [1000, 1001],
            "mode_seq_id": [10000, 10001],
        },
        schema={
            "demand_group_id": pl.UInt32,
            "demand_subgroup_id": pl.UInt32,
            "activity_seq_id": pl.UInt32,
            "time_seq_id": pl.UInt32,
            "dest_seq_id": pl.UInt32,
            "mode_seq_id": pl.UInt32,
        },
    )
    plan_keys = _with_plan_id(
        plan_keys,
        tmp_path=tmp_path,
        name="apply_transitions_probability_pruning_default_plan_ids",
    )
    current_plans = plan_keys.head(1).with_columns(utility=pl.lit(1.0), n_persons=pl.lit(10.0))
    from_state = current_plans.row(0, named=True)
    transition_probabilities = plan_keys.with_columns(
        utility_trans=pl.Series([1.0, 2.0]),
        p_transition=pl.Series([0.8, 0.2]),
    ).select(
        [
            pl.col("plan_id").alias("plan_id_trans"),
            pl.lit(from_state["demand_group_id"], dtype=pl.UInt32).alias("demand_group_id"),
            pl.lit(from_state["demand_subgroup_id"], dtype=pl.UInt32).alias("demand_subgroup_id"),
            pl.lit(from_state["activity_seq_id"], dtype=pl.UInt32).alias("activity_seq_id"),
            pl.lit(from_state["time_seq_id"], dtype=pl.UInt32).alias("time_seq_id"),
            pl.lit(from_state["dest_seq_id"], dtype=pl.UInt32).alias("dest_seq_id"),
            pl.lit(from_state["mode_seq_id"], dtype=pl.UInt32).alias("mode_seq_id"),
            pl.col("activity_seq_id").alias("activity_seq_id_trans"),
            pl.col("time_seq_id").alias("time_seq_id_trans"),
            pl.col("dest_seq_id").alias("dest_seq_id_trans"),
            pl.col("mode_seq_id").alias("mode_seq_id_trans"),
            pl.lit(1.0).alias("utility_prev_from"),
            pl.lit(1.0).alias("utility_from_updated"),
            "utility_trans",
            pl.lit(0.0).alias("tau_transition"),
            pl.col("p_transition").alias("q_transition"),
            pl.lit(1.0).alias("adjustment_factor"),
            "p_transition",
        ]
    )

    current_plans_after, transition_events = updater.apply_transitions(
        current_plans,
        transition_probabilities,
        iteration=2,
    )

    assert current_plans_after.height == 2
    assert current_plans_after["n_persons"].sort(descending=True).to_list() == pytest.approx([8.0, 2.0])
    assert transition_events.collect()["n_persons_moved"].sum() == pytest.approx(10.0)
