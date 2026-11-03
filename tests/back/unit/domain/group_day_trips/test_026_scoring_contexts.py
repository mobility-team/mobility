"""Equivalent populations share scoring without losing their original identities."""

from types import SimpleNamespace

import polars as pl
from polars.testing import assert_frame_equal
import pytest

from mobility.trips.group_day_trips.plans.scoring_contexts import ScoringContexts
from mobility.trips.group_day_trips.plans.plan_updater import PlanUpdater


def test_contexts_preserve_population_profiles_and_stable_ids():
    demand = pl.DataFrame({
        "demand_group_id": [1, 2, 3, 4, 5], "demand_subgroup_id": [0] * 5,
        "home_zone_id": [10] * 5, "country": ["FR"] * 5,
        "csp": [1, 1, 2, 1, 1], "n_persons": [20., 30., 40., 50., 60.],
        "allowed_activities": [["work", "home"], ["home", "work"],
                               ["home", "work"], ["home", "work"], ["home"]],
    })
    profiles = demand.select("demand_group_id", "demand_subgroup_id").with_columns(
        utility_profile_id=pl.Series([0, 0, 0, 1, 0]),
    )
    costs = SimpleNamespace(get_utility_profiles=lambda groups, segments: (profiles, {}))
    contexts = ScoringContexts.build(demand, costs, None)
    assert contexts.demand_groups.height == 4
    assert contexts.demand_groups["n_persons"].sum() == 200.
    shared = contexts.unit_map.filter(pl.col("demand_group_id").is_in([1, 2]))
    assert shared["_score_context_id"].n_unique() == 1
    reversed_contexts = ScoringContexts.build(demand.reverse(), costs, None)
    assert_frame_equal(contexts.unit_map.sort("demand_group_id"),
                       reversed_contexts.unit_map.sort("demand_group_id"))

    # The same physical alternative is stored once, with its full history.
    steps = demand.head(2).select("demand_group_id", "demand_subgroup_id").with_columns(
        activity_seq_id=pl.lit(1), time_seq_id=pl.lit(2),
        dest_seq_id=pl.lit(3), mode_seq_id=pl.lit(4), seq_step_index=pl.lit(0),
        first_seen_iteration=pl.Series([1, 2]), last_seen_iteration=pl.Series([2, 3]),
        last_active_iteration=pl.Series([2, 1]),
    )
    compact = contexts.collapse_step_rows(steps.lazy())
    assert compact.height == 1
    assert compact["first_seen_iteration"].item() == 1
    assert compact["last_seen_iteration"].item() == 3
    assert compact["last_active_iteration"].item() == 2
    expanded = contexts.expand_step_rows(compact).collect()
    assert expanded["demand_group_id"].sort().to_list() == [1, 2]

    allocation = steps.drop("seq_step_index", "first_seen_iteration", "last_seen_iteration",
                            "last_active_iteration").with_columns(
        n_persons=pl.Series([20., 30.]), utility=pl.lit(5.),
    )
    assert contexts.collapse_allocations(allocation)["n_persons"].item() == 50.


def test_compact_mnl_preserves_population_weighted_flows(monkeypatch):
    # Two equivalent populations contribute to the same physical OD choices.
    demand = pl.DataFrame({
        "demand_group_id": [1, 2], "demand_subgroup_id": [0, 0],
        "n_persons": [20., 30.],
    })
    compact_demand = pl.DataFrame({
        "demand_group_id": [0], "demand_subgroup_id": [0], "n_persons": [50.],
    })
    contexts = ScoringContexts(compact_demand, demand.select(
        "demand_group_id", "demand_subgroup_id",
    ).with_columns(_score_context_id=pl.lit(0)))
    choices = demand.select("demand_group_id", "demand_subgroup_id").join(
        pl.DataFrame({"dest_seq_id": [1, 2], "utility": [1., 2.]}), how="cross",
    ).with_columns(
        activity_seq_id=pl.lit(1), time_seq_id=pl.lit(1), mode_seq_id=pl.lit(1),
        is_feasible=pl.lit(True),
    ).with_row_index("plan_id")
    compact_choices = choices.filter(pl.col("demand_group_id") == 1).with_columns(
        demand_group_id=pl.lit(0),
    )
    updater = PlanUpdater()
    monkeypatch.setattr(updater, "get_current_plan_steps", lambda allocation, steps: pl.DataFrame())
    def allocate(rows, scoring_contexts):
        evaluation = SimpleNamespace(
            possible_plan_utility=rows, possible_plan_steps=pl.DataFrame(),
            scoring_contexts=scoring_contexts,
        )
        allocation, _ = updater.estimate_mnl_allocation(evaluation, demand, logit_scale=0.25)
        return allocation.group_by("dest_seq_id").agg(pl.col("n_persons").sum()).sort("dest_seq_id")
    expanded = allocate(choices, None)
    compact = allocate(compact_choices, contexts)
    assert compact["n_persons"].sum() == pytest.approx(50.)
    assert_frame_equal(expanded, compact)
