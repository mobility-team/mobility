"""Share fresh allocation probabilities only for identical available choices."""

import math
from types import SimpleNamespace

import polars as pl
import pytest

from mobility.trips.group_day_trips.plans.plan_ids import PLAN_KEY_COLS
from mobility.trips.group_day_trips.plans.plan_updater import PlanUpdater


@pytest.mark.parametrize("scale", [0.0, 1.0, 1000.0])
def test_shared_shares_match_individual_mnl_with_different_choice_sets(scale, monkeypatch):
    # Units 1 and 2 share choices. Unit 3 has only one of those choices;
    # unit 4 has the same plan ids but different utilities. Unit 5 has stay-home.
    choices = pl.DataFrame({
        "demand_group_id": [1, 1, 2, 2, 3, 4, 4, 5],
        "demand_subgroup_id": [0] * 8,
        "activity_seq_id": [1] * 7 + [0], "time_seq_id": [1] * 7 + [0],
        "dest_seq_id": [1, 2, 1, 2, 1, 1, 2, 0], "mode_seq_id": [1] * 7 + [0],
        "plan_id": list(range(8)),
        "utility": [1000.0, 1001.0, 1000.0, 1001.0, 1000.0, 1002.0, 1001.0, 0.0],
        "is_feasible": [True] * 8,
    })
    # An infeasible alternative must not change the denominator.
    choices = pl.concat([choices, choices.head(1).with_columns(
        plan_id=pl.lit(99, dtype=pl.Int64), dest_seq_id=pl.lit(99, dtype=pl.Int64),
        utility=pl.lit(10000.0), is_feasible=pl.lit(False),
    )])
    demand = pl.DataFrame({
        "demand_group_id": [1, 2, 3, 4, 5], "demand_subgroup_id": [0] * 5,
        "n_persons": [100.0, 30.0, 50.0, 25.0, 0.0],
    })
    updater = PlanUpdater()
    monkeypatch.setattr(updater, "get_current_plan_steps", lambda allocation, steps: pl.DataFrame())
    evaluation = SimpleNamespace(
        possible_plan_utility=choices, possible_plan_steps=pl.DataFrame(), scoring_contexts=None,
    )
    allocated, _ = updater.estimate_mnl_allocation(evaluation, demand, logit_scale=scale)
    assert 99 not in allocated["plan_id"].to_list()
    assert allocated["n_persons"].sum() == pytest.approx(demand["n_persons"].sum())
    for unit in demand.iter_rows(named=True):
        unit_choices = choices.filter(
            (pl.col("demand_group_id") == unit["demand_group_id"]) & pl.col("is_feasible")
        )
        utilities = unit_choices["utility"].to_list()
        weights = [math.exp((utility - max(utilities)) * scale) for utility in utilities]
        for choice, weight in zip(unit_choices.iter_rows(named=True), weights, strict=True):
            actual = allocated.filter(pl.col("plan_id") == choice["plan_id"])
            assert actual["n_persons"].item() == pytest.approx(unit["n_persons"] * weight / sum(weights))
            assert actual.select(PLAN_KEY_COLS).row(0) == tuple(choice[key] for key in PLAN_KEY_COLS)
