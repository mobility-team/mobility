from types import SimpleNamespace

import math
import pandas as pd
import polars as pl
import pytest

from mobility.trips.group_day_trips.plans.plan_ids import add_plan_id
from mobility.trips.group_day_trips.plans.plan_updater import PlanUpdater


@pytest.mark.parametrize("distance_model", [False, True])
def test_plan_steps_are_collected_when_transitions_need_them(monkeypatch, distance_model):
    """Keep the large step table out of transitions unless distances need it."""
    updater = PlanUpdater()
    steps = pl.DataFrame({"value": [1]}).lazy()
    plans = pl.DataFrame({"n_persons": [1.0]})
    evaluation = SimpleNamespace(
        candidate_plan_steps=pl.DataFrame(), possible_plan_steps=steps,
        possible_plan_utility=pl.DataFrame({"time_seq_id": [1]}),
        time_sequence_steps=pl.DataFrame({"time_seq_id": [1], "reference_time_seq_id": [1]}),
        plan_id_index=pl.DataFrame(), current_plans=plans,
    )
    settings = SimpleNamespace(
        enable_transition_distance_model=distance_model, transition_distance_threshold=1.0,
        transition_revision_probability=1.0, transition_logit_scale=1.0,
        transition_utility_pruning_delta=3.0, min_transition_utility_gain=0.0,
        transition_distance_friction=0.0, plan_embedding_dimension_weights=None,
        plan_probability_pruning_retained_share=1.0, plan_probability_pruning_min_step=1,
    )
    parameters = SimpleNamespace(
        plan_update=settings, behavior_change=SimpleNamespace(scope_at=lambda _: None),
        outputs=SimpleNamespace(cache_step_events=False),
    )
    collected = []
    collect = pl.LazyFrame.collect

    def record_collect(frame, *args, **kwargs):
        if frame is steps:
            collected.append(True)
        return collect(frame, *args, **kwargs)

    def transitions(*args, **kwargs):
        assert bool(collected) is distance_model
        return pl.DataFrame()

    def apply(current, *args, **kwargs):
        assert bool(collected) is distance_model
        return current, None

    monkeypatch.setattr(pl.LazyFrame, "collect", record_collect)
    monkeypatch.setattr(updater, "get_transition_probabilities", transitions)
    monkeypatch.setattr(updater, "apply_transitions", apply)
    monkeypatch.setattr(updater, "get_current_plan_steps", lambda *args: pl.DataFrame())
    updater.get_new_plans(
        current_plans=plans, current_plan_steps=None, candidate_plan_steps=None,
        demand_groups=None, survey_plan_steps=None, transport_costs=None,
        destination_saturation=None, activity_dur=None, iteration=1,
        resolved_activity_parameters={}, arrival_time_rigidity_by_activity={},
        destination_sequences=None, mode_sequences=None, home_night_dur=None,
        stay_home_plan=None, transport_zones=None, previous_plan_id_index=None,
        parameters=parameters, precomputed_evaluation=evaluation,
    )
    assert len(collected) == 1


@pytest.mark.parametrize("unit_count", [1, 3])
def test_plan_updater_prepares_destination_country_value_coefficient(unit_count):
    updater = PlanUpdater()
    candidates = pl.DataFrame(
        {
            "demand_group_id": [1, 1],
            "demand_subgroup_id": [0, 0],
            "country": ["fr", "fr"],
            "activity_seq_id": [10, 10],
            "time_seq_id": [1, 1],
            "dest_seq_id": [100, 101],
            "mode_seq_id": [1000, 1001],
            "seq_step_index": [0, 0],
            "activity": ["work", "work"],
            "from": [1, 1],
            "to": [10, 20],
            "mode": ["car", "car"],
            "duration_per_pers": [8.0, 8.0],
            "departure_time": [8.0, 8.0],
            "arrival_time": [9.0, 9.0],
            "next_departure_time": [17.0, 17.0],
            "iteration": [1, 1],
            "csp": ["x", "x"],
            "first_seen_iteration": [None, None],
            "last_seen_iteration": [None, None],
            "last_active_iteration": [None, None],
        }
    ).with_columns(mode=pl.col("mode").cast(pl.Enum(["car", "stay_home"]))).lazy()
    activity_dur = pl.DataFrame(
        {
            "country": ["fr"],
            "csp": ["x"],
            "activity": ["work"],
            "mean_duration_per_pers": [8.0],
        }
    ).with_columns(activity=pl.col("activity").cast(pl.Enum(["work"])))
    destination_saturation = pl.DataFrame(
        {
            "to": [10, 20],
            "activity": ["work", "work"],
            "k_saturation_utility": [1.0, 1.0],
        }
    ).with_columns(activity=pl.col("activity").cast(pl.Enum(["work"])))

    class _StubTransportCosts:
        def __init__(self):
            self.modes = [SimpleNamespace(inputs={"parameters": SimpleNamespace(name="car")})]

        def get_costs_by_od_and_mode(self, columns, detail_distances=False):
            return pl.DataFrame(
                {
                    "from": [1, 1],
                    "to": [10, 20],
                    "mode": ["car", "car"],
                    "cost": [1.0, 1.0],
                    "distance": [10.0, 10.0],
                    "time": [1.0, 1.0],
                }
            )

    transport_zones = SimpleNamespace(
        get=lambda: pd.DataFrame(
            {
                "transport_zone_id": [10, 20],
                "local_admin_unit_id": ["fr001", "ch001"],
                "country": ["fr", "ch"],
            }
        )
    )
    resolved_activity_parameters = {
        "work": SimpleNamespace(value_of_time=1.0, country_value_coefficients={"fr": 1.0, "ch": 2.0}),
    }

    # Units share plan content and utility inputs, but retain their own memory
    # dates when compact scores are attached back to the population.
    base_candidates = candidates.collect()
    candidates = pl.concat([
        base_candidates.with_columns(
            demand_group_id=pl.lit(unit, dtype=pl.Int64),
            last_seen_iteration=pl.lit(unit, dtype=pl.UInt16),
        )
        for unit in range(1, unit_count + 1)
    ]).lazy()
    result = updater.compute_plan_steps_candidates_utility(
        candidates=candidates,
        transport_costs=_StubTransportCosts(),
        destination_saturation=destination_saturation,
        activity_dur=activity_dur,
        transport_zones=transport_zones,
        resolved_activity_parameters=resolved_activity_parameters,
        min_activity_time_constant=1.0,
        allow_missing_costs_for_current_plans=False,
    ).collect()

    coefficients_by_destination = dict(
        zip(result["to"].to_list(), result["activity_utility_scale"].to_list(), strict=True)
    )
    assert coefficients_by_destination[10] == pytest.approx(8.0)
    assert coefficients_by_destination[20] == pytest.approx(16.0)
    assert "last_seen_iteration" in result.columns
    assert result.height == 2 * unit_count
    assert result.filter(pl.col("last_seen_iteration") != pl.col("demand_group_id")).is_empty()


def test_plan_updater_integrates_shadow_price_in_activity_value_when_enabled():
    updater = PlanUpdater()
    candidates = pl.DataFrame(
        {
            "demand_group_id": [1],
            "demand_subgroup_id": [0],
            "country": ["fr"],
            "activity_seq_id": [10],
            "time_seq_id": [1],
            "dest_seq_id": [100],
            "mode_seq_id": [1000],
            "seq_step_index": [0],
            "activity": ["work"],
            "from": [1],
            "to": [10],
            "mode": ["car"],
            "duration_per_pers": [8.0],
            "departure_time": [8.0],
            "arrival_time": [9.0],
            "next_departure_time": [17.0],
            "iteration": [1],
            "csp": ["x"],
            "first_seen_iteration": [None],
            "last_seen_iteration": [None],
            "last_active_iteration": [None],
        }
    ).with_columns(mode=pl.col("mode").cast(pl.Enum(["car", "stay_home"]))).lazy()
    activity_dur = pl.DataFrame(
        {
            "country": ["fr"],
            "csp": ["x"],
            "activity": ["work"],
            "mean_duration_per_pers": [8.0],
        }
    ).with_columns(activity=pl.col("activity").cast(pl.Enum(["work"])))
    destination_saturation = pl.DataFrame(
        {
            "to": [10],
            "activity": ["work"],
            "k_saturation_utility": [0.25],
            "destination_shadow_price": [-2.0],
        }
    ).with_columns(activity=pl.col("activity").cast(pl.Enum(["work"])))

    class _StubTransportCosts:
        def __init__(self):
            self.modes = [SimpleNamespace(inputs={"parameters": SimpleNamespace(name="car")})]

        def get_costs_by_od_and_mode(self, columns, detail_distances=False):
            return pl.DataFrame(
                {
                    "from": [1],
                    "to": [10],
                    "mode": ["car"],
                    "cost": [1.0],
                    "distance": [10.0],
                    "time": [1.0],
                }
            )

    transport_zones = SimpleNamespace(
        get=lambda: pd.DataFrame(
            {
                "transport_zone_id": [10],
                "local_admin_unit_id": ["fr001"],
                "country": ["fr"],
            }
        )
    )
    resolved_activity_parameters = {
        "work": SimpleNamespace(value_of_time=1.0, country_value_coefficients={"fr": 1.0}),
    }

    result = updater.compute_plan_steps_candidates_utility(
        candidates=candidates,
        transport_costs=_StubTransportCosts(),
        destination_saturation=destination_saturation,
        activity_dur=activity_dur,
        transport_zones=transport_zones,
        resolved_activity_parameters=resolved_activity_parameters,
        min_activity_time_constant=1.0,
        allow_missing_costs_for_current_plans=False,
        use_destination_shadow_prices=True,
    ).collect()

    assert result["activity_utility_scale"].item() == pytest.approx(-8.0)
    assert result["destination_shadow_price"].item() == pytest.approx(-2.0)


def test_destination_saturation_computes_shadow_price_above_soft_capacity():
    updater = PlanUpdater()
    current_plan_steps = pl.DataFrame(
        {
            "activity_seq_id": [10, 10],
            "activity": ["work", "work"],
            "to": [1, 2],
            "duration": [250.0, 100.0],
        }
    )
    opportunities = pl.DataFrame(
        {
            "activity": ["work", "work"],
            "to": [1, 2],
            "opportunity_capacity": [100.0, 100.0],
        }
    ).with_columns(activity=pl.col("activity").cast(pl.Enum(["work"])))
    resolved_activity_parameters = {
        "work": SimpleNamespace(
            value_of_time=10.0,
            saturation_fun_beta=4.0,
            saturation_fun_ref_level=1.5,
            destination_soft_capacity_factor=1.25,
            destination_shadow_price_sensitivity_coefficient=0.1,
            destination_shadow_price_min_coefficient=-0.5,
            destination_sampling_overload_gamma=1.0,
            destination_sampling_min_attraction_factor=0.05,
        )
    }

    result = updater.get_destination_saturation(
        current_plan_steps=current_plan_steps,
        opportunities=opportunities,
        resolved_activity_parameters=resolved_activity_parameters,
    ).sort("to")

    overloaded = result.row(0, named=True)
    below_soft_capacity = result.row(1, named=True)

    assert overloaded["capacity_ratio"] == pytest.approx(2.5)
    assert overloaded["destination_shadow_price"] == pytest.approx(-math.log(2.0))
    assert overloaded["destination_sampling_attraction_factor"] == pytest.approx(0.5)
    assert below_soft_capacity["destination_shadow_price"] == pytest.approx(0.0)
    assert below_soft_capacity["destination_sampling_attraction_factor"] == pytest.approx(1.0)


def test_destination_occupation_is_stable_when_step_rows_are_reordered():
    """Small durations must not disappear beside a large Float32 duration."""
    durations = [100_000_000.0] + [1.0] * 1000
    steps = pl.DataFrame({
        "activity_seq_id": [1] * len(durations),
        "activity": ["work"] * len(durations),
        "to": [1] * len(durations),
        "duration": pl.Series(durations, dtype=pl.Float32),
    })
    opportunities = pl.DataFrame({
        "activity": ["work"], "to": [1], "opportunity_capacity": [100_000_000.0],
    }).with_columns(activity=pl.col("activity").cast(pl.Enum(["work"])))
    parameters = {"work": SimpleNamespace(
        value_of_time=10.0, saturation_fun_beta=4.0, saturation_fun_ref_level=1.5,
        destination_soft_capacity_factor=1.25,
        destination_shadow_price_sensitivity_coefficient=0.1,
        destination_shadow_price_min_coefficient=-0.5,
        destination_sampling_overload_gamma=1.0,
        destination_sampling_min_attraction_factor=0.05,
    )}
    updater = PlanUpdater()
    forward = updater.get_destination_saturation(steps, opportunities, parameters)
    backward = updater.get_destination_saturation(steps.reverse(), opportunities, parameters)

    assert forward["opportunity_occupation"].dtype == pl.Float64
    assert forward["opportunity_occupation"].item() == 100_001_000.0
    assert forward.equals(backward)


def test_possible_plan_utility_subtracts_travel_time_from_home_night(tmp_path):
    """Check that plan utility does not count travel time as home/night time."""
    updater = PlanUpdater()
    possible_plan_steps, plan_id_index = add_plan_id(
        pl.DataFrame(
            {
                "demand_group_id": [1],
                "demand_subgroup_id": [0],
                "country": ["fr"],
                "activity_seq_id": [10],
                "time_seq_id": [100],
                "dest_seq_id": [1000],
                "mode_seq_id": [10000],
                "csp": ["worker"],
                "duration_per_pers": [8.0],
                "time": [2.0],
                "utility": [5.0],
                # These steps have complete, valid travel costs.
                "is_feasible": [True],
                "_missing_cost_row": [False],
                "_invalid_cost_row": [False],
            }
        ),
        previous_index=None,
    )
    possible_plan_steps = possible_plan_steps.lazy()
    home_night_dur = pl.DataFrame(
        {
            "country": ["fr"],
            "csp": ["worker"],
            "mean_home_night_per_pers": [10.0],
        }
    )
    stay_home_plan = pl.DataFrame(
        {
            "demand_group_id": [1],
            "demand_subgroup_id": [0],
            "activity_seq_id": [0],
            "time_seq_id": [0],
            "mode_seq_id": [0],
            "dest_seq_id": [0],
            "mean_home_night_per_pers": [10.0],
        }
    )

    result, _ = updater.get_possible_plan_utility(
        possible_plan_steps,
        home_night_dur,
        value_of_time_stay_home=1.0,
        stay_home_plan=stay_home_plan,
        min_activity_time_constant=1.0,
        plan_id_index=plan_id_index,
    )
    result = result.collect()

    mobile_utility = result.filter(pl.col("mode_seq_id") != 0)["utility"].item()
    expected_home_night_utility = 10.0 * math.log(14.0 / (10.0 * math.exp(-1.0)))
    assert mobile_utility == pytest.approx(5.0 + expected_home_night_utility)


def test_home_night_utility_sums_float32_steps_in_float64():
    """Small rounding errors in step times must not be amplified by utility."""
    steps = pl.DataFrame({
        "demand_group_id": [1] * 3, "demand_subgroup_id": [0] * 3,
        "country": ["fr"] * 3, "csp": ["worker"] * 3,
        "activity_seq_id": [10] * 3, "time_seq_id": [100] * 3,
        "dest_seq_id": [1000] * 3, "mode_seq_id": [10000] * 3,
        "duration_per_pers": pl.Series([7.1, 1.3, 0.17], dtype=pl.Float32),
        "time": pl.Series([0.1234567, 0.9876543, 0.14567], dtype=pl.Float32),
        "utility": [0.0] * 3, "is_feasible": [True] * 3,
        "_missing_cost_row": [False] * 3, "_invalid_cost_row": [False] * 3,
    })
    steps, index = add_plan_id(steps, previous_index=None)
    home = pl.DataFrame({"country": ["fr"], "csp": ["worker"], "mean_home_night_per_pers": [10.0]})
    stay_home = pl.DataFrame({
        "demand_group_id": [1], "demand_subgroup_id": [0], "activity_seq_id": [0],
        "time_seq_id": [0], "mode_seq_id": [0], "dest_seq_id": [0],
        "mean_home_night_per_pers": [10.0],
    })
    night = 24.0 - steps["duration_per_pers"].cast(pl.Float64).sum() - steps["time"].cast(pl.Float64).sum()
    expected = 10.0 * math.log(night / (10.0 * math.exp(-1.0)))
    for rows in (steps, steps.reverse()):
        result, _ = PlanUpdater().get_possible_plan_utility(
            rows.lazy(), home, value_of_time_stay_home=1.0, stay_home_plan=stay_home,
            min_activity_time_constant=1.0, plan_id_index=index,
        )
        value = result.collect().filter(pl.col("mode_seq_id") != 0)["utility"].item()
        assert value == pytest.approx(expected, rel=0.0, abs=1e-12)


def test_plan_utility_is_stable_when_step_rows_are_reordered():
    steps = pl.DataFrame({
        "demand_group_id": [1] * 4, "demand_subgroup_id": [0] * 4,
        "country": ["fr"] * 4, "csp": ["worker"] * 4,
        "activity_seq_id": [10] * 4, "time_seq_id": [100] * 4,
        "dest_seq_id": [1000] * 4, "mode_seq_id": [10000] * 4,
        "duration_per_pers": [1.0] * 4, "time": [0.25] * 4,
        "utility": [1e12, 0.0001, 0.0001, 0.0002], "is_feasible": [True] * 4,
        "_missing_cost_row": [False] * 4, "_invalid_cost_row": [False] * 4,
    })
    steps, index = add_plan_id(steps, previous_index=None)
    home = pl.DataFrame({"country": ["fr"], "csp": ["worker"], "mean_home_night_per_pers": [10.0]})
    stay_home = pl.DataFrame({
        "demand_group_id": [1], "demand_subgroup_id": [0], "activity_seq_id": [0],
        "time_seq_id": [0], "mode_seq_id": [0], "dest_seq_id": [0],
        "mean_home_night_per_pers": [10.0],
    })

    values = []
    for rows in (steps, steps.reverse()):
        result, _ = PlanUpdater().get_possible_plan_utility(
            rows.lazy(), home, value_of_time_stay_home=1.0, stay_home_plan=stay_home,
            min_activity_time_constant=1.0, plan_id_index=index,
        )
        values.append(result.collect().filter(pl.col("mode_seq_id") != 0)["utility"].item())
    assert values[0] == values[1]


def test_add_stay_home_plan_steps_preserves_candidate_retention_columns(tmp_path):
    updater = PlanUpdater()
    possible_plan_steps, plan_id_index = add_plan_id(
        pl.DataFrame(
            {
                "demand_group_id": [1],
                "demand_subgroup_id": [0],
                "country": ["fr"],
                "csp": ["worker"],
                "activity_seq_id": [10],
                "time_seq_id": [100],
                "dest_seq_id": [1000],
                "mode_seq_id": [10000],
                "seq_step_index": [1],
                "activity": ["work"],
                "from": [1],
                "to": [2],
                "mode": ["car"],
                "duration_per_pers": [8.0],
                "departure_time": [8.0],
                "arrival_time": [9.0],
                "next_departure_time": [17.0],
                "iteration": [2],
                "cost": [1.0],
                "distance": [10.0],
                "time": [1.0],
                "mean_duration_per_pers": [8.0],
                "value_of_time": [1.0],
                "k_saturation_utility": [1.0],
                "min_activity_time": [1.0],
                "first_seen_iteration": [2],
                "last_seen_iteration": [2],
                "last_active_iteration": [None],
                "utility": [5.0],
            }
        ),
        previous_index=None,
    )
    possible_plan_steps = possible_plan_steps.lazy()
    stay_home_plan = pl.DataFrame(
        {
            "demand_group_id": [1],
            "demand_subgroup_id": [0],
            "country": ["fr"],
            "csp": ["worker"],
            "mean_home_night_per_pers": [10.0],
            "iteration": [0],
            "activity_seq_id": [0],
            "time_seq_id": [0],
            "mode_seq_id": [0],
            "dest_seq_id": [0],
            "seq_step_index": [0],
            "activity": ["home"],
            "from": [1],
            "to": [1],
            "mode": ["stay_home"],
            "duration_per_pers": [24.0],
            "departure_time": [0.0],
            "arrival_time": [0.0],
            "next_departure_time": [24.0],
            "utility": [1.0],
            "n_persons": [1.0],
        }
    )

    result, _ = updater.add_stay_home_plan_steps(
        possible_plan_steps,
        stay_home_plan,
        plan_id_index=plan_id_index,
    )
    result = result.collect()

    assert "last_seen_iteration" in result.columns
    assert result.filter(pl.col("mode_seq_id") == 0).select("last_seen_iteration").item() is None
