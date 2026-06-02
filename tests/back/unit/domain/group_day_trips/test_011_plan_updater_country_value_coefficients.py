from types import SimpleNamespace

import math
import pandas as pd
import polars as pl
import pytest

from mobility.trips.group_day_trips.plans.plan_updater import PlanUpdater


def test_plan_updater_applies_destination_country_value_coefficient():
    updater = PlanUpdater()
    candidates = pl.DataFrame(
        {
            "demand_group_id": [1, 1],
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
            }
        )
    )
    resolved_activity_parameters = {
        "work": SimpleNamespace(value_of_time=1.0, country_value_coefficients={"fr": 1.0, "ch": 2.0}),
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
    ).collect()

    utilities_by_destination = dict(zip(result["to"].to_list(), result["utility"].to_list(), strict=True))
    assert utilities_by_destination[10] == pytest.approx(7.0)
    assert utilities_by_destination[20] == pytest.approx(15.0)


def test_plan_updater_integrates_shadow_price_in_activity_value_when_enabled():
    updater = PlanUpdater()
    candidates = pl.DataFrame(
        {
            "demand_group_id": [1],
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

    expected_utility = -8.0 * math.log(math.e) - 1.0
    assert result["utility"].item() == pytest.approx(expected_utility)
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


def test_add_stay_home_plan_steps_adds_neutral_shadow_price(tmp_path):
    updater = PlanUpdater()
    index_folder = tmp_path / "plan_index"
    index_folder.mkdir()
    possible_plan_steps = pl.DataFrame(
        {
            "demand_group_id": [1],
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
            "destination_shadow_price": [-0.5],
            "min_activity_time": [1.0],
            "first_seen_iteration": [2],
            "last_active_iteration": [None],
            "utility": [5.0],
        }
    ).lazy()
    stay_home_plan = pl.DataFrame(
        {
            "demand_group_id": [1],
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

    result = updater.add_stay_home_plan_steps(
        possible_plan_steps,
        stay_home_plan,
        sequence_index_folder=index_folder,
    ).collect()

    assert "destination_shadow_price" in result.columns
    stay_home_shadow_price = (
        result
        .filter(pl.col("mode_seq_id") == 0)
        .select("destination_shadow_price")
        .item()
    )
    assert stay_home_shadow_price == 0.0
