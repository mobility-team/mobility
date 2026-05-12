from types import SimpleNamespace

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
