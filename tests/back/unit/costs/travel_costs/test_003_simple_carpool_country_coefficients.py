from types import SimpleNamespace

import pandas as pd
import pytest

from mobility.transport.modes.carpool.simple.simple_carpool_parameters import SimpleCarpoolParameters
from mobility.transport.modes.carpool.simple.simple_carpool_travel_costs import SimpleCarpoolTravelCosts


class _FakeCarTravelCosts:
    def __init__(self):
        self.inputs = {
            "transport_zones": SimpleNamespace(
                get=lambda: pd.DataFrame(
                    {
                        "transport_zone_id": [1, 10, 2, 20],
                        "local_admin_unit_id": ["fr-1", "de-10", "de-2", "fr-20"],
                        "country": ["fr", "de", "de", "fr"],
                    }
                )
            )
        }

    def get(self):
        return pd.DataFrame(
            {
                "from": [1, 2],
                "to": [10, 20],
                "distance": [1.0, 1.0],
                "time": [1.0, 1.0],
            }
        )


def test_simple_carpool_country_coefficients_use_lookup_map(tmp_path, monkeypatch):
    monkeypatch.setenv("MOBILITY_PROJECT_DATA_FOLDER", str(tmp_path))

    class _TestSimpleCarpoolTravelCosts(SimpleCarpoolTravelCosts):
        def get(self):
            return self.compute_travel_costs()

    travel_costs = _TestSimpleCarpoolTravelCosts(
        car_travel_costs=_FakeCarTravelCosts(),
        parameters=SimpleCarpoolParameters(
            number_persons=1,
            cost_of_time_c0_short=1.0,
            cost_of_time_c0=1.0,
            cost_of_time_c1=0.0,
            country_coefficients={"fr": 1.2, "de": 1.5},
            cost_of_distance=0.0,
            cost_constant=0.0,
        ),
    )

    costs = travel_costs.compute_travel_costs()

    assert costs.loc[costs["from"] == 1, "cost"].item() == pytest.approx(3.51)
    assert costs.loc[costs["from"] == 2, "cost"].item() == pytest.approx(2.808)
