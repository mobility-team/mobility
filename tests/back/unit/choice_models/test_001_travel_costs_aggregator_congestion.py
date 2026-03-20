from types import SimpleNamespace
from unittest.mock import patch

import polars as pl
import pandas as pd
from pydantic import BaseModel

from mobility.choice_models.travel_costs_aggregator import TravelCostsAggregator
from mobility.transport_costs.od_flows_asset import VehicleODFlowsAsset


class _FakeModeParameters(BaseModel):
    name: str
    congestion: bool


def _make_mode(*, name: str, congestion: bool):
    return SimpleNamespace(
        inputs={
            "parameters": _FakeModeParameters(name=name, congestion=congestion),
            "generalized_cost": name,
        }
    )


def test_get_costs_for_next_iteration_recomputes_when_congestion_enabled():
    # Use a minimal mode stub with congestion enabled so the test only exercises
    # the aggregator decision logic, not the real routing/cost stack.
    aggregator = TravelCostsAggregator(modes=[_make_mode(name="car", congestion=True)])
    od_flows_by_mode = pl.DataFrame(
        {"from": [1], "to": [2], "mode": ["car"], "flow_volume": [10.0]}
    )

    # Mock the expensive side effects and the final cost lookup: this test is a
    # regression guard for "did we trigger congestion recomputation at all?".
    with patch.object(aggregator, "recompute_congested_costs") as recompute_mock:
        with patch.object(aggregator, "get", return_value="costs") as get_mock:
            result = aggregator.get_costs_for_next_iteration(
                iteration=1,
                cost_update_interval=1,
                od_flows_by_mode=od_flows_by_mode,
                run_key="run-key",
                is_weekday=True,
            )

    # If congestion is enabled and the update interval matches, the aggregator
    # must rebuild congested costs before returning the next iteration cost view.
    recompute_mock.assert_called_once_with(
        od_flows_by_mode,
        run_key="run-key",
        is_weekday=True,
        iteration=1,
    )
    get_mock.assert_called_once_with(congestion=True)
    assert result == "costs"


def test_vehicle_od_flow_snapshot_hash_differs_between_weekday_and_weekend():
    flows = pd.DataFrame({"from": [1], "to": [2], "vehicle_volume": [10.0]})

    weekday_asset = VehicleODFlowsAsset(
        flows,
        run_key="run-key",
        is_weekday=True,
        iteration=1,
        mode_name="car",
    )
    weekend_asset = VehicleODFlowsAsset(
        flows,
        run_key="run-key",
        is_weekday=False,
        iteration=1,
        mode_name="car",
    )

    assert weekday_asset.inputs_hash != weekend_asset.inputs_hash
    assert weekday_asset.cache_path != weekend_asset.cache_path
