from types import SimpleNamespace
from pathlib import Path
from uuid import uuid4
from unittest.mock import patch

import polars as pl
import pandas as pd
from pydantic import BaseModel

from mobility.transport.costs.congestion_state_manager import CongestionStateManager
from mobility.transport.costs.od_flows_asset import VehicleODFlowsAsset
from mobility.transport.costs.transport_costs import TransportCosts


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


def _make_project_data_folder() -> Path:
    root = Path(".pytest-local-tmp") / "project-data"
    root.mkdir(parents=True, exist_ok=True)
    path = root / f"mobility-tests-{uuid4().hex}"
    path.mkdir()
    return path


def test_get_costs_for_next_iteration_recomputes_when_congestion_enabled(monkeypatch):
    monkeypatch.setenv("MOBILITY_PROJECT_DATA_FOLDER", str(_make_project_data_folder()))

    # Use a minimal mode stub with congestion enabled so the test only exercises
    # the aggregator decision logic, not the real routing/cost stack.
    aggregator = TransportCosts(modes=[_make_mode(name="car", congestion=True)])
    od_flows_by_mode = pl.DataFrame(
        {"from": [1], "to": [2], "mode": ["car"], "flow_volume": [10.0]}
    )

    congestion_state = object()

    # Mock the expensive side effects and the final cost lookup: this test is a
    # regression guard for "did we trigger congestion recomputation at all?".
    with patch.object(aggregator, "build_congestion_state", return_value=congestion_state) as build_mock:
        with patch.object(aggregator, "asset_for_congestion_state", return_value=SimpleNamespace(
            get_costs_by_od=lambda metrics: "costs"
        )) as asset_mock:
            result_costs = aggregator.get_costs_for_next_iteration(
                run=SimpleNamespace(inputs_hash="run-key", is_weekday=True),
                iteration=1,
                od_flows_by_mode=od_flows_by_mode,
            )

    # If congestion is enabled and the update interval matches, the aggregator
    # must rebuild congested costs before returning the next iteration cost view.
    build_mock.assert_called_once_with(
        od_flows_by_mode,
        run_key="run-key",
        is_weekday=True,
        iteration=1,
    )
    asset_mock.assert_called_once_with(congestion_state)
    assert result_costs == "costs"


def test_vehicle_od_flow_snapshot_hash_differs_between_weekday_and_weekend(monkeypatch):
    monkeypatch.setenv("MOBILITY_PROJECT_DATA_FOLDER", str(_make_project_data_folder()))
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


def test_congestion_state_manager_overwrites_existing_vehicle_flow_snapshot(monkeypatch):
    monkeypatch.setenv("MOBILITY_PROJECT_DATA_FOLDER", str(_make_project_data_folder()))
    manager = CongestionStateManager(SimpleNamespace())

    manager._create_vehicle_flow_snapshot(
        pl.DataFrame({"from": [1], "to": [2], "vehicle_volume": [3.0]}),
        run_key="run-key",
        is_weekday=True,
        iteration=1,
        mode_name="car",
    )
    overwritten = manager._create_vehicle_flow_snapshot(
        pl.DataFrame({"from": [1], "to": [2], "vehicle_volume": [7.0]}),
        run_key="run-key",
        is_weekday=True,
        iteration=1,
        mode_name="car",
    )

    persisted = overwritten.get_cached_asset()
    assert persisted["vehicle_volume"].tolist() == [7.0]
