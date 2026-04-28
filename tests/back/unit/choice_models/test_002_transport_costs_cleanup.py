from pathlib import Path
from types import SimpleNamespace

import pytest

from mobility.transport.costs.congestion_state import CongestionState
from mobility.transport.costs.od_flows_asset import VehicleODFlowsAsset
from mobility.transport.costs.transport_costs import TransportCosts
from mobility.transport.costs.travel_costs_asset import TravelCostsAsset


class _FakeTravelCostsAsset(TravelCostsAsset):
    def __init__(self):
        self.inputs = {}
        self.remove_calls = []

    def remove_congestion_artifacts(self, congestion_state):
        self.remove_calls.append(congestion_state)

    def get_cached_asset(self):
        return None

    def create_and_get_asset(self):
        return None

    def get(self):
        return None


class _FakeModeParameters:
    def __init__(self, *, name: str, congestion: bool):
        self.name = name
        self.congestion = congestion


class _RemovableVariant:
    def __init__(self):
        self.remove_calls = 0

    def remove(self):
        self.remove_calls += 1


def _make_mode(*, name: str, congestion: bool, travel_costs=None):
    return SimpleNamespace(
        inputs={
            "parameters": _FakeModeParameters(name=name, congestion=congestion),
            "generalized_cost": f"{name}-costs",
            "travel_costs": travel_costs,
        }
    )


def test_remove_congestion_artifacts_removes_variant_and_delegates_to_mode_assets(
    project_dir,
    monkeypatch,
):
    travel_costs = _FakeTravelCostsAsset()
    aggregator = TransportCosts(
        modes=[
            _make_mode(name="car", congestion=True, travel_costs=travel_costs),
            _make_mode(name="walk", congestion=False, travel_costs=object()),
        ]
    )
    variant = _RemovableVariant()
    congestion_state = object()

    monkeypatch.setattr(
        aggregator,
        "asset_for_congestion_state",
        lambda state: variant,
    )

    aggregator.remove_congestion_artifacts(congestion_state)

    assert variant.remove_calls == 1
    assert travel_costs.remove_calls == [congestion_state]


@pytest.mark.parametrize(
    ("modes", "update_interval"),
    [
        ([_make_mode(name="car", congestion=False)], 1),
        ([_make_mode(name="car", congestion=True)], 0),
    ],
)
def test_iter_run_congestion_artifacts_returns_nothing_when_disabled(
    project_dir,
    modes,
    update_interval,
):
    aggregator = TransportCosts(modes=modes)
    run = SimpleNamespace(
        parameters=SimpleNamespace(n_iter_per_cost_update=update_interval, n_iterations=2),
        inputs_hash="run-key",
        is_weekday=True,
    )

    assert list(aggregator.iter_run_congestion_artifacts(run)) == []


def test_iter_run_congestion_artifacts_yields_existing_flow_assets_on_refresh_iterations(
    project_dir,
    monkeypatch,
    tmp_path,
):
    aggregator = TransportCosts(modes=[_make_mode(name="car", congestion=True)])
    next_mode = _make_mode(name="car", congestion=True)
    next_transport_costs = SimpleNamespace(
        congestion_states=SimpleNamespace(
            _iter_congestion_enabled_modes=lambda: iter([next_mode])
        )
    )
    requested_iterations = []

    def fake_for_iteration(iteration: int):
        requested_iterations.append(iteration)
        return next_transport_costs

    existing_path = tmp_path / "existing.parquet"
    existing_path.write_text("stub", encoding="utf-8")
    missing_path = tmp_path / "missing.parquet"

    def fake_from_inputs(*, iteration: int, **kwargs):
        path = existing_path if iteration == 1 else missing_path
        return SimpleNamespace(cache_path=Path(path))

    monkeypatch.setattr(aggregator, "for_iteration", fake_for_iteration)
    monkeypatch.setattr(
        VehicleODFlowsAsset,
        "from_inputs",
        staticmethod(fake_from_inputs),
    )

    run = SimpleNamespace(
        parameters=SimpleNamespace(n_iter_per_cost_update=2, n_iterations=3),
        inputs_hash="run-key",
        is_weekday=False,
    )

    artifacts = list(aggregator.iter_run_congestion_artifacts(run))

    assert requested_iterations == [2, 4]
    assert len(artifacts) == 1

    next_asset, congestion_state, flow_assets_by_mode = artifacts[0]
    assert next_asset is next_transport_costs
    assert isinstance(congestion_state, CongestionState)
    assert congestion_state.iteration == 1
    assert congestion_state.run_key == "run-key"
    assert congestion_state.is_weekday is False
    assert flow_assets_by_mode["car"].cache_path == existing_path

