from types import SimpleNamespace

from mobility.transport.costs.transport_costs import TransportCosts
from mobility.transport.costs.travel_costs_asset import TravelCostsBase


class _FakeTravelCosts(TravelCostsBase):
    def __init__(self):
        self.inputs = {}
        self.remove_calls = []

    def remove_congestion_artifacts(self, road_flow_asset):
        self.remove_calls.append(road_flow_asset)

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
    travel_costs = _FakeTravelCosts()
    aggregator = TransportCosts(
        modes=[
            _make_mode(name="car", congestion=True, travel_costs=travel_costs),
            _make_mode(name="walk", congestion=False, travel_costs=object()),
        ]
    )
    variant = _RemovableVariant()
    road_flow_asset = object()

    monkeypatch.setattr(
        aggregator,
        "asset_for_road_flows",
        lambda flow_asset: variant,
    )

    aggregator.remove_congestion_artifacts(road_flow_asset)

    assert variant.remove_calls == 1
    assert travel_costs.remove_calls == [road_flow_asset]

