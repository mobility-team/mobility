from types import SimpleNamespace

import pytest

from mobility.transport.costs.path.path_travel_costs import PathTravelCosts
from mobility.transport.costs.travel_costs_asset import TravelCostsAsset
from mobility.transport.modes.public_transport.public_transport_travel_costs import (
    PublicTransportTravelCosts,
)


class _Removable:
    def __init__(self, *, inputs=None):
        self.inputs = inputs or {}
        self.remove_calls = 0

    def remove(self):
        self.remove_calls += 1


class _LegTravelCosts(TravelCostsAsset):
    def __init__(self, variant):
        self.variant = variant
        self.inputs = {}

    def asset_for_congestion_state(self, congestion_state):
        return self.variant

    def get_cached_asset(self):
        return None

    def create_and_get_asset(self):
        return None

    def get(self):
        return None


def _make_public_transport_asset(first_leg, last_leg):
    asset = object.__new__(PublicTransportTravelCosts)
    asset.first_leg_travel_costs = first_leg
    asset.last_leg_travel_costs = last_leg
    asset.first_leg_mode_name = "walk"
    asset.last_leg_mode_name = "car"
    asset.inputs = {
        "transport_zones": object(),
        "parameters": object(),
        "first_modal_transfer": object(),
        "last_modal_transfer": object(),
        "intermodal_graph": _Removable(),
    }
    return asset


def test_public_transport_asset_for_congestion_state_returns_self_when_legs_do_not_change():
    first_leg = _LegTravelCosts(None)
    last_leg = _LegTravelCosts(None)
    asset = _make_public_transport_asset(first_leg, last_leg)

    assert asset.asset_for_congestion_state(object()) is asset


def test_public_transport_asset_for_congestion_state_rebinds_changed_leg(monkeypatch):
    first_leg_variant = object()
    first_leg = _LegTravelCosts(first_leg_variant)
    last_leg = _LegTravelCosts(None)
    asset = _make_public_transport_asset(first_leg, last_leg)

    def fake_init(
        self,
        transport_zones,
        parameters,
        first_leg_travel_costs,
        last_leg_travel_costs,
        first_leg_mode_name,
        last_leg_mode_name,
        first_modal_transfer=None,
        last_modal_transfer=None,
    ):
        self.first_leg_travel_costs = first_leg_travel_costs
        self.last_leg_travel_costs = last_leg_travel_costs
        self.first_leg_mode_name = first_leg_mode_name
        self.last_leg_mode_name = last_leg_mode_name
        self.inputs = {
            "transport_zones": transport_zones,
            "parameters": parameters,
            "first_modal_transfer": first_modal_transfer,
            "last_modal_transfer": last_modal_transfer,
            "intermodal_graph": _Removable(),
        }

    monkeypatch.setattr(PublicTransportTravelCosts, "__init__", fake_init)

    variant = asset.asset_for_congestion_state(object())

    assert variant is not asset
    assert variant.first_leg_travel_costs is first_leg_variant
    assert variant.last_leg_travel_costs is last_leg
    assert variant.first_leg_mode_name == "walk"
    assert variant.last_leg_mode_name == "car"


def test_public_transport_remove_congestion_artifacts_removes_owned_variant():
    asset = _make_public_transport_asset(first_leg=object(), last_leg=object())
    intermodal_graph = _Removable()
    variant = _Removable(inputs={"intermodal_graph": intermodal_graph})
    asset.asset_for_congestion_state = lambda congestion_state: variant

    asset.remove_congestion_artifacts(object())

    assert variant.remove_calls == 1
    assert intermodal_graph.remove_calls == 1


def test_public_transport_remove_congestion_artifacts_is_noop_when_variant_is_self():
    asset = _make_public_transport_asset(first_leg=object(), last_leg=object())
    asset.asset_for_congestion_state = lambda congestion_state: asset

    asset.remove_congestion_artifacts(object())

    assert asset.inputs["intermodal_graph"].remove_calls == 0


def _make_path_travel_costs():
    asset = object.__new__(PathTravelCosts)
    asset.inputs = {"mode_name": "car"}
    return asset


@pytest.mark.parametrize("variant_factory", [lambda asset: None, lambda asset: asset])
def test_path_remove_congestion_artifacts_returns_early_for_missing_variant(variant_factory):
    asset = _make_path_travel_costs()
    asset.asset_for_congestion_state = lambda congestion_state: variant_factory(asset)

    asset.remove_congestion_artifacts(object())


def test_path_remove_congestion_artifacts_removes_owned_graph_chain():
    asset = _make_path_travel_costs()
    congested_graph = _Removable()
    contracted_graph = _Removable(inputs={"congested_graph": congested_graph})
    variant = _Removable(inputs={"contracted_path_graph": contracted_graph})
    asset.asset_for_congestion_state = lambda congestion_state: variant

    asset.remove_congestion_artifacts(object())

    assert variant.remove_calls == 1
    assert contracted_graph.remove_calls == 1
    assert congested_graph.remove_calls == 1

