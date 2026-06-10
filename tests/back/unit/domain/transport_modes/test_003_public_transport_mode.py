from mobility.runtime.assets.in_memory_asset import InMemoryAsset
from mobility.runtime.parameter_values import ParameterValue
from mobility.transport.costs.parameters.generalized_cost_parameters import (
    GeneralizedCostParameters,
)
from mobility.transport.modes.core.transport_mode import TransportMode
from mobility.transport.modes.public_transport import public_transport as pt_module


def _fake_public_transport_travel_costs(**inputs):
    """Return a small PT travel-cost asset for constructor-only tests."""
    return InMemoryAsset(inputs)


def _fake_public_transport_generalized_cost(
    travel_costs,
    first_leg_mode_name,
    last_leg_mode_name,
    start_parameters,
    mid_parameters,
    last_parameters,
):
    """Return a small PT generalized-cost asset with resolved leg parameters."""
    return InMemoryAsset(
        {
            "travel_costs": travel_costs,
            "first_leg_mode_name": first_leg_mode_name,
            "last_leg_mode_name": last_leg_mode_name,
            "start_parameters": start_parameters,
            "mid_parameters": mid_parameters,
            "last_parameters": last_parameters,
        }
    )


def test_public_transport_for_iteration_resolves_walk_leg_parameters(monkeypatch):
    """Check that PT resolves scenario values owned by access and egress modes."""
    monkeypatch.setattr(
        pt_module,
        "PublicTransportTravelCosts",
        _fake_public_transport_travel_costs,
    )
    monkeypatch.setattr(
        pt_module,
        "PublicTransportGeneralizedCost",
        _fake_public_transport_generalized_cost,
    )

    walk_distance_cost = ParameterValue.by_scenario_and_iteration(
        {
            "default": {1: 0.0},
            "paid_walk": {1: 0.0, 2: 0.25},
        }
    )
    walk_parameters = GeneralizedCostParameters(
        cost_of_distance=walk_distance_cost,
    )
    walk_travel_costs = InMemoryAsset({"parameters": {}})
    walk_generalized_cost = InMemoryAsset(
        {
            "travel_costs": walk_travel_costs,
            "parameters": walk_parameters,
        }
    )
    walk_mode = TransportMode(
        name="walk",
        travel_costs=walk_travel_costs,
        generalized_cost=walk_generalized_cost,
        ghg_intensity=0.0,
    )

    public_transport = pt_module.PublicTransportMode(
        transport_zones="test-zones",
        first_leg_mode=walk_mode,
        last_leg_mode=walk_mode,
        routing_parameters=pt_module.PublicTransportRoutingParameters(
            gtfs_reference_date="2026-01-01",
            gtfs_sources_folder="inputs/gtfs_sources",
        ),
    )

    resolved_public_transport = public_transport.for_iteration(
        iteration=2,
        scenario="paid_walk",
    )
    resolved_generalized_cost = resolved_public_transport.inputs["generalized_cost"]

    assert (
        resolved_generalized_cost.inputs["start_parameters"].cost_of_distance
        == 0.25
    )
    assert (
        resolved_generalized_cost.inputs["last_parameters"].cost_of_distance
        == 0.25
    )
