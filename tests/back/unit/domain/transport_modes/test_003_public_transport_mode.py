from pathlib import Path

import pytest

from mobility.runtime.assets.in_memory_asset import InMemoryAsset
from mobility.runtime.parameter_values import ParameterValue
from mobility.transport.costs.parameters.generalized_cost_parameters import (
    GeneralizedCostParameters,
)
from mobility.transport.modes.core.transport_mode import TransportMode
from mobility.transport.modes.public_transport import public_transport as pt_module


def test_additional_gtfs_files_are_selected_for_each_scenario_and_iteration(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Define a future service before selecting the iteration that introduces it."""
    monkeypatch.setenv("MOBILITY_PROJECT_DATA_FOLDER", str(tmp_path))
    zones = InMemoryAsset({"zones": "Rennes"})
    zones.countries = ["fr"]

    # Only the walking network is replaced; build the real public transport assets.
    walk_costs = InMemoryAsset({"parameters": {}})
    walk_costs.active_routing_graph = InMemoryAsset({"network": "walk"})
    walk_costs.active_routing_backend = "dodgr"
    walk_costs.active_cch_graph = None
    walk = TransportMode(
        name="walk",
        travel_costs=walk_costs,
        generalized_cost=InMemoryAsset({"parameters": GeneralizedCostParameters()}),
        ghg_intensity=0.0,
    )

    # The future file need not exist while the scenarios are being defined.
    extra_feed = tmp_path / "serm.zip"
    mode = pt_module.PublicTransportMode(
        transport_zones=zones,
        first_leg_mode=walk,
        last_leg_mode=walk,
        routing_parameters=pt_module.PublicTransportRoutingParameters(
            gtfs_reference_date="2026-07-24",
            gtfs_sources_folder=tmp_path / "sources",
            additional_gtfs_files=ParameterValue.by_scenario_and_iteration(
                default=None, serm={1: [], 5: [str(extra_feed)]}
            ),
        ),
    )

    extra_feed.write_bytes(b"first timetable")
    graphs = {}
    for scenario, iteration in [("default", 5), ("serm", 1), ("serm", 4), ("serm", 5), ("serm", 6)]:
        resolved = mode.for_iteration(iteration, scenario=scenario)
        graph = resolved.travel_costs.intermodal_graph.public_transport_graph
        graphs[scenario, iteration] = graph
        expected = [str(extra_feed)] if scenario == "serm" and iteration >= 5 else None
        assert graph.gtfs_router.inputs["additional_gtfs_files"] == expected
        assert bool(graph.gtfs_router.inputs["additional_gtfs_hashes"]) == bool(expected)

    assert graphs["default", 5].cache_path == graphs["serm", 4].cache_path
    assert graphs["serm", 5].cache_path == graphs["serm", 6].cache_path
    assert graphs["serm", 4].cache_path != graphs["serm", 5].cache_path

    # Editing the added timetable must also invalidate the resolved graph.
    extra_feed.write_bytes(b"revised timetable")
    revised = mode.for_iteration(5, scenario="serm")
    revised_graph = revised.travel_costs.intermodal_graph.public_transport_graph
    assert revised_graph.cache_path != graphs["serm", 5].cache_path


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
