from pathlib import Path
from unittest.mock import Mock

import pytest

from mobility.runtime.assets.in_memory_asset import InMemoryAsset
from mobility.runtime.parameter_values import ParameterValue
from mobility.transport.costs.parameters.generalized_cost_parameters import (
    GeneralizedCostParameters,
)
from mobility.transport.modes.core.transport_mode import TransportMode
from mobility.transport.modes.public_transport import public_transport as pt_module
from mobility.transport.modes.public_transport import public_transport_graph as graph_module
from mobility.transport.modes.public_transport.gtfs_builder import GTFSBuilder


@pytest.mark.parametrize("generated", [False, True])
def test_additional_gtfs_files_are_selected_for_each_scenario_and_iteration(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, generated: bool
) -> None:
    """Define a future service before selecting the iteration that introduces it."""
    monkeypatch.setenv("MOBILITY_PROJECT_DATA_FOLDER", str(tmp_path))
    zones = InMemoryAsset({"zones": "Rennes"})
    zones.countries = ["fr"]

    # Source discovery is outside the scenario-resolution behavior tested here.
    monkeypatch.setattr(graph_module, "GTFSSources", Mock(wraps=graph_module.GTFSSources, return_value=None))
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
    builder = GTFSBuilder("serm", "SERM", "r", "R", "train", "s")
    builder.add_stops({"a": [-1.68, 48.11], "b": [-1.67, 48.11]})
    builder.add_line([("a", "b", 60)], start_time=0, end_time=600, period=300)
    additional_source = builder.build_asset() if generated else str(extra_feed)
    mode = pt_module.PublicTransportMode(
        transport_zones=zones,
        first_leg_mode=walk,
        last_leg_mode=walk,
        routing_parameters=pt_module.PublicTransportRoutingParameters(
            gtfs_reference_date="2026-07-24",
            gtfs_sources_folder=tmp_path / "sources",
            additional_gtfs_files=ParameterValue.by_scenario_and_iteration(
                default=None, serm={1: [], 5: [additional_source]}
            ),
        ),
    )

    extra_feed.write_bytes(b"first timetable")
    graphs = {}
    for scenario, iteration in [("default", 5), ("serm", 1), ("serm", 4), ("serm", 5), ("serm", 6)]:
        resolved = mode.for_iteration(iteration, scenario=scenario)
        graph = resolved.travel_costs.intermodal_graph.public_transport_graph
        graphs[scenario, iteration] = graph
        settings = graph.inputs["parameters"]
        assert "additional_gtfs_files" not in type(settings).model_fields
        assert "gtfs_reference_date" not in type(settings).model_fields
        assert resolved.travel_costs.inputs["parameters"] is settings
        assert resolved.travel_costs.intermodal_graph.inputs["parameters"] is settings
        assert settings.model_dump(mode="json")["wait_time_coeff"] == 2.0
        expected = [additional_source] if scenario == "serm" and iteration >= 5 else None
        sources = graph.gtfs_router.inputs["additional_gtfs_files"]
        if generated and expected:
            # Parameter resolution may copy assets; their input identity is retained.
            assert [source.inputs_hash for source in sources] == [additional_source.inputs_hash]
        else:
            assert sources == expected
        assert bool(graph.gtfs_router.inputs["additional_gtfs_hashes"]) == (bool(expected) and not generated)

    assert graphs["default", 5].cache_path == graphs["serm", 4].cache_path
    assert graphs["serm", 5].cache_path == graphs["serm", 6].cache_path
    assert graphs["serm", 4].cache_path != graphs["serm", 5].cache_path

    if generated:
        # Recreating an unchanged service must reuse the graph.
        parameters = mode.routing_parameters
        parameters.additional_gtfs_files = ParameterValue.by_scenario_and_iteration(
            default=None, serm={1: [], 5: [builder.build_asset()]}
        )
        repeated = mode.for_iteration(5, scenario="serm")
        repeated_graph = repeated.travel_costs.intermodal_graph.public_transport_graph
        assert repeated_graph.cache_path == graphs["serm", 5].cache_path
        assert not additional_source.cache_path.exists()

        builder.add_line([("a", "b", 60)], start_time=1200, end_time=1800, period=150)
        parameters.additional_gtfs_files = ParameterValue.by_scenario_and_iteration(
            default=None, serm={1: [], 5: [builder.build_asset()]}
        )
    else:
        extra_feed.write_bytes(b"revised timetable")

    # Only the scenario using the changed service gets a new graph hash.
    revised = mode.for_iteration(5, scenario="serm")
    revised_graph = revised.travel_costs.intermodal_graph.public_transport_graph
    assert revised_graph.cache_path != graphs["serm", 5].cache_path
    default = mode.for_iteration(5, scenario="default")
    default_graph = default.travel_costs.intermodal_graph.public_transport_graph
    assert default_graph.cache_path == graphs["default", 5].cache_path


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
