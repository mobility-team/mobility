import pathlib

import pytest

from mobility.runtime.assets.in_memory_asset import InMemoryAsset
from mobility.runtime.parameter_values import ParameterValue
from mobility.runtime.scenarios import collect_parameter_value_changes
from mobility.transport.costs.parameters.generalized_cost_parameters import (
    GeneralizedCostParameters,
)
from mobility.transport.costs.parameters.path_routing_parameters import (
    PathRoutingParameters,
)
from mobility.transport.graphs.modified.modifiers import speed_modifier
from mobility.transport.modes.bicycle import BicycleMode
from mobility.transport.modes.car import CarMode
from mobility.transport.modes.core.osm_capacity_parameters import OSMCapacityParameters


def test_speed_modifier_parameters_resolve_by_scenario_and_iteration():
    """Resolve every user-facing modifier value before it reaches the R script."""
    def project_value(default, changed):
        return ParameterValue.by_scenario_and_iteration(
            default=default,
            road_project={1: default, 3: changed},
        )

    transport_zones = InMemoryAsset({})
    modifiers = [
        speed_modifier.BorderCrossingSpeedModifier(
            transport_zones,
            max_speed=project_value(30.0, 20.0),
            time_penalty=project_value(0.0, 5.0),
            geofabrik_extract_date=project_value("260101", "270101"),
        ),
        speed_modifier.LimitedSpeedZonesModifier(
            zones_geometry_file_path=project_value(
                "current-speed-zones.gpkg",
                "project-speed-zones.gpkg",
            ),
            max_speed=project_value(30.0, 20.0),
        ),
        speed_modifier.RoadLaneNumberModifier(
            zones_geometry_file_path=project_value(
                "current-lanes.gpkg",
                "project-lanes.gpkg",
            ),
            lane_delta=project_value(0, -1),
        ),
        speed_modifier.NewRoadModifier(
            zones_geometry_file_path=project_value(
                "current-roads.gpkg",
                "project-roads.gpkg",
            ),
            max_speed=project_value(30.0, 80.0),
            capacity=project_value(1800.0, 2400.0),
            alpha=project_value(0.15, 0.2),
            beta=project_value(4.0, 5.0),
        ),
    ]

    resolved = [
        modifier.for_iteration(3, scenario="road_project")
        for modifier in modifiers
    ]

    assert resolved[0].max_speed == 20.0
    assert resolved[0].time_penalty == 5.0
    assert resolved[0].geofabrik_extract_date == "270101"
    assert resolved[1].get() == {
        "modifier_type": "limited_speed_zones",
        "max_speed": 20.0,
        "zones_geometry_file_path": "project-speed-zones.gpkg",
    }
    assert resolved[2].get() == {
        "modifier_type": "lane_number_modification",
        "lane_delta": -1,
        "zones_geometry_file_path": "project-lanes.gpkg",
    }
    assert resolved[3].get() == {
        "modifier_type": "new_road",
        "capacity": 2400.0,
        "alpha": 0.2,
        "beta": 5.0,
        "max_speed": 80.0,
        "zones_geometry_file_path": "project-roads.gpkg",
    }

    # Keep the setup reusable for every scenario and iteration.
    assert isinstance(modifiers[1].max_speed, ParameterValue)
    assert resolved[1].inputs_hash != modifiers[1].inputs_hash


def test_speed_modifier_changes_are_listed_in_the_scenario_manifest():
    """Expose modifier fields in the same scenario report as other parameters."""
    modifier = speed_modifier.LimitedSpeedZonesModifier(
        zones_geometry_file_path="speed-zones.gpkg",
        max_speed=ParameterValue.by_scenario(default=30.0, road_project=20.0),
    )

    changes = collect_parameter_value_changes({"modifier": modifier})

    assert len(changes) == 1
    assert changes[0].path.endswith("['parameters'].max_speed")
    assert changes[0].scenario_names == ("default", "road_project")


@pytest.mark.parametrize("congestion", [False, True])
def test_path_travel_costs_rebuild_the_graph_chain_for_resolved_modifiers(
    monkeypatch,
    congestion,
):
    """Bind every downstream graph to the modifier selected for an iteration."""

    class FakePathGraph:
        def __init__(
            self,
            mode_name,
            transport_zones,
            osm_capacity_parameters,
            congestion,
            congestion_flows_scaling_factor,
            target_max_vehicles_per_od_endpoint,
            congestion_assignment_max_iterations,
            congestion_assignment_max_gap,
            congestion_assignment_retained_volume_share,
            speed_modifiers,
        ):
            self.simplified = InMemoryAsset({"mode_name": mode_name})
            self.modified = InMemoryAsset(
                {
                    "mode_name": mode_name,
                    "speed_modifiers": speed_modifiers,
                }
            )
            self.contracted = InMemoryAsset({"modified_graph": self.modified})
            if congestion:
                self.cch = InMemoryAsset({"modified_graph": self.modified})
                self.congested = InMemoryAsset(
                    {
                        "modified_graph": self.modified,
                        "cch_graph": self.cch,
                    }
                )
            else:
                self.cch = None
                self.congested = None

    monkeypatch.setattr(
        "mobility.transport.costs.path.path_travel_costs.PathGraph",
        FakePathGraph,
    )

    max_speed = ParameterValue.by_scenario_and_iteration(
        default=30.0,
        road_project={1: 30.0, 3: 20.0},
    )
    modifier = speed_modifier.LimitedSpeedZonesModifier(
        "speed-zones.gpkg",
        max_speed=max_speed,
    )
    mode_class = CarMode if congestion else BicycleMode
    mode_name = "car" if congestion else "bicycle"
    mode = mode_class(
        transport_zones=InMemoryAsset({}),
        routing_parameters=PathRoutingParameters(max_beeline_distance=20.0),
        osm_capacity_parameters=OSMCapacityParameters(mode_name),
        generalized_cost_parameters=GeneralizedCostParameters(),
        speed_modifiers=[modifier],
        **({"congestion": True} if congestion else {}),
    )

    initial_mode = mode.for_iteration(1, scenario="road_project")
    resolved_mode = mode.for_iteration(3, scenario="road_project")

    assert initial_mode is not mode
    assert resolved_mode is not mode
    assert (
        initial_mode.inputs["travel_costs"].inputs["speed_modifiers"][0].max_speed
        == 30.0
    )
    assert (
        initial_mode.inputs["travel_costs"].inputs_hash
        != resolved_mode.inputs["travel_costs"].inputs_hash
    )

    resolved_travel_costs = resolved_mode.inputs["travel_costs"]
    resolved_modifier = resolved_travel_costs.inputs["speed_modifiers"][0]
    assert resolved_modifier.max_speed == 20.0
    assert (
        resolved_travel_costs.modified_path_graph.inputs["speed_modifiers"][0]
        is resolved_modifier
    )
    assert (
        resolved_travel_costs.contracted_path_graph.inputs["modified_graph"]
        is resolved_travel_costs.modified_path_graph
    )
    if congestion:
        assert (
            resolved_travel_costs.cch_path_graph.inputs["modified_graph"]
            is resolved_travel_costs.modified_path_graph
        )
        assert (
            resolved_travel_costs.congested_path_graph.inputs["modified_graph"]
            is resolved_travel_costs.modified_path_graph
        )
    assert (
        resolved_mode.inputs["generalized_cost"].inputs["travel_costs"]
        is resolved_travel_costs
    )


def test_speed_modifier_parameter_models_keep_scalar_defaults():
    """Keep existing scalar constructor behaviour after introducing models."""
    modifier = speed_modifier.NewRoadModifier(pathlib.Path("new-road.gpkg"))

    assert modifier.max_speed == 30.0
    assert modifier.capacity == 1800.0
    assert modifier.alpha == 0.15
    assert modifier.beta == 4.0
    assert modifier.get()["zones_geometry_file_path"] == "new-road.gpkg"


def test_speed_modifier_requires_a_geometry_path():
    """Give a plain error when a geometry-based modifier has no GIS file."""
    with pytest.raises(ValueError, match="zones_geometry_file_path"):
        speed_modifier.LimitedSpeedZonesModifier()


def test_unresolved_speed_modifier_cannot_prepare_graph_inputs():
    """Explain that a run context is needed instead of leaking model objects."""
    modifier = speed_modifier.LimitedSpeedZonesModifier(
        "speed-zones.gpkg",
        max_speed=ParameterValue.by_scenario(default=30.0, project=20.0),
    )

    with pytest.raises(ValueError, match=r"for_iteration\(\.\.\.\)"):
        modifier.get()
