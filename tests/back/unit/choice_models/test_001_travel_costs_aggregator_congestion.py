from types import SimpleNamespace

import polars as pl
from pydantic import BaseModel

from mobility.runtime.assets.in_memory_asset import InMemoryAsset
from mobility.transport.costs.od_flows_asset import VehicleODFlowsAsset
from mobility.transport.costs.road_flow_manager import RoadFlowManager


class _PersonODFlowsAsset(InMemoryAsset):
    def __init__(self, *, source_key: str, flows: pl.DataFrame):
        self.flows = flows
        super().__init__({"source_key": source_key})

    def get(self):
        return self.flows


class _FakeModeParameters(BaseModel):
    name: str
    congestion: bool
    persons_per_vehicle: float = 2.0


def _make_mode(*, name: str, congestion: bool, persons_per_vehicle: float = 2.0):
    return SimpleNamespace(
        inputs={
            "parameters": _FakeModeParameters(
                name=name,
                congestion=congestion,
                persons_per_vehicle=persons_per_vehicle,
            ),
            "generalized_cost": name,
        }
    )


def test_vehicle_od_flows_share_when_upstream_person_flows_are_the_same(project_dir):
    flows = pl.DataFrame(
        {
            "from": [1],
            "to": [2],
            "mode": ["car"],
            "flow_volume": [10.0],
        }
    )
    first_person_flows = _PersonODFlowsAsset(source_key="same-upstream", flows=flows)
    second_person_flows = _PersonODFlowsAsset(source_key="same-upstream", flows=flows)

    first_vehicle_flows = VehicleODFlowsAsset(
        person_od_flows_by_mode=first_person_flows,
        road_flow_parameters=[{"mode_name": "car", "vehicles_per_person": 1.0}],
    )
    second_vehicle_flows = VehicleODFlowsAsset(
        person_od_flows_by_mode=second_person_flows,
        road_flow_parameters=[{"mode_name": "car", "vehicles_per_person": 1.0}],
    )

    assert first_vehicle_flows.inputs_hash == second_vehicle_flows.inputs_hash
    assert first_vehicle_flows.cache_path == second_vehicle_flows.cache_path


def test_vehicle_od_flows_do_not_share_by_flow_content_only(project_dir):
    flows = pl.DataFrame(
        {
            "from": [1],
            "to": [2],
            "mode": ["car"],
            "flow_volume": [10.0],
        }
    )
    baseline_person_flows = _PersonODFlowsAsset(source_key="baseline", flows=flows)
    project_person_flows = _PersonODFlowsAsset(source_key="project", flows=flows)

    baseline_vehicle_flows = VehicleODFlowsAsset(
        person_od_flows_by_mode=baseline_person_flows,
        road_flow_parameters=[{"mode_name": "car", "vehicles_per_person": 1.0}],
    )
    project_vehicle_flows = VehicleODFlowsAsset(
        person_od_flows_by_mode=project_person_flows,
        road_flow_parameters=[{"mode_name": "car", "vehicles_per_person": 1.0}],
    )

    assert baseline_vehicle_flows.inputs_hash != project_vehicle_flows.inputs_hash
    assert baseline_vehicle_flows.cache_path != project_vehicle_flows.cache_path


def test_road_flow_manager_uses_one_shared_road_flow_asset_for_car_and_carpool(
    project_dir,
):
    transport_costs = SimpleNamespace(
        modes=[
            _make_mode(name="car", congestion=True),
            _make_mode(name="carpool", congestion=True, persons_per_vehicle=2.0),
            _make_mode(name="walk", congestion=False),
        ]
    )
    person_flows = _PersonODFlowsAsset(
        source_key="iteration-1",
        flows=pl.DataFrame(
            {
                "from": [1, 1],
                "to": [2, 2],
                "mode": ["car", "carpool"],
                "flow_volume": [10.0, 4.0],
            }
        ),
    )

    road_flow_asset = RoadFlowManager(transport_costs).build(person_flows)

    assert road_flow_asset is not None
    road_flows = road_flow_asset.get_cached_asset()
    assert road_flows["vehicle_volume"].tolist() == [12.0]
