import json

from mobility.runtime.assets.file_asset import FileAsset
from mobility.trips.group_day_trips.iterations.iteration_assets import CongestionFlowsAsset


class _PreviousCongestionFlows(FileAsset):
    """Small cached asset that stands in for the previous road-flow choice."""

    def __init__(self, tmp_path, road_flow_asset):
        self.road_flow_asset = road_flow_asset
        self.get_calls = 0
        super().__init__({"version": 1}, tmp_path / "previous_congestion_flows.json")

    def get_cached_asset(self):
        return self.road_flow_asset

    def create_and_get_asset(self):
        self.get_calls += 1
        self.cache_path.write_text("{}", encoding="utf-8")
        return self.road_flow_asset


class _PreviousTransportCosts(FileAsset):
    """Previous iteration transport-cost asset with a congestion-flow link."""

    def __init__(self, tmp_path, congestion_flows):
        self.congestion_flows = congestion_flows
        super().__init__(
            {
                "version": 1,
                "congestion_flows": congestion_flows,
            },
            tmp_path / "previous_transport_costs.parquet",
        )

    def get_cached_asset(self):
        return {}

    def create_and_get_asset(self):
        self.cache_path.write_text("{}", encoding="utf-8")
        return {}


class _PreviousState(FileAsset):
    """Previous iteration state exposing its transport-cost asset."""

    def __init__(self, tmp_path, transport_costs):
        super().__init__(
            {
                "version": 1,
                "transport_costs": transport_costs,
            },
            tmp_path / "previous_state.json",
        )

    def get_cached_asset(self):
        return {}

    def create_and_get_asset(self):
        self.cache_path.write_text("{}", encoding="utf-8")
        return {}


class _RoadFlowManager:
    """Road-flow settings used by the fake transport-cost asset."""

    def road_flow_parameters(self):
        return [{"mode_name": "car", "persons_per_vehicle": 1.0}]


class _TransportCosts:
    """Fake transport costs with congestion enabled."""

    def __init__(self):
        self.road_flows = _RoadFlowManager()
        self.build_calls = 0

    def has_enabled_congestion(self):
        return True

    def should_recompute_congested_costs(self, iteration, update_interval):
        return update_interval > 0 and (iteration - 1) % update_interval == 0

    def build_road_flow_asset(self, person_od_flows_by_mode):
        self.build_calls += 1
        return object()


def test_congestion_flows_reuses_previous_flows_when_refresh_is_not_due(tmp_path):
    previous_road_flow_asset = object()
    previous_congestion_flows = _PreviousCongestionFlows(
        tmp_path,
        previous_road_flow_asset,
    )
    previous_transport_costs = _PreviousTransportCosts(
        tmp_path,
        previous_congestion_flows,
    )
    previous_state = _PreviousState(tmp_path, previous_transport_costs)
    transport_costs = _TransportCosts()

    congestion_flows = CongestionFlowsAsset(
        is_weekday=True,
        iteration=3,
        base_folder=tmp_path,
        previous_state=previous_state,
        transport_costs=transport_costs,
        n_iter_per_cost_update=2,
    )

    road_flow_asset = congestion_flows.get()
    metadata = json.loads(congestion_flows.cache_path.read_text(encoding="utf-8"))

    assert road_flow_asset is previous_road_flow_asset
    assert congestion_flows.person_od_flows_by_mode is None
    assert transport_costs.build_calls == 0
    assert previous_congestion_flows.get_calls == 1
    assert metadata["has_road_flow_asset"] is True
    assert metadata["source"] == "previous"
    assert congestion_flows.get_cached_asset() is previous_road_flow_asset
