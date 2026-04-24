from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

from mobility.transport.costs.od_flows_asset import VehicleODFlowsAsset
from mobility.trips.group_day_trips.core.run import Run


class _RemovableAsset:
    def __init__(self, *, inputs=None):
        self.inputs = inputs or {}
        self.remove_calls = 0

    def remove(self):
        self.remove_calls += 1


class _FlowAsset:
    def __init__(self, path: Path):
        self.cache_path = path
        self.remove_calls = 0

    def remove(self):
        self.remove_calls += 1
        self.cache_path.unlink(missing_ok=True)


class _TravelCostsWithCongestionVariant:
    def __init__(self, variant):
        self.variant = variant

    def asset_for_congestion_state(self, congestion_state):
        return self.variant


class _TransportCostsWithCongestionVariants:
    def __init__(self, next_transport_costs):
        self.next_transport_costs = next_transport_costs

    def has_enabled_congestion(self) -> bool:
        return True

    def should_recompute_congested_costs(self, iteration: int, update_interval: int) -> bool:
        return True

    def for_iteration(self, iteration: int):
        return self.next_transport_costs


def test_remove_run_owned_congestion_artifacts_clears_mode_variant_graph_chain(monkeypatch):
    tmp_path = Path(".pytest-local-tmp") / "group-day-trips" / f"mobility-tests-{uuid4().hex}"
    tmp_path.mkdir(parents=True, exist_ok=False)
    flow_path = tmp_path / "vehicle_od_flows_car.parquet"
    flow_path.write_text("stub", encoding="utf-8")
    flow_asset = _FlowAsset(flow_path)

    congested_graph_variant = _RemovableAsset()
    contracted_graph_variant = _RemovableAsset(
        inputs={"congested_graph": congested_graph_variant}
    )
    travel_cost_variant = _RemovableAsset(
        inputs={"contracted_path_graph": contracted_graph_variant}
    )
    combined_transport_costs_variant = _RemovableAsset()

    car_mode = SimpleNamespace(
        inputs={
            "parameters": SimpleNamespace(name="car"),
            "travel_costs": _TravelCostsWithCongestionVariant(travel_cost_variant),
        }
    )
    next_transport_costs = SimpleNamespace(
        modes=[car_mode],
        congestion_states=SimpleNamespace(
            _iter_congestion_enabled_modes=lambda: iter([car_mode])
        ),
        asset_for_congestion_state=lambda congestion_state: combined_transport_costs_variant,
    )

    run = object.__new__(Run)
    run.inputs_hash = "run-key"
    run.is_weekday = True
    run.parameters = SimpleNamespace(n_iter_per_cost_update=1, n_iterations=1)
    run.transport_costs = _TransportCostsWithCongestionVariants(next_transport_costs)

    monkeypatch.setattr(
        VehicleODFlowsAsset,
        "from_inputs",
        staticmethod(lambda **kwargs: flow_asset),
    )

    run._remove_run_owned_congestion_artifacts()

    assert combined_transport_costs_variant.remove_calls == 1
    assert travel_cost_variant.remove_calls == 1
    assert contracted_graph_variant.remove_calls == 1
    assert congested_graph_variant.remove_calls == 1
    assert flow_asset.remove_calls == 1
    assert flow_path.exists() is False
