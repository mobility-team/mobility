from pathlib import Path
from types import SimpleNamespace

from mobility.transport.costs.od_flows_asset import VehicleODFlowsAsset
from mobility.transport.costs.travel_costs_asset import TravelCostsAsset
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


class _TravelCostsWithCongestionVariant(TravelCostsAsset):
    def __init__(self, variant):
        self.variant = variant
        self.inputs = {}

    def asset_for_congestion_state(self, congestion_state):
        return self.variant

    def remove_congestion_artifacts(self, congestion_state):
        self.variant.remove()
        contracted_graph = self.variant.inputs.get("contracted_path_graph")
        if contracted_graph is not None:
            contracted_graph.remove()
            congested_graph = contracted_graph.inputs.get("congested_graph")
            if congested_graph is not None:
                congested_graph.remove()

    def get_cached_asset(self):
        return None

    def create_and_get_asset(self):
        return None

    def get(self):
        return None


class _TravelCostsWithoutCongestionVariant(TravelCostsAsset):
    def __init__(self):
        self.inputs = {}
        self.remove_calls = 0

    def remove_congestion_artifacts(self, congestion_state):
        self.remove_calls += 1

    def get_cached_asset(self):
        return None

    def create_and_get_asset(self):
        return None

    def get(self):
        return None


class _TransportCostsWithCongestionVariants:
    def __init__(self, next_transport_costs):
        self.next_transport_costs = next_transport_costs

    def iter_run_congestion_artifacts(self, run):
        flow_asset = VehicleODFlowsAsset.from_inputs(
            run_key=run.inputs_hash,
            is_weekday=run.is_weekday,
            iteration=1,
            mode_name="car",
        )
        yield self.next_transport_costs, SimpleNamespace(iteration=1), {"car": flow_asset}


def test_remove_run_iteration_congestion_artifacts_clears_mode_variant_graph_chain(
    monkeypatch,
    tmp_path,
):
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
        congestion_states=SimpleNamespace(
            _iter_congestion_enabled_modes=lambda: iter([car_mode])
        ),
        remove_congestion_artifacts=lambda congestion_state: combined_transport_costs_variant.remove(),
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

    original_remove = next_transport_costs.remove_congestion_artifacts

    def remove_congestion_artifacts(congestion_state):
        original_remove(congestion_state)
        car_mode.inputs["travel_costs"].remove_congestion_artifacts(congestion_state)

    next_transport_costs.remove_congestion_artifacts = remove_congestion_artifacts

    run._remove_run_iteration_congestion_artifacts()

    assert combined_transport_costs_variant.remove_calls == 1
    assert travel_cost_variant.remove_calls == 1
    assert contracted_graph_variant.remove_calls == 1
    assert congested_graph_variant.remove_calls == 1
    assert flow_asset.remove_calls == 1
    assert flow_path.exists() is False


def test_remove_run_iteration_congestion_artifacts_keeps_non_variant_mode_cleanup_shallow(
    monkeypatch,
    tmp_path,
):
    flow_path = tmp_path / "vehicle_od_flows_car.parquet"
    flow_path.write_text("stub", encoding="utf-8")
    flow_asset = _FlowAsset(flow_path)

    combined_transport_costs_variant = _RemovableAsset()
    travel_costs = _TravelCostsWithoutCongestionVariant()

    car_mode = SimpleNamespace(
        inputs={
            "parameters": SimpleNamespace(name="car"),
            "travel_costs": travel_costs,
        }
    )
    next_transport_costs = SimpleNamespace(
        congestion_states=SimpleNamespace(
            _iter_congestion_enabled_modes=lambda: iter([car_mode])
        ),
        remove_congestion_artifacts=lambda congestion_state: (
            combined_transport_costs_variant.remove(),
            travel_costs.remove_congestion_artifacts(congestion_state),
        ),
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

    run._remove_run_iteration_congestion_artifacts()

    assert combined_transport_costs_variant.remove_calls == 1
    assert travel_costs.remove_calls == 1
    assert flow_asset.remove_calls == 1
    assert flow_path.exists() is False
