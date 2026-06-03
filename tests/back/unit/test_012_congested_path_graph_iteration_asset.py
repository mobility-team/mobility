from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from mobility.runtime.assets.in_memory_asset import InMemoryAsset
from mobility.transport.costs.od_flows_asset import VehicleODFlowsAsset
from mobility.transport.graphs.congested.congested_path_graph import CongestedPathGraph


class FakeUpstreamAsset(InMemoryAsset):
    def __init__(self, *, name: str, mode_name: str | None = None):
        self._hash_name = name
        self.mode_name = mode_name if mode_name is not None else name
        super().__init__({"name": name})

    def compute_inputs_hash(self) -> str:
        return f"fake-{self._hash_name}"

    def get(self):
        return Path("unused")


def test_congested_graph_iteration_asset_resolves_latest_active_refresh(monkeypatch, tmp_path):
    monkeypatch.setenv("MOBILITY_PROJECT_DATA_FOLDER", str(tmp_path))
    monkeypatch.setattr(
        CongestedPathGraph,
        "compute_inputs_hash",
        lambda self: "fake-congested-graph-hash",
    )

    modified_graph = FakeUpstreamAsset(name="modified", mode_name="car")
    transport_zones = FakeUpstreamAsset(name="zones")

    VehicleODFlowsAsset(
        pd.DataFrame({"from": [1], "to": [2], "vehicle_volume": [3.0]}),
        run_key="run-key",
        is_weekday=True,
        iteration=1,
        mode_name="car",
    ).get()

    graph = CongestedPathGraph(
        modified_graph=modified_graph,
        transport_zones=transport_zones,
        handles_congestion=True,
        congestion_flows_scaling_factor=0.5,
        target_max_vehicles_per_od_endpoint=1234.0,
        congestion_assignment_max_iterations=3,
        congestion_assignment_max_gap=0.2,
        congestion_assignment_retained_volume_share=0.9,
    )
    run = SimpleNamespace(
        inputs_hash="run-key",
        is_weekday=True,
        parameters=SimpleNamespace(
            run=SimpleNamespace(
                n_iter_per_cost_update=2,
                n_iterations=3,
            ),
        ),
    )

    assert graph.get_flow_asset_for_iteration(run, 1) is None

    flow_asset = graph.get_flow_asset_for_iteration(run, 2)
    assert flow_asset is not None
    assert flow_asset.inputs["iteration"] == 1

    iter_graph = graph.asset_for_iteration(run, 2)
    assert isinstance(iter_graph, CongestedPathGraph)
    assert iter_graph.inputs["vehicle_flows"] is not None
    assert iter_graph.inputs["vehicle_flows"].cache_path == flow_asset.cache_path
    assert iter_graph.inputs["target_max_vehicles_per_od_endpoint"] == 1234.0
    assert iter_graph.inputs["congestion_assignment_max_iterations"] == 3
    assert iter_graph.inputs["congestion_assignment_max_gap"] == 0.2
    assert iter_graph.inputs["congestion_assignment_retained_volume_share"] == 0.9


def test_congested_graph_iteration_asset_rejects_iteration_after_run_end(
    monkeypatch,
    tmp_path,
):
    """Check the upper iteration bound comes from parameters.run."""
    monkeypatch.setenv("MOBILITY_PROJECT_DATA_FOLDER", str(tmp_path))
    graph = CongestedPathGraph(
        modified_graph=FakeUpstreamAsset(name="modified", mode_name="car"),
        transport_zones=FakeUpstreamAsset(name="zones"),
        handles_congestion=True,
    )
    run = SimpleNamespace(
        inputs_hash="run-key",
        is_weekday=True,
        parameters=SimpleNamespace(
            run=SimpleNamespace(
                n_iter_per_cost_update=1,
                n_iterations=2,
            ),
        ),
    )

    with pytest.raises(ValueError, match="<= 2"):
        graph.asset_for_iteration(run, 3)
