from types import SimpleNamespace

import polars as pl

from mobility.runtime.assets.graph import asset_graph_key
from mobility.runtime.assets.resolver import asset_resolution_context
from mobility.transport.costs.path.path_travel_costs import PathTravelCosts
from mobility.transport.costs.path.shared_congested_costs import prepare_shared_congested_costs


class FakeAsset:
    def __init__(self, identity, path=None, inputs=None):
        self.inputs_hash = identity
        self.cache_path = path
        self.inputs = inputs or {}

    def get(self):
        return self.cache_path


def make_iteration_costs(tmp_path, name, *, graph_id="graph", update=True, mode_name="car"):
    settings = {
        "handles_congestion": True,
        "congestion_flows_scaling_factor": 0.5,
        "target_max_vehicles_per_od_endpoint": 1000.0,
        "congestion_assignment_max_iterations": 3,
        "congestion_assignment_max_gap": 0.15,
        "congestion_assignment_retained_volume_share": 0.9,
    }
    path = object.__new__(PathTravelCosts)
    path.modified_path_graph = FakeAsset(graph_id, tmp_path / "graph")
    path.cch_path_graph = FakeAsset("cch", tmp_path / "cch")
    path.transport_zones = FakeAsset("zones", tmp_path / "zones")
    path.congested_path_graph = FakeAsset("congested", inputs=settings)
    path.routing_parameters = SimpleNamespace(max_beeline_distance=60.0)
    flow = FakeAsset(name, tmp_path / (name + ".parquet"))
    table = FakeAsset("cost-" + name)
    flow.table = table
    car = SimpleNamespace(inputs={
        "parameters": SimpleNamespace(name=mode_name),
        "generalized_cost": SimpleNamespace(inputs={"travel_costs": path}),
    })
    iteration = SimpleNamespace(
        modes=[car],
        congestion_flows=SimpleNamespace(should_update_costs=update, get=lambda: flow),
    )
    return iteration, table


def test_shared_costs_keep_scenarios_and_missing_routes_separate(tmp_path, monkeypatch):
    """Two scenario columns become ordinary per-scenario costs in the resolver."""
    monkeypatch.setenv("MOBILITY_PROJECT_DATA_FOLDER", str(tmp_path))
    monkeypatch.setattr(PathTravelCosts, "asset_for_flow_asset", lambda self, flow: SimpleNamespace(congested_costs=flow.table))
    first, first_table = make_iteration_costs(tmp_path, "first")
    second, second_table = make_iteration_costs(tmp_path, "second", mode_name="other-road-mode")
    calls = []

    class FakeRunner:
        def __init__(self, path):
            pass

        def run(self, args):
            calls.append(args)
            pl.DataFrame({
                "from": [1, 2], "to": [2, 3],
                "distance_0": [1.0, 2.0], "time_0": [0.1, 0.2],
                "distance_1": [3.0, None], "time_1": [0.3, None],
            }).write_parquet(args[-2])

    monkeypatch.setattr("mobility.transport.costs.path.shared_congested_costs.RScriptRunner", FakeRunner)
    with asset_resolution_context() as resolver:
        prepare_shared_congested_costs([first, second])
        a = resolver.in_memory_values[asset_graph_key(first_table)]
        b = resolver.in_memory_values[asset_graph_key(second_table)]
        assert len(calls) == 1
        assert a["distance"].tolist() == [1.0, 2.0]
        assert b["distance"].tolist() == [3.0]
        assert b["time"].tolist() == [0.3]
        prepare_shared_congested_costs([first, second])
        assert len(calls) == 1


def test_incompatible_networks_are_not_batched(tmp_path, monkeypatch):
    """Different road networks remain on the ordinary per-scenario path."""
    monkeypatch.setenv("MOBILITY_PROJECT_DATA_FOLDER", str(tmp_path))
    first, _ = make_iteration_costs(tmp_path, "first")
    second, _ = make_iteration_costs(tmp_path, "second", graph_id="other")
    with asset_resolution_context() as resolver:
        prepare_shared_congested_costs([first, second])
        assert not resolver.in_memory_values
