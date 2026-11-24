from types import SimpleNamespace
import json
from pathlib import Path

import polars as pl
import pytest

from mobility.runtime.assets.graph import asset_graph_key
from mobility.runtime.assets.resolver import asset_resolution_context
from mobility.transport.costs.path.path_travel_costs import PathTravelCosts
from mobility.transport.costs.path.shared_congested_costs import prepare_shared_congested_costs
from mobility.transport.costs.path.shared_road_worker import SharedRoadWorker
from mobility.transport.modes.carpool.detailed.detailed_carpool_travel_costs import DetailedCarpoolTravelCostsTable


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
    path.inputs_hash = "path-" + graph_id
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


def test_road_worker_sends_each_update_and_closes(monkeypatch):
    requests = []
    responses = iter(["ready", "done", "done"])
    closed = []

    class Channel:
        def send(self, value):
            requests.append(value)

        def recv(self, timeout=None):
            return next(responses)

        def close(self):
            closed.append(True)
            return True

    channel = Channel()
    monkeypatch.setattr("mobility.transport.costs.path.shared_road_worker.pymizu.r_launcher", lambda **kwargs: lambda token, slot=None: None)
    monkeypatch.setattr("mobility.transport.costs.path.shared_road_worker.pymizu.Channel.create", lambda *args, **kwargs: channel)
    args = [str(index) for index in range(14)]
    worker = SharedRoadWorker(args)
    worker.run(args)
    worker.run(args)
    worker.close()
    assert requests[0][1:] == args
    assert requests[1:] == [["3", "10", "12"], ["3", "10", "12"]]
    assert closed == [True]


def test_shared_costs_keep_scenarios_and_missing_routes_separate(tmp_path, monkeypatch):
    """Two scenario columns become ordinary per-scenario costs in the resolver."""
    monkeypatch.setenv("MOBILITY_PROJECT_DATA_FOLDER", str(tmp_path))
    monkeypatch.setattr(PathTravelCosts, "asset_for_flow_asset", lambda self, flow: SimpleNamespace(congested_costs=flow.table))
    first, first_table = make_iteration_costs(tmp_path, "first")
    second, second_table = make_iteration_costs(tmp_path, "second", mode_name="other-road-mode")
    (tmp_path / "first.parquet").write_bytes(b"first flow")
    (tmp_path / "second.parquet").write_bytes(b"second flow")
    # Even a digest collision must not merge different input files.
    monkeypatch.setattr(
        "mobility.transport.costs.path.shared_congested_costs.hashlib.file_digest",
        lambda file, algorithm: SimpleNamespace(digest=lambda: b"same hash"),
    )
    calls = []

    class FakeRunner:
        def __init__(self, args):
            pass

        def close(self):
            pass

        def run(self, args):
            calls.append(args)
            pl.DataFrame({
                "from": [1, 2], "to": [2, 3],
                "distance_0": [1.0, 2.0], "time_0": [0.1, 0.2],
                "distance_1": [3.0, None], "time_1": [0.3, None],
            }).write_parquet(args[10])

    monkeypatch.setattr("mobility.transport.costs.path.shared_congested_costs.SharedRoadWorker", FakeRunner)
    with asset_resolution_context() as resolver:
        prepare_shared_congested_costs([first, second])
        a = resolver.in_memory_values[asset_graph_key(first_table)]
        b = resolver.in_memory_values[asset_graph_key(second_table)]
        assert len(calls) == 1
        assert a["distance"].tolist() == [1.0, 2.0]
        assert b["distance"].tolist() == [3.0]
        assert b["time"].tolist() == [0.3]
        for index in (3, 10, 12):
            assert not Path(calls[0][index]).exists()
        prepare_shared_congested_costs([first, second])
        assert len(calls) == 1


def test_identical_flow_files_share_one_assignment(tmp_path, monkeypatch):
    monkeypatch.setenv("MOBILITY_PROJECT_DATA_FOLDER", str(tmp_path))
    monkeypatch.setattr(PathTravelCosts, "asset_for_flow_asset", lambda self, flow: SimpleNamespace(congested_costs=flow.table))
    first, first_table = make_iteration_costs(tmp_path, "first")
    second, second_table = make_iteration_costs(tmp_path, "second")
    third, third_table = make_iteration_costs(tmp_path, "third")
    (tmp_path / "first.parquet").write_bytes(b"same flow")
    (tmp_path / "second.parquet").write_bytes(b"same flow")
    (tmp_path / "third.parquet").write_bytes(b"different flow")
    calls = []

    class FakeRunner:
        def __init__(self, args):
            pass

        def close(self):
            pass

        def run(self, args):
            calls.append(json.loads(Path(args[3]).read_text(encoding="utf-8")))
            pl.DataFrame({
                "from": [1], "to": [2],
                "distance_0": [1.0], "time_0": [0.1],
                "distance_1": [3.0], "time_1": [0.3],
            }).write_parquet(args[10])

    monkeypatch.setattr("mobility.transport.costs.path.shared_congested_costs.SharedRoadWorker", FakeRunner)
    with asset_resolution_context() as resolver:
        prepare_shared_congested_costs([first, second, third])
        assert len(calls) == 1
        assert len(calls[0]) == 2
        a = resolver.in_memory_values[asset_graph_key(first_table)]
        b = resolver.in_memory_values[asset_graph_key(second_table)]
        c = resolver.in_memory_values[asset_graph_key(third_table)]
        assert a is not b and a.equals(b)
        assert a["distance"].tolist() == [1.0]
        assert c["distance"].tolist() == [3.0]
        a.loc[0, "distance"] = 99.0
        assert b["distance"].tolist() == [1.0]


def test_route_requests_are_shared_only_for_matching_routing_inputs(tmp_path, monkeypatch):
    monkeypatch.setenv("MOBILITY_PROJECT_DATA_FOLDER", str(tmp_path))
    monkeypatch.setattr(PathTravelCosts, "asset_for_flow_asset", lambda self, flow: SimpleNamespace(congested_costs=flow.table))
    first, _ = make_iteration_costs(tmp_path, "first")
    second, _ = make_iteration_costs(tmp_path, "second")
    different_graph, _ = make_iteration_costs(tmp_path, "third", graph_id="other")
    different_distance, _ = make_iteration_costs(tmp_path, "fourth")
    different_zones, _ = make_iteration_costs(tmp_path, "fifth")
    different_assignment, _ = make_iteration_costs(tmp_path, "sixth")
    different_distance.modes[0].inputs["generalized_cost"].inputs["travel_costs"].routing_parameters.max_beeline_distance = 20.0
    different_zones.modes[0].inputs["generalized_cost"].inputs["travel_costs"].transport_zones.inputs_hash = "other-zones"
    different_assignment.modes[0].inputs["generalized_cost"].inputs["travel_costs"].congested_path_graph.inputs["congestion_assignment_max_iterations"] = 5
    for name in ("first", "second", "third", "fourth", "fifth", "sixth"):
        (tmp_path / (name + ".parquet")).write_bytes(name.encode())
    route_paths = []
    workers = []

    class FakeRunner:
        def __init__(self, args):
            workers.append(self)

        def close(self):
            pass

        def run(self, args):
            route_paths.append(Path(args[-1]))
            pl.DataFrame({"from": [1], "to": [2], "distance_0": [1.0], "time_0": [0.1]}).write_parquet(args[10])

    monkeypatch.setattr("mobility.transport.costs.path.shared_congested_costs.SharedRoadWorker", FakeRunner)
    with asset_resolution_context():
        for item in (first, second, different_graph, different_distance, different_zones, different_assignment):
            prepare_shared_congested_costs([item])
    assert route_paths[0] == route_paths[1]
    assert route_paths[0] == route_paths[5]
    assert len({route_paths[0], route_paths[2], route_paths[3], route_paths[4]}) == 4
    # The first two updates have the same inputs and use one worker; later
    # different routing or assignment inputs start a fresh one.
    assert len(workers) == 5


def test_incompatible_networks_are_not_batched(tmp_path, monkeypatch):
    """Different road networks are computed separately, including singleton groups."""
    monkeypatch.setenv("MOBILITY_PROJECT_DATA_FOLDER", str(tmp_path))
    monkeypatch.setattr(PathTravelCosts, "asset_for_flow_asset", lambda self, flow: SimpleNamespace(congested_costs=flow.table))
    first, _ = make_iteration_costs(tmp_path, "first")
    second, _ = make_iteration_costs(tmp_path, "second", graph_id="other")
    (tmp_path / "first.parquet").write_bytes(b"first")
    (tmp_path / "second.parquet").write_bytes(b"second")
    calls = []
    workers = []
    closed = []

    class FakeRunner:
        def __init__(self, args):
            workers.append(self)

        def close(self):
            closed.append(self)

        def run(self, args):
            calls.append(args)
            assert json.loads(Path(args[12]).read_text(encoding="utf-8")) == [""]
            pl.DataFrame({"from": [1], "to": [2], "distance_0": [1.0], "time_0": [0.1]}).write_parquet(args[10])

    monkeypatch.setattr("mobility.transport.costs.path.shared_congested_costs.SharedRoadWorker", FakeRunner)
    with asset_resolution_context() as resolver:
        prepare_shared_congested_costs([first, second])
        assert len(calls) == 2
        assert len(resolver.in_memory_values) == 2
        assert len(workers) == 2
        assert closed == workers[:1]
    assert closed == workers


def test_failed_road_worker_is_closed_and_not_reused(tmp_path, monkeypatch):
    monkeypatch.setenv("MOBILITY_PROJECT_DATA_FOLDER", str(tmp_path))
    monkeypatch.setattr(PathTravelCosts, "asset_for_flow_asset", lambda self, flow: SimpleNamespace(congested_costs=flow.table))
    iteration, table = make_iteration_costs(tmp_path, "first")
    (tmp_path / "first.parquet").write_bytes(b"flow")
    closed = []

    class FailedWorker:
        def __init__(self, args):
            pass

        def run(self, args):
            raise RuntimeError("road worker failed")

        def close(self):
            closed.append(True)

    monkeypatch.setattr("mobility.transport.costs.path.shared_congested_costs.SharedRoadWorker", FailedWorker)
    with asset_resolution_context() as resolver:
        with pytest.raises(RuntimeError, match="road worker failed"):
            prepare_shared_congested_costs([iteration])
        assert resolver.shared_road_worker is None
        assert resolver.shared_road_worker_key is None
        assert asset_graph_key(table) not in resolver.in_memory_values
    assert closed == [True]


def test_idle_watchdog_uses_standard_r_runner(tmp_path, monkeypatch):
    monkeypatch.setenv("MOBILITY_PROJECT_DATA_FOLDER", str(tmp_path))
    monkeypatch.setenv("MOBILITY_R_IDLE_TIMEOUT_SECONDS", "10")
    monkeypatch.setattr(PathTravelCosts, "asset_for_flow_asset", lambda self, flow: SimpleNamespace(congested_costs=flow.table))
    iteration, table = make_iteration_costs(tmp_path, "first")
    (tmp_path / "first.parquet").write_bytes(b"flow")
    calls = []

    class Runner:
        def __init__(self, script):
            pass

        def run(self, args):
            calls.append(True)
            pl.DataFrame({"from": [1], "to": [2], "distance_0": [1.0], "time_0": [0.1]}).write_parquet(args[10])

    monkeypatch.setattr("mobility.transport.costs.path.shared_congested_costs.RScriptRunner", Runner)
    with asset_resolution_context() as resolver:
        prepare_shared_congested_costs([iteration])
        assert resolver.shared_road_worker is None
        assert asset_graph_key(table) in resolver.in_memory_values
    assert calls == [True]


def test_all_identical_flows_use_a_single_assignment(tmp_path, monkeypatch):
    monkeypatch.setenv("MOBILITY_PROJECT_DATA_FOLDER", str(tmp_path))
    monkeypatch.setattr(PathTravelCosts, "asset_for_flow_asset", lambda self, flow: SimpleNamespace(congested_costs=flow.table))
    first, first_table = make_iteration_costs(tmp_path, "first")
    second, second_table = make_iteration_costs(tmp_path, "second")
    (tmp_path / "first.parquet").write_bytes(b"same")
    (tmp_path / "second.parquet").write_bytes(b"same")
    calls = []

    class FakeRunner:
        def __init__(self, args):
            pass

        def close(self):
            pass

        def run(self, args):
            calls.append(json.loads(Path(args[3]).read_text(encoding="utf-8")))
            pl.DataFrame({"from": [1], "to": [2], "distance_0": [1.0], "time_0": [0.1]}).write_parquet(args[10])

    monkeypatch.setattr("mobility.transport.costs.path.shared_congested_costs.SharedRoadWorker", FakeRunner)
    with asset_resolution_context() as resolver:
        prepare_shared_congested_costs([first, second])
        assert len(calls) == 1 and len(calls[0]) == 1
        assert resolver.in_memory_values[asset_graph_key(first_table)].equals(
            resolver.in_memory_values[asset_graph_key(second_table)]
        )


def test_single_scenario_shares_assigned_graph_with_carpool(tmp_path, monkeypatch):
    monkeypatch.setenv("MOBILITY_PROJECT_DATA_FOLDER", str(tmp_path))
    iteration, table = make_iteration_costs(tmp_path, "first")
    car_costs = iteration.modes[0].inputs["generalized_cost"].inputs["travel_costs"]
    assigned_graph = FakeAsset("assigned-graph", tmp_path / "unused-graph")
    monkeypatch.setattr(PathTravelCosts, "asset_for_flow_asset", lambda self, flow: SimpleNamespace(
        congested_costs=flow.table, congested_path_graph=assigned_graph,
    ))

    class FakeCarpoolCosts:
        def __init__(self, car_travel_costs):
            self.car_travel_costs = car_travel_costs

    monkeypatch.setattr(
        "mobility.transport.costs.path.shared_congested_costs.DetailedCarpoolTravelCosts", FakeCarpoolCosts,
    )
    iteration.modes.append(SimpleNamespace(inputs={
        "generalized_cost": SimpleNamespace(inputs={"travel_costs": FakeCarpoolCosts(car_costs)}),
    }))
    (tmp_path / "first.parquet").write_bytes(b"flow")
    calls = []

    class FakeRunner:
        def __init__(self, args):
            pass

        def close(self):
            pass

        def run(self, args):
            calls.append(args)
            graph_path = Path(json.loads(Path(args[12]).read_text(encoding="utf-8"))[0])
            graph_path.write_bytes(b"assigned graph")
            pl.DataFrame({"from": [1], "to": [2], "distance_0": [1.0], "time_0": [0.1]}).write_parquet(args[10])

    monkeypatch.setattr("mobility.transport.costs.path.shared_congested_costs.SharedRoadWorker", FakeRunner)
    with asset_resolution_context() as resolver:
        prepare_shared_congested_costs([iteration])
        graph_path = car_costs.get_congested_graph_path(iteration.congestion_flows.get())
        assert graph_path.read_bytes() == b"assigned graph"
        assert resolver.in_memory_values[asset_graph_key(table)]["time"].tolist() == [0.1]
        assert len(calls) == 1
        car_costs.transport_zones.study_area = SimpleNamespace(cache_path={"polygons": tmp_path / "area"})
        carpool = object.__new__(DetailedCarpoolTravelCostsTable)
        carpool.car_travel_costs = car_costs
        carpool.road_flow_asset = iteration.congestion_flows.get()
        carpool.congestion = True
        carpool.modal_transfer = SimpleNamespace(model_dump=lambda **kwargs: {})
        carpool.cache_path = tmp_path / "carpool.parquet"

        class FakeCarpoolRunner:
            def __init__(self, path):
                pass

            def run(self, args):
                assert args[2] == args[3] == str(graph_path)
                pl.DataFrame({"time": [1.0]}).write_parquet(args[-1])

        monkeypatch.setattr(
            "mobility.transport.modes.carpool.detailed.detailed_carpool_travel_costs.RScriptRunner", FakeCarpoolRunner,
        )
        assert carpool._compute_travel_costs()["time"].tolist() == [1.0]
        assert len(calls) == 1
        prepare_shared_congested_costs([iteration])
        assert len(calls) == 1
    assert not graph_path.exists()


def test_only_carpool_scenario_writes_a_graph(tmp_path, monkeypatch):
    monkeypatch.setenv("MOBILITY_PROJECT_DATA_FOLDER", str(tmp_path))
    first, _ = make_iteration_costs(tmp_path, "first")
    second, _ = make_iteration_costs(tmp_path, "second")
    car_costs = first.modes[0].inputs["generalized_cost"].inputs["travel_costs"]
    graph = FakeAsset("carpool-graph")
    monkeypatch.setattr(PathTravelCosts, "asset_for_flow_asset", lambda self, flow: SimpleNamespace(
        congested_costs=flow.table, congested_path_graph=graph,
    ))

    class FakeCarpoolCosts:
        def __init__(self, car_travel_costs):
            self.car_travel_costs = car_travel_costs

    monkeypatch.setattr(
        "mobility.transport.costs.path.shared_congested_costs.DetailedCarpoolTravelCosts", FakeCarpoolCosts,
    )
    first.modes.append(SimpleNamespace(inputs={
        "generalized_cost": SimpleNamespace(inputs={"travel_costs": FakeCarpoolCosts(car_costs)}),
    }))
    (tmp_path / "first.parquet").write_bytes(b"first")
    (tmp_path / "second.parquet").write_bytes(b"second")

    class FakeRunner:
        def __init__(self, args):
            pass

        def close(self):
            pass

        def run(self, args):
            graph_paths = json.loads(Path(args[12]).read_text(encoding="utf-8"))
            assert graph_paths[0] and graph_paths[1] == ""
            Path(graph_paths[0]).write_bytes(b"graph")
            pl.DataFrame({
                "from": [1], "to": [2], "distance_0": [1.0], "time_0": [0.1],
                "distance_1": [2.0], "time_1": [0.2],
            }).write_parquet(args[10])

    monkeypatch.setattr("mobility.transport.costs.path.shared_congested_costs.SharedRoadWorker", FakeRunner)
    with asset_resolution_context() as resolver:
        prepare_shared_congested_costs([first, second])
        assert len(resolver.congested_graph_paths) == 1
        assert len(resolver.in_memory_values) == 2


def test_missing_requested_graph_does_not_publish_costs(tmp_path, monkeypatch):
    monkeypatch.setenv("MOBILITY_PROJECT_DATA_FOLDER", str(tmp_path))
    iteration, _ = make_iteration_costs(tmp_path, "first")
    car_costs = iteration.modes[0].inputs["generalized_cost"].inputs["travel_costs"]
    graph = FakeAsset("missing-graph")
    monkeypatch.setattr(PathTravelCosts, "asset_for_flow_asset", lambda self, flow: SimpleNamespace(
        congested_costs=flow.table, congested_path_graph=graph,
    ))

    class FakeCarpoolCosts:
        def __init__(self, car_travel_costs):
            self.car_travel_costs = car_travel_costs

    monkeypatch.setattr(
        "mobility.transport.costs.path.shared_congested_costs.DetailedCarpoolTravelCosts", FakeCarpoolCosts,
    )
    iteration.modes.append(SimpleNamespace(inputs={
        "generalized_cost": SimpleNamespace(inputs={"travel_costs": FakeCarpoolCosts(car_costs)}),
    }))
    (tmp_path / "first.parquet").write_bytes(b"flow")

    class FakeRunner:
        def __init__(self, args):
            pass

        def close(self):
            pass

        def run(self, args):
            pl.DataFrame({"from": [1], "to": [2], "distance_0": [1.0], "time_0": [0.1]}).write_parquet(args[10])

    monkeypatch.setattr("mobility.transport.costs.path.shared_congested_costs.SharedRoadWorker", FakeRunner)
    with asset_resolution_context() as resolver:
        with pytest.raises(RuntimeError, match="did not produce the requested graph"):
            prepare_shared_congested_costs([iteration])
        assert not resolver.in_memory_values
        assert not resolver.congested_graph_paths
