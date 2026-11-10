from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from mobility.runtime.assets.file_asset import FileAsset
from mobility.runtime.assets.graph import asset_graph_key
from mobility.runtime.assets.resolver import asset_resolution_context
from mobility.runtime.temporary import TemporaryDiskLimitExceeded
from mobility.transport.costs.path import path_travel_costs as path_travel_costs_module
from mobility.transport.costs.path.path_travel_costs import PathTravelCostsTable


@pytest.mark.parametrize("existing_cache", [False, True])
def test_path_cost_handoff_uses_owned_files(tmp_path, monkeypatch, existing_cache):
    """Temporary route costs must not overwrite or remove a shared cache."""
    monkeypatch.setenv("MOBILITY_PROJECT_DATA_FOLDER", str(tmp_path))
    monkeypatch.delenv("MOBILITY_TEMPORARY_DISK_LIMIT_BYTES", raising=False)
    asset = object.__new__(PathTravelCostsTable)
    FileAsset.__init__(asset, {"name": "path-cost-test"}, tmp_path / "costs.parquet")
    asset.mode_name = "car"
    asset.transport_zones = SimpleNamespace(cache_path=tmp_path / "zones", get=lambda: None)
    asset.routing_graph = SimpleNamespace(get=lambda: tmp_path / "graph")
    asset.routing_parameters = SimpleNamespace(max_beeline_distance=100)
    asset.backend = "ch"
    asset.cch_graph = None
    if existing_cache:
        asset.cache_path.write_bytes(b"existing cache")
        asset.hash_path.write_text("different input hash", encoding="utf-8")

    class FakeR:
        def __init__(self, script):
            pass

        def run(self, *, args):
            pd.DataFrame({"time": [2.0]}).to_parquet(args[-1], index=False)

    monkeypatch.setattr(path_travel_costs_module, "RScriptRunner", FakeR)
    with asset_resolution_context() as resolver:
        assert asset.get()["time"].to_list() == [2.0]
        output = resolver.execution_cost_output_paths[asset_graph_key(asset)]
        assert output.exists() and output.parent.parent == tmp_path / "temporary"
        resolver.release_in_memory_asset(asset)
        assert asset.get()["time"].to_list() == [2.0]
        assert output.exists()
    assert not output.exists()
    if existing_cache:
        assert asset.cache_path.read_bytes() == b"existing cache"
        assert asset.hash_path.read_text(encoding="utf-8") == "different input hash"
    else:
        assert not asset.cache_path.exists()
        assert not asset.hash_path.exists()


def test_path_cost_handoff_cleans_up_when_limit_is_exceeded(tmp_path, monkeypatch):
    monkeypatch.setenv("MOBILITY_PROJECT_DATA_FOLDER", str(tmp_path))
    monkeypatch.setenv("MOBILITY_TEMPORARY_DISK_LIMIT_BYTES", "1")
    asset = object.__new__(PathTravelCostsTable)
    FileAsset.__init__(asset, {"name": "path-cost-limit"}, tmp_path / "costs.parquet")
    asset.mode_name = "car"
    asset.transport_zones = SimpleNamespace(cache_path=tmp_path / "zones", get=lambda: None)
    asset.routing_graph = SimpleNamespace(get=lambda: tmp_path / "graph")
    asset.routing_parameters = SimpleNamespace(max_beeline_distance=100)
    asset.backend = "ch"
    asset.cch_graph = None

    class FakeR:
        def __init__(self, script):
            pass

        def run(self, *, args):
            Path(args[-1]).write_bytes(b"too large")

    monkeypatch.setattr(path_travel_costs_module, "RScriptRunner", FakeR)
    with pytest.raises(TemporaryDiskLimitExceeded):
        with asset_resolution_context():
            asset.get()
    assert not list((tmp_path / "temporary").glob("mobility-run-assets-*"))
    assert not asset.cache_path.exists()


@pytest.mark.parametrize("fail_second", [False, True])
def test_shared_path_cost_lives_until_last_consumer_or_failure(tmp_path, monkeypatch, fail_second):
    monkeypatch.setenv("MOBILITY_PROJECT_DATA_FOLDER", str(tmp_path))
    monkeypatch.delenv("MOBILITY_TEMPORARY_DISK_LIMIT_BYTES", raising=False)
    asset = object.__new__(PathTravelCostsTable)
    FileAsset.__init__(asset, {"name": "shared-cost"}, tmp_path / "costs.parquet")
    asset.mode_name = "car"
    asset.transport_zones = SimpleNamespace(cache_path=tmp_path / "zones", get=lambda: None)
    asset.routing_graph = SimpleNamespace(get=lambda: tmp_path / "graph")
    asset.routing_parameters = SimpleNamespace(max_beeline_distance=100)
    asset.backend = "ch"
    asset.cch_graph = None
    asset.cache_path.write_bytes(b"previous shared cache")
    asset.hash_path.write_text("different input hash", encoding="utf-8")

    class FakeR:
        def __init__(self, script):
            pass

        def run(self, *, args):
            pd.DataFrame({"time": [2.0]}).to_parquet(args[-1], index=False)

    monkeypatch.setattr(path_travel_costs_module, "RScriptRunner", FakeR)
    with asset_resolution_context() as resolver:
        first, second = ("state", "first"), ("state", "second")
        resolver._register_execution_cost_use(asset, first)
        resolver._register_execution_cost_use(asset, second)
        assert asset.get()["time"].to_list() == [2.0]
        output = resolver.execution_cost_output_paths[asset_graph_key(asset)]
        resolver._consume_execution_cost_use(first)
        assert output.exists()
        if fail_second:
            with pytest.raises(RuntimeError, match="consumer failed"):
                assert asset.get()["time"].to_list() == [2.0]
                raise RuntimeError("consumer failed")
        else:
            assert asset.get()["time"].to_list() == [2.0]
            resolver._consume_execution_cost_use(second)
            assert not output.exists()
    assert not output.exists()
    assert asset.cache_path.read_bytes() == b"previous shared cache"
    assert asset.hash_path.read_text(encoding="utf-8") == "different input hash"
