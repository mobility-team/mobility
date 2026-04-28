from types import SimpleNamespace

import pandas as pd
import pytest


@pytest.fixture(autouse=True)
def patch_asset_init(monkeypatch):
    """Make Asset.__init__ a no-op so TravelCosts doesn't run heavy I/O."""
    import mobility.runtime.assets.asset as asset_mod

    def fake_init(self, inputs, cache_path):
        self.inputs = inputs
        self.cache_path = cache_path

    monkeypatch.setattr(asset_mod.Asset, "__init__", fake_init)

@pytest.fixture
def fake_transport_zones(tmp_path):
    """Minimal transport_zones stand-in with only .cache_path (what the code needs)."""
    return SimpleNamespace(cache_path=tmp_path / "transport_zones.parquet")


@pytest.fixture
def patch_osmdata(monkeypatch, tmp_path):
    """Fake OSMData to avoid real parsing and capture init args."""
    import mobility.travel_costs as mod

    created = {}

    class FakeOSMData:
        def __init__(self, tz, modes):
            created["tz"] = tz
            created["modes"] = modes
            self.cache_path = tmp_path / "osm.parquet"

    monkeypatch.setattr(mod, "OSMData", FakeOSMData)
    return created


@pytest.fixture
def patch_rscript(monkeypatch):
    """Fake RScriptRunner to capture script paths and run() args."""
    import mobility.travel_costs as mod

    calls = {"scripts": [], "runs": []}

    class FakeRScript:
        def __init__(self, script_path):
            calls["scripts"].append(str(script_path))
        def run(self, args):
            calls["runs"].append(list(args))

    monkeypatch.setattr(mod, "RScriptRunner", FakeRScript)
    return calls

