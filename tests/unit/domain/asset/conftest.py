import os
import sys
import types
from pathlib import Path
import pytest
import pandas as pd
import numpy as np

# Optional geopandas
try:
    import geopandas as gpd  # noqa: F401
    _HAS_GPD = True
except Exception:
    _HAS_GPD = False


@pytest.fixture
def project_dir(tmp_path, monkeypatch):
    """Sets MOBILITY_PROJECT_DATA_FOLDER to tmp_path and returns it."""
    monkeypatch.setenv("MOBILITY_PROJECT_DATA_FOLDER", str(tmp_path))
    return tmp_path


@pytest.fixture(autouse=True)
def patch_asset_init(monkeypatch, project_dir):
    """
    Stub Asset.__init__ but keep a reference to the original on the class.
    """
    try:
        from mobility.asset import Asset
    except ImportError as exc:
        pytest.skip(f"Cannot import mobility.asset.Asset: {exc}")
        return

    # Store the real __init__ on the class (once)
    if not hasattr(Asset, "__original_init_for_tests"):
        Asset.__original_init_for_tests = Asset.__init__

    fake_inputs_hash_value = "deadbeefdeadbeefdeadbeefdeadbeef"

    def _stubbed_init(self, inputs: dict):
        self.value = None
        self.inputs = inputs or {}
        self.inputs_hash = fake_inputs_hash_value
        filename = f"{self.__class__.__name__.lower()}.parquet"
        self.cache_path = Path(project_dir) / f"{fake_inputs_hash_value}-{filename}"
        self.hash_path = Path(project_dir) / f"{fake_inputs_hash_value}.hash"
        for key, value in self.inputs.items():
            setattr(self, key, value)

    monkeypatch.setattr(Asset, "__init__", _stubbed_init, raising=True)


@pytest.fixture
def use_real_asset_init(monkeypatch):
    """
    Restore the original Asset.__init__ for tests that need the real hashing behavior.
    """
    from mobility.asset import Asset
    original = getattr(Asset, "__original_init_for_tests", None)
    if original is None:
        pytest.fail("Asset.__original_init_for_tests missing; patch_asset_init did not run")
    monkeypatch.setattr(Asset, "__init__", original, raising=True)
    return Asset


@pytest.fixture
def fake_inputs_hash():
    return "deadbeefdeadbeefdeadbeefdeadbeef"


@pytest.fixture(autouse=True)
def no_op_progress(monkeypatch):
    """Stub rich.progress.Progress to no-op."""
    class _NoOpProgress:
        def __enter__(self): return self
        def __exit__(self, exc_type, exc, tb): return False
        def add_task(self, *a, **k): return 0
        def update(self, *a, **k): return None
        def advance(self, *a, **k): return None
        def track(self, iterable, *a, **k):
            for x in iterable:
                yield x
        def stop(self): return None

    try:
        import rich.progress
        monkeypatch.setattr(rich.progress, "Progress", _NoOpProgress, raising=True)
    except ImportError:
        pass


@pytest.fixture(autouse=True)
def patch_numpy__methods(monkeypatch):
    try:
        from numpy.core import _methods
        from numpy import _NoValue
    except Exception:
        return

    def _wrap(func):
        def inner(a, axis=None, dtype=None, out=None, keepdims=_NoValue, initial=_NoValue, where=_NoValue):
            if keepdims is _NoValue:
                keepdims = False
            if initial is _NoValue:
                initial = None
            if where is _NoValue:
                where = True
            return func(a, axis=axis, dtype=dtype, out=out, keepdims=keepdims, initial=initial, where=where)
        return inner

    if hasattr(_methods, "_sum"):
        monkeypatch.setattr(_methods, "_sum", _wrap(_methods._sum), raising=True)
    if hasattr(_methods, "_amax"):
        monkeypatch.setattr(_methods, "_amax", _wrap(_methods._amax), raising=True)


@pytest.fixture
def parquet_stubs(monkeypatch):
    state = {"last_written_path": None, "last_read_path": None, "reads": 0, "writes": 0,
             "read_return_df": pd.DataFrame({"__empty__": []})}

    def _read(path, *a, **k):
        state["last_read_path"] = Path(path)
        state["reads"] += 1
        return state["read_return_df"]

    def _write(self, path, *a, **k):
        state["last_written_path"] = Path(path)
        state["writes"] += 1

    class Controller:
        @property
        def last_written_path(self): return state["last_written_path"]
        @property
        def last_read_path(self): return state["last_read_path"]
        @property
        def reads(self): return state["reads"]
        @property
        def writes(self): return state["writes"]
        def stub_read(self, df): 
            state["read_return_df"] = df
            monkeypatch.setattr(pd, "read_parquet", _read, raising=True)
        def capture_writes(self):
            monkeypatch.setattr(pd.DataFrame, "to_parquet", _write, raising=True)

    return Controller()


@pytest.fixture
def deterministic_shortuuid(monkeypatch):
    try:
        import shortuuid
    except ImportError:
        pytest.skip("shortuuid not installed")
        return
    counter = {"i": 0}
    def fake_uuid():
        counter["i"] += 1
        return f"shortuuid-{counter['i']:04d}"
    monkeypatch.setattr(shortuuid, "uuid", fake_uuid, raising=True)


@pytest.fixture
def fake_transport_zones():
    data = {"transport_zone_id": [1, 2],
            "urban_unit_category": ["A", "B"],
            "geometry": [None, None]}
    if _HAS_GPD:
        import geopandas as gpd
        return gpd.GeoDataFrame(data, geometry="geometry")
    return pd.DataFrame(data)


@pytest.fixture
def fake_population_asset(fake_transport_zones):
    class PopAsset:
        def __init__(self):
            self.inputs = {"transport_zones": fake_transport_zones}
        def get(self):
            return pd.DataFrame({"transport_zone_id": [1, 2],
                                 "population": [100, 200]})
    return PopAsset()


@pytest.fixture(autouse=True)
def deterministic_pandas_sample(monkeypatch):
    def _df_sample_first(self, n=None, frac=None, **kwargs):
        if n is not None:
            return self.iloc[:n].copy()
        if frac is not None:
            return self.iloc[: int(len(self) * frac)].copy()
        return self.iloc[:1].copy()

    def _s_sample_first(self, n=None, frac=None, **kwargs):
        if n is not None:
            return self.iloc[:n].copy()
        if frac is not None:
            return self.iloc[: int(len(self) * frac)].copy()
        return self.iloc[:1].copy()

    monkeypatch.setattr(pd.DataFrame, "sample", _df_sample_first, raising=True)
    monkeypatch.setattr(pd.Series, "sample", _s_sample_first, raising=True)
