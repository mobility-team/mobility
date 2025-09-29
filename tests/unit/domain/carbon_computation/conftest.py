# tests/unit/carbon_computation/conftest.py
import os
import sys
import types
from pathlib import Path

import numpy as np
import pandas as pd
import pytest


@pytest.fixture(scope="function")
def project_directory_path(tmp_path, monkeypatch):
    """
    Ensure code that relies on project data folders stays inside a temporary directory.
    """
    project_dir = tmp_path / "project-data"
    package_dir = tmp_path / "package-data"
    monkeypatch.setenv("MOBILITY_PROJECT_DATA_FOLDER", str(project_dir))
    monkeypatch.setenv("MOBILITY_PACKAGE_DATA_FOLDER", str(package_dir))
    return project_dir


@pytest.fixture(scope="function", autouse=True)
def autouse_patch_asset_init(monkeypatch, project_directory_path):
    """
    Stub mobility.asset.Asset.__init__ so it does not call .get() and does not serialize inputs.
    It only sets inputs, inputs_hash, cache_path, and hash_path.
    """
    fake_inputs_hash_value = "deadbeefdeadbeefdeadbeefdeadbeef"

    if "mobility" not in sys.modules:
        sys.modules["mobility"] = types.ModuleType("mobility")
    if "mobility.asset" not in sys.modules:
        sys.modules["mobility.asset"] = types.ModuleType("mobility.asset")

    if not hasattr(sys.modules["mobility.asset"], "Asset"):
        class PlaceholderAsset:
            def __init__(self, *args, **kwargs):
                pass
        setattr(sys.modules["mobility.asset"], "Asset", PlaceholderAsset)

    def stubbed_asset_init(self, inputs=None, cache_path="cache.parquet", **_ignored):
        self.inputs = {} if inputs is None else inputs
        self.inputs_hash = fake_inputs_hash_value
        file_name_only = Path(cache_path).name
        base_directory_path = Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"])
        self.cache_path = base_directory_path / f"{self.inputs_hash}-{file_name_only}"
        self.hash_path = base_directory_path / f"{self.inputs_hash}.json"

    monkeypatch.setattr(sys.modules["mobility.asset"].Asset, "__init__", stubbed_asset_init, raising=True)


@pytest.fixture(scope="function", autouse=True)
def autouse_no_op_rich_progress(monkeypatch):
    class NoOpProgress:
        def __init__(self, *args, **kwargs): ...
        def __enter__(self): return self
        def __exit__(self, exc_type, exc, tb): return False
        def add_task(self, *args, **kwargs): return 1
        def update(self, *args, **kwargs): ...
        def advance(self, *args, **kwargs): ...
        def stop(self): ...
        def start(self): ...
        def track(self, sequence, *args, **kwargs):
            for item in sequence:
                yield item

    try:
        import rich.progress  # type: ignore
        monkeypatch.setattr(rich.progress, "Progress", NoOpProgress, raising=True)
    except Exception:
        if "rich" not in sys.modules:
            sys.modules["rich"] = types.ModuleType("rich")
        if "rich.progress" not in sys.modules:
            sys.modules["rich.progress"] = types.ModuleType("rich.progress")
        setattr(sys.modules["rich.progress"], "Progress", NoOpProgress)


@pytest.fixture(scope="function", autouse=True)
def autouse_patch_numpy_private_methods(monkeypatch):
    """
    Wrap NumPy private _methods to ignore the _NoValue sentinel to avoid rare pandas/NumPy issues.
    """
    try:
        from numpy._core import _methods as numpy_core_methods_module
    except Exception:
        try:
            from numpy.core import _methods as numpy_core_methods_module  # fallback
        except Exception:
            return

    import numpy as np
    numpy_no_value_sentinel = getattr(np, "_NoValue", None)
    original_sum_function = getattr(numpy_core_methods_module, "_sum", None)
    original_amax_function = getattr(numpy_core_methods_module, "_amax", None)

    def wrap_ignoring_no_value(function):
        if function is None:
            return None

        def wrapper(a, *args, **kwargs):
            if numpy_no_value_sentinel is not None and args:
                args = tuple(item for item in args if item is not numpy_no_value_sentinel)
            if numpy_no_value_sentinel is not None and kwargs:
                kwargs = {key: value for key, value in kwargs.items() if value is not numpy_no_value_sentinel}
            return function(a, *args, **kwargs)
        return wrapper

    if original_sum_function is not None:
        monkeypatch.setattr(numpy_core_methods_module, "_sum", wrap_ignoring_no_value(original_sum_function), raising=True)
    if original_amax_function is not None:
        monkeypatch.setattr(numpy_core_methods_module, "_amax", wrap_ignoring_no_value(original_amax_function), raising=True)


@pytest.fixture
def parquet_stubs(monkeypatch):
    """
    Opt-in parquet stub if you ever need to assert paths for parquet reads/writes.
    """
    state = {"read_return_dataframe": None, "last_written_path": None, "read_path": None}

    def set_read_result(df: pd.DataFrame):
        state["read_return_dataframe"] = df

    def fake_read_parquet(path, *args, **kwargs):
        state["read_path"] = Path(path)
        return pd.DataFrame() if state["read_return_dataframe"] is None else state["read_return_dataframe"].copy()

    def fake_to_parquet(self, path, *args, **kwargs):
        state["last_written_path"] = Path(path)

    state["set_read_result"] = set_read_result
    monkeypatch.setattr(pd, "read_parquet", fake_read_parquet, raising=True)
    monkeypatch.setattr(pd.DataFrame, "to_parquet", fake_to_parquet, raising=True)
    return state

