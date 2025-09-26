import os
import pathlib
import types
import builtins

import pytest
import pandas as pandas
import numpy as numpy

# ===========================================================
# Project directory fixture: sets up environment variables
# ===========================================================

@pytest.fixture(scope="session")
def fake_inputs_hash():
    return "deadbeefdeadbeefdeadbeefdeadbeef"


@pytest.fixture(scope="session")
def project_dir(tmp_path_factory):
    project_data_directory = tmp_path_factory.mktemp("mobility_project_data")
    os.environ["MOBILITY_PROJECT_DATA_FOLDER"] = str(project_data_directory)

    package_data_directory = tmp_path_factory.mktemp("mobility_package_data")
    os.environ["MOBILITY_PACKAGE_DATA_FOLDER"] = str(package_data_directory)

    (pathlib.Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) / "gtfs").mkdir(parents=True, exist_ok=True)

    return pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"])

# ===========================================================
# Patch Asset.__init__ so it does not call .get()
# ===========================================================

@pytest.fixture(autouse=True)
def patch_asset_init(monkeypatch, project_dir, fake_inputs_hash):
    """
    Replace mobility.asset.Asset.__init__ to avoid calling .get().
    It simply sets: inputs, cache_path, hash_path, inputs_hash.
    """
    try:
        import mobility.asset
    except Exception:
        mobility_module = types.SimpleNamespace()
        mobility_module.asset = types.SimpleNamespace()
        def dummy_init(*args, **kwargs): ...
        mobility_module.asset.Asset = type("Asset", (), {"__init__": dummy_init})
        builtins.__dict__.setdefault("mobility", mobility_module)

    def fake_asset_init(self, provided_inputs, provided_cache_path):
        base_filename = pathlib.Path(provided_cache_path).name
        hashed_filename = f"{fake_inputs_hash}-{base_filename}"
        self.inputs = provided_inputs
        self.inputs_hash = fake_inputs_hash
        self.cache_path = project_dir / hashed_filename
        self.hash_path = project_dir / f"{fake_inputs_hash}.sha1"

    monkeypatch.setattr("mobility.asset.Asset.__init__", fake_asset_init, raising=True)

# ===========================================================
# Patch rich.progress.Progress to no-op
# ===========================================================

@pytest.fixture(autouse=True)
def no_op_progress(monkeypatch):
    try:
        import rich.progress as rich_progress_module
    except Exception:
        return

    class NoOpProgressClass:
        def __init__(self, *args, **kwargs): ...
        def __enter__(self): return self
        def __exit__(self, exc_type, exc_value, traceback): return False
        def add_task(self, *args, **kwargs): return 0
        def update(self, *args, **kwargs): ...
        def advance(self, *args, **kwargs): ...
        def track(self, iterable, *args, **kwargs):
            for element in iterable:
                yield element

    monkeypatch.setattr(rich_progress_module, "Progress", NoOpProgressClass, raising=True)

# ===========================================================
# Patch numpy._methods._sum / _amax to ignore _NoValue sentinel
# ===========================================================

@pytest.fixture(autouse=True)
def patch_numpy_private_methods(monkeypatch):
    sentinel_value = getattr(numpy, "_NoValue", object())
    if hasattr(numpy, "_methods"):
        numpy_methods_module = numpy._methods

        if hasattr(numpy_methods_module, "_sum"):
            original_sum_function = numpy_methods_module._sum

            def wrapped_sum_function(array_like, axis=sentinel_value, dtype=sentinel_value,
                                     out=sentinel_value, keepdims=False, initial=sentinel_value, where=True):
                axis = None if axis is sentinel_value else axis
                dtype = None if dtype is sentinel_value else dtype
                out = None if out is sentinel_value else out
                initial = None if initial is sentinel_value else initial
                return original_sum_function(array_like, axis=axis, dtype=dtype, out=out,
                                             keepdims=keepdims, initial=initial, where=where)

            monkeypatch.setattr(numpy_methods_module, "_sum", wrapped_sum_function, raising=True)

        if hasattr(numpy_methods_module, "_amax"):
            original_amax_function = numpy_methods_module._amax

            def wrapped_amax_function(array_like, axis=sentinel_value, out=sentinel_value,
                                      keepdims=False, initial=sentinel_value, where=True):
                axis = None if axis is sentinel_value else axis
                out = None if out is sentinel_value else out
                initial = None if initial is sentinel_value else initial
                return original_amax_function(array_like, axis=axis, out=out,
                                              keepdims=keepdims, initial=initial, where=where)

            monkeypatch.setattr(numpy_methods_module, "_amax", wrapped_amax_function, raising=True)

# ===========================================================
# Parquet stubs helper fixture
# ===========================================================

@pytest.fixture
def parquet_stubs(monkeypatch):
    captured_written_paths = []
    read_function_container = {"function": None}

    def set_read_function(read_function):
        read_function_container["function"] = read_function
        monkeypatch.setattr(pandas, "read_parquet", lambda path, *args, **kwargs: read_function(path), raising=True)

    def set_write_function(captured_list=None):
        def fake_to_parquet_method(self, path, *args, **kwargs):
            if captured_list is not None:
                captured_list.append(path)
            return self
        monkeypatch.setattr(pandas.DataFrame, "to_parquet", fake_to_parquet_method, raising=True)

    return {"set_read": set_read_function, "set_write": set_write_function}

# ===========================================================
# Deterministic shortuuid helper
# ===========================================================

@pytest.fixture
def deterministic_shortuuid(monkeypatch):
    try:
        import shortuuid
    except Exception:
        return

    counter = {"value": 0}

    def fake_uuid_function():
        counter["value"] += 1
        return f"shortuuid-{counter['value']:04d}"

    monkeypatch.setattr(shortuuid, "uuid", fake_uuid_function, raising=True)

# ===========================================================
# Fake transport zones and population asset fixtures
# ===========================================================

@pytest.fixture
def fake_transport_zones(tmp_path):
    class FakeTransportZonesClass:
        def __init__(self, cache_path):
            self.cache_path = cache_path

        def get(self):
            return pandas.DataFrame(
                {
                    "transport_zone_id": [1, 2],
                    "urban_unit_category": ["A", "B"],
                    "page_url": ["https://example.com/ds1", "https://example.com/ds2"],
                }
            )

    cache_path = tmp_path / "transport_zones.gpkg"
    return FakeTransportZonesClass(cache_path=cache_path)


@pytest.fixture
def fake_population_asset(fake_transport_zones):
    class FakePopulationAssetClass:
        def __init__(self):
            self.inputs = {"transport_zones": fake_transport_zones}

        def get(self):
            return pandas.DataFrame({"population": [100, 200]})

    return FakePopulationAssetClass()

