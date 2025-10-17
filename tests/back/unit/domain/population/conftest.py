import sys
import types
import pathlib
import itertools
import logging

import pytest
import pandas as pd
import numpy as np


# --------------------------------------------------------------------------------------
# Create minimal dummy modules so mobility.population can import safely.
# --------------------------------------------------------------------------------------

def _ensure_dummy_module(module_name: str):
    if module_name in sys.modules:
        return sys.modules[module_name]
    module = types.ModuleType(module_name)
    sys.modules[module_name] = module
    return module

mobility_package = _ensure_dummy_module("mobility")
mobility_package.__path__ = []

file_asset_module = _ensure_dummy_module("mobility.file_asset")
parsers_module = _ensure_dummy_module("mobility.parsers")
parsers_admin_module = _ensure_dummy_module("mobility.parsers.admin_boundaries")
asset_module = _ensure_dummy_module("mobility.asset")

class _DummyFileAsset:
    def __init__(self, *args, **kwargs):
        self.inputs = args[0] if args else {}
        self.cache_path = args[1] if len(args) > 1 else {}

setattr(file_asset_module, "FileAsset", _DummyFileAsset)

class _DummyAsset:
    def __init__(self, *args, **kwargs):
        pass
setattr(asset_module, "Asset", _DummyAsset)

# Defaults (overridden by fixtures below)
class _DummyCityLegalPopulation:
    def get(self):
        return pd.DataFrame({"local_admin_unit_id": [], "legal_population": []})
setattr(parsers_module, "CityLegalPopulation", _DummyCityLegalPopulation)

class _DummyCensusLocalizedIndividuals:
    def __init__(self, region=None):
        self.region = region
    def get(self):
        return pd.DataFrame()
setattr(parsers_module, "CensusLocalizedIndividuals", _DummyCensusLocalizedIndividuals)

def _dummy_regions_boundaries():
    return pd.DataFrame({"INSEE_REG": [], "geometry": []})
def _dummy_cities_boundaries():
    return pd.DataFrame({"INSEE_COM": [], "INSEE_CAN": []})
setattr(parsers_admin_module, "get_french_regions_boundaries", _dummy_regions_boundaries)
setattr(parsers_admin_module, "get_french_cities_boundaries", _dummy_cities_boundaries)


# --------------------------------------------------------------------------------------
# Project/environment fixtures
# --------------------------------------------------------------------------------------

@pytest.fixture
def project_dir(tmp_path, monkeypatch):
    """Isolated project data folder for tests."""
    monkeypatch.setenv("MOBILITY_PROJECT_DATA_FOLDER", str(tmp_path))
    monkeypatch.setenv("MOBILITY_PACKAGE_DATA_FOLDER", str(tmp_path))
    return tmp_path


@pytest.fixture
def fake_inputs_hash():
    return "deadbeefdeadbeefdeadbeefdeadbeef"


@pytest.fixture(autouse=True)
def patch_asset_init(monkeypatch, project_dir, fake_inputs_hash):
    """
    Patch both mobility.asset.Asset and mobility.file_asset.FileAsset __init__ so it:
      - only sets attributes,
      - sets self.inputs_hash and self.hash_path,
      - rewrites cache paths to <project_dir>/<hash>-<base_name>,
      - mirrors every key from inputs onto the instance (e.g., self.switzerland_census).
    """
    def _patch_for(qualified: str):
        module_name, class_name = qualified.rsplit(".", 1)
        module = sys.modules.get(module_name)
        if not module or not hasattr(module, class_name):
            return
        class_object = getattr(module, class_name)

        def _init(self, inputs, cache_path):
            self.inputs = inputs
            if isinstance(inputs, dict):
                for key, value in inputs.items():
                    setattr(self, key, value)
            self.inputs_hash = fake_inputs_hash
            self.hash_path = pathlib.Path(project_dir) / f"{fake_inputs_hash}.hash"
            if isinstance(cache_path, dict):
                rewritten = {}
                for key, given_path in cache_path.items():
                    base_name = pathlib.Path(given_path).name
                    rewritten[key] = pathlib.Path(project_dir) / f"{fake_inputs_hash}-{base_name}"
                self.cache_path = rewritten
            else:
                base_name = pathlib.Path(cache_path).name
                self.cache_path = pathlib.Path(project_dir) / f"{fake_inputs_hash}-{base_name}"

        monkeypatch.setattr(class_object, "__init__", _init, raising=True)

    _patch_for("mobility.asset.Asset")
    _patch_for("mobility.file_asset.FileAsset")


@pytest.fixture(autouse=True)
def no_op_progress(monkeypatch):
    """Stub rich.progress.Progress to a no-op."""
    class _NoOpProgress:
        def __enter__(self): return self
        def __exit__(self, exc_type, exc, tb): return False
        def add_task(self, *a, **k): return 0
        def update(self, *a, **k): return None
    try:
        import rich.progress as rich_progress_module
        monkeypatch.setattr(rich_progress_module, "Progress", _NoOpProgress, raising=True)
    except Exception:
        pass


@pytest.fixture(autouse=True)
def patch_numpy__methods(monkeypatch):
    """
    Wrap NumPy private _methods._sum/_amax to ignore np._NoValue sentinel.
    Prevents pandas/NumPy _NoValueType crash paths.
    """
    try:
        from numpy import _methods as numpy_private_methods
        numpy_no_value = getattr(np, "_NoValue", None)
    except Exception:
        numpy_private_methods = None
        numpy_no_value = None

    def _clean(kwargs: dict):
        cleaned = dict(kwargs)
        for key in ("initial", "where", "dtype", "out", "keepdims"):
            if cleaned.get(key, None) is numpy_no_value:
                cleaned.pop(key, None)
        return cleaned

    if numpy_private_methods is not None and hasattr(numpy_private_methods, "_sum"):
        def safe_sum(a, axis=None, dtype=None, out=None, keepdims=False, initial=np._NoValue, where=np._NoValue):
            return np.sum(**_clean(locals()))
        monkeypatch.setattr(numpy_private_methods, "_sum", safe_sum, raising=True)

    if numpy_private_methods is not None and hasattr(numpy_private_methods, "_amax"):
        def safe_amax(a, axis=None, out=None, keepdims=False, initial=np._NoValue, where=np._NoValue):
            return np.amax(**_clean(locals()))
        monkeypatch.setattr(numpy_private_methods, "_amax", safe_amax, raising=True)


@pytest.fixture
def parquet_stubs(monkeypatch):
    """
    Monkeypatch pandas parquet IO. Controller lets tests inject read result and inspect write paths.
    """
    internal_state = {"read_result": None, "writes": [], "last_read_path": None}

    def fake_read_parquet(path, *args, **kwargs):
        internal_state["last_read_path"] = pathlib.Path(path)
        return internal_state["read_result"]

    def fake_to_parquet(self, path, *args, **kwargs):
        internal_state["writes"].append(pathlib.Path(path))

    monkeypatch.setattr(pd, "read_parquet", fake_read_parquet, raising=True)
    monkeypatch.setattr(pd.DataFrame, "to_parquet", fake_to_parquet, raising=True)

    class ParquetController:
        @property
        def writes(self):
            return list(internal_state["writes"])
        @property
        def last_read_path(self):
            return internal_state["last_read_path"]
        def set_read_result(self, data_frame):
            internal_state["read_result"] = data_frame

    return ParquetController()


@pytest.fixture
def deterministic_shortuuid(monkeypatch):
    """shortuuid.uuid() -> id-0001, id-0002, ..."""
    import shortuuid as shortuuid_module
    counter = itertools.count(1)
    def fixed_uuid():
        return f"id-{next(counter):04d}"
    monkeypatch.setattr(shortuuid_module, "uuid", fixed_uuid, raising=True)
    return fixed_uuid


@pytest.fixture
def deterministic_sampling(monkeypatch):
    """Make DataFrame/Series.sample deterministic: take first N (or first floor(frac*N))."""
    original_dataframe_sample = pd.DataFrame.sample
    original_series_sample = pd.Series.sample

    def dataframe_sample(self, n=None, frac=None, replace=False, weights=None, random_state=None, axis=None, ignore_index=False):
        if n is not None:
            sampled = self.head(n)
            return sampled if not ignore_index else sampled.reset_index(drop=True)
        if frac is not None:
            count = int(np.floor(len(self) * frac))
            sampled = self.head(count)
            return sampled if not ignore_index else sampled.reset_index(drop=True)
        return original_dataframe_sample(self, n=n, frac=frac, replace=replace, weights=weights,
                                        random_state=random_state, axis=axis, ignore_index=ignore_index)

    def series_sample(self, n=None, frac=None, replace=False, weights=None, random_state=None, axis=None, ignore_index=False):
        if n is not None:
            sampled = self.head(n)
            return sampled if not ignore_index else sampled.reset_index(drop=True)
        if frac is not None:
            count = int(np.floor(len(self) * frac))
            sampled = self.head(count)
            return sampled if not ignore_index else sampled.reset_index(drop=True)
        return original_series_sample(self, n=n, frac=frac, replace=replace, weights=weights,
                                      random_state=random_state, axis=axis, ignore_index=ignore_index)

    monkeypatch.setattr(pd.DataFrame, "sample", dataframe_sample, raising=True)
    monkeypatch.setattr(pd.Series, "sample", series_sample, raising=True)


# --------------------------------------------------------------------------------------
# Domain helpers / fakes
# --------------------------------------------------------------------------------------

@pytest.fixture
def fake_transport_zones():
    """
    Minimal GeoDataFrame asset with .get():
      columns: transport_zone_id, local_admin_unit_id, weight, geometry
    """
    import geopandas as geopandas_module
    data_frame = pd.DataFrame({
        "transport_zone_id": ["tz-1", "tz-2"],
        "local_admin_unit_id": ["fr-75056", "fr-92050"],
        "weight": [0.6, 0.4],
        "geometry": [None, None],
    })
    geo_data_frame = geopandas_module.GeoDataFrame(data_frame, geometry="geometry")

    class TransportZonesAsset:
        def __init__(self, geo_data_frame):
            self._geo_data_frame = geo_data_frame
            self.inputs = {}
        def get(self):
            return self._geo_data_frame.copy()

    return TransportZonesAsset(geo_data_frame)


@pytest.fixture(autouse=True)
def patch_geopandas_sjoin(monkeypatch):
    """
    Replace geopandas.sjoin with a simple function that attaches INSEE_REG='11'.
    Autouse so French path never needs spatial libs.
    """
    import geopandas as geopandas_module
    def fake_sjoin(left_geo_data_frame, right_geo_data_frame, predicate=None, how="inner"):
        joined_geo_data_frame = left_geo_data_frame.copy()
        joined_geo_data_frame["INSEE_REG"] = "11"
        return joined_geo_data_frame
    monkeypatch.setattr(sys.modules["geopandas"], "sjoin", fake_sjoin, raising=True)


@pytest.fixture(autouse=True)
def patch_mobility_parsers(monkeypatch):
    """
    Patch parsers to provide consistent tiny datasets AND also patch the already-imported
    names inside mobility.population (because it uses `from ... import ...`).
    """
    import mobility.parsers as parsers_module_local
    import mobility.parsers.admin_boundaries as admin_boundaries_module
    population_module = sys.modules.get("mobility.population")

    class CityLegalPopulationFake:
        def get(self):
            data_frame = pd.DataFrame({
                "local_admin_unit_id": ["fr-75056", "fr-92050", "ch-2601"],
                "legal_population": [2_000_000, 100_000, 5_000],
            })
            data_frame["local_admin_unit_id"] = data_frame["local_admin_unit_id"].astype(str)
            return data_frame

    class CensusLocalizedIndividualsFake:
        def __init__(self, region=None):
            self.region = region
        def get(self):
            return pd.DataFrame({
                "CANTVILLE": ["C1", "C1", "C2"],
                "age": [30, 45, 22],
                "socio_pro_category": ["spA", "spB", "spA"],
                "ref_pers_socio_pro_category": ["rspA", "rspB", "rspA"],
                "n_pers_household": [2, 3, 1],
                "n_cars": [0, 1, 0],
                "weight": [100.0, 200.0, 50.0],
            })

    def regions_boundaries_fake():
        return pd.DataFrame({"INSEE_REG": ["11"], "geometry": [None]})

    def cities_boundaries_fake():
        # Must match transport_zones.local_admin_unit_id which includes 'fr-' prefix
        return pd.DataFrame({
            "INSEE_COM": ["fr-75056", "fr-92050"],
            "INSEE_CAN": ["C1", "C2"],
        })

    # Patch the parser modules
    monkeypatch.setattr(parsers_module_local, "CityLegalPopulation", CityLegalPopulationFake, raising=True)
    monkeypatch.setattr(parsers_module_local, "CensusLocalizedIndividuals", CensusLocalizedIndividualsFake, raising=True)
    monkeypatch.setattr(admin_boundaries_module, "get_french_regions_boundaries", regions_boundaries_fake, raising=True)
    monkeypatch.setattr(admin_boundaries_module, "get_french_cities_boundaries", cities_boundaries_fake, raising=True)

    # Also patch the already-imported names inside mobility.population (if loaded)
    if population_module is not None:
        # population.py did: from mobility.parsers import CityLegalPopulation, CensusLocalizedIndividuals
        monkeypatch.setattr(population_module, "CityLegalPopulation", CityLegalPopulationFake, raising=True)
        monkeypatch.setattr(population_module, "CensusLocalizedIndividuals", CensusLocalizedIndividualsFake, raising=True)
        # and: from mobility.parsers.admin_boundaries import get_french_regions_boundaries, get_french_cities_boundaries
        monkeypatch.setattr(population_module, "get_french_regions_boundaries", regions_boundaries_fake, raising=True)
        monkeypatch.setattr(population_module, "get_french_cities_boundaries", cities_boundaries_fake, raising=True)


# --------------------------------------------------------------------------------------
# Import the module under test after bootstrapping exists.
# --------------------------------------------------------------------------------------

@pytest.fixture(scope="session", autouse=True)
def _import_population_module_once():
    import importlib  # noqa: F401
    import mobility.population as _  # noqa: F401
    importlib.reload(sys.modules["mobility.population"])


# --------------------------------------------------------------------------------------
# Keep logging quiet by default
# --------------------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _silence_logging():
    logging.getLogger().setLevel(logging.WARNING)
