from __future__ import annotations

import os
import sys
import types
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
import geopandas as gpd
import numpy as np
import pytest


@pytest.fixture
def project_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """
    Per-test project directory. Ensures NO I/O outside tmp_path.
    Sets MOBILITY_PROJECT_DATA_FOLDER so any code under test resolves paths here.
    """
    monkeypatch.setenv("MOBILITY_PROJECT_DATA_FOLDER", str(tmp_path))
    # Some projects read this too; set it to the same place if accessed.
    if "MOBILITY_PACKAGE_DATA_FOLDER" not in os.environ:
        monkeypatch.setenv("MOBILITY_PACKAGE_DATA_FOLDER", str(tmp_path))
    return tmp_path


@pytest.fixture
def fake_inputs_hash() -> str:
    """
    A deterministic inputs hash string used across tests.
    """
    return "deadbeefdeadbeefdeadbeefdeadbeef"


@pytest.fixture(autouse=True)
def patch_asset_init(
    project_dir: Path,
    fake_inputs_hash: str,
    monkeypatch: pytest.MonkeyPatch,
):
    """
    Stub mobility.asset.Asset.__init__ so it does NOT call .get().
    Creates a minimal Asset class if mobility.asset is not importable to avoid ImportErrors.
    Sets:
      - self.inputs
      - self.inputs_hash
      - self.cache_path
      - self.hash_path
    Computes:
      cache_path = <project_dir>/<fake_inputs_hash>-<filename>
      hash_path = <project_dir>/<fake_inputs_hash>-<filename>.hash (filename used as base)
    """
    # Ensure a module path exists for monkeypatching even if project does not provide it.
    if "mobility" not in sys.modules:
        mobility_module = types.ModuleType("mobility")
        sys.modules["mobility"] = mobility_module
    if "mobility.asset" not in sys.modules:
        asset_module = types.ModuleType("mobility.asset")
        sys.modules["mobility.asset"] = asset_module

    import importlib

    asset_mod = importlib.import_module("mobility.asset")

    class _PatchedAsset:
        def __init__(self, *args, **kwargs):
            # Accept flexible signatures:
            # possible kwargs: inputs, filename, base_name, cache_filename
            inputs = kwargs.get("inputs", None)
            if inputs is None and len(args) >= 1:
                inputs = args[0]
            filename = (
                kwargs.get("filename")
                or kwargs.get("base_name")
                or kwargs.get("cache_filename")
                or "asset.parquet"
            )
            filename = str(filename)
            self.inputs: Dict[str, Any] = inputs if isinstance(inputs, dict) else {"value": inputs}
            self.inputs_hash: str = fake_inputs_hash

            base_name = Path(filename).name
            cache_file = f"{fake_inputs_hash}-{base_name}"
            self.cache_path: Path = project_dir / cache_file
            self.hash_path: Path = project_dir / f"{cache_file}.hash"
            # Intentionally DO NOT call self.get()

    # If the real Asset exists, patch its __init__. Otherwise, expose a stub.
    if hasattr(asset_mod, "Asset"):
        original_cls = getattr(asset_mod, "Asset")

        def _patched_init(self, *args, **kwargs):
            inputs = kwargs.get("inputs", None)
            if inputs is None and len(args) >= 1:
                inputs = args[0]
            filename = (
                kwargs.get("filename")
                or kwargs.get("base_name")
                or kwargs.get("cache_filename")
                or "asset.parquet"
            )
            filename = str(filename)
            self.inputs = inputs if isinstance(inputs, dict) else {"value": inputs}
            self.inputs_hash = fake_inputs_hash
            base_name = Path(filename).name
            cache_file = f"{fake_inputs_hash}-{base_name}"
            self.cache_path = project_dir / cache_file
            self.hash_path = project_dir / f"{cache_file}.hash"

        monkeypatch.setattr(original_cls, "__init__", _patched_init, raising=True)
    else:
        setattr(asset_mod, "Asset", _PatchedAsset)


@pytest.fixture(autouse=True)
def no_op_progress(monkeypatch: pytest.MonkeyPatch):
    """
    Stub rich.progress.Progress to a no-op implementation.
    """
    try:
        import rich.progress  # type: ignore

        class _NoOpProgress:
            def __init__(self, *args, **kwargs):
                pass

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            # common API
            def add_task(self, *args, **kwargs):
                return 0

            def update(self, *args, **kwargs):
                return None

            def advance(self, *args, **kwargs):
                return None

            def track(self, sequence, *args, **kwargs):
                for item in sequence:
                    yield item

            def stop(self):
                return None

        monkeypatch.setattr(rich.progress, "Progress", _NoOpProgress, raising=True)
    except Exception:
        # rich may not be installed in minimal envs; ignore.
        pass


@pytest.fixture(autouse=True)
def patch_numpy__methods(monkeypatch: pytest.MonkeyPatch):
    """
    Wrap NumPy’s private _methods._sum and _amax to ignore the _NoValue sentinel.
    This prevents pandas/NumPy _NoValueType crashes in some environments.
    """
    try:
        from numpy.core import _methods as _np_methods  # type: ignore
        import numpy as _np  # noqa

        sentinel_candidates: List[Any] = []
        for attr_name in ("_NoValue", "NoValue", "noValue"):
            if hasattr(_np, attr_name):
                sentinel_candidates.append(getattr(_np, attr_name))
        if hasattr(_np_methods, "_NoValue"):
            sentinel_candidates.append(getattr(_np_methods, "_NoValue"))
        _SENTINELS = tuple({id(x): x for x in sentinel_candidates}.values())

        def _strip_no_value_from_kwargs(kwargs: Dict[str, Any]) -> Dict[str, Any]:
            clean = {}
            for key, val in kwargs.items():
                if val in _SENTINELS:
                    # Behave like kwargs not provided at all
                    continue
                clean[key] = val
            return clean

        if hasattr(_np_methods, "_sum"):
            _orig_sum = _np_methods._sum

            def _wrapped_sum(a, axis=None, dtype=None, out=None, keepdims=False, initial=None, where=True):
                kwargs = _strip_no_value_from_kwargs(
                    dict(axis=axis, dtype=dtype, out=out, keepdims=keepdims, initial=initial, where=where)
                )
                return _orig_sum(a, **kwargs)

            monkeypatch.setattr(_np_methods, "_sum", _wrapped_sum, raising=True)

        if hasattr(_np_methods, "_amax"):
            _orig_amax = _np_methods._amax

            def _wrapped_amax(a, axis=None, out=None, keepdims=False, initial=None, where=True):
                kwargs = _strip_no_value_from_kwargs(
                    dict(axis=axis, out=out, keepdims=keepdims, initial=initial, where=where)
                )
                return _orig_amax(a, **kwargs)

            monkeypatch.setattr(_np_methods, "_amax", _wrapped_amax, raising=True)

    except Exception:
        # If private API shape differs in the environment, avoid failing tests.
        pass


@pytest.fixture
def parquet_stubs(monkeypatch: pytest.MonkeyPatch):
    """
    Provide helpers to stub pd.read_parquet and DataFrame.to_parquet.
    - read_parquet: set return value and capture the path used by code under test.
    - to_parquet: capture the path and optionally echo back the frame for assertions.
    """
    state: Dict[str, Any] = {
        "read_path": None,
        "write_path": None,
        "read_return": pd.DataFrame({"dummy": [1]}),
    }

    def set_read_return(df: pd.DataFrame):
        state["read_return"] = df

    def get_read_path() -> Path | None:
        return state["read_path"]

    def get_write_path() -> Path | None:
        return state["write_path"]

    def fake_read_parquet(path, *args, **kwargs):
        state["read_path"] = Path(path)
        return state["read_return"]

    def fake_to_parquet(self: pd.DataFrame, path, *args, **kwargs):
        state["write_path"] = Path(path)
        # behave like a write without side-effects

    monkeypatch.setattr(pd, "read_parquet", fake_read_parquet, raising=True)
    monkeypatch.setattr(pd.DataFrame, "to_parquet", fake_to_parquet, raising=True)

    class _ParquetHelpers:
        set_read_return = staticmethod(set_read_return)
        get_read_path = staticmethod(get_read_path)
        get_write_path = staticmethod(get_write_path)

    return _ParquetHelpers


@pytest.fixture
def deterministic_shortuuid(monkeypatch: pytest.MonkeyPatch):
    """
    Monkeypatch shortuuid.uuid to return incrementing ids.
    """
    try:
        import shortuuid  # type: ignore
    except Exception:
        # Provide a minimal stand-in if package is absent.
        shortuuid = types.ModuleType("shortuuid")
        sys.modules["shortuuid"] = shortuuid

    counter = {"i": 0}

    def _next_uuid():
        counter["i"] += 1
        return f"shortuuid-{counter['i']:04d}"

    monkeypatch.setattr(sys.modules["shortuuid"], "uuid", _next_uuid, raising=False)


@pytest.fixture
def fake_transport_zones() -> gpd.GeoDataFrame:
    """
    Minimal GeoDataFrame-like structure with columns that downstream code might expect.
    Geometry is set to None to avoid GIS dependencies.
    """
    df = pd.DataFrame(
        {
            "transport_zone_id": [1, 2],
            "urban_unit_category": ["urban", "rural"],
            "geometry": [None, None],
        }
    )
    # Return a GeoDataFrame for compatibility if geopandas is present.
    try:
        gdf = gpd.GeoDataFrame(df, geometry="geometry", crs=None)
        return gdf
    except Exception:
        # Fallback to plain DataFrame if geopandas not fully available.
        return df  # type: ignore[return-value]


@pytest.fixture
def fake_population_asset(fake_transport_zones) -> Any:
    """
    Tiny stand-in asset with .get() and .inputs containing {"transport_zones": fake_transport_zones}.
    """
    class _PopAsset:
        def __init__(self):
            self.inputs = {"transport_zones": fake_transport_zones}

        def get(self):
            # minimal, deterministic frame
            return pd.DataFrame(
                {
                    "transport_zone_id": [1, 2],
                    "population": [100, 50],
                }
            )

    return _PopAsset()


@pytest.fixture
def patch_mobility_survey(monkeypatch: pytest.MonkeyPatch):
    """
    Monkeypatch any survey parser class to return small DataFrames with expected columns.
    Usage pattern (example):
        from mobility.parsers.survey import SurveyParser
        parser = SurveyParser(...)
        parsed = parser.parse()
    This fixture replaces SurveyParser with a stub whose parse() returns a dict of tiny DataFrames.
    """
    # Ensure module path exists
    if "mobility.parsers" not in sys.modules:
        parent = types.ModuleType("mobility.parsers")
        sys.modules["mobility.parsers"] = parent

    survey_mod = types.ModuleType("mobility.parsers.survey")
    sys.modules["mobility.parsers.survey"] = survey_mod

    class _SurveyParserStub:
        def __init__(self, *args, **kwargs):
            self.args = args
            self.kwargs = kwargs

        def parse(self) -> Dict[str, pd.DataFrame]:
            return {
                "households": pd.DataFrame({"household_id": [1]}),
                "persons": pd.DataFrame({"person_id": [10], "household_id": [1]}),
                "trips": pd.DataFrame(
                    {"person_id": [10], "trip_id": [100], "distance_km": [1.2], "mode": ["walk"]}
                ),
            }

    monkeypatch.setattr(survey_mod, "SurveyParser", _SurveyParserStub, raising=True)
    return _SurveyParserStub


# ---------- Helpers to seed Trips-like instances for direct method tests ----------

@pytest.fixture
def seed_trips_helpers():
    """
    Provide helpers to seed attributes on a Trips instance for tests that call
    algorithmic methods directly.
    """
    def seed_minimal_trips(trips_instance: Any):
        trips_instance.p_immobility = pd.DataFrame({"person_id": [1], "immobile": [False]})
        trips_instance.n_travels_db = pd.DataFrame({"person_id": [1], "n_travels": [1]})
        trips_instance.travels_db = pd.DataFrame({"trip_id": [1], "person_id": [1]})
        trips_instance.long_trips_db = pd.DataFrame({"trip_id": [1], "is_long": [False]})
        trips_instance.days_trip_db = pd.DataFrame({"trip_id": [1], "day": [1]})
        trips_instance.short_trips_db = pd.DataFrame({"trip_id": [1], "is_short": [True]})
        return trips_instance

    return types.SimpleNamespace(seed_minimal_trips=seed_minimal_trips)


# ---------- Deterministic pandas sampling ----------

@pytest.fixture(autouse=True)
def deterministic_pandas_sampling(monkeypatch: pytest.MonkeyPatch):
    """
    Make DataFrame.sample / Series.sample deterministic (take first N without shuffling).
    """
    def _df_sample(self, n=None, frac=None, replace=False, weights=None, random_state=None, axis=None, ignore_index=False):
        if n is None and frac is not None:
            n = int(len(self) * float(frac))
        if n is None:
            n = 1
        n = max(0, min(int(n), len(self)))
        result = self.iloc[:n]
        if ignore_index:
            result = result.reset_index(drop=True)
        return result

    monkeypatch.setattr(pd.DataFrame, "sample", _df_sample, raising=True)
    monkeypatch.setattr(pd.Series, "sample", _df_sample, raising=True)

