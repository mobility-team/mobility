import os
import types
import pathlib
import logging
import builtins

import pytest

# Third-party
import numpy as np
import pandas as pd
import geopandas as gpd


# ----------------------------
# Core environment + utilities
# ----------------------------

@pytest.fixture
def project_dir(tmp_path, monkeypatch):
    """
    Create an isolated project data directory and set required env vars.
    Always compare paths with pathlib.Path in tests (Windows-safe).
    """
    mobility_project_path = tmp_path / "project_data"
    mobility_project_path.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("MOBILITY_PROJECT_DATA_FOLDER", str(mobility_project_path))
    # Some codepaths may use this; safe to point it to the same tmp.
    monkeypatch.setenv("MOBILITY_PACKAGE_DATA_FOLDER", str(mobility_project_path))
    return mobility_project_path


@pytest.fixture
def fake_inputs_hash():
    """Deterministic fake inputs hash used by Asset hashing logic in tests."""
    return "deadbeefdeadbeefdeadbeefdeadbeef"


# -----------------------------------------------------------
# Patch the exact FileAsset used by mobility.transport_zones
# -----------------------------------------------------------

@pytest.fixture(autouse=True)
def patch_transportzones_fileasset_init(monkeypatch, project_dir, fake_inputs_hash):
    """
    Patch the *exact* FileAsset class used by mobility.transport_zones at import time.
    This avoids guessing module paths or MRO tricks and guarantees interception.

    Behavior:
      - never calls .get()
      - sets .inputs, .inputs_hash, .cache_path, .hash_path, .value
      - exposes inputs as attributes on self
      - rewrites cache_path to <project_dir>/<fake_inputs_hash>-<basename>
    """
    import mobility.transport_zones as tz_module

    # Grab the class object that TransportZones actually extends
    FileAssetClass = tz_module.FileAsset

    def fake_init(self, *args, **kwargs):
        # TransportZones uses super().__init__(inputs, cache_path)
        if len(args) >= 2:
            inputs, cache_path = args[0], args[1]
        elif "inputs" in kwargs and "cache_path" in kwargs:
            inputs, cache_path = kwargs["inputs"], kwargs["cache_path"]
        elif len(args) == 1 and isinstance(args[0], dict):
            # Extremely defensive fallback
            inputs = args[0]
            cache_path = inputs.get("cache_path", "asset.parquet")
        else:
            raise AssertionError(
                f"Unexpected FileAsset.__init__ signature in tests: args={args}, kwargs={kwargs}"
            )

        # Normalize cache_path in case a dict accidentally reached here
        if isinstance(cache_path, dict):
            candidate = cache_path.get("path") or cache_path.get("polygons") or "asset.parquet"
            cache_path = candidate

        cache_path = pathlib.Path(cache_path)
        hashed_name = f"{fake_inputs_hash}-{cache_path.name}"
        hashed_path = pathlib.Path(project_dir) / hashed_name

        # Set required attributes
        self.inputs = inputs
        self.inputs_hash = fake_inputs_hash
        self.cache_path = hashed_path
        self.hash_path = hashed_path
        self.value = None

        # Surface inputs as attributes on self (e.g., self.study_area, self.osm_buildings)
        if isinstance(self.inputs, dict):
            for key, value in self.inputs.items():
                setattr(self, key, value)

    # Monkeypatch the *exact* class used by TransportZones
    monkeypatch.setattr(FileAssetClass, "__init__", fake_init, raising=True)


# ---------------------------------
# Autouse safety / stability patches
# ---------------------------------

@pytest.fixture(autouse=True)
def no_op_progress(monkeypatch):
    """
    Stub rich.progress.Progress to a no-op class so tests never produce TTY noise
    and never require a live progress loop.
    """
    try:
        import rich.progress as rich_progress  # noqa: F401
    except Exception:
        return  # If rich is absent in the test env, nothing to patch.

    class _NoOpProgress:
        def __init__(self, *args, **kwargs): ...
        def __enter__(self): return self
        def __exit__(self, exc_type, exc, tb): return False
        def add_task(self, *args, **kwargs): return 0
        def update(self, *args, **kwargs): ...
        def advance(self, *args, **kwargs): ...
        def track(self, sequence, *args, **kwargs):
            # Pass-through iterator
            for item in sequence:
                yield item

    monkeypatch.setattr("rich.progress.Progress", _NoOpProgress, raising=True)


@pytest.fixture(autouse=True)
def patch_numpy__methods(monkeypatch):
    """
    Wrap numpy.core._methods._sum and _amax to ignore numpy._NoValue sentinels.
    This prevents pandas/NumPy _NoValueType crashes in some environments.
    """
    try:
        import numpy.core._methods as _np_methods  # type: ignore
    except Exception:
        return

    _NoValue = getattr(np, "_NoValue", object())

    def _clean_args_and_call(func, *args, **kwargs):
        cleaned_args = tuple(None if a is _NoValue else a for a in args)
        cleaned_kwargs = {k: v for k, v in kwargs.items() if v is not _NoValue}
        return func(*cleaned_args, **cleaned_kwargs)

    if hasattr(_np_methods, "_sum"):
        original_sum = _np_methods._sum
        def wrapped_sum(*args, **kwargs):
            return _clean_args_and_call(original_sum, *args, **kwargs)
        monkeypatch.setattr(_np_methods, "_sum", wrapped_sum, raising=True)

    if hasattr(_np_methods, "_amax"):
        original_amax = _np_methods._amax
        def wrapped_amax(*args, **kwargs):
            return _clean_args_and_call(original_amax, *args, **kwargs)
        monkeypatch.setattr(_np_methods, "_amax", wrapped_amax, raising=True)


# -----------------------------------
# Parquet stubs (available if needed)
# -----------------------------------

@pytest.fixture
def parquet_stubs(monkeypatch):
    """
    Helper to stub pandas read_parquet and DataFrame.to_parquet as needed per test.
    Use by customizing the returned dataframe and captured writes inline in tests.
    """
    state = types.SimpleNamespace()
    state.read_return_df = pd.DataFrame({"dummy_column": []})
    state.written_paths = []

    def fake_read_parquet(path, *args, **kwargs):
        state.last_read_path = pathlib.Path(path)
        return state.read_return_df

    def fake_to_parquet(self, path, *args, **kwargs):
        state.written_paths.append(pathlib.Path(path))
        # No disk I/O.

    monkeypatch.setattr(pd, "read_parquet", fake_read_parquet, raising=True)
    monkeypatch.setattr(pd.DataFrame, "to_parquet", fake_to_parquet, raising=True)
    return state


# ----------------------------------------------------------
# Fake minimal geodataframe for transport zones expectations
# ----------------------------------------------------------

@pytest.fixture
def fake_transport_zones():
    """
    Minimal GeoDataFrame with columns typical code would expect.
    Geometry can be None for simplicity; CRS added for consistency.
    """
    df = pd.DataFrame(
        {
            "transport_zone_id": [1, 2],
            "urban_unit_category": ["core", "peripheral"],
            "geometry": [None, None],
        }
    )
    gdf = gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326")
    return gdf


# -------------------------------------------------------------
# Fake Population Asset (not used here but provided as requested)
# -------------------------------------------------------------

@pytest.fixture
def fake_population_asset(fake_transport_zones):
    """
    Tiny stand-in object with .get() returning a dataframe and
    .inputs containing {"transport_zones": fake_transport_zones}.
    """
    class _FakePopulationAsset:
        def __init__(self):
            self.inputs = {"transport_zones": fake_transport_zones}
        def get(self):
            return pd.DataFrame(
                {"population": [100, 200], "transport_zone_id": [1, 2]}
            )
    return _FakePopulationAsset()


# ---------------------------------------------------------
# Patch dependencies used by TransportZones to safe fakes
# ---------------------------------------------------------

@pytest.fixture
def dependency_fakes(monkeypatch, tmp_path):
    """
    Patch StudyArea, OSMData, and RScript to safe test doubles that:
      - record constructor calls/args
      - never touch network or external binaries
      - expose minimal interfaces used by the module.
    Importantly: also patch the symbols *inside mobility.transport_zones* since that
    module imported the classes with `from ... import ...`.
    """
    state = types.SimpleNamespace()

    # --- Fake StudyArea ---
    class _FakeStudyArea:
        def __init__(self, local_admin_unit_id, radius, cutout_geometries=None):
            self.local_admin_unit_id = local_admin_unit_id
            self.radius = radius
            self.cutout_geometries = cutout_geometries
            # TransportZones.create_and_get_asset expects a dict-like cache_path with keys like "polygons"
            self.cache_path = {
                "polygons": str(tmp_path / "study_area_polygons.gpkg"),
                "boundary": str(tmp_path / "study_area_boundary.geojson"),
            }

    state.study_area_inits = []

    def _StudyArea_spy(local_admin_unit_id, radius, cutout_geometries=None, *args, **kwargs):
        instance = _FakeStudyArea(local_admin_unit_id, radius, cutout_geometries)
        # Record only the keys asserted by tests
        state.study_area_inits.append(
            {
                "local_admin_unit_id": local_admin_unit_id,
                "radius": radius,
            }
        )
        return instance

    # --- Fake OSMData ---
    class _FakeOSMData:
        def __init__(self, study_area, object_type, key, geofabrik_extract_date, split_local_admin_units):
            self.study_area = study_area
            self.object_type = object_type
            self.key = key
            self.geofabrik_extract_date = geofabrik_extract_date
            self.split_local_admin_units = split_local_admin_units
            self.get_return_path = str(tmp_path / "osm_buildings.gpkg")

        def get(self):
            state.osm_get_called = True
            return self.get_return_path

    state.osm_inits = []

    def _OSMData_spy(study_area, object_type, key, geofabrik_extract_date, split_local_admin_units):
        instance = _FakeOSMData(study_area, object_type, key, geofabrik_extract_date, split_local_admin_units)
        state.osm_inits.append(
            {
                "study_area": study_area,
                "object_type": object_type,
                "key": key,
                "geofabrik_extract_date": geofabrik_extract_date,
                "split_local_admin_units": split_local_admin_units,
            }
        )
        return instance

    # --- Fake RScript ---
    class _FakeRScript:
        def __init__(self, script_path):
            self.script_path = script_path
            state.rscript_init_path = script_path
            state.rscript_runs = []

        def run(self, args):
            # Record call; do NOT execute anything.
            state.rscript_runs.append({"args": list(args)})

    def _RScript_spy(script_path):
        return _FakeRScript(script_path)

    # Apply patches both to the origin modules and to the names imported
    # into mobility.transport_zones.
    import mobility.transport_zones as tz_module
    monkeypatch.setattr("mobility.study_area.StudyArea", _StudyArea_spy, raising=True)
    monkeypatch.setattr("mobility.parsers.osm.OSMData", _OSMData_spy, raising=True)
    monkeypatch.setattr("mobility.r_utils.r_script.RScript", _RScript_spy, raising=True)

    # Crucial: patch the symbols inside the transport_zones module too
    monkeypatch.setattr(tz_module, "StudyArea", _StudyArea_spy, raising=True)
    monkeypatch.setattr(tz_module, "OSMData", _OSMData_spy, raising=True)
    monkeypatch.setattr(tz_module, "RScript", _RScript_spy, raising=True)

    return state


# --------------------------------------------------
# Optional: deterministic IDs if shortuuid is in use
# --------------------------------------------------

@pytest.fixture
def deterministic_shortuuid(monkeypatch):
    """
    Monkeypatch shortuuid.uuid to return incrementing ids deterministically.
    Only used by tests that explicitly request it.
    """
    counter = {"n": 0}
    def _fixed_uuid():
        counter["n"] += 1
        return f"shortuuid-{counter['n']:04d}"
    try:
        import shortuuid  # noqa: F401
        monkeypatch.setattr("shortuuid.uuid", _fixed_uuid, raising=True)
    except Exception:
        # shortuuid not installed/used in this code path
        pass
