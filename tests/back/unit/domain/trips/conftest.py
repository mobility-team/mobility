import os
import types
from pathlib import Path
import itertools

import pytest
import pandas as pd
import geopandas as gpd
import numpy as np


# ---------------------------
# Core environment & pathing
# ---------------------------

@pytest.fixture(scope="session")
def fake_inputs_hash() -> str:
    """Deterministic hash string used by Asset-like classes in tests."""
    return "deadbeefdeadbeefdeadbeefdeadbeef"


@pytest.fixture(scope="session")
def project_dir(tmp_path_factory, fake_inputs_hash):
    """
    Create a per-session project directory and set MOBILITY_PROJECT_DATA_FOLDER to it.

    All cache paths will be rewritten to:
        <project_dir>/<fake_inputs_hash>-<file_name>
    """
    project_directory = tmp_path_factory.mktemp("project")
    os.environ["MOBILITY_PROJECT_DATA_FOLDER"] = str(project_directory)
    os.environ.setdefault("MOBILITY_PACKAGE_DATA_FOLDER", str(project_directory))
    return Path(project_directory)


# -------------------------------------------------
# Autouse: Patch Asset/FileAsset initializers robustly (order-agnostic)
# -------------------------------------------------

@pytest.fixture(autouse=True)
def patch_asset_init(monkeypatch, project_dir, fake_inputs_hash):
    """
    Make FileAsset/Asset initializers accept either positional order:
      - (inputs, cache_path) OR (cache_path, inputs) OR just (inputs)
    Never call super().__init__ or do any I/O. Always set deterministic paths:
      <project_dir>/<fake_inputs_hash>-<file_name or asset.parquet>

    We also patch the FileAsset class that mobility.trips imported (aliases/re-exports),
    and walk its MRO to catch unexpected base classes.
    """
    from pathlib import Path as _Path

    def is_pathlike(value):
        try:
            return isinstance(value, (str, os.PathLike, _Path)) or hasattr(value, "__fspath__")
        except Exception:
            return False

    def make_fake_fileasset_init():
        def fake_file_asset_init(self, arg1, arg2=None, *args, **kwargs):
            if isinstance(arg1, dict) and (arg2 is None or is_pathlike(arg2)):
                inputs_mapping, cache_path_object = arg1, arg2
            elif is_pathlike(arg1) and isinstance(arg2, dict):
                cache_path_object, inputs_mapping = arg1, arg2
            elif isinstance(arg1, dict) and arg2 is None:
                inputs_mapping, cache_path_object = arg1, None
            else:
                inputs_mapping, cache_path_object = arg1, arg2

            self.inputs = inputs_mapping if isinstance(inputs_mapping, dict) else {}
            self.inputs_hash = fake_inputs_hash

            original_file_name = "asset.parquet"
            if cache_path_object is not None and is_pathlike(cache_path_object):
                try:
                    original_file_name = _Path(cache_path_object).name
                except Exception:
                    pass

            rewritten_cache_path = project_dir / f"{fake_inputs_hash}-{original_file_name}"
            self.cache_path = _Path(rewritten_cache_path)
            self.hash_path = _Path(rewritten_cache_path)
        return fake_file_asset_init

    def make_fake_asset_init():
        def fake_asset_init(self, arg1, *args, **kwargs):
            inputs_mapping = arg1
            if not isinstance(inputs_mapping, dict) and len(args) >= 1 and isinstance(args[0], dict):
                inputs_mapping = args[0]
            self.inputs = inputs_mapping if isinstance(inputs_mapping, dict) else {}
            if not hasattr(self, "inputs_hash"):
                self.inputs_hash = fake_inputs_hash
            if not hasattr(self, "cache_path"):
                rewritten_cache_path = project_dir / f"{fake_inputs_hash}-asset.parquet"
                self.cache_path = _Path(rewritten_cache_path)
                self.hash_path = _Path(rewritten_cache_path)
        return fake_asset_init

    fake_file_asset_init = make_fake_fileasset_init()
    fake_asset_init = make_fake_asset_init()

    # Patch known modules + whatever Trips imported (handles aliases / re-exports)
    for module_name, class_name, replacement in [
        ("mobility.file_asset", "FileAsset", fake_file_asset_init),
        ("mobility.asset", "Asset", fake_asset_init),
        ("mobility.trips", "FileAsset", fake_file_asset_init),
    ]:
        try:
            module = __import__(module_name, fromlist=[class_name])
            if hasattr(module, class_name):
                cls = getattr(module, class_name)
                monkeypatch.setattr(cls, "__init__", replacement, raising=True)
                # Walk MRO to ensure any base also accepts lenient args
                for base_class in cls.__mro__:
                    if base_class in (object, cls):
                        continue
                    try:
                        if base_class.__name__.lower().endswith("fileasset"):
                            monkeypatch.setattr(base_class, "__init__", fake_file_asset_init, raising=True)
                        else:
                            monkeypatch.setattr(base_class, "__init__", fake_asset_init, raising=True)
                    except Exception:
                        pass
        except ModuleNotFoundError:
            pass


# -----------------------------------------------
# Autouse: Make rich.progress.Progress a no-op
# -----------------------------------------------

@pytest.fixture(autouse=True)
def no_op_progress(monkeypatch):
    """
    Replace rich.progress.Progress with a no-op context manager that records nothing.
    """
    class NoOpProgress:
        def __init__(self, *args, **kwargs):
            pass
        def __enter__(self):
            return self
        def __exit__(self, exc_type, exc, tb):
            return False
        def add_task(self, *args, **kwargs):
            return 1  # dummy task id
        def update(self, *args, **kwargs):
            return None

    monkeypatch.setattr("rich.progress.Progress", NoOpProgress, raising=True)


# -----------------------------------------------------------------------
# Autouse: Wrap NumPy private _methods to ignore np._NoValue sentinels
# -----------------------------------------------------------------------

@pytest.fixture(autouse=True)
def patch_numpy__methods(monkeypatch):
    """
    Wrap NumPy’s private _methods._sum and _amax to strip np._NoValue sentinels
    from kwargs that Pandas sometimes forwards (prevents ValueErrors).
    """
    from numpy.core import _methods as numpy_private_methods

    def wrap_ignore_no_value(original_function):
        def inner(array, *args, **kwargs):
            cleaned_kwargs = {k: v for k, v in kwargs.items() if not (v is getattr(np, "_NoValue", None))}
            return original_function(array, *args, **cleaned_kwargs)
        return inner

    if hasattr(numpy_private_methods, "_sum"):
        monkeypatch.setattr(numpy_private_methods, "_sum", wrap_ignore_no_value(numpy_private_methods._sum), raising=True)
    if hasattr(numpy_private_methods, "_amax"):
        monkeypatch.setattr(numpy_private_methods, "_amax", wrap_ignore_no_value(numpy_private_methods._amax), raising=True)


# ---------------------------------------------------------
# Deterministic shortuuid for stable trip_id generation
# ---------------------------------------------------------

@pytest.fixture
def deterministic_shortuuid(monkeypatch):
    """
    Monkeypatch shortuuid.uuid to return incrementing ids for predictability.
    """
    import shortuuid as shortuuid_module
    incrementing_counter = itertools.count(1)

    def fake_uuid():
        return f"id{next(incrementing_counter)}"

    monkeypatch.setattr(shortuuid_module, "uuid", fake_uuid, raising=True)
    return fake_uuid


# ---------------------------------------------------------
# Autouse: deterministic pandas sampling (first N rows)
# ---------------------------------------------------------

@pytest.fixture(autouse=True)
def deterministic_pandas_sample(monkeypatch):
    """
    Make DataFrame.sample/Series.sample deterministic: always return the first N rows.
    This avoids randomness in tests while keeping the method behavior compatible.
    """
    def dataframe_sample(self, n=None, frac=None, replace=False, *args, **kwargs):
        if n is None and frac is None:
            n = 1
        if frac is not None:
            n = int(np.floor(len(self) * frac))
        n = max(0, int(n))
        return self.iloc[:n].copy()

    def series_sample(self, n=None, frac=None, replace=False, *args, **kwargs):
        if n is None and frac is None:
            n = 1
        if frac is not None:
            n = int(np.floor(len(self) * frac))
        n = max(0, int(n))
        return self.iloc[:n].copy()

    monkeypatch.setattr(pd.DataFrame, "sample", dataframe_sample, raising=True)
    monkeypatch.setattr(pd.Series, "sample", series_sample, raising=True)


# ---------------------------------------------------------
# Autouse safety net: fallback pd.read_parquet for sentinel paths
# ---------------------------------------------------------

@pytest.fixture(autouse=True)
def fallback_population_read_parquet(monkeypatch):
    """
    Wrap pd.read_parquet so that if a test (or stub) points to a non-existent
    individuals parquet (e.g., 'unused.parquet', 'population_individuals.parquet'),
    we return a tiny, valid individuals dataframe instead of hitting the filesystem.

    This wrapper delegates to the original pd.read_parquet for any other path.
    Tests that need to capture calls can still override with their own monkeypatch.
    """
    original_read_parquet = pd.read_parquet

    tiny_individuals_dataframe = pd.DataFrame(
        {
            "individual_id": [1],
            "transport_zone_id": [101],
            "socio_pro_category": ["1"],
            "ref_pers_socio_pro_category": ["1"],
            "n_pers_household": ["2"],
            "n_cars": ["1"],
            "country": ["FR"],
        }
    )

    sentinel_basenames = {"unused.parquet", "population_individuals.parquet"}

    def wrapped_read_parquet(path, *args, **kwargs):
        try:
            path_name = Path(path).name if isinstance(path, (str, os.PathLike, Path)) else ""
        except Exception:
            path_name = ""
        if path_name in sentinel_basenames:
            return tiny_individuals_dataframe.copy()
        return original_read_parquet(path, *args, **kwargs)

    monkeypatch.setattr(pd, "read_parquet", wrapped_read_parquet, raising=True)


# ---------------------------------------------------------
# Optional per-test parquet stubs
# ---------------------------------------------------------

@pytest.fixture
def parquet_stubs(monkeypatch):
    """
    Monkeypatch pd.read_parquet and pd.DataFrame.to_parquet for the current test.

    Usage inside a test:
        call_records = {"read": [], "write": []}
        def install_read(return_dataframe): ...
        def install_write(): ...
    """
    call_records = {"read": [], "write": []}

    def install_read(return_dataframe):
        def fake_read(path, *args, **kwargs):
            call_records["read"].append(Path(path))
            return return_dataframe
        monkeypatch.setattr(pd, "read_parquet", fake_read, raising=True)

    def install_write():
        def fake_write(self, path, *args, **kwargs):
            call_records["write"].append(Path(path))
            # no real I/O
        monkeypatch.setattr(pd.DataFrame, "to_parquet", fake_write, raising=True)

    return {"calls": call_records, "install_read": install_read, "install_write": install_write}


# ---------------------------------------------------------
# Minimal transport zones & study area fixtures
# ---------------------------------------------------------

@pytest.fixture
def fake_transport_zones():
    """
    Minimal GeoDataFrames with the columns expected by Trips.get_population_trips().
    """
    transport_zones_geodataframe = gpd.GeoDataFrame(
        {
            "transport_zone_id": [101, 102],
            "local_admin_unit_id": [1, 2],
            "geometry": [None, None],
        }
    )

    study_area_geodataframe = gpd.GeoDataFrame(
        {
            "local_admin_unit_id": [1, 2],
            "urban_unit_category": ["C", "B"],
            "geometry": [None, None],
        }
    )

    class TransportZonesAsset:
        def __init__(self, transport_zones_dataframe, study_area_dataframe):
            self._transport_zones_geodataframe = transport_zones_dataframe
            self.study_area = types.SimpleNamespace(get=lambda: study_area_dataframe)
        def get(self):
            return self._transport_zones_geodataframe

    transport_zones_asset = TransportZonesAsset(
        transport_zones_geodataframe,
        study_area_geodataframe
    )
    return {
        "transport_zones": transport_zones_geodataframe,
        "study_area": study_area_geodataframe,
        "asset": transport_zones_asset
    }


# ---------------------------------------------------------
# Fake population asset
# ---------------------------------------------------------

@pytest.fixture
def fake_population_asset(fake_transport_zones):
    """
    Stand-in object for a population FileAsset with the exact attributes Trips.create_and_get_asset expects.
    - .get() returns a mapping containing the path to individuals parquet (content provided by parquet stub).
    - .inputs contains {"transport_zones": <asset with .get() and .study_area.get()>}
    """
    individuals_parquet_path = "population_individuals.parquet"

    class PopulationAsset:
        def __init__(self):
            self.inputs = {"transport_zones": fake_transport_zones["asset"]}
        def get(self):
            return {"individuals": individuals_parquet_path}

    return PopulationAsset()


# ---------------------------------------------------------
# Autouse: Patch filter_database to be index-agnostic & arg-order-agnostic
# ---------------------------------------------------------

@pytest.fixture(autouse=True)
def patch_filter_database(monkeypatch):
    """
    Make mobility.safe_sample.filter_database robust for our test stubs:
    - Accept filters via positional and/or keyword args without conflict
    - Work whether filters live in index levels or columns
    - If the filter would produce an empty dataframe, FALL BACK to the unfiltered rows
      (keeps Trips flow alive and prevents 'No objects to concatenate')
    """
    import pandas as pd

    def stub_filter_database(input_dataframe, *positional_args, **keyword_args):
        # Accept both positional and keyword filters in this order: csp, n_cars, city_category
        csp_value = keyword_args.pop("csp", None)
        n_cars_value = keyword_args.pop("n_cars", None)
        city_category_value = keyword_args.pop("city_category", None)

        # Fill from positional only if not set by keywords
        if len(positional_args) >= 1 and csp_value is None:
            csp_value = positional_args[0]
        if len(positional_args) >= 2 and n_cars_value is None:
            n_cars_value = positional_args[1]
        if len(positional_args) >= 3 and city_category_value is None:
            city_category_value = positional_args[2]

        filtered_dataframe = input_dataframe.copy()

        # Normalize index to columns for uniform filtering
        if isinstance(filtered_dataframe.index, pd.MultiIndex) or filtered_dataframe.index.name is not None:
            filtered_dataframe = filtered_dataframe.reset_index()

        # Build mask only for columns that exist
        boolean_mask = pd.Series(True, index=filtered_dataframe.index)

        if csp_value is not None and "csp" in filtered_dataframe.columns:
            boolean_mask &= filtered_dataframe["csp"] == csp_value

        if n_cars_value is not None and "n_cars" in filtered_dataframe.columns:
            boolean_mask &= filtered_dataframe["n_cars"] == n_cars_value

        if city_category_value is not None and "city_category" in filtered_dataframe.columns:
            boolean_mask &= filtered_dataframe["city_category"] == city_category_value

        result_dataframe = filtered_dataframe.loc[boolean_mask].reset_index(drop=True)

        # --- Critical fallback: if empty after filtering, return the unfiltered rows
        if result_dataframe.empty:
            result_dataframe = filtered_dataframe.reset_index(drop=True)

        return result_dataframe

    # Patch both the original module and the alias imported inside mobility.trips
    try:
        import mobility.safe_sample as safe_sample_module
        monkeypatch.setattr(safe_sample_module, "filter_database", stub_filter_database, raising=True)
    except Exception:
        pass

    try:
        import mobility.trips as trips_module
        monkeypatch.setattr(trips_module, "filter_database", stub_filter_database, raising=True)
    except Exception:
        pass


# ---------------------------------------------------------
# Autouse: Patch sampling helpers to be deterministic & robust
# ---------------------------------------------------------

@pytest.fixture(autouse=True)
def patch_sampling_helpers(monkeypatch):
    """
    Make sampling helpers predictable and length-safe so Trip generation
    never ends up with empty selections or length mismatches.

    - sample_travels: always returns the first k row positions (per sample)
    - safe_sample: filters by present columns; if result < n, repeats rows
                   to reach exactly n; returns a DataFrame with original columns
    """
    import numpy as np
    import pandas as pd

    def repeat_to_required_length(input_dataframe, required_length):
        if required_length <= 0:
            return input_dataframe.iloc[:0].copy()
        if len(input_dataframe) == 0:
            empty_row = {c: np.nan for c in input_dataframe.columns}
            synthetic_dataframe = pd.DataFrame([empty_row])
            synthetic_dataframe = pd.concat([synthetic_dataframe] * required_length, ignore_index=True)
            if "day_id" in synthetic_dataframe.columns:
                synthetic_dataframe["day_id"] = np.arange(1, required_length + 1, dtype=int)
            return synthetic_dataframe
        if len(input_dataframe) >= required_length:
            return input_dataframe.iloc[:required_length].reset_index(drop=True)
        repetitions = int(np.ceil(required_length / len(input_dataframe)))
        expanded_dataframe = pd.concat([input_dataframe] * repetitions, ignore_index=True)
        return expanded_dataframe.iloc[:required_length].reset_index(drop=True)

    # --- Stub for mobility.sample_travels.sample_travels
    def stub_sample_travels(
        travels_dataframe,
        start_col="day_of_year",
        length_col="n_nights",
        weight_col="pondki",
        burnin=0,
        k=1,
        num_samples=1,
        *args,
        **kwargs,
    ):
        total_rows = len(travels_dataframe)
        k = max(0, int(k))
        chosen_positions = np.arange(min(k, total_rows), dtype=int)
        return [chosen_positions.copy() for _ in range(int(num_samples))]

    # --- Stub for mobility.safe_sample.safe_sample
    def stub_safe_sample(
        input_dataframe,
        n,
        weights=None,
        csp=None,
        n_cars=None,
        weekday=None,
        city_category=None,
        **unused_kwargs,
    ):
        filtered_dataframe = input_dataframe.copy()

        if isinstance(filtered_dataframe.index, pd.MultiIndex) or filtered_dataframe.index.name is not None:
            filtered_dataframe = filtered_dataframe.reset_index()

        if csp is not None and "csp" in filtered_dataframe.columns:
            filtered_dataframe = filtered_dataframe.loc[filtered_dataframe["csp"] == csp]
        if n_cars is not None and "n_cars" in filtered_dataframe.columns:
            filtered_dataframe = filtered_dataframe.loc[filtered_dataframe["n_cars"] == n_cars]
        if weekday is not None and "weekday" in filtered_dataframe.columns:
            filtered_dataframe = filtered_dataframe.loc[filtered_dataframe["weekday"] == bool(weekday)]
        if city_category is not None and "city_category" in filtered_dataframe.columns:
            filtered_dataframe = filtered_dataframe.loc[filtered_dataframe["city_category"] == city_category]

        n = int(n) if n is not None else 0
        result_dataframe = repeat_to_required_length(filtered_dataframe, n)

        if "day_id" not in result_dataframe.columns:
            result_dataframe = result_dataframe.copy()
            result_dataframe["day_id"] = np.arange(1, len(result_dataframe) + 1, dtype=int)

        return result_dataframe

    # Patch both the original modules and the aliases imported in mobility.trips
    try:
        import mobility.sample_travels as module_sample_travels
        monkeypatch.setattr(module_sample_travels, "sample_travels", stub_sample_travels, raising=True)
    except Exception:
        pass
    try:
        import mobility.trips as trips_module
        monkeypatch.setattr(trips_module, "sample_travels", stub_sample_travels, raising=True)
    except Exception:
        pass
    try:
        import mobility.safe_sample as module_safe_sample
        monkeypatch.setattr(module_safe_sample, "safe_sample", stub_safe_sample, raising=True)
    except Exception:
        pass
    try:
        import mobility.trips as trips_module_again
        monkeypatch.setattr(trips_module_again, "safe_sample", stub_safe_sample, raising=True)
    except Exception:
        pass


# ---------------------------------------------------------
# Autouse: Patch DefaultGWP.as_dataframe to return int mode_id
# ---------------------------------------------------------

@pytest.fixture(autouse=True)
def patch_default_gwp_dataframe(monkeypatch):
    """
    Ensure GWP lookup merges cleanly with trips (mode_id dtype alignment).
    Return a tiny deterministic table with integer mode_id.
    """
    import pandas as pd

    def stub_as_dataframe(self=None):
        return pd.DataFrame(
            {
                "mode_id": pd.Series([1, 2, 3], dtype="int64"),
                "gwp": pd.Series([0.1, 0.2, 0.3], dtype="float64"),
            }
        )

    # Patch source class and any alias imported in mobility.trips
    targets = [
        "mobility.transport_modes.default_gwp.DefaultGWP.as_dataframe",
        "mobility.trips.DefaultGWP.as_dataframe",
    ]
    for target in targets:
        try:
            monkeypatch.setattr(target, stub_as_dataframe, raising=True)
        except Exception:
            pass


# ---------------------------------------------------------
# Patch MobilitySurveyAggregator + EMP to deterministic tiny DBs
# ---------------------------------------------------------

@pytest.fixture
def patch_mobility_survey(monkeypatch):
    """
    Layout chosen to match how Trips + helpers slice:
      - short_trips: MultiIndex [country]           (one-level MultiIndex)
      - days_trip:   MultiIndex [country, csp]
      - travels:     MultiIndex [country, csp, n_cars, city_category]
      - long_trips:  MultiIndex [country, travel_id]
      - n_travels, p_immobility, p_car: MultiIndex [country, csp]
    Also patches both the source modules and the mobility.trips aliases.
    """
    import pandas as pd

    country_code = "FR"
    csp_code = "1"

    def one_level_country_multiindex(length: int):
        return pd.MultiIndex.from_tuples([(country_code,)] * length, names=["country"])

    # short_trips: one-level MI on country
    short_trips_dataframe = pd.DataFrame(
        {
            "day_id": [1, 1, 2, 2],
            "daily_trip_index": [0, 1, 0, 1],
            "previous_motive": ["home", "home", "home", "home"],
            "motive": ["work", "shop", "leisure", "other"],
            "mode_id": [1, 2, 1, 3],
            "distance": [5.0, 2.0, 3.5, 1.0],
            "n_other_passengers": [0, 0, 1, 0],
        },
        index=one_level_country_multiindex(4),
    )

    # days_trip: MI on (country, csp) — 'csp' is an index level, not a column
    days_trip_dataframe = pd.DataFrame(
        {
            "day_id": [1, 2, 3, 4],
            "n_cars": ["1", "1", "1", "1"],
            "weekday": [True, False, True, False],
            "city_category": ["C", "C", "B", "B"],
            "pondki": [1.0, 1.0, 1.0, 1.0],
        },
        index=pd.MultiIndex.from_tuples(
            [(country_code, csp_code)] * 4, names=["country", "csp"]
        ),
    )

    # travels: MI on (country, csp, n_cars, city_category)
    travels_dataframe = pd.DataFrame(
        {
            "travel_id": [1001, 1002],
            "month": [1, 6],
            "weekday": [2, 4],
            "n_nights": [2, 1],
            "pondki": [1.0, 1.0],
            "motive": ["9_pro", "1_pers"],
            "destination_city_category": ["B", "C"],
        },
        index=pd.MultiIndex.from_tuples(
            [(country_code, csp_code, "1", "C"), (country_code, csp_code, "1", "C")],
            names=["country", "csp", "n_cars", "city_category"],
        ),
    )

    # long_trips: MI on (country, travel_id)
    long_trips_dataframe = pd.DataFrame(
        {
            "previous_motive": ["home", "work", "home"],
            "motive": ["work", "return", "leisure"],
            "mode_id": [1, 1, 2],
            "distance": [120.0, 120.0, 40.0],
            "n_other_passengers": [0, 0, 1],
            "n_nights_at_destination": [1, 1, 0],
        },
        index=pd.MultiIndex.from_tuples(
            [(country_code, 1001), (country_code, 1001), (country_code, 1002)],
            names=["country", "travel_id"],
        ),
    )

    # n_travels / probabilities: MI on (country, csp)
    n_travels_series = pd.Series(
        [1],
        index=pd.MultiIndex.from_tuples([(country_code, csp_code)], names=["country", "csp"]),
        name="n_travels",
    )
    immobility_probability_dataframe = pd.DataFrame(
        {"immobility_weekday": [0.0], "immobility_weekend": [0.0]},
        index=pd.MultiIndex.from_tuples([(country_code, csp_code)], names=["country", "csp"]),
    )
    car_probability_dataframe = pd.DataFrame(
        {"p": [1.0]},
        index=pd.MultiIndex.from_tuples([(country_code, csp_code)], names=["country", "csp"]),
    )

    class StubMobilitySurveyAggregator:
        def __init__(self, population, surveys):
            self._population = population
            self._surveys = surveys
        def get(self):
            return {
                "short_trips": short_trips_dataframe,
                "days_trip": days_trip_dataframe,
                "long_trips": long_trips_dataframe,
                "travels": travels_dataframe,
                "n_travels": n_travels_series,
                "p_immobility": immobility_probability_dataframe,
                "p_car": car_probability_dataframe,
            }

    class StubEMPMobilitySurvey:
        """Lightweight stub so default surveys = {'fr': EMPMobilitySurvey()} does not error."""
        def __init__(self, *args, **kwargs):
            pass

    # Patch both the source modules and the mobility.trips aliases
    monkeypatch.setattr(
        "mobility.parsers.mobility_survey.MobilitySurveyAggregator",
        StubMobilitySurveyAggregator,
        raising=True,
    )
    monkeypatch.setattr(
        "mobility.trips.MobilitySurveyAggregator",
        StubMobilitySurveyAggregator,
        raising=True,
    )
    monkeypatch.setattr(
        "mobility.parsers.mobility_survey.france.EMPMobilitySurvey",
        StubEMPMobilitySurvey,
        raising=True,
    )
    monkeypatch.setattr(
        "mobility.trips.EMPMobilitySurvey",
        StubEMPMobilitySurvey,
        raising=True,
    )

    return {
        "short_trips": short_trips_dataframe,
        "days_trip": days_trip_dataframe,
        "long_trips": long_trips_dataframe,
        "travels": travels_dataframe,
        "n_travels": n_travels_series,
        "p_immobility": immobility_probability_dataframe,
        "p_car": car_probability_dataframe,
        "country": country_code,
    }


# ---------------------------------------------------------
# Helper: seed a Trips instance attributes for direct calls
# ---------------------------------------------------------

@pytest.fixture
def seed_trips_with_minimal_databases(patch_mobility_survey):
    """
    Returns a function that seeds a Trips instance with minimal, consistent
    databases so get_individual_trips can be called directly.
    """
    def _seed(trips_instance):
        mobility_survey_data = patch_mobility_survey
        trips_instance.short_trips_db = mobility_survey_data["short_trips"]
        trips_instance.days_trip_db = mobility_survey_data["days_trip"]
        trips_instance.long_trips_db = mobility_survey_data["long_trips"]
        trips_instance.travels_db = mobility_survey_data["travels"]
        trips_instance.n_travels_db = mobility_survey_data["n_travels"]
        trips_instance.p_immobility = mobility_survey_data["p_immobility"]
        trips_instance.p_car = mobility_survey_data["p_car"]
    return _seed
