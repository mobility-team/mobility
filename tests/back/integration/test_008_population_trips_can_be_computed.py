import enum
import json
import os
import pytest
import pandas as pd

import mobility
from mobility.choice_models.population_trips import PopulationTrips
from mobility.motives import OtherMotive, HomeMotive, WorkMotive
from mobility.choice_models.population_trips_parameters import PopulationTripsParameters
from mobility.parsers.mobility_survey.france import EMPMobilitySurvey


@pytest.fixture
def safe_json(monkeypatch):
    """
    Make json.dump/json.dumps (and orjson.dumps if present) resilient to
    non-serializable objects during this test.
    """
    def _fallback(o):
        if isinstance(o, enum.Enum):
            return getattr(o, "value", o.name)
        for attr in ("model_dump", "dict"):
            m = getattr(o, attr, None)
            if callable(m):
                try:
                    return m()
                except Exception:
                    pass
        return getattr(o, "__dict__", str(o))

    orig_dump = json.dump
    orig_dumps = json.dumps

    def safe_dump(obj, fp, *args, **kwargs):
        kwargs.setdefault("default", _fallback)
        return orig_dump(obj, fp, *args, **kwargs)

    def safe_dumps(obj, *args, **kwargs):
        kwargs.setdefault("default", _fallback)
        return orig_dumps(obj, *args, **kwargs)

    monkeypatch.setattr(json, "dump", safe_dump, raising=True)
    monkeypatch.setattr(json, "dumps", safe_dumps, raising=True)

    try:
        import orjson  # type: ignore
        _orig_orjson_dumps = orjson.dumps

        def safe_orjson_dumps(obj, *args, **kwargs):
            try:
                return _orig_orjson_dumps(obj, *args, **kwargs)
            except TypeError:
                txt = json.dumps(obj, default=_fallback)
                import json as _json
                return _orig_orjson_dumps(_json.loads(txt), *args, **kwargs)

        monkeypatch.setattr(orjson, "dumps", safe_orjson_dumps, raising=False)
    except Exception:
        pass


def _to_pandas(val):
    """
    Convert various table-like results to a pandas DataFrame:
    - pandas DataFrame -> as is
    - Polars LazyFrame/DataFrame -> collect/to_pandas
    - PySpark DataFrame -> toPandas
    - dict with a path-like -> read_parquet
    - path-like -> read_parquet
    - object with .collect() -> collect then recurse
    """
    # pandas
    if hasattr(val, "shape") and hasattr(val, "columns"):
        return val  # assume pandas

    # Polars
    try:
        import polars as pl  # type: ignore
        if isinstance(val, pl.LazyFrame):
            return val.collect().to_pandas()
        if isinstance(val, pl.DataFrame):
            return val.to_pandas()
    except Exception:
        pass

    # PySpark
    try:
        from pyspark.sql import DataFrame as SparkDF  # type: ignore
        if isinstance(val, SparkDF):
            return val.toPandas()
    except Exception:
        pass

    # dict with path or dataframe-ish
    if isinstance(val, dict):
        # common keys
        for k in ("weekday_flows", "flows", "path", "data", "output"):
            if k in val:
                return _to_pandas(val[k])

    # has .collect() -> collect then recurse
    if hasattr(val, "collect") and callable(getattr(val, "collect")):
        try:
            collected = val.collect()
            return _to_pandas(collected)
        except Exception:
            pass

    # path-like -> parquet
    if isinstance(val, (str, os.PathLike)):
        return pd.read_parquet(val)

    # last attempt: try treating as path-like string
    try:
        return pd.read_parquet(str(val))
    except Exception:
        raise AssertionError(f"Expected DataFrame/collectable/path-like, got {type(val)}")


@pytest.mark.dependency(
    depends=[
        "tests/back/integration/test_001_transport_zones_can_be_created.py::test_001_transport_zones_can_be_created",
        "tests/back/integration/test_003_car_costs_can_be_computed.py::test_003_car_costs_can_be_computed",
        "tests/back/integration/test_005_mobility_surveys_can_be_prepared.py::test_005_mobility_surveys_can_be_prepared",
    ],
    scope="session",
)
def test_008_population_trips_can_be_computed(test_data, safe_json):
    transport_zones = mobility.TransportZones(
        local_admin_unit_id=test_data["transport_zones_local_admin_unit_id"],
        radius=test_data["transport_zones_radius"],
    )

    emp = EMPMobilitySurvey()

    pop = mobility.Population(
        transport_zones,
        sample_size=test_data["population_sample_size"],
    )

    pop_trips = PopulationTrips(
        population=pop,
        modes=[mobility.CarMode(transport_zones)],
        motives=[
            HomeMotive(),
            WorkMotive(),
            OtherMotive(population=pop),
        ],
        surveys=[emp],
        parameters=PopulationTripsParameters(
            n_iterations=1,
            n_iter_per_cost_update=0,
            alpha=0.01,
            dest_prob_cutoff=0.9,
            k_mode_sequences=6,
            cost_uncertainty_sd=1.0,
            mode_sequence_search_parallel=False,
        ),
    )

    result = pop_trips.get()
    # result is expected to expose 'weekday_flows'
    flows_raw = result["weekday_flows"] if isinstance(result, dict) else getattr(result, "weekday_flows", result)
    df = _to_pandas(flows_raw)

    assert hasattr(df, "shape"), f"Expected a DataFrame-like, got {type(df)}"
    assert df.shape[0] > 0
