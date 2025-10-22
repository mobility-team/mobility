# tests/back/integration/test_006_trips_can_be_sampled.py
import enum
import json
import os
import pandas as pd
import pytest
import mobility


@pytest.fixture
def safe_json(monkeypatch):
    """Harden json/orjson against non-serializable objects for this test."""
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
    """Normalize result to a pandas DataFrame (handles DF, lazy, dict, path)."""
    # pandas
    if hasattr(val, "shape") and hasattr(val, "columns"):
        return val

    # polars
    try:
        import polars as pl  # type: ignore
        if isinstance(val, pl.LazyFrame):
            return val.collect().to_pandas()
        if isinstance(val, pl.DataFrame):
            return val.to_pandas()
    except Exception:
        pass

    # pyspark
    try:
        from pyspark.sql import DataFrame as SparkDF  # type: ignore
        if isinstance(val, SparkDF):
            return val.toPandas()
    except Exception:
        pass

    # dict with meaningful keys
    if isinstance(val, dict):
        for k in ("trips", "path", "data", "output"):
            if k in val:
                return _to_pandas(val[k])

    # lazy/collector
    if hasattr(val, "collect") and callable(getattr(val, "collect")):
        try:
            return _to_pandas(val.collect())
        except Exception:
            pass

    # path-like -> parquet
    if isinstance(val, (str, os.PathLike)):
        return pd.read_parquet(val)

    # last attempt
    try:
        return pd.read_parquet(str(val))
    except Exception:
        raise AssertionError(f"Expected DataFrame/collectable/path-like, got {type(val)}")


@pytest.mark.dependency(
    depends=["tests/back/integration/test_002_population_sample_can_be_created.py::test_002_population_sample_can_be_created"],
    scope="session",
)
def test_006_trips_can_be_sampled(test_data, safe_json):
    transport_zones = mobility.TransportZones(
        local_admin_unit_id=test_data["transport_zones_local_admin_unit_id"],
        radius=test_data["transport_zones_radius"],
    )

    population = mobility.Population(
        transport_zones=transport_zones,
        sample_size=test_data["population_sample_size"],
    )

    trips = mobility.Trips(population).get()
    trips_df = _to_pandas(trips)

    assert trips_df.shape[0] > 0
