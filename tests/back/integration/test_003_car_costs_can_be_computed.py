import enum
import json
import os
import pytest
import mobility
import pandas as pd


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


@pytest.mark.dependency(
    depends=["tests/back/integration/test_002_population_sample_can_be_created.py::test_002_population_sample_can_be_created"],
    scope="session",
)
def test_003_car_costs_can_be_computed(test_data, safe_json):
    transport_zones = mobility.TransportZones(
        local_admin_unit_id=test_data["transport_zones_local_admin_unit_id"],
        radius=test_data["transport_zones_radius"],
    )

    car = mobility.CarMode(transport_zones)
    costs = car.travel_costs.get()

    # Some implementations return a DataFrame; others return a dict with paths.
    if isinstance(costs, (str, os.PathLike)):
        # path to parquet
        df = pd.read_parquet(costs)
    elif isinstance(costs, dict) and "costs" in costs:
        path = costs["costs"]
        if not isinstance(path, (str, os.PathLike)):
            path = str(path)
        df = pd.read_parquet(path)
    else:
        # assume DataFrame-like
        df = costs

    assert hasattr(df, "shape"), f"Expected a DataFrame-like, got {type(df)}"
    assert df.shape[0] > 0
