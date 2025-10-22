import enum
import json
import os
import pandas as pd
import pytest
import mobility


@pytest.fixture
def safe_json(monkeypatch):
    """
    Make json.dump/json.dumps (and orjson.dumps if present) resilient to
    non-serializable objects (e.g., LocalAdminUnitsCategories).
    """
    def _fallback(o):
        # Enum -> value or name
        if isinstance(o, enum.Enum):
            return getattr(o, "value", o.name)
        # Rich objects (pydantic/dataclasses-like)
        for attr in ("model_dump", "dict"):
            m = getattr(o, attr, None)
            if callable(m):
                try:
                    return m()
                except Exception:
                    pass
        # __dict__ if available, else str()
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

    # If the lib uses orjson, patch it too
    try:
        import orjson  # type: ignore

        _orig_orjson_dumps = orjson.dumps

        def safe_orjson_dumps(obj, *args, **kwargs):
            try:
                return _orig_orjson_dumps(obj, *args, **kwargs)
            except TypeError:
                # Fallback: go through json.dumps with our default
                txt = json.dumps(obj, default=_fallback)
                import json as _json
                return _orig_orjson_dumps(_json.loads(txt), *args, **kwargs)

        monkeypatch.setattr(orjson, "dumps", safe_orjson_dumps, raising=False)
    except Exception:
        pass


@pytest.mark.dependency(
    depends=["tests/back/integration/test_001_transport_zones_can_be_created.py::test_001_transport_zones_can_be_created"],
    scope="session",
)
def test_002_population_sample_can_be_created(test_data, safe_json):
    # Build transport zones
    transport_zones = mobility.TransportZones(
        local_admin_unit_id=test_data["transport_zones_local_admin_unit_id"],
        radius=test_data["transport_zones_radius"],
    )

    # Build population
    population = mobility.Population(
        transport_zones=transport_zones,
        sample_size=test_data["population_sample_size"],
    )

    # Resolve the path to the parquet produced by Population.get()
    pop_result = population.get()
    individuals_path = pop_result["individuals"]
    if not isinstance(individuals_path, (str, os.PathLike)):
        # Defensive: some libs return custom path objects
        individuals_path = str(individuals_path)

    df = pd.read_parquet(individuals_path)

    # Basic sanity check
    assert hasattr(df, "shape"), f"Expected a DataFrame-like, got {type(df)}"
    assert df.shape[0] > 0
