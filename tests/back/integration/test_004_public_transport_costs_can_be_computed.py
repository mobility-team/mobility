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


def _to_df(maybe_df_or_path):
    """Accept a DataFrame, a path-like to a parquet, or a dict with a 'path'-like value."""
    if hasattr(maybe_df_or_path, "shape"):
        return maybe_df_or_path
    if isinstance(maybe_df_or_path, dict):
        # common keys that might carry the parquet path
        for k in ("costs", "generalized_cost", "path", "data", "output"):
            if k in maybe_df_or_path:
                val = maybe_df_or_path[k]
                if hasattr(val, "shape"):
                    return val
                if isinstance(val, (str, os.PathLike)):
                    return pd.read_parquet(val)
    if isinstance(maybe_df_or_path, (str, os.PathLike)):
        return pd.read_parquet(maybe_df_or_path)
    # last resort: try pandas if it looks like a file
    try:
        return pd.read_parquet(str(maybe_df_or_path))
    except Exception:
        raise AssertionError(f"Expected DataFrame or path-like, got {type(maybe_df_or_path)}")


@pytest.mark.dependency(
    depends=["tests/back/integration/test_002_population_sample_can_be_created.py::test_002_population_sample_can_be_created"],
    scope="session",
)
def test_004_public_transport_costs_can_be_computed(test_data, safe_json):
    transport_zones = mobility.TransportZones(
        local_admin_unit_id=test_data["transport_zones_local_admin_unit_id"],
        radius=test_data["transport_zones_radius"],
    )

    walk = mobility.WalkMode(transport_zones)

    transfer = mobility.IntermodalTransfer(
        max_travel_time=20.0 / 60.0,
        average_speed=5.0,
        transfer_time=1.0,
    )

    gen_cost_parms = mobility.GeneralizedCostParameters(
        cost_constant=0.0,
        cost_of_distance=0.0,
        cost_of_time=mobility.CostOfTimeParameters(
            intercept=7.0,
            breaks=[0.0, 2.0, 10.0, 50.0, 10000.0],
            slopes=[0.0, 1.0, 0.1, 0.067],
            max_value=21.0,
        ),
    )

    public_transport = mobility.PublicTransportMode(
        transport_zones,
        first_leg_mode=walk,
        first_intermodal_transfer=transfer,
        last_leg_mode=walk,
        last_intermodal_transfer=transfer,
        generalized_cost_parameters=gen_cost_parms,
        routing_parameters=mobility.PublicTransportRoutingParameters(
            max_traveltime=10.0,
            max_perceived_time=10.0,
        ),
    )

    costs_raw = public_transport.travel_costs.get()
    gen_raw = public_transport.generalized_cost.get(["distance", "time"])

    costs_df = _to_df(costs_raw)
    gen_df = _to_df(gen_raw)

    assert hasattr(costs_df, "shape"), f"Expected a DataFrame-like for costs, got {type(costs_df)}"
    assert hasattr(gen_df, "shape"), f"Expected a DataFrame-like for generalized costs, got {type(gen_df)}"
    assert costs_df.shape[0] > 0
    assert gen_df.shape[0] > 0
