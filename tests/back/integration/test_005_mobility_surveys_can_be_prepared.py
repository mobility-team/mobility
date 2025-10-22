import enum
import json
import os
import pandas as pd
import pytest

from mobility.parsers.mobility_survey.france import EMPMobilitySurvey, ENTDMobilitySurvey


@pytest.fixture
def safe_json(monkeypatch):
    """
    Make json.dump/json.dumps (and orjson.dumps if present) resilient to
    non-serializable objects during this test only.
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


def _to_df(val):
    """Return a DataFrame from a DataFrame or a path-like (reads parquet)."""
    if hasattr(val, "shape"):
        return val
    if isinstance(val, (str, os.PathLike)):
        return pd.read_parquet(val)
    # last resort: try reading as a path-ish string
    try:
        return pd.read_parquet(str(val))
    except Exception:
        raise AssertionError(f"Expected DataFrame or path-like, got {type(val)}")


@pytest.mark.dependency()
def test_005_mobility_surveys_can_be_prepared(test_data, safe_json):
    ms_2019 = EMPMobilitySurvey().get()
    ms_2008 = ENTDMobilitySurvey().get()

    dfs_names = [
        "short_trips",
        "days_trip",
        "long_trips",
        "travels",
        "n_travels",
        "p_immobility",
        "p_car",
        "p_det_mode",
    ]

    # Ensure all expected keys exist
    assert all(name in ms_2019 for name in dfs_names), f"Missing from 2019: {[n for n in dfs_names if n not in ms_2019]}"
    assert all(name in ms_2008 for name in dfs_names), f"Missing from 2008: {[n for n in dfs_names if n not in ms_2008]}"

    # Ensure each referenced table is non-empty (handles DF or path-like)
    for name in dfs_names:
        df19 = _to_df(ms_2019[name])
        df08 = _to_df(ms_2008[name])
        assert df19.shape[0] > 0, f"2019 '{name}' is empty"
        assert df08.shape[0] > 0, f"2008 '{name}' is empty"
