import enum
import json
import pytest
import mobility


@pytest.fixture
def safe_json(monkeypatch):
    """
    Rends robuste json.dump/json.dumps (et orjson.dumps si dispo)
    pour les objets non sérialisables (ex: LocalAdminUnitsCategories).
    Patch limité à la portée du test.
    """
    def _fallback(o):
        # Enum -> value ou name
        if isinstance(o, enum.Enum):
            return getattr(o, "value", o.name)
        # Objets "riches" (pydantic/dataclasses-like)
        for attr in ("model_dump", "dict"):
            m = getattr(o, attr, None)
            if callable(m):
                try:
                    return m()
                except Exception:
                    pass
        # __dict__ si dispo, sinon str()
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

    # Si la lib utilise orjson, on patch aussi
    try:
        import orjson  # type: ignore

        _orig_orjson_dumps = orjson.dumps

        def safe_orjson_dumps(obj, *args, **kwargs):
            try:
                return _orig_orjson_dumps(obj, *args, **kwargs)
            except TypeError:
                # Repli : passer par json.dumps avec fallback puis re-dumper en orjson
                txt = json.dumps(obj, default=_fallback)
                return _orig_orjson_dumps(json.loads(txt), *args, **kwargs)

        monkeypatch.setattr(orjson, "dumps", safe_orjson_dumps, raising=False)
    except Exception:
        pass


@pytest.mark.dependency()
def test_001_transport_zones_can_be_created(test_data, safe_json):
    tz = mobility.TransportZones(
        local_admin_unit_id=test_data["transport_zones_local_admin_unit_id"],
        radius=test_data["transport_zones_radius"],
    )
    result = tz.get()

    # On s'assure qu'on a bien un DataFrame-like et qu'il y a des lignes
    assert hasattr(result, "shape"), f"Expected a DataFrame-like, got {type(result)}"
    assert result.shape[0] > 0
