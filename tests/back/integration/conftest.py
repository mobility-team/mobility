import enum
import json

import pytest


# -------------------- Patch JSON global (dès l'import) --------------------
# Replace the default encoder to support "exotic" objects during json.dumps/json.dump
class _FallbackJSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, enum.Enum):
            return getattr(o, "value", o.name)
        for attr in ("model_dump", "dict"):
            method = getattr(o, attr, None)
            if callable(method):
                try:
                    return method()
                except Exception:
                    pass
        if hasattr(o, "__dict__"):
            return o.__dict__
        return str(o)


# Important: replace _default_encoder so it applies even if other modules did `from json import dumps`.
json._default_encoder = _FallbackJSONEncoder()  # type: ignore[attr-defined]


# -------------------- Fixtures de test --------------------
def get_test_data():
    return {
        "transport_zones_local_admin_unit_id": "fr-09261",  # Saint-Girons
        "transport_zones_radius": 10.0,
        "population_sample_size": 10,
    }

@pytest.fixture
def test_data():
    return get_test_data()
