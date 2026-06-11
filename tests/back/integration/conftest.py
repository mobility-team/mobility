import enum
import json
import os
import pathlib

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
        "transport_zones_local_admin_unit_id": "fr-87085",  # Limoges keeps tests inside the smaller Limousin OSM extract.
        "transport_zones_radius": 10.0,
        "population_sample_size": 10,
    }

@pytest.fixture
def test_data():
    return get_test_data()


@pytest.fixture
def gtfs_sources_folder():
    """Store GTFS source cache files in the Mobility project test cache."""
    folder = pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"]) / "inputs" / "gtfs_sources"
    folder.mkdir(parents=True, exist_ok=True)
    return str(folder)
