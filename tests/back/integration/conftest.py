# tests/back/integration/conftest.py
import os
import enum
import json
import pathlib
import shutil

import dotenv
import pytest
import mobility


# -------------------- Patch JSON global (dès l'import) --------------------
# Remplace l'encodeur par défaut pour supporter les objets exotiques (ex: LocalAdminUnitsCategories)
class _FallbackJSONEncoder(json.JSONEncoder):
    def default(self, o):
        # Enum -> valeur (ou nom)
        if isinstance(o, enum.Enum):
            return getattr(o, "value", o.name)
        # Objets "riches" (pydantic/dataclass-like)
        for attr in ("model_dump", "dict"):
            method = getattr(o, attr, None)
            if callable(method):
                try:
                    return method()
                except Exception:
                    pass
        # Fallback __dict__
        if hasattr(o, "__dict__"):
            return o.__dict__
        # Dernier recours : str()
        return str(o)

# ⚠️ Important : on remplace _default_encoder, utilisé par json.dumps/json.dump
# même si d'autres modules ont fait `from json import dumps`.
json._default_encoder = _FallbackJSONEncoder()  # type: ignore[attr-defined]


# -------------------- Helpers --------------------
def _truthy(v):
    return str(v or "").lower() in {"1", "true", "yes", "y", "on"}

def _repo_root() -> pathlib.Path:
    # .../tests/back/integration/conftest.py -> repo root
    return pathlib.Path(__file__).resolve().parents[3]

def _load_dotenv_from_repo_root():
    dotenv.load_dotenv(_repo_root() / ".env")


# -------------------- Mobility setup --------------------
def do_mobility_setup(local: bool, clear_inputs: bool, clear_results: bool):
    if local:
        _load_dotenv_from_repo_root()
        data_folder = os.environ.get("MOBILITY_PACKAGE_DATA_FOLDER")
        project_folder = os.environ.get("MOBILITY_PACKAGE_PROJECT_FOLDER")

        if not data_folder or not project_folder:
            raise RuntimeError(
                "MOBILITY_PACKAGE_DATA_FOLDER et MOBILITY_PACKAGE_PROJECT_FOLDER "
                "doivent être définies en mode local (par ex. dans le .env à la racine)."
            )

        if clear_inputs:
            shutil.rmtree(data_folder, ignore_errors=True)
        if clear_results:
            shutil.rmtree(project_folder, ignore_errors=True)

        mobility.set_params(
            package_data_folder_path=data_folder,
            project_data_folder_path=project_folder,
        )
    else:
        mobility.set_params(
            package_data_folder_path=pathlib.Path.home() / ".mobility/data",
            project_data_folder_path=pathlib.Path.home() / ".mobility/projects/tests",
            r_packages=False,
        )
        # Valeur par défaut inoffensive (surchageable via env/.env)
        os.environ.setdefault("MOBILITY_GTFS_DOWNLOAD_DATE", "2025-01-01")


def pytest_configure(config):
    """Exécuté tôt, avant l'import des modules de test de ce dossier."""
    _load_dotenv_from_repo_root()
    local = _truthy(os.environ.get("MOBILITY_LOCAL"))
    clear_inputs = _truthy(os.environ.get("MOBILITY_CLEAR_INPUTS"))
    clear_results = _truthy(os.environ.get("MOBILITY_CLEAR_RESULTS"))
    do_mobility_setup(local, clear_inputs, clear_results)


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
