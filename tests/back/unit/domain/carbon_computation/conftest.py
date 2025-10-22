# tests/back/integration/conftest.py
import os
import pathlib
import shutil
import dotenv
import pytest
import mobility


# ------------- config helpers -------------
def _truthy(v):
    return str(v).lower() in {"1", "true", "yes", "y", "on"} if v is not None else False

def _repo_root() -> pathlib.Path:
    # .../tests/back/integration/conftest.py  -> repo root (parents[3])
    return pathlib.Path(__file__).resolve().parents[3]

def _load_dotenv_from_repo_root():
    dotenv.load_dotenv(_repo_root() / ".env")


# ------------- mobility setup -------------
def _do_mobility_setup(*, local: bool, clear_inputs: bool, clear_results: bool):
    if local:
        # require vars to be present (from shell or .env)
        data_folder = os.environ.get("MOBILITY_PACKAGE_DATA_FOLDER")
        project_folder = os.environ.get("MOBILITY_PACKAGE_PROJECT_FOLDER")

        if not data_folder or not project_folder:
            raise RuntimeError(
                "MOBILITY_PACKAGE_DATA_FOLDER and MOBILITY_PACKAGE_PROJECT_FOLDER must be set "
                "when running with local mode. Define them in your repo-root .env or env."
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
        # default; can be overridden by env/.env
        os.environ.setdefault("MOBILITY_GTFS_DOWNLOAD_DATE", "2025-01-01")


# ------------- IMPORTANT: run setup at import time -------------
# This executes *before* pytest imports any test modules in this directory,
# avoiding “too late” initialization without needing a plugin.
_load_dotenv_from_repo_root()
_local = _truthy(os.environ.get("MOBILITY_LOCAL"))
_clear_inputs = _truthy(os.environ.get("MOBILITY_CLEAR_INPUTS"))
_clear_results = _truthy(os.environ.get("MOBILITY_CLEAR_RESULTS"))
_do_mobility_setup(local=_local, clear_inputs=_clear_inputs, clear_results=_clear_results)


# ------------- fixtures used by tests -------------
def get_test_data():
    return {
        # fails with Foix 09122 and Rodez 12202, let's test Saint-Girons
        "transport_zones_local_admin_unit_id": "fr-09261",
        "transport_zones_radius": 10.0,
        "population_sample_size": 10,
    }

@pytest.fixture
def test_data():
    return get_test_data()
