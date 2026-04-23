import os
import pathlib
import shutil

import dotenv
import mobility
import pytest

def pytest_addoption(parser):
    parser.addoption("--local", action="store_true", default=False)
    parser.addoption("--clear_inputs", action="store_true", default=False)
    parser.addoption("--clear_results", action="store_true", default=False)
    parser.addoption("--use-truststore", action="store_true", default=False)
    parser.addoption("--debug-r", action="store_true", default=False)

@pytest.fixture(scope="session")
def clear_inputs(request):
    return request.config.getoption("--clear_inputs")

@pytest.fixture(scope="session")
def clear_results(request):
    return request.config.getoption("--clear_results")

@pytest.fixture(scope="session")
def local(request):
    return request.config.getoption("--local")

@pytest.fixture(scope="session")
def use_truststore(request):
    return request.config.getoption("--use-truststore")

@pytest.fixture(scope="session")
def debug_r(request):
    return request.config.getoption("--debug-r")

def _repo_root() -> pathlib.Path:
    # .../tests/conftest.py -> repo root
    return pathlib.Path(__file__).resolve().parents[1]

def _load_dotenv_from_repo_root() -> None:
    dotenv.load_dotenv(_repo_root() / ".env")

def do_mobility_setup(local, clear_inputs, clear_results, debug_r):
    if local:
        _load_dotenv_from_repo_root()

    data_folder = os.environ.get("MOBILITY_PACKAGE_DATA_FOLDER")
    project_folder = os.environ.get("MOBILITY_PACKAGE_PROJECT_FOLDER")

    if data_folder and project_folder:
        package_data_folder_path = data_folder
        project_data_folder_path = project_folder
        extra_params = {}
    else:
        if local:
            raise RuntimeError(
                "MOBILITY_PACKAGE_DATA_FOLDER and MOBILITY_PACKAGE_PROJECT_FOLDER must be set in local mode "
                "(e.g. in repo-root .env)."
            )
        package_data_folder_path = pathlib.Path.home() / ".mobility/data"
        project_data_folder_path = pathlib.Path.home() / ".mobility/projects/tests"
        extra_params = {"r_packages": False}

    if clear_inputs is True:
        shutil.rmtree(package_data_folder_path, ignore_errors=True)

    if clear_results is True:
        shutil.rmtree(project_data_folder_path, ignore_errors=True)

    mobility.set_params(
        package_data_folder_path=package_data_folder_path,
        project_data_folder_path=project_data_folder_path,
        debug=debug_r,
        **extra_params,
    )

    # Default for tests; can be overridden by env/.env
    os.environ.setdefault("MOBILITY_GTFS_DOWNLOAD_DATE", "2025-01-01")

def pytest_configure(config):
    do_mobility_setup(
        config.getoption("--local"),
        config.getoption("--clear_inputs"),
        config.getoption("--clear_results"),
        config.getoption("--debug-r"),
    )

    if config.getoption("--use-truststore"):
        import truststore

        truststore.inject_into_ssl()

def get_test_data():
    return {
        "transport_zones_local_admin_unit_id": "fr-09261", #fails with Foix 09122 and Rodez 12202, let's test Saint-Girons
        "transport_zones_radius": 10.0,
        "population_sample_size": 10
    }

@pytest.fixture
def test_data():
    return get_test_data()
