import mobility
import pytest
import dotenv
import shutil
import os
import pathlib

def pytest_addoption(parser):
    parser.addoption("--local", action="store_true", default=False)
    parser.addoption("--clear_inputs", action="store_true", default=False)
    parser.addoption("--clear_results", action="store_true", default=False)

@pytest.fixture(scope="session")
def clear_inputs(request):
    return request.config.getoption("--clear_inputs")

@pytest.fixture(scope="session")
def clear_results(request):
    return request.config.getoption("--clear_results")

@pytest.fixture(scope="session")
def local(request):
    return request.config.getoption("--local")

def do_mobility_setup(local, clear_inputs, clear_results):

    if local:

        dotenv.load_dotenv()

        if clear_inputs is True:
            shutil.rmtree(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"], ignore_errors=True)

        if clear_results is True:
            shutil.rmtree(os.environ["MOBILITY_PACKAGE_PROJECT_FOLDER"], ignore_errors=True)

        mobility.set_params(
            package_data_folder_path=os.environ["MOBILITY_PACKAGE_DATA_FOLDER"],
            project_data_folder_path=os.environ["MOBILITY_PACKAGE_PROJECT_FOLDER"]
        )

    else:

        mobility.set_params(
            package_data_folder_path=pathlib.Path.home() / ".mobility/data",
            project_data_folder_path=pathlib.Path.home() / ".mobility/projects/tests",
            r_packages=False
        )

        # Set the env var directly for now
        # TO DO : see how could do this differently...
        os.environ["MOBILITY_GTFS_DOWNLOAD_DATE"] = "2025-01-01"

@pytest.fixture(scope="session", autouse=True)
def setup_mobility(local, clear_inputs, clear_results):
    do_mobility_setup(local, clear_inputs, clear_results)


def get_test_data():
    return {
        "transport_zones_local_admin_unit_id": "fr-09261", #fails with Foix 09122 and Rodez 12202, let's test Saint-Girons
        "transport_zones_radius": 10.0,
        "population_sample_size": 10
    }

@pytest.fixture
def test_data():
    return get_test_data()
