import os
from pathlib import Path
from mobility.set_params import setup_package_data_folder_path, setup_project_data_folder_path

def test_setup_project_data_folder_provided_creates_and_sets_env(tmp_path):
    provided = tmp_path / "projdata"
    assert not provided.exists()
    setup_project_data_folder_path(str(provided))
    assert provided.exists()
    assert Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"]) == provided

def test_setup_project_data_folder_default_accepts(tmp_home, fake_input_yes):
    # First, set package data folder default (under tmp_home)
    setup_package_data_folder_path(None)
    # Then, call project with None => defaults to <package>/projects
    setup_project_data_folder_path(None)
    base = Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"])
    default_project = base / "projects"
    assert default_project.exists()
    assert os.environ["MOBILITY_PROJECT_DATA_FOLDER"] == str(default_project)
