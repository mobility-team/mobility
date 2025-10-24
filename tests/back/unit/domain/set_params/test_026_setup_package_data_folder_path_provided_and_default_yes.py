import os
from pathlib import Path
from mobility.set_params import setup_package_data_folder_path

def test_setup_package_data_folder_path_provided_creates_and_sets_env(tmp_path):
    provided = tmp_path / "pkgdata"
    assert not provided.exists()
    setup_package_data_folder_path(str(provided))
    assert provided.exists()
    assert Path(os.environ["MOBILITY_PACKAGE_DATA_FOLDER"]) == provided

def test_setup_package_data_folder_path_default_accepts(tmp_home, fake_input_yes):
    # No argument -> default to ~/.mobility/data under our tmp_home
    from mobility.set_params import setup_package_data_folder_path
    setup_package_data_folder_path(None)
    default_path = Path(tmp_home) / ".mobility" / "data"
    assert default_path.exists()
    assert os.environ["MOBILITY_PACKAGE_DATA_FOLDER"] == str(default_path)
