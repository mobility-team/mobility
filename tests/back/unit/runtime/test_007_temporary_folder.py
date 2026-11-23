from mobility.runtime.temporary import temporary_folder


def test_temporary_folder_uses_project_data(tmp_path, monkeypatch):
    monkeypatch.setenv("MOBILITY_PROJECT_DATA_FOLDER", str(tmp_path))
    assert temporary_folder() == tmp_path / "temporary"
    assert (tmp_path / "temporary").is_dir()


def test_temporary_folder_defaults_to_package_data(tmp_path, monkeypatch):
    monkeypatch.delenv("MOBILITY_PROJECT_DATA_FOLDER", raising=False)
    monkeypatch.setenv("MOBILITY_PACKAGE_DATA_FOLDER", str(tmp_path))
    assert temporary_folder() == tmp_path / "projects" / "temporary"
