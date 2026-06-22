import logging
import sqlite3
import sys

import pytest

from mobility.runtime.assets.file_asset import FileAsset
from mobility.runtime.project_cache import (
    ProjectCache,
    register_current_script_if_available,
)


class _TextFileAsset(FileAsset):
    """Tiny file asset used to test project-cache registration."""

    def __init__(self, *, name, cache_folder):
        self.name = name
        super().__init__({"name": name}, cache_folder / f"{name}.txt")

    def get_cached_asset(self):
        return self.cache_path.read_text(encoding="utf-8")

    def create_and_get_asset(self):
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        self.cache_path.write_text(self.name, encoding="utf-8")
        return self.name


class _ParentWritingChildAsset(FileAsset):
    """Asset that writes a child file while it is being rebuilt."""

    def __init__(self, *, child, cache_folder):
        self.child = child
        super().__init__({"child": child}, cache_folder / "parent.txt")

    def get_cached_asset(self):
        return self.cache_path.read_text(encoding="utf-8")

    def create_and_get_asset(self):
        self.child.get()
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        self.cache_path.write_text("parent", encoding="utf-8")
        return "parent"


def _asset_paths(project_folder):
    with sqlite3.connect(project_folder / ".mobility" / "cache.sqlite") as con:
        return {
            row[0]
            for row in con.execute("SELECT path FROM asset_cache_entries")
        }


def test_register_cache_source_links_file_asset_to_current_source(
    tmp_path,
    monkeypatch,
):
    """A file asset read under a cache source is kept by cleanup."""
    monkeypatch.setenv("MOBILITY_PROJECT_DATA_FOLDER", str(tmp_path))
    cache = ProjectCache()
    assert cache.register_cache_source("baseline notebook") is None
    asset = _TextFileAsset(name="kept", cache_folder=tmp_path / "cache")

    asset.get()

    assert str(asset.cache_path.resolve()) in _asset_paths(tmp_path)
    report = cache.unused_files_preview()

    assert report.files_to_delete == []
    assert report.kept_files_count == 2


def test_old_script_version_assets_become_deletable(tmp_path, monkeypatch):
    """Cleanup keeps only the latest active version of one script source."""
    monkeypatch.setenv("MOBILITY_PROJECT_DATA_FOLDER", str(tmp_path))
    cache = ProjectCache()
    script = tmp_path / "run_scenarios.py"
    script.write_text("version = 1\n", encoding="utf-8")
    cache._register_cache_source(str(script), ref_type="script")
    old_asset = _TextFileAsset(name="old", cache_folder=tmp_path / "cache")
    old_asset.get()

    script.write_text("version = 2\n", encoding="utf-8")
    cache._register_cache_source(str(script), ref_type="script")
    new_asset = _TextFileAsset(name="new", cache_folder=tmp_path / "cache")
    new_asset.get()

    report = cache.unused_files_preview()

    paths_to_delete = {path.name for path in report.files_to_delete}
    assert old_asset.cache_path.name in paths_to_delete
    assert new_asset.cache_path.name not in paths_to_delete


def test_modified_script_blocks_real_cleanup(tmp_path, monkeypatch):
    """A changed active script must be rerun before deleting cache files."""
    monkeypatch.setenv("MOBILITY_PROJECT_DATA_FOLDER", str(tmp_path))
    cache = ProjectCache()
    script = tmp_path / "run_scenarios.py"
    script.write_text("version = 1\n", encoding="utf-8")
    cache._register_cache_source(str(script), ref_type="script")
    _TextFileAsset(name="kept", cache_folder=tmp_path / "cache").get()

    script.write_text("version = 2\n", encoding="utf-8")

    report = cache.unused_files_preview()
    assert "changed since its last registered run" in report.blockers[0]
    with pytest.raises(RuntimeError, match="cache sources need attention"):
        cache.remove_unused_files()


def test_modified_script_still_allows_untracked_cleanup(
    tmp_path,
    monkeypatch,
):
    """A changed script blocks tracked files, not untracked cleanup."""
    monkeypatch.setenv("MOBILITY_PROJECT_DATA_FOLDER", str(tmp_path))
    cache = ProjectCache()
    script = tmp_path / "run_scenarios.py"
    script.write_text("version = 1\n", encoding="utf-8")
    cache._register_cache_source(str(script), ref_type="script")
    asset = _TextFileAsset(name="kept", cache_folder=tmp_path / "cache")
    asset.get()

    untracked_file = tmp_path / "exports" / "old-export.txt"
    untracked_file.parent.mkdir()
    untracked_file.write_text("delete me", encoding="utf-8")
    script.write_text("version = 2\n", encoding="utf-8")

    report = cache.remove_untracked_files()

    assert untracked_file in report.deleted_files
    assert not untracked_file.exists()
    assert asset.cache_path.exists()
    assert report.tracked_files_to_delete_count == 0
    assert report.untracked_files_to_delete_count == 1
    assert report.blockers == []


def test_modified_script_registration_does_not_make_previous_assets_unused(
    tmp_path,
    monkeypatch,
):
    """A cleanup-only script edit must not make previous assets unused."""
    monkeypatch.setenv("MOBILITY_PROJECT_DATA_FOLDER", str(tmp_path))
    cache = ProjectCache()
    script = tmp_path / "run_scenarios.py"
    script.write_text("version = 1\n", encoding="utf-8")
    cache._register_cache_source(str(script), ref_type="script")
    asset = _TextFileAsset(name="kept", cache_folder=tmp_path / "cache")
    asset.get()

    script.write_text("version = 2\n", encoding="utf-8")
    cache._register_cache_source(str(script), ref_type="script")

    report = cache.unused_files_preview()

    assert report.blockers
    assert asset.cache_path not in report.files_to_delete
    assert report.tracked_files_to_delete_count == 0
    with pytest.raises(RuntimeError, match="cache sources need attention"):
        cache.remove_unused_files()


def test_missing_script_blocks_real_cleanup(tmp_path, monkeypatch):
    """A removed active script must be archived before deleting cache files."""
    monkeypatch.setenv("MOBILITY_PROJECT_DATA_FOLDER", str(tmp_path))
    cache = ProjectCache()
    script = tmp_path / "old_scenarios.py"
    script.write_text("version = 1\n", encoding="utf-8")
    cache._register_cache_source(str(script), ref_type="script")
    _TextFileAsset(name="kept", cache_folder=tmp_path / "cache").get()

    script.unlink()

    report = cache.unused_files_preview()
    assert "is missing" in report.blockers[0]
    with pytest.raises(RuntimeError, match="cache sources need attention"):
        cache.remove_unused_files()


def test_archived_cache_source_stops_protecting_assets(tmp_path, monkeypatch):
    """Archiving a cache source makes its indexed assets cleanup candidates."""
    monkeypatch.setenv("MOBILITY_PROJECT_DATA_FOLDER", str(tmp_path))
    cache = ProjectCache()
    cache.register_cache_source("temporary notebook")
    asset = _TextFileAsset(name="temporary", cache_folder=tmp_path / "cache")
    asset.get()

    cache.archive_cache_source("temporary notebook")
    report = cache.remove_unused_files()

    assert asset.cache_path in report.deleted_files
    assert not asset.cache_path.exists()
    assert str(asset.cache_path.resolve()) not in _asset_paths(tmp_path)


def test_protected_project_data_is_never_deleted(tmp_path, monkeypatch):
    """Protected paths override normal cleanup candidates."""
    monkeypatch.setenv("MOBILITY_PROJECT_DATA_FOLDER", str(tmp_path))
    cache = ProjectCache()
    cache.register_cache_source("temporary notebook")
    asset = _TextFileAsset(name="temporary", cache_folder=tmp_path / "cache")
    asset.get()
    cache.archive_cache_source("temporary notebook")

    cache.protect("cache")
    report = cache.remove_unused_files()

    assert asset.cache_path.exists()
    assert asset.cache_path not in report.deleted_files
    assert cache.protected_paths() == [(tmp_path / "cache").resolve()]
    assert "Protected project data:" in str(report)
    assert str((tmp_path / "cache").resolve()) in str(report)


def test_untracked_files_are_reported_but_not_deleted_by_default(
    tmp_path, monkeypatch
):
    """Untracked files are reported only by untracked-file preview."""
    monkeypatch.setenv("MOBILITY_PROJECT_DATA_FOLDER", str(tmp_path))
    cache = ProjectCache()
    untracked_file = tmp_path / "reports" / "manual-note.txt"
    untracked_file.parent.mkdir(parents=True)
    untracked_file.write_text("keep this by default", encoding="utf-8")

    report = cache.remove_unused_files()

    assert untracked_file.exists()
    assert report.untracked_files_count == 0
    assert report.deleted_files == []

    untracked_report = cache.untracked_files_preview()
    assert untracked_report.untracked_files_count == 1
    assert "Cleanup candidates:" in str(untracked_report)


def test_auto_register_current_script_uses_main_file(tmp_path, monkeypatch):
    """set_params can register a normal Python entry script automatically."""
    monkeypatch.setenv("MOBILITY_PROJECT_DATA_FOLDER", str(tmp_path))
    script = tmp_path / "scripts" / "run.py"
    script.parent.mkdir()
    script.write_text("print('run')\n", encoding="utf-8")
    main_module = type("MainModule", (), {"__file__": str(script)})()
    monkeypatch.setitem(sys.modules, "__main__", main_module)

    version_id = register_current_script_if_available()

    assert version_id is not None
    sources = ProjectCache().cache_sources()
    assert sources[0]["name"] == str(script.relative_to(tmp_path))
    assert sources[0]["source_type"] == "script"
    assert sources[0]["status"] == "active"


def test_remove_untracked_files_can_delete_untracked_project_files(tmp_path, monkeypatch):
    """Untracked project file removal uses a dedicated method."""
    monkeypatch.setenv("MOBILITY_PROJECT_DATA_FOLDER", str(tmp_path))
    untracked_file = tmp_path / "reports" / "manual-note.txt"
    untracked_file.parent.mkdir(parents=True)
    untracked_file.write_text(
        "delete this only when explicitly requested",
        encoding="utf-8",
    )

    report = ProjectCache(tmp_path).remove_untracked_files()

    assert untracked_file in report.deleted_files
    assert not untracked_file.exists()
    assert report.untracked_files_to_delete_count == 1

    follow_up = ProjectCache(tmp_path).untracked_files_preview()
    assert follow_up.untracked_files_count == 0


def test_missing_untracked_rows_are_removed_from_reports(tmp_path, monkeypatch):
    """Untracked files deleted outside Mobility disappear from later reports."""
    monkeypatch.setenv("MOBILITY_PROJECT_DATA_FOLDER", str(tmp_path))
    cache = ProjectCache()
    untracked_file = tmp_path / "reports" / "manual-note.txt"
    untracked_file.parent.mkdir(parents=True)
    untracked_file.write_text("old file", encoding="utf-8")

    first_report = cache.untracked_files_preview()
    assert first_report.untracked_files_count == 1

    untracked_file.unlink()
    second_report = cache.untracked_files_preview()

    assert second_report.untracked_files_count == 0


def test_seen_untracked_file_becomes_tracked_cache_file(tmp_path, monkeypatch):
    """An untracked file reused by a cache source is no longer a cleanup candidate."""
    monkeypatch.setenv("MOBILITY_PROJECT_DATA_FOLDER", str(tmp_path))
    cache = ProjectCache()
    asset = _TextFileAsset(name="old", cache_folder=tmp_path / "group_day_trips")
    asset.create_and_get_asset()

    first_report = cache.untracked_files_preview()
    assert first_report.untracked_files_count == 2

    cache.register_cache_source("kept notebook")
    asset.get()

    second_report = cache.remove_untracked_files()
    assert second_report.untracked_files_count == 0
    assert second_report.deleted_files == []
    assert asset.cache_path.exists()


def test_child_asset_written_during_rebuild_is_registered(tmp_path, monkeypatch):
    """Files written by nested assets are not treated as unregistered files."""
    monkeypatch.setenv("MOBILITY_PROJECT_DATA_FOLDER", str(tmp_path))
    cache = ProjectCache()
    cache.register_cache_source("model script")
    child = _TextFileAsset(name="child", cache_folder=tmp_path / "cache")
    parent = _ParentWritingChildAsset(child=child, cache_folder=tmp_path / "cache")

    parent.get()

    untracked_report = cache.untracked_files_preview()

    assert child.cache_path not in untracked_report.files_to_delete
    assert child.hash_path not in untracked_report.files_to_delete
    assert parent.cache_path not in untracked_report.files_to_delete
    assert parent.hash_path not in untracked_report.files_to_delete


def test_unused_files_preview_never_deletes_files(tmp_path, monkeypatch):
    """ProjectCache.unused_files_preview returns the report without deleting files."""
    monkeypatch.setenv("MOBILITY_PROJECT_DATA_FOLDER", str(tmp_path))
    cache = ProjectCache()
    cache.register_cache_source("temporary notebook")
    asset = _TextFileAsset(name="temporary", cache_folder=tmp_path / "cache")
    asset.get()
    cache.archive_cache_source("temporary notebook")

    report = cache.unused_files_preview()

    assert report.preview is True
    assert asset.cache_path in report.files_to_delete
    assert asset.cache_path.exists()
    assert "Preview: no files were deleted." in str(report)
    assert repr(report) == str(report)
    assert "ProjectCacheReport(" not in repr(report)


def test_untracked_files_preview_logs_debug_scan_details(tmp_path, monkeypatch, caplog):
    """Debug logs show where project-cache cleanup spends time."""
    monkeypatch.setenv("MOBILITY_PROJECT_DATA_FOLDER", str(tmp_path))
    untracked_file = tmp_path / "reports" / "manual-note.txt"
    untracked_file.parent.mkdir()
    untracked_file.write_text("inspect me", encoding="utf-8")

    cache = ProjectCache()
    with caplog.at_level(logging.DEBUG, logger="mobility.runtime.project_cache"):
        report = cache.untracked_files_preview()

    messages = [record.getMessage() for record in caplog.records]
    assert report.untracked_files_count == 1
    assert any("Project cache cleanup started" in message for message in messages)
    assert any(
        "Project cache untracked-file scan started" in message for message in messages
    )
    assert any(
        "Project cache untracked-file scan finished" in message for message in messages
    )
    assert any("Project cache cleanup finished" in message for message in messages)


