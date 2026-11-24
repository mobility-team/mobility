from pathlib import Path

import polars as pl
import pytest

from mobility.runtime.assets.file_asset import FileAsset
from mobility.runtime.assets.in_memory_asset import InMemoryAsset
from mobility.runtime.assets.resolver import asset_resolution_context


class _TransientValue(InMemoryAsset):
    def __init__(self):
        super().__init__({"name": "example"})
        self.calls = 0

    def create_and_get_asset(self):
        self.calls += 1
        return self.calls


def test_transient_values_are_shared_only_within_one_execution():
    asset = _TransientValue()
    with asset_resolution_context() as resolver:
        assert asset.get() == 1
        assert asset.get() == 1
        resolver.release_in_memory_asset(asset)
        assert asset.get() == 2
    with asset_resolution_context():
        assert asset.get() == 3


def test_shared_table_can_be_reloaded_after_releasing_memory():
    class SharedTable(InMemoryAsset):
        def __init__(self):
            super().__init__({"name": "shared"})
            self.calls = 0

        def create_and_get_asset(self):
            self.calls += 1
            return pl.DataFrame({"value": [1, 2]})

    shared = SharedTable()
    roots = [InMemoryAsset({"name": name, "dependency": shared}) for name in ("first", "second")]
    with asset_resolution_context() as resolver:
        resolver.retain_assets_shared_by_roots(roots)
        assert shared.get().to_dict(as_series=False) == {"value": [1, 2]}
        spill_folder = resolver._spill_directory.name
        resolver.release_in_memory_asset(shared)
        assert shared.get().to_dict(as_series=False) == {"value": [1, 2]}
        assert shared.calls == 1

    assert not Path(spill_folder).exists()

def test_file_hash_is_not_written_before_success(tmp_path):
    class FailedFile(FileAsset):
        def get_cached_asset(self):
            return None

        def create_and_get_asset(self):
            raise ValueError("failed to create")

    asset = FailedFile({"name": "example"}, tmp_path / "data.txt")
    assert not asset.hash_path.exists()
    # An incomplete output from a failed earlier run must not be trusted.
    asset.cache_path.write_text("incomplete", encoding="utf-8")
    try:
        asset.get()
    except ValueError:
        pass
    else:
        raise AssertionError("Expected the asset creation to fail")
    assert not asset.hash_path.exists()
    assert asset.is_update_needed()


def test_shared_custom_state_is_saved_and_restored(tmp_path, monkeypatch):
    monkeypatch.setenv("MOBILITY_PROJECT_DATA_FOLDER", str(tmp_path))
    state = _TransientValue()
    roots = [InMemoryAsset({"name": name, "dependency": state}) for name in ("first", "second")]
    calls = []

    def save(value, path):
        calls.append("save")
        path.write_text(str(value), encoding="utf-8")

    def load(path):
        calls.append("load")
        return int(path.read_text(encoding="utf-8"))

    with asset_resolution_context() as resolver:
        resolver.retain_assets_shared_by_roots(roots)
        assert resolver.get_in_memory_asset(state, lambda: 42, spill_to_disk=save, load_from_disk=load) == 42
        assert Path(resolver._spill_directory.name).parent == tmp_path / "temporary"
        resolver.release_transient_values()
        assert resolver.get_in_memory_asset(state, lambda: -1, spill_to_disk=save, load_from_disk=load) == 42
        assert calls == ["save", "load"]


def test_shared_value_stays_in_memory_when_disk_write_fails(tmp_path, monkeypatch):
    monkeypatch.setenv("MOBILITY_PROJECT_DATA_FOLDER", str(tmp_path))
    state = _TransientValue()
    roots = [InMemoryAsset({"name": name, "dependency": state}) for name in ("first", "second")]

    def fail_to_save(value, path):
        raise OSError("disk full")

    with asset_resolution_context() as resolver:
        resolver.retain_assets_shared_by_roots(roots)
        assert resolver.get_in_memory_asset(state, lambda: 42, spill_to_disk=fail_to_save) == 42
        resolver.release_transient_values()
        assert resolver.get_in_memory_asset(state, lambda: -1, spill_to_disk=fail_to_save) == 42


def test_nonshared_values_are_recomputed_after_release():
    state = _TransientValue()
    with asset_resolution_context() as resolver:
        assert state.get() == 1
        resolver.release_transient_values()
        assert state.get() == 2


def test_transient_value_without_an_explicit_context_is_not_retained():
    asset = _TransientValue()
    assert asset.get() == 1
    assert asset.get() == 2


def test_unimplemented_transient_asset_has_a_clear_error():
    with pytest.raises(NotImplementedError, match="create_and_get_asset"):
        InMemoryAsset({"name": "missing"}).get()


def test_shared_table_stays_in_memory_if_writing_parquet_fails(tmp_path, monkeypatch):
    monkeypatch.setenv("MOBILITY_PROJECT_DATA_FOLDER", str(tmp_path))

    class SharedTable(InMemoryAsset):
        def __init__(self):
            super().__init__({"name": "shared-failing-table"})
            self.calls = 0

        def create_and_get_asset(self):
            self.calls += 1
            return pl.DataFrame({"value": [1]})

    def fail_to_write(self, path, *args, **kwargs):
        raise OSError("disk full")

    monkeypatch.setattr(pl.DataFrame, "write_parquet", fail_to_write)
    shared = SharedTable()
    roots = [InMemoryAsset({"name": name, "dependency": shared}) for name in ("first", "second")]
    with asset_resolution_context() as resolver:
        resolver.retain_assets_shared_by_roots(roots)
        assert shared.get()["value"].to_list() == [1]
        resolver.release_in_memory_asset(shared)
        assert shared.get()["value"].to_list() == [1]
        assert shared.calls == 1


def test_branching_states_are_marked_for_reuse(tmp_path, monkeypatch):
    monkeypatch.setenv("MOBILITY_PROJECT_DATA_FOLDER", str(tmp_path))

    class State(InMemoryAsset):
        is_execution_state = True

    initial = State({"name": "initial"})
    left = State({"name": "left", "previous": initial})
    right = State({"name": "right", "previous": initial})
    root = InMemoryAsset({"left": left, "right": right})
    with asset_resolution_context() as resolver:
        resolver.retain_assets_shared_by_roots([root])
        resolver.get_in_memory_asset(initial, lambda: pl.DataFrame({"value": [1]}))
        assert Path(resolver._spill_directory.name).parent == tmp_path / "temporary"
