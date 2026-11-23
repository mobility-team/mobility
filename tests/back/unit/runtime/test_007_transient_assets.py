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
