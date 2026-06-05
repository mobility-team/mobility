from mobility.runtime.assets.file_asset import FileAsset
from mobility.runtime.assets.in_memory_asset import InMemoryAsset


class _CountingAsset(FileAsset):
    create_calls = 0

    def __init__(self, *, name, cache_folder):
        super().__init__({"name": name}, cache_folder / "counting_asset.txt")

    def get_cached_asset(self):
        return self.cache_path.read_text(encoding="utf-8")

    def create_and_get_asset(self):
        _CountingAsset.create_calls += 1
        self.cache_path.write_text("ready", encoding="utf-8")
        return "ready"


class _ParentAsset(FileAsset):
    def __init__(self, *, children, cache_folder):
        super().__init__({"children": children}, cache_folder / "parent_asset.txt")

    def get_cached_asset(self):
        return None

    def create_and_get_asset(self):
        return None


class _MemoryWrapperAsset(InMemoryAsset):
    def __init__(self, *, child):
        super().__init__({"child": child})


def test_update_ancestors_deduplicates_logically_identical_file_assets(tmp_path):
    """Do not rebuild the same upstream asset twice when it appears as two objects."""
    _CountingAsset.create_calls = 0
    first_child = _CountingAsset(name="same", cache_folder=tmp_path)
    second_child = _CountingAsset(name="same", cache_folder=tmp_path)
    parent = _ParentAsset(
        children=[first_child, second_child],
        cache_folder=tmp_path,
    )

    parent.update_ancestors_if_needed()

    assert _CountingAsset.create_calls == 1
    assert first_child.cache_path.exists()


def test_update_ancestors_rebuilds_file_assets_below_in_memory_file_inputs(tmp_path):
    """Rebuild file assets even when an in-memory asset sits below another FileAsset."""
    _CountingAsset.create_calls = 0
    child = _CountingAsset(name="hidden", cache_folder=tmp_path / "child")
    wrapper = _MemoryWrapperAsset(child=child)
    bridge = _ParentAsset(children=[wrapper], cache_folder=tmp_path / "bridge")
    parent = _ParentAsset(children=[bridge], cache_folder=tmp_path / "parent")

    parent.update_ancestors_if_needed()

    assert _CountingAsset.create_calls == 1
    assert child.cache_path.exists()
