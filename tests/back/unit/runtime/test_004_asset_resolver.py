from mobility.runtime.assets.file_asset import FileAsset
from mobility.runtime.assets.resolver import asset_resolution_context


class _CountingFileAsset(FileAsset):
    create_calls = []
    status_checks = {}

    def __init__(self, *, name, cache_folder, child=None):
        self.name = name
        inputs = {"name": name, "child": child}
        super().__init__(inputs, cache_folder / f"{name}.txt")

    def is_update_needed(self):
        _CountingFileAsset.status_checks[self.name] = (
            _CountingFileAsset.status_checks.get(self.name, 0) + 1
        )
        return super().is_update_needed()

    def get_cached_asset(self):
        return self.cache_path.read_text(encoding="utf-8")

    def create_and_get_asset(self):
        _CountingFileAsset.create_calls.append(self.name)
        value = f"created-{self.name}"
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        self.cache_path.write_text(value, encoding="utf-8")
        return value


class _NestedReadAsset(_CountingFileAsset):
    def create_and_get_asset(self):
        self.inputs["child"].get()
        self.inputs["child"].get()
        return super().create_and_get_asset()


class _ArgumentAsset(FileAsset):
    def __init__(self, *, cache_folder):
        super().__init__({"name": "argument"}, cache_folder / "argument.txt")

    def get_cached_asset(self, token=None):
        return f"cached-{token}-{self.cache_path.read_text(encoding='utf-8')}"

    def create_and_get_asset(self, token=None):
        self.cache_path.write_text(str(token), encoding="utf-8")
        return f"created-{token}"


def _reset_counts():
    _CountingFileAsset.create_calls = []
    _CountingFileAsset.status_checks = {}


def _mark_cached(asset):
    asset.create_and_get_asset()
    asset.update_hash(asset.inputs_hash)


def test_resolver_rebuilds_downstream_assets_when_upstream_is_stale(tmp_path):
    """A stale upstream file asset invalidates all downstream file assets."""
    _reset_counts()
    child = _CountingFileAsset(name="child", cache_folder=tmp_path / "child")
    bridge = _CountingFileAsset(
        name="bridge",
        cache_folder=tmp_path / "bridge",
        child=child,
    )
    root = _CountingFileAsset(name="root", cache_folder=tmp_path / "root", child=bridge)
    for asset in [child, bridge, root]:
        _mark_cached(asset)
    child.hash_path.write_text("old-child-inputs", encoding="utf-8")
    _reset_counts()

    value = root.get()

    assert value == "created-root"
    assert _CountingFileAsset.create_calls == ["child", "bridge", "root"]


def test_resolver_reuses_ready_assets_for_nested_get_calls(tmp_path):
    """Nested reads use the active resolver instead of rechecking the same child."""
    _reset_counts()
    child = _CountingFileAsset(name="child", cache_folder=tmp_path / "child")
    root = _NestedReadAsset(name="root", cache_folder=tmp_path / "root", child=child)

    root.get()

    assert _CountingFileAsset.create_calls == ["child", "root"]
    assert _CountingFileAsset.status_checks["child"] == 1


def test_resolver_context_shares_ready_assets_across_root_reads(tmp_path):
    """One explicit resolver context can cover several root asset reads."""
    _reset_counts()
    first_child = _CountingFileAsset(name="shared", cache_folder=tmp_path / "child")
    second_child = _CountingFileAsset(name="shared", cache_folder=tmp_path / "child")
    first_root = _CountingFileAsset(
        name="first-root",
        cache_folder=tmp_path / "first-root",
        child=first_child,
    )
    second_root = _CountingFileAsset(
        name="second-root",
        cache_folder=tmp_path / "second-root",
        child=second_child,
    )

    with asset_resolution_context():
        first_root.get()
        second_root.get()

    assert _CountingFileAsset.create_calls == ["shared", "first-root", "second-root"]
    assert _CountingFileAsset.status_checks["shared"] == 1


def test_resolver_does_not_recheck_ready_root_asset(tmp_path):
    """Repeated reads of the same root asset are cheap inside one context."""
    _reset_counts()
    asset = _CountingFileAsset(name="root", cache_folder=tmp_path)

    with asset_resolution_context():
        assert asset.get() == "created-root"
        assert asset.get() == "created-root"

    assert _CountingFileAsset.create_calls == ["root"]
    assert _CountingFileAsset.status_checks["root"] == 1


def test_update_ancestors_keeps_existing_upstream_only_contract(tmp_path):
    """The compatibility method still rebuilds ancestors, not the root asset."""
    _reset_counts()
    child = _CountingFileAsset(name="child", cache_folder=tmp_path / "child")
    root = _CountingFileAsset(name="root", cache_folder=tmp_path / "root", child=child)

    root.update_ancestors_if_needed()

    assert _CountingFileAsset.create_calls == ["child"]


def test_resolver_passes_get_arguments_to_requested_root_asset(tmp_path):
    """Root get arguments are preserved for assets that use them while creating."""
    asset = _ArgumentAsset(cache_folder=tmp_path)

    with asset_resolution_context():
        assert asset.get(token="bbox") == "created-bbox"
        assert asset.get(token="other") == "cached-other-bbox"
