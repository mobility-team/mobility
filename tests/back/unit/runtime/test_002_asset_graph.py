from mobility.runtime.assets.file_asset import FileAsset
from mobility.runtime.assets.graph import (
    build_asset_graph,
    build_asset_graph_from_roots,
    build_upstream_file_asset_graph,
)
from mobility.runtime.assets.in_memory_asset import InMemoryAsset


class _TextAsset(FileAsset):
    status_checks = 0

    def __init__(self, *, name, cache_folder):
        super().__init__({"name": name}, cache_folder / "text_asset.txt")

    def is_update_needed(self):
        _TextAsset.status_checks += 1
        return super().is_update_needed()

    def get_cached_asset(self):
        return self.cache_path.read_text(encoding="utf-8")

    def create_and_get_asset(self):
        self.cache_path.write_text("ready", encoding="utf-8")
        return "ready"


class _ParentAsset(FileAsset):
    def __init__(self, *, children, cache_folder):
        super().__init__({"children": children}, cache_folder / "parent_asset.txt")

    def get_cached_asset(self):
        return None

    def create_and_get_asset(self):
        return None


class _MultiOutputAsset(FileAsset):
    def __init__(self, *, cache_folder):
        super().__init__(
            {"name": "multi"},
            {
                "first": cache_folder / "first.txt",
                "second": cache_folder / "second.txt",
            },
        )

    def get_cached_asset(self):
        return None

    def create_and_get_asset(self):
        return None


class _MemoryAsset(InMemoryAsset):
    def __init__(self, *, name, child=None):
        super().__init__({"name": name, "child": child})


def test_build_asset_graph_deduplicates_equivalent_file_assets(tmp_path):
    """Show one node for two file assets that point to the same cached output."""
    first_child = _TextAsset(name="same", cache_folder=tmp_path)
    second_child = _TextAsset(name="same", cache_folder=tmp_path)
    parent = _ParentAsset(
        children=[first_child, second_child],
        cache_folder=tmp_path,
    )

    graph = build_asset_graph(parent)

    assert graph.number_of_nodes() == 2
    assert graph.number_of_edges() == 1


def test_build_upstream_file_asset_graph_leaves_out_the_root_asset(tmp_path):
    """The rebuild graph contains only upstream assets, not the asset being read."""
    child = _TextAsset(name="child", cache_folder=tmp_path)
    parent = _ParentAsset(children=[child], cache_folder=tmp_path)

    graph = build_upstream_file_asset_graph(parent)

    assert list(graph.nodes) == [child]


def test_build_upstream_file_asset_graph_does_not_check_cache_status(tmp_path):
    """Runtime graph building must not do viewer-only filesystem checks."""
    _TextAsset.status_checks = 0
    child = _TextAsset(name="child", cache_folder=tmp_path)
    parent = _ParentAsset(children=[child], cache_folder=tmp_path)

    build_upstream_file_asset_graph(parent)

    assert _TextAsset.status_checks == 0


def test_build_upstream_file_asset_graph_looks_through_in_memory_assets(tmp_path):
    """Runtime graph building finds file assets nested below in-memory assets."""
    child = _TextAsset(name="hidden-file", cache_folder=tmp_path)
    wrapper = _MemoryAsset(name="wrapper", child=child)
    parent = _ParentAsset(children=[wrapper], cache_folder=tmp_path)

    graph = build_upstream_file_asset_graph(parent)

    assert list(graph.nodes) == [child]


def test_build_asset_graph_reports_missing_cached_and_stale_assets(tmp_path):
    """Expose simple cache states for graph views and exports."""
    missing = _TextAsset(name="missing", cache_folder=tmp_path)
    cached = _TextAsset(name="cached", cache_folder=tmp_path)
    stale = _TextAsset(name="stale", cache_folder=tmp_path)
    parent = _ParentAsset(
        children=[missing, cached, stale],
        cache_folder=tmp_path,
    )

    cached.create_and_get_asset()
    stale.create_and_get_asset()
    stale.hash_path.write_text("old-inputs", encoding="utf-8")

    graph = build_asset_graph(parent)
    statuses_by_name = {
        asset.inputs["name"]: data["status"]
        for asset, data in graph.nodes(data=True)
        if isinstance(asset, _TextAsset)
    }

    assert statuses_by_name == {
        "missing": "missing",
        "cached": "cached",
        "stale": "stale",
    }


def test_build_asset_graph_reports_which_outputs_are_missing(tmp_path):
    """Show which file is missing when a multi-output asset is incomplete."""
    asset = _MultiOutputAsset(cache_folder=tmp_path)
    asset.cache_path["first"].write_text("ready", encoding="utf-8")

    graph = build_asset_graph(asset)
    node_data = graph.nodes[asset]

    assert node_data["status"] == "missing"
    assert node_data["existing_outputs"] == (("first", str(asset.cache_path["first"])),)
    assert node_data["missing_outputs"] == (("second", str(asset.cache_path["second"])),)
    assert node_data["cache_path"] == (
        ("first", str(asset.cache_path["first"])),
        ("second", str(asset.cache_path["second"])),
    )


def test_multi_root_asset_graph_marks_equivalent_children_as_shared(tmp_path):
    """Equivalent assets reached from several roots get all run contexts."""
    first_child = _TextAsset(name="same-child", cache_folder=tmp_path)
    second_child = _TextAsset(name="same-child", cache_folder=tmp_path)
    first_root = _ParentAsset(
        children=[first_child],
        cache_folder=tmp_path / "first-root",
    )
    second_root = _ParentAsset(
        children=[second_child],
        cache_folder=tmp_path / "second-root",
    )
    first_root.scenario = "baseline"
    first_root.replication = 0
    first_root.is_weekday = True
    second_root.scenario = "project"
    second_root.replication = 1
    second_root.is_weekday = False

    graph = build_asset_graph_from_roots([first_root, second_root])
    shared_children = [
        data
        for asset, data in graph.nodes(data=True)
        if isinstance(asset, _TextAsset)
    ]

    assert len(shared_children) == 1
    assert shared_children[0]["run_contexts"] == (
        "baseline|weekday|0",
        "project|weekend|1",
    )
