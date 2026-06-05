import pytest

from mobility.runtime.assets.dag_ui import (
    DEFAULT_LAYOUT_NAME,
    asset_dag_layout,
    asset_dag_layout_options,
    asset_graph_cytoscape_elements,
    create_asset_dag_app,
    filter_asset_graph_elements,
    group_asset_graph_elements_by_run_context,
)
from mobility.runtime.assets.file_asset import FileAsset
from mobility.runtime.assets.graph import (
    build_asset_graph,
    build_asset_graph_from_roots,
    build_upstream_file_asset_graph,
)
from mobility.runtime.assets.in_memory_asset import InMemoryAsset


class _TextAsset(FileAsset):
    create_calls = 0

    def __init__(self, *, name, cache_folder):
        super().__init__({"name": name}, cache_folder / "text_asset.txt")

    def get_cached_asset(self):
        return self.cache_path.read_text(encoding="utf-8")

    def create_and_get_asset(self):
        _TextAsset.create_calls += 1
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


class _StatusCountingAsset(FileAsset):
    status_checks = 0

    def __init__(self, *, child=None, cache_folder):
        inputs = {"name": "status-counting", "child": child}
        super().__init__(inputs, cache_folder / "status_counting.txt")

    def is_update_needed(self):
        _StatusCountingAsset.status_checks += 1
        return False

    def get_cached_asset(self):
        return None

    def create_and_get_asset(self):
        return None


class _MemoryAsset(InMemoryAsset):
    def __init__(self, *, name):
        super().__init__({"name": name})


class _MemoryWrapperAsset(InMemoryAsset):
    def __init__(self, *, child):
        super().__init__({"child": child})


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
    _StatusCountingAsset.status_checks = 0
    child = _StatusCountingAsset(cache_folder=tmp_path)
    parent = _StatusCountingAsset(child=child, cache_folder=tmp_path)

    build_upstream_file_asset_graph(parent)

    assert _StatusCountingAsset.status_checks == 0


def test_build_upstream_file_asset_graph_looks_through_in_memory_assets(tmp_path):
    """Runtime graph building finds file assets nested below in-memory assets."""
    child = _TextAsset(name="hidden-file", cache_folder=tmp_path)
    wrapper = _MemoryWrapperAsset(child=child)
    parent = _ParentAsset(children=[wrapper], cache_folder=tmp_path)

    graph = build_upstream_file_asset_graph(parent)

    assert list(graph.nodes) == [child]


def test_build_upstream_file_asset_graph_looks_through_in_memory_file_inputs(tmp_path):
    """Runtime graph building finds file assets nested below upstream FileAssets."""
    child = _TextAsset(name="hidden-file", cache_folder=tmp_path / "child")
    wrapper = _MemoryWrapperAsset(child=child)
    bridge = _ParentAsset(children=[wrapper], cache_folder=tmp_path / "bridge")
    parent = _ParentAsset(children=[bridge], cache_folder=tmp_path / "parent")

    graph = build_upstream_file_asset_graph(parent)

    assert set(graph.nodes) == {bridge, child}


def test_build_asset_graph_reports_missing_cached_and_stale_assets(tmp_path):
    """Expose simple cache states for the viewer."""
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


def test_asset_graph_cytoscape_elements_can_be_filtered(tmp_path):
    """Build Cytoscape data without starting a Dash server."""
    missing = _TextAsset(name="missing", cache_folder=tmp_path)
    cached = _TextAsset(name="cached", cache_folder=tmp_path)
    parent = _ParentAsset(
        children=[missing, cached],
        cache_folder=tmp_path,
    )
    cached.create_and_get_asset()

    graph = build_asset_graph(parent)
    elements = asset_graph_cytoscape_elements(graph)
    filtered_elements = filter_asset_graph_elements(
        elements,
        selected_asset_types=["_TextAsset"],
        selected_statuses=["cached"],
        selected_iterations=None,
    )

    filtered_nodes = [
        element
        for element in filtered_elements
        if "source" not in element["data"]
    ]
    assert len(filtered_nodes) == 1
    assert filtered_nodes[0]["data"]["status"] == "cached"


def test_empty_filters_keep_the_full_asset_graph(tmp_path):
    """Empty filter values mean all nodes are visible."""
    child = _TextAsset(name="child", cache_folder=tmp_path)
    parent = _ParentAsset(children=[child], cache_folder=tmp_path)

    graph = build_asset_graph(parent)
    elements = asset_graph_cytoscape_elements(graph)
    filtered_elements = filter_asset_graph_elements(
        elements,
        selected_asset_types=[],
        selected_statuses=[],
        selected_iterations=[],
    )

    assert filtered_elements == elements


def test_asset_dag_layout_options_include_the_default_layout():
    """Expose several useful graph layouts without changing the Python API."""
    options = asset_dag_layout_options()
    option_values = {option["value"] for option in options}

    assert DEFAULT_LAYOUT_NAME in option_values
    assert asset_dag_layout(DEFAULT_LAYOUT_NAME)["rankDir"] == "TB"
    assert asset_dag_layout("dagre_lr")["rankDir"] == "LR"


def test_group_asset_graph_elements_by_run_context_adds_subgroup_nodes(tmp_path):
    """Group graph nodes into scenario, day-type, and replication boxes."""
    child = _TextAsset(name="child", cache_folder=tmp_path)
    parent = _ParentAsset(children=[child], cache_folder=tmp_path)
    child.iteration = 1
    parent.scenario = "baseline"
    parent.replication = 2
    parent.is_weekday = True

    graph = build_asset_graph_from_roots([parent])
    elements = asset_graph_cytoscape_elements(graph)
    grouped_elements = group_asset_graph_elements_by_run_context(elements)

    node_data_by_id = {
        element["data"]["id"]: element["data"]
        for element in grouped_elements
        if "source" not in element["data"]
    }
    scenario_id = "run-group-scenario-baseline"
    day_type_id = "run-group-scenario-baseline-day-weekday"
    replication_id = "run-group-scenario-baseline-day-weekday-replication-2"
    run_level_id = "run-group-scenario-baseline-day-weekday-replication-2-run-level"
    iteration_id = "run-group-scenario-baseline-day-weekday-replication-2-iteration-1"

    assert scenario_id in node_data_by_id
    assert node_data_by_id[day_type_id]["parent"] == scenario_id
    assert node_data_by_id[replication_id]["parent"] == day_type_id
    assert node_data_by_id[run_level_id]["parent"] == replication_id
    assert node_data_by_id[iteration_id]["parent"] == replication_id
    assert node_data_by_id["asset-0"]["parent"] == run_level_id
    assert node_data_by_id["asset-1"]["parent"] == iteration_id


def test_group_asset_graph_elements_by_run_context_groups_shared_iterations(tmp_path):
    """Shared iteration assets still appear in an iteration subgroup."""
    child = _TextAsset(name="shared-iteration", cache_folder=tmp_path)
    child.iteration = 1
    parent = _ParentAsset(children=[child], cache_folder=tmp_path)

    graph = build_asset_graph(parent)
    elements = asset_graph_cytoscape_elements(graph)
    for element in elements:
        if element["data"].get("asset_type") == "_TextAsset":
            element["data"]["run_contexts"] = [
                "baseline|weekday|0",
                "project|weekday|0",
            ]
    grouped_elements = group_asset_graph_elements_by_run_context(elements)

    node_data_by_id = {
        element["data"]["id"]: element["data"]
        for element in grouped_elements
        if "source" not in element["data"]
    }
    shared_id = "run-group-shared"
    shared_day_type_id = "run-group-shared-day-weekday"
    shared_replication_id = "run-group-shared-day-weekday-replication-0"
    shared_iteration_id = "run-group-shared-day-weekday-replication-0-iteration-1"

    assert node_data_by_id[shared_day_type_id]["parent"] == shared_id
    assert node_data_by_id[shared_replication_id]["parent"] == shared_day_type_id
    assert node_data_by_id[shared_iteration_id]["parent"] == shared_replication_id
    assert node_data_by_id["asset-1"]["parent"] == shared_iteration_id


def test_multi_root_asset_graph_marks_equivalent_children_as_shared(tmp_path):
    """Equivalent assets reached from several roots get all run contexts."""
    first_child = _TextAsset(name="same-survey-plans", cache_folder=tmp_path)
    second_child = _TextAsset(name="same-survey-plans", cache_folder=tmp_path)
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
    second_root.replication = 0
    second_root.is_weekday = True

    graph = build_asset_graph_from_roots([first_root, second_root])
    shared_children = [
        data
        for asset, data in graph.nodes(data=True)
        if isinstance(asset, _TextAsset)
    ]

    assert len(shared_children) == 1
    assert shared_children[0]["run_contexts"] == (
        "baseline|weekday|0",
        "project|weekday|0",
    )


def test_multi_root_asset_graph_marks_equivalent_in_memory_assets_as_shared(tmp_path):
    """Equivalent in-memory assets also get all run contexts."""
    first_child = _MemoryAsset(name="same-generalized-cost")
    second_child = _MemoryAsset(name="same-generalized-cost")
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
    second_root.replication = 0
    second_root.is_weekday = True

    graph = build_asset_graph_from_roots([first_root, second_root])
    shared_children = [
        data
        for asset, data in graph.nodes(data=True)
        if isinstance(asset, _MemoryAsset)
    ]

    assert len(shared_children) == 1
    assert shared_children[0]["run_contexts"] == (
        "baseline|weekday|0",
        "project|weekday|0",
    )


def test_create_asset_dag_app_builds_a_dash_layout(tmp_path):
    """Smoke-test the static Dash app when optional UI packages are installed."""
    pytest.importorskip("dash")
    pytest.importorskip("dash_cytoscape")

    child = _TextAsset(name="child", cache_folder=tmp_path)
    parent = _ParentAsset(children=[child], cache_folder=tmp_path)

    app = create_asset_dag_app(parent, title="Asset DAG")

    assert app.title == "Asset DAG"
    assert app.layout is not None
