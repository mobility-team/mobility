import pytest

import mobility.runtime.assets.dag_ui as dag_ui
from mobility.runtime.assets.dag_ui import (
    DEFAULT_LAYOUT_NAME,
    asset_dag_layout,
    asset_dag_index_string,
    asset_dag_layout_options,
    asset_graph_cytoscape_elements,
    asset_graph_stylesheet,
    create_asset_dag_app,
    create_asset_dag_app_from_graph,
    create_group_day_trips_dag_app,
    filter_asset_graph_elements,
    format_cache_path,
    format_output_paths,
    group_asset_graph_elements_by_run_context,
    group_day_trips_dag_roots,
    parse_run_context,
    run_group_id,
    selected_group_day_trips_day_types,
    selected_group_day_trips_replications,
    selected_group_day_trips_scenarios,
    shared_run_context_groups,
    show_asset_dag,
    show_group_day_trips_dag,
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


class _FakeGroupDayTrips:
    """Small setup object with the attributes used by the DAG viewer."""

    def __init__(self, *, tmp_path, scenarios=None, n_replications=2, simulate_weekend=True):
        self.tmp_path = tmp_path
        self.run_calls = []
        self.scenarios = type("Scenarios", (), {"names": scenarios or ["project"]})()
        self.parameters = type(
            "Parameters",
            (),
            {
                "run": type("RunParameters", (), {"n_replications": n_replications})(),
                "periods": type("Periods", (), {"simulate_weekend": simulate_weekend})(),
            },
        )()

    def run(self, day_type, *, scenario, replication):
        self.run_calls.append((scenario, replication, day_type))
        asset = _ParentAsset(
            children=[],
            cache_folder=self.tmp_path / scenario / day_type / str(replication),
        )
        asset.scenario = scenario
        asset.replication = replication
        asset.is_weekday = day_type == "weekday"
        return asset


class _FakeDashApp:
    def __init__(self):
        self.run_calls = []
        self.callback_calls = []
        self.callback_functions = []
        self.index_string = None
        self.layout = None
        self.title = None

    def callback(self, *args, **kwargs):
        self.callback_calls.append((args, kwargs))

        def decorator(callback_function):
            self.callback_functions.append(callback_function)
            return callback_function

        return decorator

    def run(self, **kwargs):
        self.run_calls.append(kwargs)


class _FakeDashModule:
    def __init__(self):
        self.apps = []

    def Dash(self, name):
        app = _FakeDashApp()
        app.name = name
        self.apps.append(app)
        return app


class _FakeComponentNamespace:
    def __getattr__(self, component_name):
        def build(*children, **properties):
            return {
                "component": component_name,
                "children": children,
                "properties": properties,
            }

        return build


class _FakeCytoscapeNamespace(_FakeComponentNamespace):
    def __init__(self):
        self.load_extra_layout_calls = 0

    def load_extra_layouts(self):
        self.load_extra_layout_calls += 1


def _install_fake_dash(monkeypatch):
    """Install small Dash stand-ins so tests can run without optional packages."""
    fake_dash = _FakeDashModule()
    fake_cyto = _FakeCytoscapeNamespace()
    fake_html = _FakeComponentNamespace()
    fake_dcc = _FakeComponentNamespace()

    monkeypatch.setattr(dag_ui, "dash", fake_dash)
    monkeypatch.setattr(dag_ui, "cyto", fake_cyto)
    monkeypatch.setattr(dag_ui, "html", fake_html)
    monkeypatch.setattr(dag_ui, "dcc", fake_dcc)
    monkeypatch.setattr(dag_ui, "Input", lambda *args: ("Input", args))
    monkeypatch.setattr(dag_ui, "Output", lambda *args: ("Output", args))
    monkeypatch.setattr(dag_ui, "_DASH_IMPORT_ERROR", None)
    return fake_dash, fake_cyto


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
    assert asset_dag_layout(None)["rankDir"] == "TB"
    assert asset_dag_layout("cose_bilkent")["name"] == "cose-bilkent"
    assert asset_dag_layout("unknown-layout")["name"] == "breadthfirst"


def test_asset_graph_display_helpers_format_optional_values(tmp_path):
    """Format cache metadata as text before it is sent to Dash."""
    first_path = tmp_path / "first.parquet"
    second_path = tmp_path / "second.parquet"

    assert format_cache_path(None) is None
    assert format_cache_path(first_path) == str(first_path)
    assert format_cache_path((("first", first_path), ("second", second_path))) == (
        f"first: {first_path}\nsecond: {second_path}"
    )
    assert format_output_paths(()) == ""
    assert format_output_paths((("output", first_path),)) == f"output: {first_path}"


def test_asset_graph_stylesheet_and_index_include_run_groups():
    """Keep the CSS hooks used by the grouped graph view."""
    stylesheet = asset_graph_stylesheet()
    selectors = {rule["selector"] for rule in stylesheet}
    index_string = asset_dag_index_string()

    assert ".run_group" in selectors
    assert ".cached" in selectors
    assert ".missing" in selectors
    assert "asset-dag-main" in index_string


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


def test_run_context_helpers_handle_unknown_and_shared_values():
    """Use stable labels when assets are shared or have malformed contexts."""
    assert parse_run_context("scenario|weekday|2") == ("scenario", "weekday", "2")
    assert parse_run_context("bad-context") == ("unknown", "unknown", "unknown")
    assert shared_run_context_groups(["a|weekday|0", "b|weekday|0"]) == ("weekday", "0")
    assert shared_run_context_groups(["a|weekday|0", "b|weekend|1"]) == (
        "several-day-types",
        "several-replications",
    )
    assert run_group_id("scenario", "saleve jura", "path\\part") == (
        "run-group-scenario-saleve-jura-path-part"
    )


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


def test_group_day_trips_dag_roots_build_selected_run_assets(tmp_path):
    """Build one root asset for each selected scenario, replication, and day type."""
    group_day_trips = _FakeGroupDayTrips(
        tmp_path=tmp_path,
        scenarios=["default", "project"],
        n_replications=2,
        simulate_weekend=True,
    )

    assert selected_group_day_trips_scenarios(group_day_trips, None) == [
        "default",
        "project",
    ]
    assert selected_group_day_trips_scenarios(group_day_trips, ["custom"]) == ["custom"]
    assert selected_group_day_trips_replications(group_day_trips, None) == [0, 1]
    assert selected_group_day_trips_replications(group_day_trips, range(1, 3)) == [1, 2]
    assert selected_group_day_trips_day_types(group_day_trips, None) == [
        "weekday",
        "weekend",
    ]
    assert selected_group_day_trips_day_types(group_day_trips, ["weekday"]) == ["weekday"]

    roots = group_day_trips_dag_roots(
        group_day_trips,
        scenarios=["project"],
        replications=[1],
        day_types=["weekend"],
    )

    assert len(roots) == 1
    assert group_day_trips.run_calls == [("project", 1, "weekend")]
    assert roots[0].scenario == "project"
    assert roots[0].replication == 1
    assert roots[0].is_weekday is False


def test_group_day_trips_dag_app_uses_selected_roots(tmp_path, monkeypatch):
    """Create the group-day-trips app from selected roots without computing outputs."""
    group_day_trips = _FakeGroupDayTrips(tmp_path=tmp_path, simulate_weekend=False)
    captured = {}

    def fake_create_asset_dag_app_from_graph(graph, *, title):
        captured["node_count"] = graph.number_of_nodes()
        captured["title"] = title
        return "app"

    monkeypatch.setattr(dag_ui, "_raise_if_dash_is_missing", lambda: None)
    monkeypatch.setattr(
        dag_ui,
        "create_asset_dag_app_from_graph",
        fake_create_asset_dag_app_from_graph,
    )

    app = create_group_day_trips_dag_app(
        group_day_trips,
        title="Selected runs",
        scenarios=["project"],
        replications=[0, 1],
        day_types=["weekday"],
    )

    assert app == "app"
    assert captured == {"node_count": 2, "title": "Selected runs"}
    assert group_day_trips.run_calls == [
        ("project", 0, "weekday"),
        ("project", 1, "weekday"),
    ]


def test_create_asset_dag_app_builds_layout_without_optional_dash(tmp_path, monkeypatch):
    """Build the Dash app structure with fake components for CI coverage."""
    fake_dash, fake_cyto = _install_fake_dash(monkeypatch)
    child = _TextAsset(name="child", cache_folder=tmp_path)
    parent = _ParentAsset(children=[child], cache_folder=tmp_path)

    app = create_asset_dag_app(parent, title="Asset DAG")

    assert app is fake_dash.apps[0]
    assert app.title == "Asset DAG"
    assert app.layout["component"] == "Div"
    assert len(app.callback_calls) == 3
    assert app.index_string.startswith("\n<!DOCTYPE html>")
    assert fake_cyto.load_extra_layout_calls == 1

    filter_elements, update_layout, show_node_details = app.callback_functions
    filtered_elements = filter_elements(["_TextAsset"], ["missing"], [], [])
    grouped_elements = filter_elements(None, None, None, ["run_context"])
    grouped_nodes = [element for element in grouped_elements if "source" not in element["data"]]
    grouped_node_ids = {element["data"]["id"] for element in grouped_nodes}
    grouped_asset_nodes = [
        element for element in grouped_nodes if element["data"].get("asset_type") != "RunGroup"
    ]
    selected_details = show_node_details(
        {
            "label": "_TextAsset",
            "asset_type": "_TextAsset",
            "status": "missing",
            "iteration": None,
            "scenario": None,
            "replication": None,
            "is_weekday": None,
            "run_contexts": [],
            "inputs_hash": "inputs",
            "cached_hash": None,
            "missing_outputs": "output: missing.parquet",
            "existing_outputs": "",
            "cache_path": "missing.parquet",
        }
    )

    assert len([element for element in filtered_elements if "source" not in element["data"]]) == 1
    assert "run-group-scenario-default" in grouped_node_ids
    assert all("parent" in element["data"] for element in grouped_asset_nodes)
    assert update_layout("dagre_tb", ["run_context"])["padding"] == 55
    assert update_layout("dagre_lr", [])["rankDir"] == "LR"
    assert show_node_details(None)["component"] == "Div"
    assert selected_details["component"] == "Div"


def test_create_asset_dag_app_from_graph_builds_layout_without_optional_dash(
    tmp_path,
    monkeypatch,
):
    """Build an app from a pre-built graph, as used by the multi-run viewer."""
    fake_dash, fake_cyto = _install_fake_dash(monkeypatch)
    child = _TextAsset(name="child", cache_folder=tmp_path)
    parent = _ParentAsset(children=[child], cache_folder=tmp_path)
    graph = build_asset_graph(parent)

    app = create_asset_dag_app_from_graph(graph, title="Pre-built graph")

    assert app is fake_dash.apps[0]
    assert app.title == "Pre-built graph"
    assert app.layout["component"] == "Div"
    assert len(app.callback_calls) == 3
    assert fake_cyto.load_extra_layout_calls == 1


def test_create_asset_dag_app_builds_a_dash_layout(tmp_path):
    """Smoke-test the static Dash app when optional UI packages are installed."""
    pytest.importorskip("dash")
    pytest.importorskip("dash_cytoscape")

    child = _TextAsset(name="child", cache_folder=tmp_path)
    parent = _ParentAsset(children=[child], cache_folder=tmp_path)

    app = create_asset_dag_app(parent, title="Asset DAG")

    assert app.title == "Asset DAG"
    assert app.layout is not None


def test_dash_app_creation_explains_missing_optional_dependencies(tmp_path, monkeypatch):
    """Tell users how to fix a broken install without Dash."""
    child = _TextAsset(name="child", cache_folder=tmp_path)
    monkeypatch.setattr(dag_ui, "_DASH_IMPORT_ERROR", ImportError("dash missing"))

    with pytest.raises(ImportError, match="pip install mobility-tools"):
        create_asset_dag_app(child)


def test_show_asset_dag_passes_server_options(tmp_path, monkeypatch):
    """Forward server options to the local Dash app."""
    app = _FakeDashApp()
    child = _TextAsset(name="child", cache_folder=tmp_path)
    monkeypatch.setattr(dag_ui, "create_asset_dag_app", lambda *args, **kwargs: app)

    show_asset_dag(
        child,
        title="Asset DAG",
        host="0.0.0.0",
        port=9090,
        debug=True,
    )

    assert app.run_calls == [{"host": "0.0.0.0", "port": 9090, "debug": True}]


def test_show_group_day_trips_dag_passes_server_options(tmp_path, monkeypatch):
    """Forward selected runs and server options to the group-day-trips Dash app."""
    app = _FakeDashApp()
    group_day_trips = _FakeGroupDayTrips(tmp_path=tmp_path)
    captured = {}

    def fake_create_group_day_trips_dag_app(*args, **kwargs):
        captured["args"] = args
        captured["kwargs"] = kwargs
        return app

    monkeypatch.setattr(
        dag_ui,
        "create_group_day_trips_dag_app",
        fake_create_group_day_trips_dag_app,
    )

    show_group_day_trips_dag(
        group_day_trips,
        title="Runs",
        scenarios=["project"],
        replications=[1],
        day_types=["weekday"],
        host="0.0.0.0",
        port=9091,
        debug=True,
    )

    assert captured["args"] == (group_day_trips,)
    assert captured["kwargs"] == {
        "title": "Runs",
        "scenarios": ["project"],
        "replications": [1],
        "day_types": ["weekday"],
    }
    assert app.run_calls == [{"host": "0.0.0.0", "port": 9091, "debug": True}]
