from __future__ import annotations

from typing import Any

import networkx as nx

from mobility.runtime.assets.asset import Asset
from mobility.runtime.assets.graph import build_asset_graph, build_asset_graph_from_roots

try:
    import dash
    import dash_cytoscape as cyto
    from dash import Input, Output, dcc, html
except ImportError as exc:
    dash = None
    cyto = None
    dcc = None
    html = None
    Input = None
    Output = None
    _DASH_IMPORT_ERROR = exc
else:
    _DASH_IMPORT_ERROR = None


DASH_INSTALL_MESSAGE = (
    "The asset DAG viewer needs Dash dependencies. "
    "Install Mobility again with `pip install mobility-tools`."
)


STATUS_COLORS = {
    "cached": "#2E7D32",
    "missing": "#C62828",
    "stale": "#EF6C00",
    "not_file_asset": "#546E7A",
}

DEFAULT_LAYOUT_NAME = "dagre_tb"
RUN_GROUP_PREFIX = "run-group"

LAYOUT_LABELS = {
    "breadthfirst": "Dependency levels",
    "dagre_lr": "Layered left to right",
    "dagre_tb": "Layered top to bottom",
    "cose_bilkent": "Clustered force",
}


def create_asset_dag_app(
    root_asset: Asset,
    *,
    title: str | None = None,
) -> "dash.Dash":
    """Create a static Dash app that shows an asset dependency graph."""

    _raise_if_dash_is_missing()

    cyto.load_extra_layouts()
    graph = build_asset_graph_from_roots([root_asset])
    elements = asset_graph_cytoscape_elements(graph)
    nodes = asset_graph_nodes(elements)
    edge_count = len(elements) - len(nodes)
    asset_types = sorted({node["data"]["asset_type"] for node in nodes})
    statuses = sorted({node["data"]["status"] for node in nodes})
    iterations = sorted(
        {
            node["data"]["iteration"]
            for node in nodes
            if node["data"].get("iteration") is not None
        }
    )

    app = dash.Dash(__name__)
    app.title = title or f"{root_asset.__class__.__name__} asset DAG"
    app.layout = html.Div(
        [
            html.Div(
                [
                    html.Div(
                        [
                            html.H1(app.title),
                            html.Div(
                                [
                                    html.Span(f"{len(nodes)} assets"),
                                    html.Span(f"{edge_count} links"),
                                ],
                                className="asset-dag-summary",
                            ),
                        ],
                        className="asset-dag-heading",
                    ),
                    html.Div(
                        [
                            html.Div(
                                [
                                    html.Label("Asset type"),
                                    dcc.Dropdown(
                                        id="asset-type-filter",
                                        options=[
                                            {"label": asset_type, "value": asset_type}
                                            for asset_type in asset_types
                                        ],
                                        value=[],
                                        multi=True,
                                        clearable=True,
                                        placeholder="All asset types",
                                    ),
                                ],
                                className="asset-dag-filter",
                            ),
                            html.Div(
                                [
                                    html.Label("Status"),
                                    dcc.Dropdown(
                                        id="asset-status-filter",
                                        options=[
                                            {"label": status, "value": status}
                                            for status in statuses
                                        ],
                                        value=[],
                                        multi=True,
                                        clearable=True,
                                        placeholder="All statuses",
                                    ),
                                ],
                                className="asset-dag-filter",
                            ),
                            html.Div(
                                [
                                    html.Label("Iteration"),
                                    dcc.Dropdown(
                                        id="asset-iteration-filter",
                                        options=[
                                            {"label": str(iteration), "value": iteration}
                                            for iteration in iterations
                                        ],
                                        value=[],
                                        multi=True,
                                        clearable=True,
                                        placeholder="All iterations",
                                    ),
                                ],
                                className="asset-dag-filter",
                            ),
                            html.Div(
                                [
                                    html.Label("Layout"),
                                    dcc.Dropdown(
                                        id="asset-layout-filter",
                                        options=asset_dag_layout_options(),
                                        value=DEFAULT_LAYOUT_NAME,
                                        clearable=False,
                                    ),
                                ],
                                className="asset-dag-filter",
                            ),
                            html.Div(
                                [
                                    html.Label("Group"),
                                    dcc.Checklist(
                                        id="asset-group-filter",
                                        options=[
                                            {
                                                "label": "Run contexts",
                                                "value": "run_context",
                                            }
                                        ],
                                        value=[],
                                        className="asset-dag-checklist",
                                    ),
                                ],
                                className="asset-dag-filter",
                            ),
                        ],
                        className="asset-dag-filters",
                    ),
                    html.Div(
                        [legend_item(status, color) for status, color in STATUS_COLORS.items()],
                        className="asset-dag-legend",
                    ),
                ],
                className="asset-dag-toolbar",
            ),
            html.Div(
                [
                    cyto.Cytoscape(
                        id="asset-dag-graph",
                        elements=elements,
                        layout=asset_dag_layout(DEFAULT_LAYOUT_NAME),
                        stylesheet=asset_graph_stylesheet(),
                        style={"width": "100%", "height": "100%"},
                        className="asset-dag-graph",
                        wheelSensitivity=0.2,
                        responsive=True,
                    ),
                    html.Div(
                        id="asset-dag-details",
                        className="asset-dag-details",
                    ),
                ],
                className="asset-dag-main",
            ),
        ],
        className="asset-dag-page",
    )

    register_asset_dag_callbacks(app, elements)
    app.index_string = asset_dag_index_string()
    return app


def create_group_day_trips_dag_app(
    group_day_trips,
    *,
    title: str | None = None,
    scenarios: list[str] | tuple[str, ...] | None = None,
    replications: list[int] | range | None = None,
    day_types: list[str] | tuple[str, ...] | None = None,
) -> "dash.Dash":
    """Create a Dash app for all selected group-day-trips runs.

    The setup object builds run assets lazily. This helper creates the selected
    run objects so their asset DAGs can be inspected, but it does not call
    ``get()`` and does not compute the model outputs.
    """

    _raise_if_dash_is_missing()

    roots = group_day_trips_dag_roots(
        group_day_trips,
        scenarios=scenarios,
        replications=replications,
        day_types=day_types,
    )
    graph = build_asset_graph_from_roots(roots)
    app_title = title or "PopulationGroupDayTrips asset DAG"
    return create_asset_dag_app_from_graph(graph, title=app_title)


def show_group_day_trips_dag(
    group_day_trips,
    *,
    title: str | None = None,
    scenarios: list[str] | tuple[str, ...] | None = None,
    replications: list[int] | range | None = None,
    day_types: list[str] | tuple[str, ...] | None = None,
    host: str = "127.0.0.1",
    port: int = 8050,
    debug: bool = False,
) -> None:
    """Open a local Dash server for a PopulationGroupDayTrips setup DAG."""

    app = create_group_day_trips_dag_app(
        group_day_trips,
        title=title,
        scenarios=scenarios,
        replications=replications,
        day_types=day_types,
    )
    app.run(host=host, port=port, debug=debug)


def group_day_trips_dag_roots(
    group_day_trips,
    *,
    scenarios: list[str] | tuple[str, ...] | None,
    replications: list[int] | range | None,
    day_types: list[str] | tuple[str, ...] | None,
) -> list[Asset]:
    """Build the selected Run assets for a PopulationGroupDayTrips setup."""

    scenario_names = selected_group_day_trips_scenarios(group_day_trips, scenarios)
    replication_ids = selected_group_day_trips_replications(group_day_trips, replications)
    selected_day_types = selected_group_day_trips_day_types(group_day_trips, day_types)

    roots = []
    for scenario in scenario_names:
        for replication in replication_ids:
            for day_type in selected_day_types:
                roots.append(
                    group_day_trips.run(
                        day_type,
                        scenario=scenario,
                        replication=replication,
                    )
                )
    return roots


def selected_group_day_trips_scenarios(
    group_day_trips,
    scenarios: list[str] | tuple[str, ...] | None,
) -> list[str]:
    """Return scenario names to include in the top-level DAG."""

    if scenarios is not None:
        return list(scenarios)

    scenario_names = ["default"]
    for scenario_name in getattr(group_day_trips.scenarios, "names", []):
        if scenario_name not in scenario_names:
            scenario_names.append(scenario_name)
    return scenario_names


def selected_group_day_trips_replications(
    group_day_trips,
    replications: list[int] | range | None,
) -> list[int]:
    """Return replication ids to include in the top-level DAG."""

    if replications is not None:
        return list(replications)
    return list(range(group_day_trips.parameters.run.n_replications))


def selected_group_day_trips_day_types(
    group_day_trips,
    day_types: list[str] | tuple[str, ...] | None,
) -> list[str]:
    """Return day types to include in the top-level DAG."""

    if day_types is not None:
        return list(day_types)

    selected_day_types = ["weekday"]
    if group_day_trips.parameters.periods.simulate_weekend:
        selected_day_types.append("weekend")
    return selected_day_types


def show_asset_dag(
    root_asset: Asset,
    *,
    title: str | None = None,
    host: str = "127.0.0.1",
    port: int = 8050,
    debug: bool = False,
) -> None:
    """Open a local Dash server for an asset dependency graph."""

    app = create_asset_dag_app(root_asset, title=title)
    app.run(host=host, port=port, debug=debug)


def create_asset_dag_app_from_graph(
    graph: nx.DiGraph,
    *,
    title: str,
) -> "dash.Dash":
    """Create a static Dash app from an already-built asset graph."""

    _raise_if_dash_is_missing()

    cyto.load_extra_layouts()
    elements = asset_graph_cytoscape_elements(graph)
    nodes = asset_graph_nodes(elements)
    edge_count = len(elements) - len(nodes)
    asset_types = sorted({node["data"]["asset_type"] for node in nodes})
    statuses = sorted({node["data"]["status"] for node in nodes})
    iterations = sorted(
        {
            node["data"]["iteration"]
            for node in nodes
            if node["data"].get("iteration") is not None
        }
    )

    app = dash.Dash(__name__)
    app.title = title
    app.layout = html.Div(
        [
            html.Div(
                [
                    html.Div(
                        [
                            html.H1(app.title),
                            html.Div(
                                [
                                    html.Span(f"{len(nodes)} assets"),
                                    html.Span(f"{edge_count} links"),
                                ],
                                className="asset-dag-summary",
                            ),
                        ],
                        className="asset-dag-heading",
                    ),
                    html.Div(
                        [
                            html.Div(
                                [
                                    html.Label("Asset type"),
                                    dcc.Dropdown(
                                        id="asset-type-filter",
                                        options=[
                                            {"label": asset_type, "value": asset_type}
                                            for asset_type in asset_types
                                        ],
                                        value=[],
                                        multi=True,
                                        clearable=True,
                                        placeholder="All asset types",
                                    ),
                                ],
                                className="asset-dag-filter",
                            ),
                            html.Div(
                                [
                                    html.Label("Status"),
                                    dcc.Dropdown(
                                        id="asset-status-filter",
                                        options=[
                                            {"label": status, "value": status}
                                            for status in statuses
                                        ],
                                        value=[],
                                        multi=True,
                                        clearable=True,
                                        placeholder="All statuses",
                                    ),
                                ],
                                className="asset-dag-filter",
                            ),
                            html.Div(
                                [
                                    html.Label("Iteration"),
                                    dcc.Dropdown(
                                        id="asset-iteration-filter",
                                        options=[
                                            {"label": str(iteration), "value": iteration}
                                            for iteration in iterations
                                        ],
                                        value=[],
                                        multi=True,
                                        clearable=True,
                                        placeholder="All iterations",
                                    ),
                                ],
                                className="asset-dag-filter",
                            ),
                            html.Div(
                                [
                                    html.Label("Layout"),
                                    dcc.Dropdown(
                                        id="asset-layout-filter",
                                        options=asset_dag_layout_options(),
                                        value=DEFAULT_LAYOUT_NAME,
                                        clearable=False,
                                    ),
                                ],
                                className="asset-dag-filter",
                            ),
                            html.Div(
                                [
                                    html.Label("Group"),
                                    dcc.Checklist(
                                        id="asset-group-filter",
                                        options=[
                                            {
                                                "label": "Run contexts",
                                                "value": "run_context",
                                            }
                                        ],
                                        value=[],
                                        className="asset-dag-checklist",
                                    ),
                                ],
                                className="asset-dag-filter",
                            ),
                        ],
                        className="asset-dag-filters",
                    ),
                    html.Div(
                        [legend_item(status, color) for status, color in STATUS_COLORS.items()],
                        className="asset-dag-legend",
                    ),
                ],
                className="asset-dag-toolbar",
            ),
            html.Div(
                [
                    cyto.Cytoscape(
                        id="asset-dag-graph",
                        elements=elements,
                        layout=asset_dag_layout(DEFAULT_LAYOUT_NAME),
                        stylesheet=asset_graph_stylesheet(),
                        style={"width": "100%", "height": "100%"},
                        className="asset-dag-graph",
                        wheelSensitivity=0.2,
                        responsive=True,
                    ),
                    html.Div(
                        id="asset-dag-details",
                        className="asset-dag-details",
                    ),
                ],
                className="asset-dag-main",
            ),
        ],
        className="asset-dag-page",
    )

    register_asset_dag_callbacks(app, elements)
    app.index_string = asset_dag_index_string()
    return app


def asset_graph_cytoscape_elements(graph: nx.DiGraph) -> list[dict[str, Any]]:
    """Convert an asset graph to Cytoscape elements."""

    node_ids = {asset: f"asset-{index}" for index, asset in enumerate(graph.nodes)}
    elements = []

    for asset, node_id in node_ids.items():
        data = graph.nodes[asset]
        node_data = {
            "id": node_id,
            "label": data["asset_type"],
            "asset_type": data["asset_type"],
            "status": data["status"],
            "inputs_hash": data["inputs_hash"],
            "cache_path": format_cache_path(data["cache_path"]),
            "existing_outputs": format_output_paths(data["existing_outputs"]),
            "missing_outputs": format_output_paths(data["missing_outputs"]),
            "cached_hash": data["cached_hash"],
            "iteration": data.get("iteration"),
            "replication": data.get("replication"),
            "run_contexts": list(data.get("run_contexts", ())),
            "scenario": data.get("scenario"),
            "is_weekday": data.get("is_weekday"),
        }
        elements.append(
            {
                "data": node_data,
                "classes": data["status"],
            }
        )

    for source, target, edge_data in graph.edges(data=True):
        source_id = node_ids[source]
        target_id = node_ids[target]
        elements.append(
            {
                "data": {
                    "id": f"{source_id}-{target_id}",
                    "source": source_id,
                    "target": target_id,
                    "input_name": edge_data.get("input_name"),
                }
            }
        )

    return elements


def asset_graph_nodes(elements: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Return only node elements from a Cytoscape element list."""

    return [element for element in elements if "source" not in element["data"]]


def register_asset_dag_callbacks(app: "dash.Dash", elements: list[dict[str, Any]]) -> None:
    """Register filtering and detail callbacks for the static graph app."""

    @app.callback(
        Output("asset-dag-graph", "elements"),
        Input("asset-type-filter", "value"),
        Input("asset-status-filter", "value"),
        Input("asset-iteration-filter", "value"),
        Input("asset-group-filter", "value"),
    )
    def filter_elements(
        selected_asset_types,
        selected_statuses,
        selected_iterations,
        selected_groups,
    ):
        filtered_elements = filter_asset_graph_elements(
            elements,
            selected_asset_types=selected_asset_types,
            selected_statuses=selected_statuses,
            selected_iterations=selected_iterations,
        )
        if "run_context" in (selected_groups or []):
            return group_asset_graph_elements_by_run_context(filtered_elements)
        return filtered_elements

    @app.callback(
        Output("asset-dag-graph", "layout"),
        Input("asset-layout-filter", "value"),
        Input("asset-group-filter", "value"),
    )
    def update_layout(layout_name, selected_groups):
        layout = asset_dag_layout(layout_name)
        if "run_context" in (selected_groups or []):
            return {
                **layout,
                "padding": max(layout.get("padding", 35), 55),
                "spacingFactor": max(layout.get("spacingFactor", 1.0), 1.25),
            }
        return layout

    @app.callback(
        Output("asset-dag-details", "children"),
        Input("asset-dag-graph", "tapNodeData"),
    )
    def show_node_details(node_data):
        if not node_data:
            return html.Div(
                [
                    html.H2("Asset"),
                    html.P("Select an asset node."),
                ]
            )

        rows = [
            ("Type", node_data.get("asset_type")),
            ("Status", node_data.get("status")),
            ("Iteration", node_data.get("iteration")),
            ("Scenario", node_data.get("scenario")),
            ("Replication", node_data.get("replication")),
            ("Weekday", node_data.get("is_weekday")),
            ("Run contexts", ", ".join(node_data.get("run_contexts") or [])),
            ("Inputs hash", node_data.get("inputs_hash")),
            ("Cached hash", node_data.get("cached_hash")),
        ]
        missing_outputs = node_data.get("missing_outputs") or ""
        existing_outputs = node_data.get("existing_outputs") or ""
        return html.Div(
            [
                html.H2(node_data.get("label", "Asset")),
                html.Table(
                    [
                        html.Tr([html.Th(label), html.Td("" if value is None else str(value))])
                        for label, value in rows
                    ]
                ),
                html.Details(
                    [
                        html.Summary("Missing outputs"),
                        html.Pre(missing_outputs or "No missing output."),
                    ],
                    open=bool(missing_outputs),
                ),
                html.Details(
                    [
                        html.Summary("Existing outputs"),
                        html.Pre(existing_outputs or "No existing output."),
                    ],
                    open=False,
                ),
                html.Details(
                    [
                        html.Summary("Cache path"),
                        html.Pre(node_data.get("cache_path") or ""),
                    ],
                    open=False,
                ),
            ]
        )


def filter_asset_graph_elements(
    elements: list[dict[str, Any]],
    *,
    selected_asset_types: list[str] | None,
    selected_statuses: list[str] | None,
    selected_iterations: list[int] | None,
) -> list[dict[str, Any]]:
    """Filter Cytoscape elements while keeping only edges between visible nodes."""

    selected_asset_types = selected_asset_types or []
    selected_statuses = selected_statuses or []
    selected_iterations = selected_iterations or []
    keep_all_asset_types = len(selected_asset_types) == 0
    keep_all_statuses = len(selected_statuses) == 0
    keep_all_iterations = len(selected_iterations) == 0
    visible_node_ids = set()
    filtered_elements = []

    for element in asset_graph_nodes(elements):
        data = element["data"]
        if not keep_all_asset_types and data["asset_type"] not in selected_asset_types:
            continue
        if not keep_all_statuses and data["status"] not in selected_statuses:
            continue
        if not keep_all_iterations and data.get("iteration") not in selected_iterations:
            continue
        visible_node_ids.add(data["id"])
        filtered_elements.append(element)

    for element in elements:
        data = element["data"]
        if "source" not in data:
            continue
        if data["source"] in visible_node_ids and data["target"] in visible_node_ids:
            filtered_elements.append(element)

    return filtered_elements


def group_asset_graph_elements_by_run_context(
    elements: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Add Cytoscape compound parent nodes grouped by run context."""

    group_nodes_by_id = {}
    grouped_nodes = []

    def add_group(group_id: str, label: str, parent: str | None = None) -> None:
        if group_id in group_nodes_by_id:
            return
        data = {
            "id": group_id,
            "label": label,
            "asset_type": "RunGroup",
            "status": "run_group",
        }
        if parent is not None:
            data["parent"] = parent
        group_nodes_by_id[group_id] = {
            "data": {
                **data,
            },
            "classes": "run_group",
            "grabbable": False,
        }

    for element in elements:
        data = element["data"]
        if "source" in data:
            continue

        parent_id, group_specs = run_context_parent_and_groups(data)
        for group_id, label, parent in group_specs:
            add_group(group_id, label, parent)

        grouped_data = {
            **data,
            "parent": parent_id,
        }
        grouped_nodes.append(
            {
                **element,
                "data": grouped_data,
            }
        )

    return (
        list(group_nodes_by_id.values())
        + grouped_nodes
        + [element for element in elements if "source" in element["data"]]
    )


def run_context_parent_and_groups(
    node_data: dict[str, Any],
) -> tuple[str, list[tuple[str, str, str | None]]]:
    """Return the parent id and group nodes for one asset node."""

    contexts = node_data.get("run_contexts") or []
    if len(contexts) != 1:
        shared_group_id = run_group_id("shared")
        day_type, replication = shared_run_context_groups(contexts)
        day_type_id = run_group_id("shared", "day", day_type)
        replication_id = run_group_id("shared", "day", day_type, "replication", replication)
        iteration = node_data.get("iteration")
        if iteration is None:
            shared_child_id = run_group_id("shared", "day", day_type, "replication", replication, "run-level")
            shared_child_label = "Run-level assets"
        else:
            shared_child_id = run_group_id(
                "shared",
                "day",
                day_type,
                "replication",
                replication,
                "iteration",
                iteration,
            )
            shared_child_label = f"Iteration: {iteration}"
        return shared_child_id, [
            (shared_group_id, "Shared by several runs", None),
            (day_type_id, f"Day type: {day_type}", shared_group_id),
            (replication_id, f"Replication: {replication}", day_type_id),
            (shared_child_id, shared_child_label, replication_id),
        ]

    scenario, day_type, replication = parse_run_context(contexts[0])
    iteration = node_data.get("iteration")
    scenario_id = run_group_id("scenario", scenario)
    day_type_id = run_group_id("scenario", scenario, "day", day_type)
    replication_id = run_group_id(
        "scenario",
        scenario,
        "day",
        day_type,
        "replication",
        replication,
    )
    if iteration is None:
        iteration_label = "Run-level assets"
        iteration_id = run_group_id(
            "scenario",
            scenario,
            "day",
            day_type,
            "replication",
            replication,
            "run-level",
        )
    else:
        iteration_label = f"Iteration: {iteration}"
        iteration_id = run_group_id(
            "scenario",
            scenario,
            "day",
            day_type,
            "replication",
            replication,
            "iteration",
            iteration,
        )

    return iteration_id, [
        (scenario_id, f"Scenario: {scenario}", None),
        (day_type_id, f"Day type: {day_type}", scenario_id),
        (replication_id, f"Replication: {replication}", day_type_id),
        (iteration_id, iteration_label, replication_id),
    ]


def parse_run_context(context: str) -> tuple[str, str, str]:
    """Split a compact run context into scenario, day type, and replication."""

    parts = str(context).split("|")
    if len(parts) != 3:
        return "unknown", "unknown", "unknown"
    return parts[0], parts[1], parts[2]


def shared_run_context_groups(contexts: list[str]) -> tuple[str, str]:
    """Return shared day-type and replication labels for several contexts."""

    parsed_contexts = [parse_run_context(context) for context in contexts]
    day_types = sorted({day_type for _, day_type, _ in parsed_contexts})
    replications = sorted({replication for _, _, replication in parsed_contexts})

    day_type = day_types[0] if len(day_types) == 1 else "several-day-types"
    replication = replications[0] if len(replications) == 1 else "several-replications"
    return day_type, replication


def run_group_id(*parts: Any) -> str:
    """Return a stable Cytoscape group id."""

    safe_parts = [
        str(part).replace(" ", "-").replace("/", "-").replace("\\", "-")
        for part in parts
    ]
    return f"{RUN_GROUP_PREFIX}-{'-'.join(safe_parts)}"


def asset_dag_layout_options() -> list[dict[str, str]]:
    """Return user-facing layout choices for the DAG viewer."""

    return [
        {"label": label, "value": value}
        for value, label in LAYOUT_LABELS.items()
    ]


def asset_dag_layout(layout_name: str | None) -> dict[str, Any]:
    """Return Cytoscape layout options for one named graph layout."""

    layout_name = layout_name or DEFAULT_LAYOUT_NAME

    if layout_name == "dagre_lr":
        return {
            "name": "dagre",
            "rankDir": "LR",
            "fit": True,
            "padding": 35,
            "nodeSep": 44,
            "rankSep": 110,
            "edgeSep": 14,
        }

    if layout_name == "dagre_tb":
        return {
            "name": "dagre",
            "rankDir": "TB",
            "fit": True,
            "padding": 35,
            "nodeSep": 34,
            "rankSep": 100,
            "edgeSep": 12,
        }

    if layout_name == "cose_bilkent":
        return {
            "name": "cose-bilkent",
            "fit": True,
            "padding": 35,
            "nodeRepulsion": 7000,
            "idealEdgeLength": 115,
            "edgeElasticity": 0.25,
            "gravity": 0.35,
            "numIter": 2500,
            "tile": True,
        }

    return {
        "name": "breadthfirst",
        "directed": True,
        "circle": False,
        "fit": True,
        "padding": 35,
        "spacingFactor": 1.35,
        "avoidOverlap": True,
        "maximalAdjustments": 4,
        "animate": False,
    }


def asset_graph_stylesheet() -> list[dict[str, Any]]:
    """Return Cytoscape styles for the asset graph."""

    stylesheet = [
        {
            "selector": "node",
            "style": {
                "label": "data(label)",
                "shape": "round-rectangle",
                "width": "label",
                "height": 40,
                "padding": "8px",
                "background-color": "#546E7A",
                "border-width": 1,
                "border-color": "#263238",
                "color": "#FFFFFF",
                "font-size": 10,
                "text-wrap": "wrap",
                "text-max-width": 120,
                "text-valign": "center",
                "text-halign": "center",
            },
        },
        {
            "selector": "edge",
            "style": {
                "width": 1.5,
                "line-color": "#90A4AE",
                "target-arrow-color": "#90A4AE",
                "target-arrow-shape": "triangle",
                "curve-style": "bezier",
            },
        },
        {
            "selector": ":selected",
            "style": {
                "border-width": 3,
                "border-color": "#1565C0",
            },
        },
        {
            "selector": ".run_group",
            "style": {
                "label": "data(label)",
                "shape": "round-rectangle",
                "background-opacity": 0.16,
                "background-color": "#D9F99D",
                "border-width": 2,
                "border-style": "solid",
                "border-color": "#86A86A",
                "color": "#3F6212",
                "font-size": 12,
                "font-weight": "bold",
                "text-valign": "top",
                "text-halign": "center",
                "padding": "32px",
                "corner-radius": 18,
                "z-compound-depth": "bottom",
            },
        },
    ]

    for status, color in STATUS_COLORS.items():
        stylesheet.append(
            {
                "selector": f".{status}",
                "style": {"background-color": color},
            }
        )

    return stylesheet


def legend_item(status: str, color: str):
    """Return one compact status legend item."""

    return html.Div(
        [
            html.Span(style={"backgroundColor": color}),
            html.Small(status),
        ],
        className="asset-dag-legend-item",
    )


def format_cache_path(cache_path: Any) -> str | None:
    """Format a cache path value for display in Dash."""

    if cache_path is None:
        return None
    if isinstance(cache_path, tuple):
        return "\n".join(f"{key}: {path}" for key, path in cache_path)
    return str(cache_path)


def format_output_paths(output_paths: Any) -> str:
    """Format existing or missing output paths for display in Dash."""

    if not output_paths:
        return ""
    return "\n".join(f"{key}: {path}" for key, path in output_paths)


def _raise_if_dash_is_missing() -> None:
    if _DASH_IMPORT_ERROR is None:
        return
    raise ImportError(DASH_INSTALL_MESSAGE) from _DASH_IMPORT_ERROR


def asset_dag_index_string() -> str:
    """Return the HTML template and CSS used by the local Dash app."""

    return """
<!DOCTYPE html>
<html>
    <head>
        {%metas%}
        <title>{%title%}</title>
        {%favicon%}
        {%css%}
        <style>
            body {
                margin: 0;
                font-family: Arial, sans-serif;
                color: #1F2933;
                background: #F6F8FA;
            }
            .asset-dag-page {
                height: 100vh;
                display: flex;
                flex-direction: column;
                overflow: hidden;
            }
            .asset-dag-toolbar {
                padding: 10px 14px 9px;
                border-bottom: 1px solid #D9E2EC;
                background: #FFFFFF;
            }
            .asset-dag-heading {
                display: flex;
                align-items: center;
                justify-content: space-between;
                gap: 16px;
                margin-bottom: 8px;
            }
            .asset-dag-toolbar h1 {
                margin: 0;
                font-size: 17px;
                line-height: 1.3;
                font-weight: 600;
            }
            .asset-dag-summary {
                display: flex;
                flex-wrap: wrap;
                justify-content: flex-end;
                gap: 8px;
                color: #52606D;
                font-size: 12px;
            }
            .asset-dag-filters {
                display: grid;
                grid-template-columns: 1.4fr 0.8fr 0.65fr 0.75fr 128px;
                gap: 10px;
            }
            .asset-dag-filter label {
                display: block;
                margin-bottom: 3px;
                color: #52606D;
                font-size: 11px;
                font-weight: 600;
            }
            .asset-dag-checklist label {
                display: inline-flex;
                align-items: center;
                gap: 5px;
                min-height: 36px;
                margin: 0;
                color: #1F2933;
                font-size: 12px;
                font-weight: 400;
            }
            .asset-dag-legend {
                display: flex;
                flex-wrap: wrap;
                gap: 12px;
                margin-top: 8px;
            }
            .asset-dag-legend-item {
                display: inline-flex;
                align-items: center;
                gap: 5px;
            }
            .asset-dag-legend-item span {
                width: 10px;
                height: 10px;
                border-radius: 50%;
                display: inline-block;
            }
            .asset-dag-main {
                min-height: 0;
                flex: 1;
                display: grid;
                grid-template-columns: minmax(0, 1fr) 300px;
            }
            .asset-dag-graph {
                width: 100%;
                height: 100%;
                min-height: 0;
                background: #FFFFFF;
            }
            .asset-dag-details {
                padding: 14px;
                border-left: 1px solid #D9E2EC;
                background: #FFFFFF;
                overflow: auto;
            }
            .asset-dag-details h2 {
                margin: 0 0 12px;
                font-size: 16px;
            }
            .asset-dag-details table {
                width: 100%;
                border-collapse: collapse;
                font-size: 12px;
            }
            .asset-dag-details th,
            .asset-dag-details td {
                vertical-align: top;
                text-align: left;
                padding: 7px 0;
                border-bottom: 1px solid #E6EAF0;
                word-break: break-word;
            }
            .asset-dag-details th {
                width: 82px;
                color: #52606D;
                font-weight: 600;
            }
            .asset-dag-details details {
                margin-top: 10px;
                font-size: 12px;
            }
            .asset-dag-details summary {
                cursor: pointer;
                color: #334E68;
                font-weight: 600;
            }
            .asset-dag-details pre {
                max-height: 40vh;
                overflow: auto;
                white-space: pre-wrap;
                word-break: break-word;
                margin: 8px 0 0;
                padding: 8px;
                background: #F6F8FA;
                border: 1px solid #D9E2EC;
                font-family: Consolas, monospace;
                font-size: 11px;
                line-height: 1.35;
            }
            @media (max-width: 900px) {
                .asset-dag-filters,
                .asset-dag-main {
                    grid-template-columns: 1fr;
                }
                .asset-dag-heading {
                    align-items: flex-start;
                    flex-direction: column;
                }
                .asset-dag-summary {
                    justify-content: flex-start;
                }
                .asset-dag-graph {
                    height: 65vh;
                }
                .asset-dag-details {
                    border-left: 0;
                    border-top: 1px solid #D9E2EC;
                }
            }
        </style>
    </head>
    <body>
        {%app_entry%}
        <footer>
            {%config%}
            {%scripts%}
            {%renderer%}
        </footer>
    </body>
</html>
"""
