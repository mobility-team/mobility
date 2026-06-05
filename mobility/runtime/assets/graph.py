import pathlib
from typing import Any

import networkx as nx

from mobility.runtime.assets.asset import Asset


def build_asset_graph(
    root_asset: Asset,
    *,
    include_root: bool = True,
    file_assets_only: bool = False,
    include_node_data: bool = True,
) -> nx.DiGraph:
    """Build the dependency graph under one asset.

    Args:
        root_asset: Asset used as the graph root.
        include_root: If ``True``, include ``root_asset`` as a graph node.
            If ``False``, only include assets found in its inputs.
        file_assets_only: If ``True``, only keep file-backed assets. This is
            the execution graph used by ``FileAsset`` before it rebuilds stale
            dependencies.
        include_node_data: If ``True``, attach cache status and display
            metadata to graph nodes. Runtime rebuild logic keeps this disabled
            so it does not do viewer-only filesystem checks.

    Returns:
        A directed graph where edges point from an input asset to the asset that
        depends on it.
    """

    graph = nx.DiGraph()
    canonical_assets = {}
    visited_asset_keys = set()

    def canonical_asset(asset: Asset) -> Asset:
        key = asset_graph_key(asset)
        if key not in canonical_assets:
            canonical_assets[key] = asset
        return canonical_assets[key]

    def add_asset(asset: Asset) -> Asset:
        asset = canonical_asset(asset)
        asset_key = asset_graph_key(asset)
        if include_node_data:
            graph.add_node(asset, **asset_graph_node_data(asset))
        else:
            graph.add_node(asset)

        if asset_key in visited_asset_keys:
            return asset

        visited_asset_keys.add(asset_key)
        for input_name, input_value in asset.inputs.items():
            for dependency in iter_asset_dependencies(
                input_value,
                file_assets_only=file_assets_only,
            ):
                dependency = add_asset(dependency)
                graph.add_edge(dependency, asset, input_name=input_name)

        return asset

    if include_root and (not file_assets_only or is_file_asset(root_asset)):
        add_asset(root_asset)
    else:
        for input_value in root_asset.inputs.values():
            for dependency in iter_asset_dependencies(
                input_value,
                file_assets_only=file_assets_only,
            ):
                add_asset(dependency)

    return graph


def build_asset_graph_from_roots(
    root_assets: list[Asset] | tuple[Asset, ...],
    *,
    include_node_data: bool = True,
) -> nx.DiGraph:
    """Build one dependency graph from several root assets."""

    graph = nx.DiGraph()
    canonical_assets = {}

    def canonical_asset(asset: Asset) -> Asset:
        key = asset_graph_key(asset)
        if key not in canonical_assets:
            canonical_assets[key] = asset
        return canonical_assets[key]

    for root_asset in root_assets:
        root_context = asset_run_context(root_asset)
        root_graph = build_asset_graph(
            root_asset,
            include_root=True,
            file_assets_only=False,
            include_node_data=include_node_data,
        )
        if include_node_data:
            for _, data in root_graph.nodes(data=True):
                data["run_contexts"] = (root_context,)

        for asset, data in root_graph.nodes(data=True):
            asset = canonical_asset(asset)
            if asset in graph:
                if include_node_data:
                    contexts = set(graph.nodes[asset].get("run_contexts", ()))
                    contexts.update(data.get("run_contexts", ()))
                    graph.nodes[asset]["run_contexts"] = tuple(sorted(contexts))
            else:
                graph.add_node(asset, **data)

        for source, target, data in root_graph.edges(data=True):
            graph.add_edge(
                canonical_asset(source),
                canonical_asset(target),
                **data,
            )
    return graph


def build_upstream_file_asset_graph(root_asset: Asset) -> nx.DiGraph:
    """Build the file-backed upstream graph used before an asset is rebuilt."""

    return build_asset_graph(
        root_asset,
        include_root=False,
        file_assets_only=True,
        include_node_data=False,
    )


def iter_asset_dependencies(
    value: Any,
    *,
    file_assets_only: bool = False,
    visited_assets: set[int] | None = None,
):
    """Yield assets nested in one input value."""

    if visited_assets is None:
        visited_assets = set()

    if isinstance(value, Asset):
        asset_id = id(value)
        if asset_id in visited_assets:
            return
        visited_assets.add(asset_id)

        if not file_assets_only or is_file_asset(value):
            yield value
            return

        # Runtime cache checks keep the graph limited to FileAssets, but some
        # FileAssets are stored below in-memory assets such as transport modes.
        # Look through these in-memory assets so hidden file dependencies are
        # still rebuilt before their descendants are read.
        for nested in value.inputs.values():
            yield from iter_asset_dependencies(
                nested,
                file_assets_only=file_assets_only,
                visited_assets=visited_assets,
            )
        return

    if isinstance(value, dict):
        for nested in value.values():
            yield from iter_asset_dependencies(
                nested,
                file_assets_only=file_assets_only,
                visited_assets=visited_assets,
            )
        return

    if isinstance(value, (list, tuple, set)):
        for nested in value:
            yield from iter_asset_dependencies(
                nested,
                file_assets_only=file_assets_only,
                visited_assets=visited_assets,
            )


def is_file_asset(asset: Asset) -> bool:
    """Return True for assets backed by cache files."""

    return (
        hasattr(asset, "cache_path")
        and hasattr(asset, "hash_path")
        and hasattr(asset, "is_update_needed")
    )


def asset_graph_key(asset: Asset) -> tuple:
    """Return a stable key used to deduplicate equivalent graph assets."""

    if is_file_asset(asset):
        return (
            asset.__class__,
            asset.inputs_hash,
            cache_path_value(asset),
        )
    return (asset.__class__, asset.inputs_hash)


def asset_graph_node_data(asset: Asset) -> dict[str, Any]:
    """Return plain node metadata for graph views and exports."""

    existing_outputs, missing_outputs = asset_output_paths_by_existence(asset)
    data = {
        "asset_type": asset.__class__.__name__,
        "inputs_hash": getattr(asset, "inputs_hash", None),
        "status": asset_cache_status(asset),
        "cache_path": cache_path_value(asset),
        "existing_outputs": existing_outputs,
        "missing_outputs": missing_outputs,
        "cached_hash": asset_cached_hash(asset),
    }

    # These attributes are common on simulation assets and help filter long
    # group-day-trips graphs without adding model-specific code here.
    for attribute_name in ["iteration", "scenario", "replication", "is_weekday"]:
        if hasattr(asset, attribute_name):
            data[attribute_name] = getattr(asset, attribute_name)

    return data


def asset_run_context(asset: Asset) -> str:
    """Return the scenario/day-type/replication context for a root asset."""

    scenario = getattr(asset, "scenario", None) or "default"
    replication = getattr(asset, "replication", None)
    if replication is None:
        replication = "unknown"

    is_weekday = getattr(asset, "is_weekday", None)
    if is_weekday is True:
        day_type = "weekday"
    elif is_weekday is False:
        day_type = "weekend"
    else:
        day_type = "unknown"

    return f"{scenario}|{day_type}|{replication}"


def asset_cache_status(asset: Asset) -> str:
    """Return a simple cache status for one asset."""

    if not is_file_asset(asset):
        return "not_file_asset"

    if asset.inputs_changed():
        return "stale"
    if asset.assets_missing():
        return "missing"
    return "cached"


def asset_output_paths_by_existence(asset: Asset) -> tuple[tuple[str, str], tuple[str, str]]:
    """Split cache output paths into existing and missing groups."""

    if not is_file_asset(asset):
        return (), ()

    existing_outputs = []
    missing_outputs = []

    cache_path = getattr(asset, "cache_path", None)
    if cache_path is None:
        return (), ()
    if isinstance(cache_path, dict):
        output_paths = [
            (str(key), pathlib.Path(path))
            for key, path in sorted(cache_path.items())
        ]
    else:
        output_paths = [("output", pathlib.Path(cache_path))]

    for key, path in output_paths:
        row = (key, str(path))
        if path.exists():
            existing_outputs.append(row)
        else:
            missing_outputs.append(row)

    return tuple(existing_outputs), tuple(missing_outputs)


def asset_cached_hash(asset: Asset) -> str | None:
    """Return the on-disk input hash when an asset exposes one."""

    if not is_file_asset(asset):
        return None
    return asset.get_cached_hash()


def cache_path_value(asset: Asset) -> str | tuple[tuple[str, str], ...] | None:
    """Return cache paths as simple values that can be used in JSON-like data."""

    cache_path = getattr(asset, "cache_path", None)
    if cache_path is None:
        return None

    if isinstance(cache_path, dict):
        return tuple(
            (str(key), str(pathlib.Path(path)))
            for key, path in sorted(cache_path.items())
        )
    return str(pathlib.Path(cache_path))
