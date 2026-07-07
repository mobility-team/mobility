from __future__ import annotations

import logging
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any, Iterator

import networkx as nx

from mobility.runtime.assets.asset import Asset
from mobility.runtime.assets.graph import asset_graph_key, build_asset_graph
from mobility.runtime.project_cache import record_file_asset_use


_current_asset_resolver: ContextVar["AssetResolver | None"] = ContextVar(
    "current_asset_resolver",
    default=None,
)
_REQUESTED_ASSET_WAS_NOT_REBUILT = object()


class AssetResolver:
    """Prepare file-backed assets inside one model execution.

    A file-backed asset is a cached result, for example transport zones,
    travel costs, or one group-day-trips iteration state. Before reading a
    cached file, the package must check whether the asset inputs are still
    valid and whether upstream cached files still exist.

    The old code rebuilt the upstream dependency graph every time `.get()` was
    called. During a model run, many assets ask for the same transport zones,
    costs, survey files, and iteration states several times. This resolver keeps
    a small memory of what was already checked during the current execution, so
    repeated reads do not redo the same graph walk.

    The resolver is intentionally short-lived. One run or one explicit
    `asset_resolution_context()` gets one resolver. There is no global package
    cache shared between unrelated runs.
    """

    def __init__(self) -> None:
        # Asset keys that have already been checked or rebuilt in this resolver
        # context. A key identifies the logical cached file, not just one Python
        # object instance.
        self.prepared_asset_keys = set()

        # Dependency graphs already built during this context. These graphs are
        # pure structure, so they can be reused safely while the same execution
        # is resolving several assets.
        self.saved_dependency_graphs = {}

    def get(self, requested_asset: Asset, *args, **kwargs) -> Any:
        """Return one cached asset after preparing the files it depends on."""
        rebuilt_value = self.prepare_requested_asset(
            requested_asset,
            requested_asset_args=args,
            requested_asset_kwargs=kwargs,
        )
        if rebuilt_value is not _REQUESTED_ASSET_WAS_NOT_REBUILT:
            return rebuilt_value
        return requested_asset.get_cached_asset(*args, **kwargs)

    def prepare_requested_asset(
        self,
        requested_asset: Asset,
        *,
        requested_asset_args: tuple = (),
        requested_asset_kwargs: dict | None = None,
    ) -> Any:
        """Prepare the requested asset and all upstream cached files.

        The requested asset is the one whose `.get()` method the caller asked
        for. If it needs to be rebuilt, its `create_and_get_asset()` return
        value is passed back to the caller, preserving the old `FileAsset.get()`
        behavior.
        """
        return self._prepare_dependency_graph(
            requested_asset,
            include_requested_asset=True,
            requested_asset_args=requested_asset_args,
            requested_asset_kwargs=requested_asset_kwargs or {},
        )

    def prepare_upstream_assets(self, asset: Asset) -> None:
        """Prepare only the files that the given asset depends on.

        This keeps the old `FileAsset.update_ancestors_if_needed()` contract:
        rebuild stale dependencies, but do not rebuild the asset itself.
        """
        self._prepare_dependency_graph(
            asset,
            include_requested_asset=False,
            requested_asset_args=(),
            requested_asset_kwargs={},
        )

    def _prepare_dependency_graph(
        self,
        requested_asset: Asset,
        *,
        include_requested_asset: bool,
        requested_asset_args: tuple,
        requested_asset_kwargs: dict,
    ) -> Any:
        requested_asset_key = asset_graph_key(requested_asset)

        # If this exact requested asset was already checked in this resolver,
        # the caller can directly read its cached file. This removes repeated
        # graph walks inside long model runs.
        if (
            include_requested_asset
            and requested_asset_key in self.prepared_asset_keys
        ):
            return _REQUESTED_ASSET_WAS_NOT_REBUILT

        dependency_graph = self._get_dependency_graph(
            requested_asset,
            include_requested_asset=include_requested_asset,
        )
        rebuilt_requested_asset_value = _REQUESTED_ASSET_WAS_NOT_REBUILT

        logging.debug(
            "Asset resolver graph for %s (%s) has %s file assets and %s dependency edges.",
            requested_asset.__class__.__name__,
            requested_asset.inputs_hash,
            str(dependency_graph.number_of_nodes()),
            str(dependency_graph.number_of_edges()),
        )

        asset_keys_to_rebuild = set()
        for dependency_asset in dependency_graph.nodes:
            dependency_asset_key = asset_graph_key(dependency_asset)

            # This asset was already checked or rebuilt during the current
            # execution. Do not ask the filesystem about it again.
            if dependency_asset_key in self.prepared_asset_keys:
                continue

            if dependency_asset.is_update_needed():
                # If one upstream file is stale or missing, every file asset
                # that depends on it must be rebuilt in this request graph.
                asset_keys_to_rebuild.add(dependency_asset_key)
                for downstream_asset in nx.descendants(
                    dependency_graph,
                    dependency_asset,
                ):
                    asset_keys_to_rebuild.add(asset_graph_key(downstream_asset))

        try:
            assets_in_dependency_order = list(nx.topological_sort(dependency_graph))
        except nx.NetworkXUnfeasible:
            raise RuntimeError("Dependency cycle detected among FileAssets")

        for dependency_asset in assets_in_dependency_order:
            dependency_asset_key = asset_graph_key(dependency_asset)
            if dependency_asset_key in asset_keys_to_rebuild:
                if dependency_asset_key == requested_asset_key:
                    rebuilt_requested_asset_value = self._rebuild_asset(
                        dependency_asset,
                        *requested_asset_args,
                        **requested_asset_kwargs,
                    )
                else:
                    self._rebuild_asset(dependency_asset)

            # Mark the asset as prepared whether it was rebuilt or already
            # valid. Later reads in the same execution can skip it.
            self.prepared_asset_keys.add(dependency_asset_key)
            record_file_asset_use(dependency_asset)

        return rebuilt_requested_asset_value

    def _get_dependency_graph(
        self,
        requested_asset: Asset,
        *,
        include_requested_asset: bool,
    ) -> nx.DiGraph:
        dependency_graph_key = (
            asset_graph_key(requested_asset),
            include_requested_asset,
        )
        if dependency_graph_key not in self.saved_dependency_graphs:
            dependency_graph = build_asset_graph(
                requested_asset,
                include_root=include_requested_asset,
                file_assets_only=True,
                include_node_data=False,
            )
            self.saved_dependency_graphs[dependency_graph_key] = dependency_graph
        return self.saved_dependency_graphs[dependency_graph_key]

    def _rebuild_asset(self, asset: Asset, *args, **kwargs) -> Any:
        logging.debug(
            "Rebuilding asset %s (%s)...",
            asset.__class__.__name__,
            asset.inputs_hash,
        )
        value = asset.create_and_get_asset(*args, **kwargs)
        asset.update_hash(asset.inputs_hash)
        logging.debug(
            "Asset %s (%s) is ready.",
            asset.__class__.__name__,
            asset.inputs_hash,
        )
        return value


def get_current_asset_resolver() -> AssetResolver | None:
    """Return the resolver currently shared by nested asset reads."""
    return _current_asset_resolver.get()


@contextmanager
def asset_resolution_context(
    resolver: AssetResolver | None = None,
) -> Iterator[AssetResolver]:
    """Run asset reads with one shared resolver instance.

    `contextvars` makes the resolver available to nested `.get()` calls without
    passing an extra argument through every transport model class.
    """
    active_resolver = resolver or AssetResolver()
    token = _current_asset_resolver.set(active_resolver)
    try:
        yield active_resolver
    finally:
        _current_asset_resolver.reset(token)
