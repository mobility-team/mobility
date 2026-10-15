from __future__ import annotations

import logging
import pathlib
import tempfile
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any, Callable, Iterator

import networkx as nx
import polars as pl

from mobility.runtime.assets.asset import Asset
from mobility.runtime.assets.graph import (
    asset_graph_key,
    build_asset_graph,
    is_file_asset,
)
from mobility.runtime.project_cache import record_file_asset_use


_current_asset_resolver: ContextVar["AssetResolver | None"] = ContextVar(
    "current_asset_resolver",
    default=None,
)
_REQUESTED_ASSET_WAS_NOT_REBUILT = object()


class AssetResolver:
    """Prepare file-backed assets and transient values inside one execution.

    A file-backed asset is a cached result, for example transport zones,
    travel costs, or a compact restart checkpoint. Before reading a cached
    file, the package must check whether the asset inputs are still valid and
    whether upstream cached files still exist.

    The old code rebuilt the upstream dependency graph every time `.get()` was
    called. During a model run, many assets ask for the same transport zones,
    costs, survey files, and iteration states several times. This resolver keeps
    a small memory of what was already checked during the current execution and
    holds transient iteration states while their consumers need them.

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

        # Values from non-file assets live only for this execution. Shared
        # iteration states can spill compact data to temporary disk so scenario
        # runs can reuse them without keeping every state in memory.
        self.in_memory_values: dict[tuple, Any] = {}
        self.in_memory_assets: dict[tuple, Asset] = {}
        self.shared_asset_keys: set[tuple] = set()
        self.spilled_asset_keys: set[tuple] = set()
        self._temporary_value_paths: dict[tuple, pathlib.Path] = {}
        self.execution_cost_assets: dict[tuple, Asset] = {}
        self.execution_cost_consumers: dict[tuple, set[tuple]] = {}
        self._execution_cost_consumer_stack: list[tuple] = []
        self._spill_directory: tempfile.TemporaryDirectory | None = None

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

    def get_execution_scoped_file_asset(
        self,
        asset: Asset,
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        """Read a cost FileAsset once, then keep its value only for this run."""
        self._register_current_execution_cost_use(asset)

        def compute():
            try:
                return self.get(asset, *args, **kwargs)
            finally:
                # Cost tables are useful while this resolver is active. Their
                # Parquet files are only needed for the R/Python handoff or the
                # initial cache read, so remove them as soon as that read ends.
                asset.remove()
                self.prepared_asset_keys.discard(asset_graph_key(asset))

        return self.get_in_memory_asset(
            asset,
            compute,
            spill_to_disk=getattr(asset, "spill_execution_value", None),
            load_from_disk=getattr(asset, "load_execution_value", None),
        )

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

    def get_in_memory_asset(
        self,
        asset: Asset,
        compute: Callable[[], Any],
        *,
        spill_to_disk: Callable[[Any, pathlib.Path], None] | None = None,
        load_from_disk: Callable[[pathlib.Path], Any] | None = None,
    ) -> Any:
        """Compute or restore a transient asset needed by this execution."""
        asset_key = asset_graph_key(asset)
        if asset_key in self.in_memory_values:
            return self.in_memory_values[asset_key]

        is_shared = asset_key in self.shared_asset_keys
        spill_path = None
        if is_shared:
            spill_path = self._spill_path(
                asset,
                suffix=(
                    ".state"
                    if getattr(asset, "is_execution_state", False)
                    else ".parquet"
                ),
            )

        can_load_spill = (
            spill_path is not None
            and spill_path.exists()
            and (load_from_disk is not None or spill_path.is_file())
        )
        if can_load_spill:
            value = (
                load_from_disk(spill_path)
                if load_from_disk is not None
                else pl.read_parquet(spill_path)
            )
            self.spilled_asset_keys.add(asset_key)
            self._temporary_value_paths[asset_key] = spill_path
        else:
            value = compute()
            if spill_path is not None and spill_to_disk is not None:
                try:
                    spill_to_disk(value, spill_path)
                except OSError:
                    logging.warning(
                        "Could not spill shared %s state to temporary disk; "
                        "keeping it in memory for this execution.",
                        asset.__class__.__name__,
                        exc_info=True,
                    )
                else:
                    self.spilled_asset_keys.add(asset_key)
                    self._temporary_value_paths[asset_key] = spill_path
            elif spill_path is not None and isinstance(value, pl.DataFrame):
                try:
                    value.write_parquet(spill_path)
                except OSError:
                    logging.warning(
                        "Could not spill shared %s table to temporary disk; "
                        "keeping it in memory for this execution.",
                        asset.__class__.__name__,
                        exc_info=True,
                    )
                else:
                    self.spilled_asset_keys.add(asset_key)
                    self._temporary_value_paths[asset_key] = spill_path

        self.in_memory_values[asset_key] = value
        self.in_memory_assets[asset_key] = asset
        return self.in_memory_values[asset_key]

    def release_in_memory_asset(self, asset: Asset) -> None:
        """Release a transient value while keeping any shared disk spill."""
        asset_key = asset_graph_key(asset)
        if (
            asset_key not in self.shared_asset_keys
            or asset_key in self.spilled_asset_keys
        ):
            self.in_memory_values.pop(asset_key, None)
            self.in_memory_assets.pop(asset_key, None)

    def release_transient_values(self) -> None:
        """Release resolved values, keeping only shared values that could not spill."""
        for asset in tuple(self.in_memory_assets.values()):
            self.release_in_memory_asset(asset)

    def close(self) -> None:
        """Remove temporary files used to spill shared transient assets."""
        if self._spill_directory is not None:
            self._spill_directory.cleanup()
            self._spill_directory = None

    @contextmanager
    def execution_cost_consumer(self, consumer: Asset) -> Iterator[None]:
        """Track cost assets used while one iteration state is being built."""
        consumer_key = ("state", asset_graph_key(consumer))
        self._execution_cost_consumer_stack.append(consumer_key)
        try:
            yield
        finally:
            self._execution_cost_consumer_stack.pop()

    def inherit_execution_cost_uses(self, parent: Asset, child: Asset) -> None:
        """Give a lazily-created route-cost table its parent's planned lifetime."""
        parent_key = asset_graph_key(parent)
        child_key = asset_graph_key(child)
        self.execution_cost_assets[child_key] = child
        consumers = self.execution_cost_consumers.get(parent_key)
        if consumers:
            self.execution_cost_consumers.setdefault(child_key, set()).update(consumers)
        elif self._execution_cost_consumer_stack:
            self.execution_cost_consumers.setdefault(child_key, set()).add(
                self._execution_cost_consumer_stack[-1]
            )
        if len(self.execution_cost_consumers.get(child_key, ())) > 1:
            self.shared_asset_keys.add(child_key)

    def consume_execution_cost_state(self, state_asset: Asset) -> None:
        """Release cost values whose last planned iteration has completed."""
        self._consume_execution_cost_use(("state", asset_graph_key(state_asset)))

    def consume_execution_cost_root(self, root_asset: Asset) -> None:
        """Release cost values after a requested cost output has been built."""
        self._consume_execution_cost_use(("output", asset_graph_key(root_asset)))

    def _spill_path(self, asset: Asset, *, suffix: str) -> pathlib.Path:
        """Return this execution's temporary file path for one shared asset."""
        if self._spill_directory is None:
            self._spill_directory = tempfile.TemporaryDirectory(
                prefix="mobility-shared-assets-"
            )
        filename = f"{asset.__class__.__name__}-{asset.inputs_hash}{suffix}"
        return pathlib.Path(self._spill_directory.name) / filename

    def temporary_asset_path(self, asset: Asset, *, suffix: str) -> pathlib.Path:
        """Return a path for a temporary file needed by an external tool."""
        if self._spill_directory is None:
            self._spill_directory = tempfile.TemporaryDirectory(
                prefix="mobility-run-assets-"
            )
        filename = f"{asset.__class__.__name__}-{asset.inputs_hash}{suffix}"
        return pathlib.Path(self._spill_directory.name) / filename

    def retain_assets_shared_by_roots(self, root_assets: list[Asset]) -> None:
        """Identify transient assets reused by roots and shared state branches."""
        root_use_count: dict[tuple, int] = {}
        state_children: dict[tuple, set[tuple]] = {}
        for root_asset in root_assets:
            graph = build_asset_graph(
                root_asset,
                include_root=True,
                file_assets_only=False,
                include_node_data=False,
            )

            # Costs may be reused across several iteration states inside one
            # result root, even when they appear in only one metric output.
            for state_asset in graph.nodes:
                if not getattr(state_asset, "is_execution_state", False):
                    continue
                cost_asset = getattr(
                    state_asset,
                    "transport_costs",
                    getattr(state_asset, "initial_transport_costs", None),
                )
                if cost_asset is None:
                    continue
                consumer_key = ("state", asset_graph_key(state_asset))
                self._register_execution_cost_dependencies(
                    cost_asset,
                    consumer_key,
                )

            # Final plan-step tables and explicit cost requests use the full
            # cost table after their iteration state has completed.
            output_cost_assets = getattr(root_asset, "execution_cost_assets", ())
            if output_cost_assets:
                consumer_key = ("output", asset_graph_key(root_asset))
                for cost_asset in output_cost_assets:
                    self._register_execution_cost_dependencies(
                        cost_asset,
                        consumer_key,
                    )

            # Iteration results are consumed in order. A state that only feeds
            # the next state should stay live for that handoff, not spill just
            # because several result roots include it as an ancestor. Spill a
            # state only when it feeds distinct later states, such as the last
            # shared iteration before two scenarios diverge.
            for source, target in graph.edges:
                if (
                    getattr(source, "is_execution_state", False)
                    and getattr(target, "is_execution_state", False)
                ):
                    source_key = asset_graph_key(source)
                    state_children.setdefault(source_key, set()).add(
                        asset_graph_key(target)
                    )

            seen_for_root = {
                asset_graph_key(asset)
                for asset in graph.nodes
                if (
                    not is_file_asset(asset)
                    or getattr(asset, "is_execution_scoped_cost_asset", False)
                )
                and not getattr(asset, "is_execution_state", False)
                and not getattr(asset, "is_streaming_output", False)
            }
            for asset_key in seen_for_root:
                root_use_count[asset_key] = root_use_count.get(asset_key, 0) + 1

        self.shared_asset_keys.update(
            asset_key
            for asset_key, use_count in root_use_count.items()
            if use_count > 1
        )
        self.shared_asset_keys.update(
            asset_key
            for asset_key, children in state_children.items()
            if len(children) > 1
        )
        self.shared_asset_keys.update(
            asset_key
            for asset_key, consumers in self.execution_cost_consumers.items()
            if len(consumers) > 1
        )

    def _register_execution_cost_dependencies(
        self,
        root_asset: Asset,
        consumer_key: tuple,
    ) -> None:
        """Register cost nodes and their lazy output tables for one consumer."""
        for cost_asset in self._execution_cost_nodes(root_asset):
            self._register_execution_cost_use(cost_asset, consumer_key)
            planned_tables = getattr(
                cost_asset,
                "planned_execution_cost_tables",
                None,
            )
            if planned_tables is None:
                continue
            for table in planned_tables():
                self._register_execution_cost_use(table, consumer_key)

    @staticmethod
    def _execution_cost_nodes(root_asset: Asset) -> set[Asset]:
        """Collect run-cost assets without walking into earlier iteration states."""
        graph = build_asset_graph(
            root_asset,
            include_root=True,
            file_assets_only=False,
            include_node_data=False,
        )
        nodes = set()
        visited = set()
        pending = [root_asset]
        while pending:
            asset = pending.pop()
            asset_key = asset_graph_key(asset)
            if asset_key in visited:
                continue
            visited.add(asset_key)
            if asset is not root_asset and getattr(asset, "is_execution_state", False):
                continue
            if getattr(asset, "is_execution_scoped_cost_asset", False):
                nodes.add(asset)
            pending.extend(graph.predecessors(asset))
        return nodes

    def _register_current_execution_cost_use(self, asset: Asset) -> None:
        if self._execution_cost_consumer_stack:
            self._register_execution_cost_use(
                asset,
                self._execution_cost_consumer_stack[-1],
            )
        else:
            self.execution_cost_assets[asset_graph_key(asset)] = asset

    def _register_execution_cost_use(
        self,
        asset: Asset,
        consumer_key: tuple,
    ) -> None:
        asset_key = asset_graph_key(asset)
        self.execution_cost_assets[asset_key] = asset
        consumers = self.execution_cost_consumers.setdefault(asset_key, set())
        consumers.add(consumer_key)
        if len(consumers) > 1:
            self.shared_asset_keys.add(asset_key)

    def _consume_execution_cost_use(self, consumer_key: tuple) -> None:
        for asset_key, consumers in tuple(self.execution_cost_consumers.items()):
            if consumer_key not in consumers:
                continue
            consumers.remove(consumer_key)
            asset = self.execution_cost_assets[asset_key]
            if consumers:
                # Shared values can leave RAM after this iteration. The spill
                # remains available to the later scenario or output that needs it.
                self.release_in_memory_asset(asset)
            else:
                self._discard_execution_cost_asset(asset_key)
                self.execution_cost_consumers.pop(asset_key, None)

    def _discard_execution_cost_asset(self, asset_key: tuple) -> None:
        asset = self.execution_cost_assets.pop(asset_key, None)
        self.in_memory_values.pop(asset_key, None)
        self.in_memory_assets.pop(asset_key, None)
        self.spilled_asset_keys.discard(asset_key)
        self.shared_asset_keys.discard(asset_key)
        self.prepared_asset_keys.discard(asset_key)
        spill_path = self._temporary_value_paths.pop(asset_key, None)
        if spill_path is not None and spill_path.exists():
            spill_path.unlink()
        if asset is not None and is_file_asset(asset):
            asset.remove()

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
        cache_path = getattr(asset, "cache_path", None)
        if isinstance(cache_path, dict):
            for path in cache_path.values():
                pathlib.Path(path).parent.mkdir(parents=True, exist_ok=True)
        elif cache_path is not None:
            pathlib.Path(cache_path).parent.mkdir(parents=True, exist_ok=True)
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
        if resolver is None:
            active_resolver.close()
