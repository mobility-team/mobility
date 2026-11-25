"""Prepare compatible scenario road costs in one R execution."""

import json
import logging
import os
import pathlib
from importlib import resources

import polars as pl

from mobility.runtime.assets.graph import asset_graph_key
from mobility.runtime.assets.in_memory_asset import InMemoryAsset
from mobility.runtime.assets.resolver import get_current_asset_resolver
from mobility.runtime.r_integration.r_script_runner import RScriptRunner
from mobility.runtime.temporary import check_temporary_disk_limit, write_temporary_bytes
from mobility.transport.costs.path.path_travel_costs import PathTravelCosts
from mobility.transport.costs.path.shared_road_worker import SharedRoadWorker
from mobility.transport.modes.carpool.detailed.detailed_carpool_travel_costs import DetailedCarpoolTravelCosts


def prepare_shared_congested_costs(cost_assets):
    """Prime per-scenario path costs for compatible congestion updates.

    The resolver owns the results for this execution only. Scenarios whose
    road network, routing settings or assignment parameters differ are never
    batched together. A matching carpool mode also receives the assigned graph.
    """
    resolver = get_current_asset_resolver()
    if resolver is None:
        return

    groups = {}
    for iteration_costs in cost_assets:
        if not iteration_costs.congestion_flows.should_update_costs:
            continue
        carpool_graphs = set()
        for mode in iteration_costs.modes:
            costs = mode.inputs["generalized_cost"].inputs.get("travel_costs")
            if isinstance(costs, DetailedCarpoolTravelCosts):
                carpool_graphs.add(costs.car_travel_costs.inputs_hash)
        for mode in iteration_costs.modes:
            generalized_cost = mode.inputs["generalized_cost"]
            travel_costs = generalized_cost.inputs.get("travel_costs")
            if not isinstance(travel_costs, PathTravelCosts) or not travel_costs._handles_congestion():
                continue
            graph_inputs = travel_costs.congested_path_graph.inputs
            settings = (
                graph_inputs["congestion_flows_scaling_factor"],
                graph_inputs["target_max_vehicles_per_od_endpoint"],
                graph_inputs["congestion_assignment_max_iterations"],
                graph_inputs["congestion_assignment_max_gap"],
                graph_inputs["congestion_assignment_retained_volume_share"],
            )
            key = (
                travel_costs.modified_path_graph.inputs_hash,
                travel_costs.cch_path_graph.inputs_hash,
                travel_costs.transport_zones.inputs_hash,
                travel_costs.routing_parameters.max_beeline_distance,
                settings,
            )
            groups.setdefault(key, []).append(
                (iteration_costs, travel_costs, travel_costs.inputs_hash in carpool_graphs)
            )

    for key, members in groups.items():
        entries = []
        seen = {}
        for iteration_costs, travel_costs, needs_graph in members:
            flow = iteration_costs.congestion_flows.get()
            if flow is None:
                continue
            variant = travel_costs.asset_for_flow_asset(flow)
            table = variant.congested_costs
            table_key = asset_graph_key(table)
            if table_key in seen:
                if needs_graph:
                    entries[seen[table_key]][2] = variant.congested_path_graph
                continue
            if table_key in resolver.in_memory_values:
                continue
            seen[table_key] = len(entries)
            entries.append([flow, table, variant.congested_path_graph if needs_graph else None])
        if not entries:
            continue

        # Flow assets with the same inputs represent the same assignment.
        # Different input identities stay separate even if their files happen
        # to contain the same values.
        unique_flows = []
        groups_by_identity = {}
        for flow, table, graph in entries:
            flow_key = asset_graph_key(flow)
            match = groups_by_identity.get(flow_key)
            if match is None:
                match = (flow.get(), [], [])
                groups_by_identity[flow_key] = match
                unique_flows.append(match)
            match[1].append(table)
            if graph is not None:
                match[2].append(graph)

        travel_costs = members[0][1]
        flow_paths = [str(path) for path, _, _ in unique_flows]
        manifest_path = resolver.temporary_asset_path(entries[0][1], suffix="-shared-flows.json")
        output_path = resolver.temporary_asset_path(entries[0][1], suffix="-shared-costs.parquet")
        graph_manifest_path = resolver.temporary_asset_path(entries[0][1], suffix="-shared-graphs.json")
        # Route requests depend on the network and zones, not on current flow.
        # Keep them only for this resolver so later cost updates can reuse them.
        route_requests_asset = InMemoryAsset({"road_routing_inputs": key[:4]})
        route_requests_path = resolver.temporary_asset_path(route_requests_asset, suffix="-route-requests.parquet")
        write_temporary_bytes(json.dumps(flow_paths).encode("utf-8"), manifest_path)
        graph_paths = []
        for _, _, graphs in unique_flows:
            if graphs:
                graph_dir = resolver.temporary_asset_path(graphs[0], suffix="-shared-graph")
                graph_path = graph_dir / "congested" / (graphs[0].inputs_hash + "-congested-path-graph")
                graph_path.parent.mkdir(parents=True, exist_ok=True)
                graph_paths.append(str(graph_path))
            else:
                graph_paths.append("")
        write_temporary_bytes(json.dumps(graph_paths).encode("utf-8"), graph_manifest_path)
        travel_costs.transport_zones.get()
        graph_path = travel_costs.modified_path_graph.get()
        cch_path = travel_costs.cch_path_graph.get()
        logging.info(
            "Preparing shared congested path costs for %s scenarios using %s distinct flow tables.",
            len(entries), len(unique_flows),
        )
        worker_args = [
            str(graph_path), str(travel_costs.transport_zones.cache_path), "True",
            str(manifest_path), str(cch_path), *(str(value) for value in key[-1]),
            str(output_path), str(travel_costs.routing_parameters.max_beeline_distance), str(graph_manifest_path),
            str(route_requests_path),
        ]
        if int(os.environ.get("MOBILITY_R_IDLE_TIMEOUT_SECONDS") or "0") > 0:
            # The standard runner owns the optional CPU-idle watchdog.
            RScriptRunner(resources.files("mobility.transport.costs.path").joinpath(
                "prepare_shared_congested_costs.R"
            )).run(worker_args)
        else:
            # Retain one loaded network per execution. Different networks or
            # assignment settings replace it so workers do not accumulate.
            if resolver.shared_road_worker_key != key:
                if resolver.shared_road_worker is not None:
                    resolver.shared_road_worker.close()
                resolver.shared_road_worker = None
                resolver.shared_road_worker_key = None
                resolver.shared_road_worker = SharedRoadWorker(worker_args)
                resolver.shared_road_worker_key = key
            try:
                resolver.shared_road_worker.run(worker_args)
            except BaseException:
                failed_worker = resolver.shared_road_worker
                resolver.shared_road_worker = None
                resolver.shared_road_worker_key = None
                try:
                    failed_worker.close()
                except Exception:
                    logging.exception("Could not close the failed road worker.")
                raise

        # R writes this file directly, so its size can only be checked once
        # the external process returns.
        check_temporary_disk_limit()

        wide = pl.read_parquet(output_path)
        for graph_path in graph_paths:
            if graph_path and not pathlib.Path(graph_path).is_file():
                raise RuntimeError("The congestion calculation did not produce the requested graph.")
        # Publish only after the whole batch was successfully computed. Each
        # view has the same logical key and schema as the ordinary path table.
        views = []
        for index, (_, tables, graphs) in enumerate(unique_flows):
            view = wide.select(
                "from", "to",
                pl.col(f"distance_{index}").alias("distance"),
                pl.col(f"time_{index}").alias("time"),
            ).filter(pl.col("distance").is_not_null() & pl.col("time").is_not_null())
            views.append((tables, graphs, view.to_pandas(), graph_paths[index]))
        for tables, graphs, view, graph_path in views:
            for index, table in enumerate(tables):
                table_key = asset_graph_key(table)
                # Each asset previously owned its own mutable pandas table.
                resolver.in_memory_values[table_key] = view if index == 0 else view.copy(deep=True)
                resolver.in_memory_assets[table_key] = table
            for graph in graphs:
                resolver.congested_graph_paths[asset_graph_key(graph)] = pathlib.Path(graph_path)
        # The worker has finished with the manifests and the result is in memory.
        for path in (manifest_path, graph_manifest_path, output_path):
            try:
                path.unlink(missing_ok=True)
            except OSError:
                logging.warning("Could not remove temporary routing handoff %s; it will be removed at run close.", path)
