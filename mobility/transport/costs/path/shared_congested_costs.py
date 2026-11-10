"""Prepare compatible scenario road costs in one R execution."""

import json
import logging
from importlib import resources

import polars as pl

from mobility.runtime.assets.graph import asset_graph_key
from mobility.runtime.assets.resolver import get_current_asset_resolver
from mobility.runtime.r_integration.r_script_runner import RScriptRunner
from mobility.runtime.temporary import check_temporary_disk_limit, write_temporary_bytes
from mobility.transport.costs.path.path_travel_costs import PathTravelCosts


def prepare_shared_congested_costs(cost_assets):
    """Prime per-scenario path costs for compatible congestion updates.

    The resolver owns the results for this execution only. Scenarios whose
    road network, routing settings or assignment parameters differ are never
    batched together. Single scenarios retain the ordinary cost path.
    """
    resolver = get_current_asset_resolver()
    if resolver is None:
        return

    groups = {}
    for iteration_costs in cost_assets:
        if not iteration_costs.congestion_flows.should_update_costs:
            continue
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
            groups.setdefault(key, []).append((iteration_costs, travel_costs))

    for key, members in groups.items():
        if len(members) < 2:
            continue
        entries = []
        seen = set()
        for iteration_costs, travel_costs in members:
            flow = iteration_costs.congestion_flows.get()
            if flow is None:
                continue
            table = travel_costs.asset_for_flow_asset(flow).congested_costs
            table_key = asset_graph_key(table)
            if table_key in seen or table_key in resolver.in_memory_values:
                continue
            seen.add(table_key)
            entries.append((flow, table))
        if len(entries) < 2:
            continue

        travel_costs = members[0][1]
        flow_paths = [str(flow.get()) for flow, _ in entries]
        manifest_path = resolver.temporary_asset_path(entries[0][1], suffix="-shared-flows.json")
        output_path = resolver.temporary_asset_path(entries[0][1], suffix="-shared-costs.parquet")
        write_temporary_bytes(json.dumps(flow_paths).encode("utf-8"), manifest_path)
        travel_costs.transport_zones.get()
        graph_path = travel_costs.modified_path_graph.get()
        cch_path = travel_costs.cch_path_graph.get()
        logging.info("Preparing shared congested path costs for %s scenarios.", len(entries))
        RScriptRunner(resources.files("mobility.transport.costs.path").joinpath(
            "prepare_shared_congested_costs.R"
        )).run(args=[
            str(graph_path), str(travel_costs.transport_zones.cache_path), "True",
            str(manifest_path), str(cch_path), *(str(value) for value in key[-1]),
            str(output_path), str(travel_costs.routing_parameters.max_beeline_distance),
        ])

        # R writes this file directly, so its size can only be checked once
        # the external process returns.
        check_temporary_disk_limit()

        wide = pl.read_parquet(output_path)
        # Publish only after the whole batch was successfully computed. Each
        # view has the same logical key and schema as the ordinary path table.
        views = []
        for index, (_, table) in enumerate(entries):
            view = wide.select(
                "from", "to",
                pl.col(f"distance_{index}").alias("distance"),
                pl.col(f"time_{index}").alias("time"),
            ).filter(pl.col("distance").is_not_null() & pl.col("time").is_not_null())
            views.append((table, view.to_pandas()))
        for table, view in views:
            table_key = asset_graph_key(table)
            resolver.in_memory_values[table_key] = view
            resolver.in_memory_assets[table_key] = table
