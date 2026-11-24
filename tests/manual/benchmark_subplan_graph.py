"""Build and evaluate shared plan prefixes from captured modeled candidates.

Usage: pixi run python tests/manual/benchmark_subplan_graph.py TIMING CAPTURE
       HOME_DURATIONS OUTPUT

The input capture already contains expanded modeled steps. Building the graph
from it is a diagnostic, not a claim that production candidate generation can
yet build the same structure directly. Nodes share an exact scored step and
an identical preceding prefix. All plan alternatives remain distinct.
"""
import argparse
import json
import threading
from pathlib import Path
from time import perf_counter

import numpy as np
import polars as pl
import psutil

from mobility.trips.group_day_trips.plans.plan_ids import PLAN_KEY_COLS


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("timing", type=Path)
parser.add_argument("capture", type=Path)
parser.add_argument("home_durations", type=Path)
parser.add_argument("output", type=Path)
parser.add_argument("--constant", type=float, default=1.25)
parser.add_argument("--repetitions", type=int, default=3)
args = parser.parse_args()
args.output.mkdir(parents=True, exist_ok=True)
unit = ["demand_group_id", "demand_subgroup_id"]
step_key = PLAN_KEY_COLS + ["seq_step_index"]
component_columns = ["activity", "from", "to", "mode", "departure_time", "arrival_time", "next_departure_time",
                     "duration_per_pers", "time", "cost", "min_activity_time", "activity_utility_scale", "step_feasible"]
process = psutil.Process()
stop = threading.Event()
rss_peak = [process.memory_info().rss]


def monitor():
    while not stop.wait(0.01):
        rss_peak[0] = max(rss_peak[0], process.memory_info().rss)


thread = threading.Thread(target=monitor, daemon=True)
thread.start()
start = perf_counter()
source = (pl.read_parquet(args.timing / "indexed-2.parquet")
          .unique(subset=step_key, keep="first").sort(step_key)
          .with_columns(step_feasible=(pl.col("is_feasible").fill_null(False)
                       & ((pl.col("next_departure_time") >= pl.col("arrival_time"))
                          | (pl.col("seq_step_index") == pl.col("seq_step_index").max().over(PLAN_KEY_COLS))))))
contexts = source.select(unit).unique()
candidate = (pl.scan_parquet(args.capture / "candidate-utility.parquet")
             .join(contexts.lazy(), on=unit, how="semi").collect(engine="streaming"))
mobile = candidate.filter(pl.col("mode_seq_id") != 0).select(PLAN_KEY_COLS + ["plan_id", "utility", "is_feasible"])
plans = (source.select(PLAN_KEY_COLS).unique(maintain_order=True)
         .join(mobile, on=PLAN_KEY_COLS, how="left", maintain_order="left")
         .with_row_index("graph_plan"))
if plans["plan_id"].null_count() or plans.height != mobile.height:
    raise ValueError("Modeled candidates do not match the plan-utility capture.")
home = pl.read_parquet(args.home_durations).select("country", "csp", "mean_home_night_per_pers")
profiles = pl.read_parquet(args.capture / "contexts.parquet").select(unit + ["country", "csp"])
stay = candidate.filter(pl.col("mode_seq_id") == 0).select(unit + [pl.col("utility").alias("stay_utility")])
plan_parameters = (plans.select(["graph_plan"] + unit).join(profiles, on=unit, maintain_order="left")
                   .join(home, on=["country", "csp"], maintain_order="left")
                   .join(stay, on=unit, maintain_order="left"))
if plan_parameters.height != plans.height or plan_parameters["stay_utility"].null_count():
    raise ValueError("Missing home-night parameters.")
home_min = plan_parameters["mean_home_night_per_pers"].to_numpy() * np.exp(-args.constant)
home_scale = plan_parameters["stay_utility"].to_numpy() / args.constant
components = source.select(component_columns).unique(maintain_order=True).with_row_index("component")
links = (source.select(PLAN_KEY_COLS + ["seq_step_index"] + component_columns)
         .join(plans.select(PLAN_KEY_COLS + ["graph_plan"]), on=PLAN_KEY_COLS, maintain_order="left")
         .join(components, on=component_columns, maintain_order="left")
         .select("graph_plan", "seq_step_index", "component"))
if links.height != source.height or links["component"].null_count():
    raise ValueError("Cannot resolve every captured step to an exact component.")
input_seconds = perf_counter() - start

# Prefix node 0 is the empty plan. A node is uniquely determined by its parent
# prefix and the next exact component; distinct complete plans can share it.
start = perf_counter()
frontier = plans.select("graph_plan").with_columns(parent=pl.lit(0, dtype=pl.UInt32))
leaf = np.zeros(plans.height, dtype=np.uint32)
node_frames = []
depth_ranges = []
next_node = 1
for depth in sorted(links["seq_step_index"].unique().to_list()):
    layer = (links.filter(pl.col("seq_step_index") == depth)
             .join(frontier, on="graph_plan", how="inner", maintain_order="left"))
    if layer.is_empty():
        continue
    distinct = (layer.select("parent", "component").unique(maintain_order=True)
                .with_row_index("node", offset=next_node))
    assigned = layer.join(distinct, on=["parent", "component"], how="left", maintain_order="left")
    leaf[assigned["graph_plan"].to_numpy()] = assigned["node"].to_numpy()
    frontier = assigned.select("graph_plan", pl.col("node").alias("parent"))
    node_frames.append(distinct.select("node", "parent", "component"))
    depth_ranges.append((next_node, next_node + distinct.height))
    next_node += distinct.height
nodes = pl.concat(node_frames).sort("node")
if nodes.height != next_node - 1 or (leaf == 0).any():
    raise ValueError("A plan has no complete prefix path.")
parent = np.r_[np.uint32(0), nodes["parent"].to_numpy()]
component = np.r_[np.uint32(0), nodes["component"].to_numpy()]
if np.any(parent[1:] >= np.arange(1, next_node)):
    raise ValueError("A prefix must precede its child.")
build_seconds = perf_counter() - start

duration = components["duration_per_pers"].to_numpy().astype(np.float64)
travel_time = components["time"].fill_null(0).to_numpy()
cost = components["cost"].to_numpy()
scale = components["activity_utility_scale"].to_numpy()
threshold = components["min_activity_time"].to_numpy()
component_utility = scale * np.maximum(np.log(duration / threshold), 0) - cost
component_feasible = components["step_feasible"].to_numpy()
expected_feasible = plans["is_feasible"].to_numpy()
expected_utility = plans["utility"].to_numpy()
flat_plan = links["graph_plan"].to_numpy()
flat_offsets = np.flatnonzero(np.r_[True, flat_plan[1:] != flat_plan[:-1]])
if len(flat_offsets) != plans.height or not np.array_equal(flat_plan[flat_offsets], np.arange(plans.height)):
    raise ValueError("Captured plan steps are not in contiguous plan order.")
flat_duration = source["duration_per_pers"].to_numpy().astype(np.float64)
flat_travel = source["time"].fill_null(0).to_numpy()
flat_cost = source["cost"].to_numpy()
flat_scale = source["activity_utility_scale"].to_numpy()
flat_threshold = source["min_activity_time"].to_numpy()
flat_feasible = source["step_feasible"].to_numpy()
flat_score_times = []
score_times = []
for repeat in range(args.repetitions):
    start = perf_counter()
    flat_step_utility = flat_scale * np.maximum(np.log(flat_duration / flat_threshold), 0) - flat_cost
    flat_utility = np.add.reduceat(flat_step_utility, flat_offsets)
    flat_home = np.maximum(24 - np.add.reduceat(flat_duration, flat_offsets)
                           - np.add.reduceat(flat_travel, flat_offsets), 0)
    flat_utility += home_scale * np.maximum(np.log(np.maximum(flat_home, np.finfo(np.float64).tiny) / home_min), 0)
    flat_plan_feasible = np.logical_and.reduceat(flat_feasible, flat_offsets)
    flat_score_times.append(perf_counter() - start)
    start = perf_counter()
    node_utility = np.zeros(next_node, dtype=np.float64)
    node_duration = np.zeros(next_node, dtype=np.float64)
    node_travel = np.zeros(next_node, dtype=np.float64)
    node_feasible = np.ones(next_node, dtype=bool)
    for first, last in depth_ranges:
        node_utility[first:last] = node_utility[parent[first:last]] + component_utility[component[first:last]]
        node_duration[first:last] = node_duration[parent[first:last]] + duration[component[first:last]]
        node_travel[first:last] = node_travel[parent[first:last]] + travel_time[component[first:last]]
        node_feasible[first:last] = node_feasible[parent[first:last]] & component_feasible[component[first:last]]
    home_hours = np.maximum(24 - node_duration[leaf] - node_travel[leaf], 0)
    utilities = node_utility[leaf] + home_scale * np.maximum(
        np.log(np.maximum(home_hours, np.finfo(np.float64).tiny) / home_min), 0
    )
    score_times.append(perf_counter() - start)
    if not np.array_equal(node_feasible[leaf], expected_feasible) or not np.array_equal(flat_plan_feasible, expected_feasible):
        raise ValueError("Graph and captured feasibility differ.")
    finite = expected_feasible & np.isfinite(expected_utility) & np.isfinite(utilities)
    error = np.abs(utilities[finite] - expected_utility[finite])
    if error.size and error.max() > 1e-8:
        raise ValueError(f"Graph and captured plan utility differ by {error.max()}.")
    if not np.allclose(flat_utility[finite], utilities[finite], rtol=1e-12, atol=1e-10):
        raise ValueError("Graph and direct array plan utilities differ.")

unit_map = pl.read_parquet(args.capture / "unit-map.parquet")
allocated = (pl.read_parquet(args.capture / "allocated-plans.parquet").filter(pl.col("mode_seq_id") != 0)
             .drop("plan_id").join(unit_map, on=unit, how="left")
             .drop(unit).with_columns(demand_group_id=pl.col("_score_context_id"),
                                      demand_subgroup_id=pl.lit(0, dtype=pl.UInt32))
             .join(contexts, on=unit, how="semi"))
if allocated.join(plans, on=PLAN_KEY_COLS, how="anti").height:
    raise ValueError("An allocated plan is absent from the graph.")
allocated = allocated.join(plans.select(PLAN_KEY_COLS + ["graph_plan"]), on=PLAN_KEY_COLS)
allocation_by_plan = np.bincount(allocated["graph_plan"].to_numpy(),
                                 weights=allocated["n_persons"].to_numpy(), minlength=plans.height)
flow_times = []
active_node_count = 0
for repeat in range(args.repetitions):
    start = perf_counter()
    node_volume = np.bincount(leaf, weights=allocation_by_plan, minlength=next_node)
    for first, last in reversed(depth_ranges):
        active = np.flatnonzero(node_volume[first:last]) + first
        np.add.at(node_volume, parent[active], node_volume[active])
    active = np.flatnonzero(node_volume[1:]) + 1
    active_node_count = len(active)
    component_volume = np.bincount(component[active], weights=node_volume[active], minlength=components.height)
    component_flows = components.with_columns(pl.Series("persons", component_volume)).filter(pl.col("persons") != 0)
    od = (component_flows.group_by("from", "to", "mode")
          .agg(pl.col("persons").sum().alias("volume")).sort("from", "to", "mode"))
    occupation = (component_flows.filter(pl.col("activity") != "home")
                  .group_by("to", "activity")
                  .agg((pl.col("persons") * pl.col("duration_per_pers").cast(pl.Float64))
                       .sum().alias("occupation")).sort("to", "activity"))
    flow_times.append(perf_counter() - start)
od.write_parquet(args.output / "od.parquet")
occupation.write_parquet(args.output / "occupation.parquet")

# Count shared physical prefixes independently from scoring factors. These
# cannot directly reuse utility across profiles, but could share structure in
# an implementation that keeps timing, costs and utility parameters separate.
start = perf_counter()
physical_columns = ["activity", "from", "to", "mode", "departure_time", "arrival_time",
                    "next_departure_time", "duration_per_pers", "time", "step_feasible"]
physical = source.select(physical_columns).unique(maintain_order=True).with_row_index("physical_component")
physical_links = (source.select(PLAN_KEY_COLS + ["seq_step_index"] + physical_columns)
                  .join(plans.select(PLAN_KEY_COLS + ["graph_plan"]), on=PLAN_KEY_COLS, maintain_order="left")
                  .join(physical, on=physical_columns, maintain_order="left")
                  .select("graph_plan", "seq_step_index", "physical_component"))
physical_frontier = plans.select("graph_plan").with_columns(parent=pl.lit(0, dtype=pl.UInt32))
physical_node_count = 0
physical_frames = []
physical_step_frames = []
physical_leaf = np.zeros(plans.height, dtype=np.uint32)
for depth in sorted(physical_links["seq_step_index"].unique().to_list()):
    layer = (physical_links.filter(pl.col("seq_step_index") == depth)
             .join(physical_frontier, on="graph_plan", how="inner", maintain_order="left"))
    distinct = (layer.select("parent", "physical_component").unique(maintain_order=True)
                .with_row_index("node", offset=physical_node_count + 1))
    assigned = layer.join(distinct, on=["parent", "physical_component"], maintain_order="left")
    physical_leaf[assigned["graph_plan"].to_numpy()] = assigned["node"].to_numpy()
    physical_step_frames.append(assigned.select("graph_plan", "seq_step_index", "node"))
    physical_frames.append(distinct.select("node", "parent", "physical_component")
                           .with_columns(depth=pl.lit(depth, dtype=pl.UInt8)))
    physical_frontier = assigned.select("graph_plan", pl.col("node").alias("parent"))
    physical_node_count += distinct.height
physical_census_seconds = perf_counter() - start

# Reuse physical prefixes across scoring contexts while keeping utility values
# distinct by country, socioeconomic category and transport utility profile.
# Validate this key against every captured input rather than assuming it works.
start = perf_counter()
physical_nodes = pl.concat(physical_frames).sort("node")
physical_steps = pl.concat(physical_step_frames)
score_profile = ["country", "csp", "utility_profile_id"]
profile_by_context = (pl.read_parquet(args.capture / "contexts.parquet")
                      .select(unit + score_profile))
score_values = ["cost", "min_activity_time", "activity_utility_scale"]
physical_scores = (source.select(PLAN_KEY_COLS + ["seq_step_index"] + score_values)
                   .join(plans.select(PLAN_KEY_COLS + ["graph_plan"]), on=PLAN_KEY_COLS)
                   .join(physical_steps, on=["graph_plan", "seq_step_index"])
                   .join(profile_by_context, on=unit)
                   .select(["node"] + score_profile + score_values).unique())
score_key = ["node"] + score_profile
if physical_scores.select(score_key).n_unique() != physical_scores.height:
    raise ValueError("The physical-prefix scoring profile has conflicting utility inputs.")
physical_scores = (physical_scores.join(physical_nodes, on="node")
                   .join(physical.select("physical_component", "duration_per_pers", "time", "step_feasible"),
                         on="physical_component")
                   .sort(["node"] + score_profile).with_row_index("score_node", offset=1))
parent_lookup = physical_scores.select([pl.col("node").alias("parent")] + score_profile
                                       + [pl.col("score_node").alias("score_parent")])
physical_scores = (physical_scores.join(parent_lookup, on=["parent"] + score_profile, how="left",
                                         maintain_order="left")
                   .with_columns(pl.col("score_parent").fill_null(0)))
if physical_scores.filter((pl.col("parent") != 0) & (pl.col("score_parent") == 0)).height:
    raise ValueError("A profile-specific prefix is missing its parent score.")
physical_leaf_plan = (plans.select(["graph_plan"] + unit)
                      .with_columns(pl.Series("node", physical_leaf))
                      .join(profile_by_context, on=unit, maintain_order="left")
                      .join(physical_scores.select(score_key + ["score_node"]), on=score_key, how="left",
                            maintain_order="left"))
if physical_leaf_plan.height != plans.height or physical_leaf_plan["score_node"].null_count():
    raise ValueError("A complete plan has no profile-specific prefix score.")
profile_leaf = physical_leaf_plan["score_node"].to_numpy()
profile_parent = np.r_[np.uint32(0), physical_scores["score_parent"].to_numpy()]
profile_duration = physical_scores["duration_per_pers"].to_numpy().astype(np.float64)
profile_travel = physical_scores["time"].fill_null(0).to_numpy()
profile_component_utility = (physical_scores["activity_utility_scale"].to_numpy()
                             * np.maximum(np.log(profile_duration / physical_scores["min_activity_time"].to_numpy()), 0)
                             - physical_scores["cost"].to_numpy())
profile_feasible = physical_scores["step_feasible"].to_numpy()
profile_depth = physical_scores["depth"].to_numpy()
profile_ranges = []
for depth in np.unique(profile_depth):
    positions = np.flatnonzero(profile_depth == depth)
    if len(positions) and positions[-1] - positions[0] + 1 != len(positions):
        raise ValueError("Profile scores must be ordered by prefix depth.")
    profile_ranges.append((int(positions[0]) + 1, int(positions[-1]) + 2))
profile_build_seconds = perf_counter() - start
profile_score_times = []
for repeat in range(args.repetitions):
    start = perf_counter()
    size = physical_scores.height + 1
    acc_u, acc_d, acc_t = (np.zeros(size, dtype=np.float64) for _ in range(3))
    acc_feasible = np.ones(size, dtype=bool)
    for first, last in profile_ranges:
        parent_ids = profile_parent[first:last]
        acc_u[first:last] = acc_u[parent_ids] + profile_component_utility[first - 1:last - 1]
        acc_d[first:last] = acc_d[parent_ids] + profile_duration[first - 1:last - 1]
        acc_t[first:last] = acc_t[parent_ids] + profile_travel[first - 1:last - 1]
        acc_feasible[first:last] = acc_feasible[parent_ids] & profile_feasible[first - 1:last - 1]
    night = np.maximum(24 - acc_d[profile_leaf] - acc_t[profile_leaf], 0)
    profile_utility = acc_u[profile_leaf] + home_scale * np.maximum(
        np.log(np.maximum(night, np.finfo(np.float64).tiny) / home_min), 0)
    profile_score_times.append(perf_counter() - start)
    if (not np.array_equal(acc_feasible[profile_leaf], expected_feasible)
            or not np.allclose(profile_utility[finite], expected_utility[finite], rtol=1e-12, atol=1e-8)):
        raise ValueError("Profile-specific physical prefix scoring does not match captured utilities.")

profile_score_count = physical_scores.height
profile_score_bytes = physical_scores.estimated_size()
del physical_scores, parent_lookup, profile_parent, profile_component_utility

# Score one profile at a time on the same shared physical graph. This keeps
# profile-specific scores transient. The captured source is still expanded;
# measuring a generator that creates these inputs directly is future work.
start = perf_counter()
profiled_plans = plans.select(["graph_plan"] + unit).join(profile_by_context, on=unit, maintain_order="left")
physical_parent = np.r_[np.uint32(0), physical_nodes["parent"].to_numpy()]
physical_node_component = np.r_[np.uint32(0), physical_nodes["physical_component"].to_numpy()]
physical_depth = np.r_[np.uint8(0), physical_nodes["depth"].to_numpy()]
physical_duration = physical["duration_per_pers"].to_numpy().astype(np.float64)
physical_travel = physical["time"].fill_null(0).to_numpy()
physical_valid = physical["step_feasible"].to_numpy()
streamed_utility = np.zeros(plans.height, dtype=np.float64)
streamed_feasible = np.zeros(plans.height, dtype=bool)
largest_profile_nodes = largest_profile_components = largest_profile_rows = profile_groups = 0
largest_profile_working_bytes = 0
for profile_group in profiled_plans.partition_by(score_profile, maintain_order=True):
    profile_groups += 1
    plan_ids = profile_group["graph_plan"].to_numpy()
    relevant_contexts = profile_group.select(unit).unique()
    relevant = (source.join(relevant_contexts, on=unit, how="semi")
                .select(physical_columns + score_values)
                .join(physical, on=physical_columns)
                .select(["physical_component"] + score_values).unique())
    if relevant["physical_component"].n_unique() != relevant.height:
        raise ValueError("A profile has conflicting scores for a physical component.")
    relevant_ids = relevant["physical_component"].to_numpy()
    # Dense arrays are temporary for this profile and indexed by shared IDs.
    values_cost = np.full(physical.height, np.nan)
    values_scale = np.full(physical.height, np.nan)
    values_min = np.full(physical.height, np.nan)
    values_cost[relevant_ids] = relevant["cost"].to_numpy()
    values_scale[relevant_ids] = relevant["activity_utility_scale"].to_numpy()
    values_min[relevant_ids] = relevant["min_activity_time"].to_numpy()
    needed = np.unique(physical_leaf[plan_ids])
    while True:
        more = np.unique(np.r_[needed, physical_parent[needed]])
        if len(more) == len(needed):
            break
        needed = more
    needed = needed[needed != 0]
    needed_components = physical_node_component[needed]
    if np.isnan(values_cost[needed_components]).any():
        raise ValueError("A required physical component has no score for its profile.")
    acc_u = np.zeros(physical_node_count + 1, dtype=np.float64)
    acc_d = np.zeros(physical_node_count + 1, dtype=np.float64)
    acc_t = np.zeros(physical_node_count + 1, dtype=np.float64)
    acc_ok = np.ones(physical_node_count + 1, dtype=bool)
    for depth in np.unique(physical_depth[needed]):
        ids = needed[physical_depth[needed] == depth]
        component_ids = physical_node_component[ids]
        parent_ids = physical_parent[ids]
        term = (values_scale[component_ids]
                * np.maximum(np.log(physical_duration[component_ids] / values_min[component_ids]), 0)
                - values_cost[component_ids])
        acc_u[ids] = acc_u[parent_ids] + term
        acc_d[ids] = acc_d[parent_ids] + physical_duration[component_ids]
        acc_t[ids] = acc_t[parent_ids] + physical_travel[component_ids]
        acc_ok[ids] = acc_ok[parent_ids] & physical_valid[component_ids]
    leaves = physical_leaf[plan_ids]
    night = np.maximum(24 - acc_d[leaves] - acc_t[leaves], 0)
    streamed_utility[plan_ids] = acc_u[leaves] + home_scale[plan_ids] * np.maximum(
        np.log(np.maximum(night, np.finfo(np.float64).tiny) / home_min[plan_ids]), 0)
    streamed_feasible[plan_ids] = acc_ok[leaves]
    largest_profile_nodes = max(largest_profile_nodes, len(needed))
    largest_profile_components = max(largest_profile_components, relevant.height)
    largest_profile_rows = max(largest_profile_rows, profile_group.height)
    largest_profile_working_bytes = max(
        largest_profile_working_bytes,
        relevant.estimated_size() + sum(array.nbytes for array in
            (values_cost, values_scale, values_min, needed, acc_u, acc_d, acc_t, acc_ok)),
    )
streamed_seconds = perf_counter() - start
if (not np.array_equal(streamed_feasible, expected_feasible)
        or not np.allclose(streamed_utility[finite], expected_utility[finite], rtol=1e-12, atol=1e-8)):
    raise ValueError("Profile-at-a-time scoring does not match captured utilities.")
stop.set()
thread.join()
print("SUBPLAN_GRAPH", json.dumps({"contexts": contexts.height, "plans": plans.height,
      "step_occurrences": source.height, "components": components.height, "prefix_nodes": nodes.height,
      "depth_ranges": [last - first for first, last in depth_ranges],
      "allocated_unit_plans": allocated.height, "input_preparation_seconds": input_seconds,
      "prefix_build_seconds": build_seconds, "flat_array_score_seconds": flat_score_times,
      "score_seconds": score_times, "flow_seconds": flow_times, "allocated_prefix_nodes": active_node_count,
      "max_feasible_utility_error": float(error.max()) if error.size else 0,
      "node_and_leaf_array_bytes": parent.nbytes + component.nbytes + leaf.nbytes,
      "component_table_bytes": components.estimated_size(),
      "expanded_component_columns_bytes": source.select(component_columns).estimated_size(),
      "physical_components": physical.height, "physical_prefix_nodes": physical_node_count,
      "physical_census_seconds": physical_census_seconds,
      "profile_prefix_scores": profile_score_count, "profile_score_build_seconds": profile_build_seconds,
      "profile_score_seconds": profile_score_times, "profile_score_table_bytes": profile_score_bytes,
      "streamed_profile_groups": profile_groups, "streamed_max_prefix_nodes": largest_profile_nodes,
      "streamed_max_components": largest_profile_components, "streamed_max_plans": largest_profile_rows,
      "streamed_largest_working_arrays_bytes_lower_bound": largest_profile_working_bytes,
      "streamed_profile_seconds": streamed_seconds,
      "peak_process_rss_bytes": rss_peak[0]}), flush=True)
