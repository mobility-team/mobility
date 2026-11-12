"""Run a cache-only Grand Genève probe without changing the scenario script.

Run from the Mobility Pixi environment. The source project and its input caches
must already exist. This probe refuses to rebuild input assets, so it cannot
start a download; a missing input is reported instead.
"""

import ast
import _thread
import gc
import json
import logging
import os
import shutil
import sys
import tempfile
import threading
from pathlib import Path
from time import perf_counter

import polars as pl
import pandas as pd
import psutil

from mobility.runtime.assets.resolver import AssetResolver, get_current_asset_resolver
from mobility.runtime.r_integration.r_script_runner import RScriptRunner
from mobility.runtime.temporary import managed_temporary_disk_usage
import mobility.trips.group_day_trips.core.multi_scenario_run as coordinator_module
from mobility.trips.group_day_trips.core.multi_scenario_run import MultiScenarioRunAsset
from mobility.trips.group_day_trips.core.performance_trace import _ACTIVE_TRACE, simulation_step, trace_simulation_step
from mobility.trips.group_day_trips.plans.compact_candidate_steps import CompactCandidateSteps
from mobility.trips.group_day_trips.plans.plan_ids import PLAN_KEY_COLS
from mobility.trips.group_day_trips.plans.plan_updater import PlanUpdater
from mobility.trips.group_day_trips.plans.plan_schedule_updater import PlanScheduleUpdater
from mobility.trips.group_day_trips.core.parameters import BehaviorChangePhase, BehaviorChangeScope
from mobility.trips.group_day_trips.results.assets.metric import Metric
from mobility.transport.costs.path.path_travel_costs import PathTravelCostsTable
from mobility.transport.costs.path.path_generalized_cost import PathGeneralizedCost
from mobility.transport.modes.public_transport.public_transport_generalized_cost import PublicTransportGeneralizedCost
from mobility.transport.graphs.congested.congested_path_graph import CongestedPathGraph
from mobility.transport.modes.public_transport.gtfs_builder import GTFSBuilder
from mobility.transport.modes.public_transport.gtfs.gtfs_router import GTFSRouter
from mobility.transport.modes.public_transport.gtfs.gtfs_data import GTFSData
from mobility.transport.modes.public_transport.public_transport_graph import PublicTransportGraph
from mobility.transport.modes.public_transport.intermodal_transport_graph import IntermodalTransportGraph
from mobility.transport.modes.public_transport.public_transport_travel_costs import PublicTransportTravelCosts


project = Path(os.environ.get(
    "MOBILITY_GRAND_GENEVE_PROJECT", Path(__file__).resolve().parents[3] / "mobility-grand-geneve"
))
source_path = project / "run_scenarios.py"
source = source_path.read_text(encoding="utf-8-sig").split(
    "# Declare the scenarios to compare before asking for results."
)[0]


class CurrentStepNames(ast.NodeTransformer):
    """Adapt the older local script without editing the user's file."""

    names = {
        "n_iterations": "n_steps",
        "n_iter_per_cost_update": "n_steps_per_cost_update",
        "cache_iteration_events": "cache_step_events",
        "checkpoint_each_iteration": "checkpoint_each_step",
        "start_iteration": "start_step",
        "n_warmup_iterations": "n_warmup_steps",
        "plan_probability_pruning_min_iteration": "plan_probability_pruning_min_step",
        "by_scenario_and_iteration": "by_scenario_and_step",
    }

    def visit_Attribute(self, node):
        self.generic_visit(node)
        node.attr = self.names.get(node.attr, node.attr)
        return node

    def visit_Call(self, node):
        self.generic_visit(node)
        overrides = {
            "n_steps": "MOBILITY_PROBE_STEPS",
            "k_activity_sequences": "MOBILITY_PROBE_ACTIVITIES",
            "k_mode_sequences": "MOBILITY_PROBE_MODES",
        }
        for keyword in node.keywords:
            keyword.arg = self.names.get(keyword.arg, keyword.arg)
            if keyword.arg in overrides and overrides[keyword.arg] in os.environ:
                keyword.value = ast.Constant(value=int(os.environ[overrides[keyword.arg]]))
            if keyword.arg == "additional_gtfs_files" and os.environ.get("MOBILITY_PROBE_REAL_GTFS") != "1":
                # These generated feeds belong to scenarios outside this probe.
                keyword.value = ast.Constant(value=None)
        name = node.func.id if isinstance(node.func, ast.Name) else None
        if name == "GroupDayTripsDestinationSequenceParameters" and "MOBILITY_PROBE_DESTINATIONS" in os.environ:
            node.keywords = [keyword for keyword in node.keywords if keyword.arg != "k_destination_sequences"]
            node.keywords.append(ast.keyword(arg="k_destination_sequences", value=ast.Constant(
                value=int(os.environ["MOBILITY_PROBE_DESTINATIONS"])
            )))
        if name == "GroupDayTripsParameters" and "MOBILITY_PROBE_BATCHES" in os.environ:
            node.keywords = [keyword for keyword in node.keywords if keyword.arg != "adaptive_search"]
            node.keywords.append(ast.keyword(arg="adaptive_search", value=ast.Dict(
                keys=[ast.Constant(value="max_search_batches")],
                values=[ast.Constant(value=int(os.environ["MOBILITY_PROBE_BATCHES"]))],
            )))
        if isinstance(node.func, ast.Attribute) and node.func.attr == "set_params":
            for keyword in node.keywords:
                if keyword.arg == "logging_level":
                    keyword.value = ast.Constant(value="INFO")
            node.keywords.extend([
                ast.keyword(arg="r_packages", value=ast.Constant(value=False)),
                ast.keyword(arg="track_project_cache", value=ast.Constant(value=False)),
                ast.keyword(arg="temporary_disk_limit_gb", value=ast.Constant(
                    value=float(os.environ["MOBILITY_PROBE_TEMP_GB"])
                    if "MOBILITY_PROBE_TEMP_GB" in os.environ else None
                )),
            ])
        return node


original_rebuild = AssetResolver._rebuild_asset
original_inner_loop = MultiScenarioRunAsset._run_inner_loop
original_advance = MultiScenarioRunAsset._advance_cohort_step
original_expand_evaluation = PlanUpdater.expand_context_evaluation
original_gtfs_hash = GTFSData.file_sha256
gtfs_hash_timings = {}
mode_cost_timings = {}

if os.environ.get("MOBILITY_PROBE_TRACE_SHARED_ROAD") == "1":
    original_record_r_output = RScriptRunner._record_output
    road_output_lock = threading.Lock()

    def record_road_output(runner, message, is_error):
        original_record_r_output(runner, message, is_error)
        if Path(runner.script_path).name != "prepare_shared_congested_costs.R":
            return
        event = {"seconds": perf_counter(), "message": message.strip(), "stderr": is_error}
        road_output_lock.acquire()
        try:
            file = (work / "shared-road-output.jsonl").open("a", encoding="utf-8")
            try:
                file.write(json.dumps(event) + "\n")
            finally:
                file.close()
        finally:
            road_output_lock.release()

    RScriptRunner._record_output = record_road_output

if os.environ.get("MOBILITY_PROBE_CAPTURE_SHARED_ROAD") == "1":
    original_run_r_script = RScriptRunner.run
    road_capture_done = [False]

    def capture_road_inputs(runner, args):
        if Path(runner.script_path).name != "prepare_shared_congested_costs.R" or road_capture_done[0]:
            return original_run_r_script(runner, args)
        flow_paths = json.loads(Path(args[3]).read_text(encoding="utf-8"))
        if len(flow_paths) < 2:
            return original_run_r_script(runner, args)
        captured = work / "road-input"
        captured.mkdir()
        copies = []
        for index, source in enumerate(flow_paths):
            target = captured / f"flow-{index}.parquet"
            shutil.copyfile(source, target)
            copies.append(str(target))
        manifest = captured / "flows.json"
        manifest.write_text(json.dumps(copies), encoding="utf-8")
        captured_args = list(map(str, args))
        captured_args[3] = str(manifest)
        captured_args[10] = str(captured / "output.parquet")
        (captured / "args.json").write_text(json.dumps(captured_args), encoding="utf-8")
        result = original_run_r_script(runner, args)
        shutil.copyfile(args[10], captured / "reference.parquet")
        road_capture_done[0] = True
        print("SHARED_ROAD_INPUT_CAPTURED", captured, flush=True)
        return result

    RScriptRunner.run = capture_road_inputs


def measure_mode_costs(cost_class):
    """Time existing generalized-cost calls without changing their results."""
    original = cost_class.get

    def measured(cost, *args, **kwargs):
        started = perf_counter()
        try:
            return original(cost, *args, **kwargs)
        finally:
            name = cost.inputs.get("mode_name", cost_class.__name__)
            key = (name, bool(kwargs.get("road_flow_asset")))
            count, seconds = mode_cost_timings.get(key, (0, 0.0))
            mode_cost_timings[key] = (count + 1, seconds + perf_counter() - started)

    cost_class.get = measured


if os.environ.get("MOBILITY_PROBE_MEASURE_MODE_COSTS") == "1":
    measure_mode_costs(PathGeneralizedCost)
    measure_mode_costs(PublicTransportGeneralizedCost)


def report_gtfs_hash(path):
    """Time existing validation hashes without hashing any extra data."""
    started = perf_counter()
    try:
        return original_gtfs_hash(path)
    finally:
        name = str(path)
        count, seconds = gtfs_hash_timings.get(name, (0, 0.0))
        gtfs_hash_timings[name] = (count + 1, seconds + perf_counter() - started)


if os.environ.get("MOBILITY_PROBE_MEASURE_HASHES") == "1":
    GTFSData.file_sha256 = staticmethod(report_gtfs_hash)


def expand_without_early_compaction(updater, *args, **kwargs):
    """Reproduce the previous expansion path for an opt-in comparison."""
    previous = CompactCandidateSteps.__dict__["from_frame"]
    CompactCandidateSteps.from_frame = classmethod(lambda cls, frame, **kwargs: frame)
    try:
        return original_expand_evaluation(updater, *args, **kwargs)
    finally:
        CompactCandidateSteps.from_frame = previous


if os.environ.get("MOBILITY_PROBE_REFERENCE_EXPAND") == "1":
    PlanUpdater.expand_context_evaluation = expand_without_early_compaction

selected_expand_evaluation = PlanUpdater.expand_context_evaluation


def report_evaluation_compaction(updater, *args, **kwargs):
    result = selected_expand_evaluation(updater, *args, **kwargs)
    print("EARLY_HISTORY_COMPACT", isinstance(result.candidate_plan_steps, CompactCandidateSteps), flush=True)
    return result


PlanUpdater.expand_context_evaluation = report_evaluation_compaction

if os.environ.get("MOBILITY_PROBE_EXPLAIN_SCORING_STEP"):
    original_update_timings = PlanScheduleUpdater.update_plan_timings
    explained_scoring = set()

    def explain_scoring(updater, *args, **kwargs):
        result = original_update_timings(updater, *args, **kwargs)
        active = _ACTIVE_TRACE.get()
        labels = active[2] if active else {}
        step = labels.get("step")
        if step == int(os.environ["MOBILITY_PROBE_EXPLAIN_SCORING_STEP"]) and step not in explained_scoring:
            (work / f"scoring-query-step-{step}.txt").write_text(
                result.explain(optimized=True), encoding="utf-8"
            )
            explained_scoring.add(step)
            print("SCORING_QUERY_PLAN", step, flush=True)
        return result

    PlanScheduleUpdater.update_plan_timings = explain_scoring


if os.environ.get("MOBILITY_PROBE_CAPTURE_TIMING_STEP"):
    original_timing_update = PlanScheduleUpdater.update_plan_timings
    captured_timing = set()

    def capture_timing_input(updater, possible_plan_steps, **kwargs):
        active = _ACTIVE_TRACE.get()
        step = active[2].get("step") if active else None
        if step == int(os.environ["MOBILITY_PROBE_CAPTURE_TIMING_STEP"]) and step not in captured_timing:
            # This extra collection is diagnostic only. The simulation keeps
            # its original query so the capture does not change its execution.
            possible_plan_steps.collect(engine="streaming").write_parquet(work / "timing-input.parquet")
            (work / "timing-settings.json").write_text(json.dumps(kwargs), encoding="utf-8")
            captured_timing.add(step)
            print("TIMING_INPUT_CAPTURED", step, flush=True)
        return original_timing_update(updater, possible_plan_steps, **kwargs)

    PlanScheduleUpdater.update_plan_timings = capture_timing_input


if os.environ.get("MOBILITY_PROBE_EXPLAIN_EVALUATION_STEP"):
    original_collect = pl.LazyFrame.collect
    explained_collects = set()
    target_evaluation_step = int(os.environ["MOBILITY_PROBE_EXPLAIN_EVALUATION_STEP"])

    def inspect_evaluation_collect(query, *args, **kwargs):
        active = _ACTIVE_TRACE.get()
        caller = sys._getframe(1)
        if (not active or active[2].get("step") != target_evaluation_step
                or Path(caller.f_code.co_filename).name != "plan_updater.py"):
            return original_collect(query, *args, **kwargs)
        line = caller.f_lineno
        function = caller.f_code.co_name
        # Table estimates can include shared buffers; RSS is measured separately.
        tables = {
            name: {"rows": value.height, "columns": value.width, "bytes": value.estimated_size()}
            for name, value in caller.f_locals.items() if isinstance(value, pl.DataFrame)
        }
        del caller
        location = (function, line)
        if location not in explained_collects:
            (work / f"evaluation-query-{function}-{line}.txt").write_text(
                query.explain(optimized=True), encoding="utf-8"
            )
            explained_collects.add(location)
        started = perf_counter()
        rss_before = psutil.Process().memory_info().rss
        with simulation_step("evaluation collect", collect_function=function, collect_line=line):
            result = original_collect(query, *args, **kwargs)
        record = {
            "function": function, "line": line, "labels": active[2],
            "seconds": perf_counter() - started, "rss_before": rss_before,
            "rss_after": psutil.Process().memory_info().rss,
            "output_rows": result.height, "output_columns": result.width,
            "output_bytes": result.estimated_size(), "input_tables_in_scope": tables,
        }
        file = (work / "evaluation-collects.jsonl").open("a", encoding="utf-8")
        try:
            file.write(json.dumps(record) + "\n")
        finally:
            file.close()
        return result

    pl.LazyFrame.collect = inspect_evaluation_collect


def report_candidate_pool(coordinator, *args, **kwargs):
    result = original_inner_loop(coordinator, *args, **kwargs)
    print(
        "CANDIDATE_POOL_STEP",
        kwargs.get("iteration"),
        len(result["pool"].candidate_ids),
        flush=True,
    )
    catalogue = result["pool"].candidate_catalogue
    if catalogue is not None:
        # These are separate table estimates; their buffers can be shared.
        print("CANDIDATE_TABLE_BYTES", kwargs.get("iteration"), {
            name: getattr(catalogue, name).estimated_size()
            for name in ("plans", "steps", "variants", "membership")
        }, flush=True)
    if os.environ.get("MOBILITY_PROBE_COMPARE_EVALUATIONS") == "1":
        evaluations = list(result.get("evaluations", {}).items())
        if len(evaluations) >= 2:
            first_key, first = evaluations[0]
            second_key, second = evaluations[1]
            for name in ("possible_plan_steps", "possible_plan_utility"):
                left = getattr(first, name)
                right = getattr(second, name)
                if isinstance(left, pl.DataFrame) and isinstance(right, pl.DataFrame):
                    keys = PLAN_KEY_COLS + (["seq_step_index"] if "seq_step_index" in left.columns else [])
                    left = left.sort(keys)
                    right = right.sort(keys)
                    equal_columns = [column for column in left.columns
                                     if column in right.columns and left[column].equals(right[column])]
                    comparison = {
                        "step": kwargs.get("iteration"), "table": name,
                        "scenarios": [coordinator.runs[first_key].scenario, coordinator.runs[second_key].scenario],
                        "left_rows": left.height, "right_rows": right.height,
                        "same_schema": left.schema == right.schema,
                        "different_columns": [column for column in left.columns if column not in equal_columns],
                    }
                    print("EVALUATION_COMPARISON", comparison, flush=True)
                    file = (work / "evaluation-comparisons.jsonl").open("a", encoding="utf-8")
                    try:
                        file.write(json.dumps(comparison) + "\n")
                    finally:
                        file.close()
    for run_key, evaluation in result.get("evaluations", {}).items():
        sizes = {
            name: getattr(evaluation, name).estimated_size()
            for name in ("candidate_plan_steps", "possible_plan_steps", "possible_plan_utility", "plan_id_index",
                         "current_plans", "time_sequence_steps", "time_mapping")
            if isinstance(getattr(evaluation, name), (pl.DataFrame, CompactCandidateSteps))
        }
        print("FINAL_EVALUATION_TABLE_BYTES", kwargs.get("iteration"),
              coordinator.runs[run_key].scenario, sizes, flush=True)
    return result


def report_live_state(coordinator, iteration, states, **kwargs):
    assets, next_states, search = original_advance(coordinator, iteration, states, **kwargs)
    for name in ("time_sequence_steps", "current_plans", "current_plan_steps", "plan_id_index",
                 "destination_sequence_index", "mode_sequence_index"):
        tables = [getattr(state, name) for state in next_states.values()
                  if getattr(state, name) is not None]
        print("SHARED_STATE_TABLES", iteration, name,
              len({id(table) for table in tables}), "of", len(tables), flush=True)
    for name in ("steps", "membership", "unit_map"):
        tables = [getattr(state.candidate_plan_steps, name) for state in next_states.values()
                  if isinstance(state.candidate_plan_steps, CompactCandidateSteps)
                  and getattr(state.candidate_plan_steps, name) is not None]
        print("SHARED_CANDIDATE_TABLES", iteration, name,
              len({id(table) for table in tables}), "of", len(tables), flush=True)
    for run_key, state in next_states.items():
        sizes = {
            name: getattr(state, name).estimated_size()
            for name in ("current_plans", "current_plan_steps", "plan_id_index", "time_sequence_steps",
                         "destination_saturation", "costs")
            if isinstance(getattr(state, name), pl.DataFrame)
        }
        candidates = state.candidate_plan_steps
        if isinstance(candidates, CompactCandidateSteps):
            sizes["candidate_steps"] = candidates.steps.estimated_size()
            sizes["candidate_membership"] = candidates.membership.estimated_size()
            if candidates.unit_map is not None:
                sizes["candidate_unit_map"] = candidates.unit_map.estimated_size()
            if os.environ.get("MOBILITY_PROBE_CANDIDATE_AGE") == "1":
                age = coordinator.runs[run_key].parameters.plan_update.max_inactive_age
                warmup = coordinator.runs[run_key].parameters.plan_update.n_warmup_steps
                history = candidates.memberships().collect(engine="streaming")
                latest = pl.max_horizontal(
                    pl.col("first_seen_iteration"), pl.col("last_seen_iteration"),
                    pl.col("last_active_iteration").fill_null(0),
                )
                stale = (pl.lit(iteration) - latest.cast(pl.Int64) > age) if iteration > warmup else pl.lit(False)
                old_members = history.filter(stale)
                old_unoccupied = old_members.join(
                    state.current_plans.select(PLAN_KEY_COLS), on=PLAN_KEY_COLS, how="anti"
                )
                print("CANDIDATE_AGE", iteration, coordinator.runs[run_key].scenario, {
                    "memberships": history.height,
                    "older_than_limit": old_members.height,
                    "older_and_unoccupied": old_unoccupied.height,
                    "max_inactive_age": age,
                    "warmup_steps": warmup,
                }, flush=True)
        elif isinstance(candidates, pl.DataFrame):
            sizes["candidate_plan_steps"] = candidates.estimated_size()
        print("LIVE_STATE_TABLE_BYTES", iteration, coordinator.runs[run_key].scenario, sizes, flush=True)
    if os.environ.get("MOBILITY_PROBE_MEMORY_INVENTORY_AT_STEP") == str(iteration):
        # Estimates can include shared buffers and are not process RSS. This
        # inventory is opt-in because walking Python objects is expensive.
        frames = []
        for obj in gc.get_objects():
            if isinstance(obj, pl.DataFrame):
                frames.append({"kind": "polars", "bytes": obj.estimated_size(),
                               "rows": obj.height, "columns": obj.columns})
            elif isinstance(obj, pd.DataFrame):
                frames.append({"kind": "pandas", "bytes": int(obj.memory_usage(index=True, deep=False).sum()),
                               "rows": len(obj), "columns": list(map(str, obj.columns))})
        resolver = get_current_asset_resolver()
        assets_by_type = {}
        if resolver is not None:
            for key, value in resolver.in_memory_values.items():
                asset = resolver.in_memory_assets.get(key)
                name = type(asset).__name__ if asset is not None else "unknown"
                assets_by_type[name] = assets_by_type.get(name, 0) + 1
        inventory = {"step": iteration, "rss_bytes": psutil.Process().memory_info().rss,
                     "frame_count": len(frames), "largest_frames": sorted(frames, key=lambda x: -x["bytes"])[:40],
                     "resolver_values": assets_by_type}
        (work / f"memory-inventory-step-{iteration}.json").write_text(json.dumps(inventory, indent=2), encoding="utf-8")
        print("MEMORY_INVENTORY", iteration, len(frames), "frames", flush=True)
    return assets, next_states, search


def rebuild_without_downloads(resolver, asset, *args, **kwargs):
    allowed = (Metric, PathTravelCostsTable, CongestedPathGraph)
    if os.environ.get("MOBILITY_PROBE_BUILD_LOCAL_TRANSIT") == "1":
        # The router and graph can be built from cached feeds. Missing GTFSData
        # and source assets remain blocked, so this cannot start a download.
        allowed += (GTFSRouter, PublicTransportGraph, IntermodalTransportGraph, PublicTransportTravelCosts)
    if not isinstance(asset, allowed):
        raise RuntimeError(
            f"Grand Genève probe needs an input cache that is missing or stale: {type(asset).__name__}. "
            "The probe stopped without rebuilding it."
        )
    return original_rebuild(resolver, asset, *args, **kwargs)


AssetResolver._rebuild_asset = rebuild_without_downloads
MultiScenarioRunAsset._run_inner_loop = report_candidate_pool
MultiScenarioRunAsset._advance_cohort_step = report_live_state
coordinator_module.prepare_shared_congested_costs = trace_simulation_step(
    "prepare shared congested costs"
)(coordinator_module.prepare_shared_congested_costs)
work = Path(tempfile.mkdtemp(prefix="mobility-grand-geneve-probe-"))
print("PROBE_FILES", str(work), flush=True)
if os.environ.get("MOBILITY_PROBE_REAL_GTFS") == "1":
    # Keep a stable probe-only path: the router's cache identity includes the
    # feed paths, and a new folder on every run would invalidate its caches.
    # This avoids overwriting existing files in the user's project directory.
    feed_folder = Path(tempfile.gettempdir()) / "mobility-grand-geneve-probe-feeds"
    feed_folder.mkdir(exist_ok=True)
    GTFSBuilder.write_project_zip = lambda builder, file_name: builder.write_zip(feed_folder / file_name)
if os.environ.get("MOBILITY_PROBE_TRACE", "1") == "1":
    os.environ["MOBILITY_SIMULATION_TRACE"] = str(work / "trace.jsonl")
else:
    os.environ.pop("MOBILITY_SIMULATION_TRACE", None)
os.chdir(project)
sys.path.insert(0, str(project))
tree = ast.fix_missing_locations(CurrentStepNames().visit(ast.parse(source)))
namespace = {"__file__": str(source_path), "__name__": "grand_geneve_probe"}
exec(compile(tree, str(source_path), "exec"), namespace)

model = namespace["pop_group_day_trips"]
if "MOBILITY_PROBE_MODE_FROM_STEP" in os.environ:
    model.parameters.behavior_change.phases = [BehaviorChangePhase(
        start_step=int(os.environ["MOBILITY_PROBE_MODE_FROM_STEP"]),
        scope=BehaviorChangeScope.MODE_REPLANNING,
    )]
if os.environ.get("MOBILITY_PROBE_SCORE_ALL") == "1":
    PlanUpdater._reachable_scoring_candidates = staticmethod(
        lambda candidates, origins, schedule_steps, scope, forced_contexts: candidates
    )
elif os.environ.get("MOBILITY_PROBE_REPORT_RESTRICTED") == "1":
    restrict_candidates = PlanUpdater._reachable_scoring_candidates

    def report_restricted(candidates, origins, schedule_steps, scope, forced_contexts):
        selected = restrict_candidates(candidates, origins, schedule_steps, scope, forced_contexts)
        print("RESTRICTED_SCORING_ROWS", candidates.height, selected.height, flush=True)
        return selected

    PlanUpdater._reachable_scoring_candidates = staticmethod(report_restricted)
if os.environ.get("MOBILITY_PROBE_LATE_HISTORY") == "1":
    MultiScenarioRunAsset._compact_completed_candidate_history = staticmethod(
        lambda evaluation, *args, **kwargs: evaluation
    )
if os.environ.get("MOBILITY_PROBE_EXPLAIN_FALLBACK") == "1":
    logging.getLogger("mobility.trips.group_day_trips.plans.shared_candidate_pool").setLevel(logging.DEBUG)
    logging.getLogger("mobility.trips.group_day_trips.plans.scoring_contexts").setLevel(logging.DEBUG)
scenario_names = os.environ.get("MOBILITY_PROBE_SCENARIOS", "default,carbon_tax").split(",")
scenario_names = [name.strip() for name in scenario_names if name.strip()]
replication = int(os.environ.get("MOBILITY_PROBE_REPLICATION", "0"))
print("PROBE_SETTINGS", {
    "scenarios": scenario_names, "replication": replication,
    "steps": model.parameters.run.n_steps,
    "activity_sequences": model.parameters.activity_sequences.k_activity_sequences,
    "destination_sequences": model.parameters.destination_sequences.k_destination_sequences,
    "mode_sequences": model.parameters.mode_sequences.k_mode_sequences,
    "max_batches": model.parameters.adaptive_search.max_search_batches,
    "real_scenario_gtfs": os.environ.get("MOBILITY_PROBE_REAL_GTFS") == "1",
    "build_local_transit": os.environ.get("MOBILITY_PROBE_BUILD_LOCAL_TRANSIT") == "1",
    "temporary_disk_limit_gb": float(os.environ["MOBILITY_PROBE_TEMP_GB"])
    if "MOBILITY_PROBE_TEMP_GB" in os.environ else None,
    "memory_limit_gib": os.environ.get("MOBILITY_PROBE_MEMORY_LIMIT_GIB", "none"),
    "reference_expansion": os.environ.get("MOBILITY_PROBE_REFERENCE_EXPAND") == "1",
    "trace": os.environ.get("MOBILITY_PROBE_TRACE", "1") == "1",
    "measure_existing_gtfs_hashes": os.environ.get("MOBILITY_PROBE_MEASURE_HASHES") == "1",
    "mode_from_step": os.environ.get("MOBILITY_PROBE_MODE_FROM_STEP"),
    "score_all_candidates": os.environ.get("MOBILITY_PROBE_SCORE_ALL") == "1",
    "late_history_compaction": os.environ.get("MOBILITY_PROBE_LATE_HISTORY") == "1",
}, flush=True)
results = model.results("weekday", scenarios=scenario_names, replication=replication)
metric = results.metrics.trip_count(by_variable="mode", steps="all")
metric.cache_path = work / "trip-count.parquet"
metric.hash_path = work / "trip-count.inputs-hash"
diagnostics = results.tables.adaptive_search_diagnostics(
    columns=["scenario", "outer_step", "unique_pool_size"]
)
started = perf_counter()
stop_monitor = threading.Event()
resource_samples = {
    "managed_temporary_peak_bytes": managed_temporary_disk_usage(),
    "peak_rss": 0, "peak_process_tree_rss": 0,
    "process_tree_incomplete": False,
}
memory_setting = os.environ.get("MOBILITY_PROBE_MEMORY_LIMIT_GIB", "none")
memory_limit = None if memory_setting.lower() == "none" else float(memory_setting) * 1024**3


def monitor_resources():
    process = psutil.Process()
    count = 0
    while not stop_monitor.wait(2):
        count += 1
        rss = process.memory_info().rss
        resource_samples["peak_rss"] = max(resource_samples["peak_rss"], rss)
        # Include routing subprocesses, which can be large and are not part of
        # the simulation trace's Python-process RSS. The sum may count shared
        # memory more than once; it is a conservative guard, not unique RAM.
        tree_rss = rss
        try:
            children = process.children(recursive=True)
        except OSError:
            # Windows may be unable to enumerate processes when its paging
            # file is full. Keep sampling the main process in that case.
            children = []
            resource_samples["process_tree_incomplete"] = True
        for child in children:
            try:
                tree_rss += child.memory_info().rss
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
        resource_samples["peak_process_tree_rss"] = max(resource_samples["peak_process_tree_rss"], tree_rss)
        if memory_limit is not None and (tree_rss > memory_limit or psutil.virtual_memory().available < 8 * 1024**3):
            print("STOPPING: the probe reached its memory safety limit.", flush=True)
            _thread.interrupt_main()
            return
        if count % 5 == 0:
            resource_samples["managed_temporary_peak_bytes"] = max(
                resource_samples["managed_temporary_peak_bytes"], managed_temporary_disk_usage()
            )


threading.Thread(target=monitor_resources, daemon=True).start()
try:
    outputs = results.get_all(trips=metric, diagnostics=diagnostics)
finally:
    stop_monitor.set()
    print("PROJECT_MANAGED_TEMPORARY_PEAK_BYTES", resource_samples["managed_temporary_peak_bytes"], flush=True)
    print("SAMPLED_PROCESS_PEAK_BYTES", resource_samples["peak_rss"], flush=True)
    print("SAMPLED_PROCESS_TREE_RSS_PEAK_BYTES", resource_samples["peak_process_tree_rss"], flush=True)
    print("SAMPLED_PROCESS_TREE_INCOMPLETE", resource_samples["process_tree_incomplete"], flush=True)
    if gtfs_hash_timings:
        print("GTFS_HASH_TIMINGS", sorted(
            ((name, count, round(seconds, 3)) for name, (count, seconds) in gtfs_hash_timings.items()),
            key=lambda row: -row[2],
        ), flush=True)
    if mode_cost_timings:
        print("MODE_COST_TIMINGS", sorted(
            ((name, flow, count, round(seconds, 3)) for (name, flow), (count, seconds)
             in mode_cost_timings.items()), key=lambda row: -row[3],
        ), flush=True)
elapsed = perf_counter() - started
print("TRIP_COUNTS", outputs["trips"].to_dicts(), flush=True)
print("CANDIDATE_POOL", outputs["diagnostics"].collect().to_dicts(), flush=True)
outputs["diagnostics"].close()
trace_path = work / "trace.jsonl"
if trace_path.exists():
    spans = [json.loads(line) for line in trace_path.read_text(encoding="utf-8").splitlines()]
    roots = [span for span in spans if span["operation"] == "coordinated run"]
    if roots:
        print("PEAK_PROCESS_RSS_BYTES", max(span["rss_peak_bytes"] for span in roots), flush=True)
print("WALL_SECONDS", round(elapsed, 3), flush=True)
print("PROBE_FILES", str(work), flush=True)
