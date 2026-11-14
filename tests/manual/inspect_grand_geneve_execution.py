"""Run a cache-only Grand Genève probe without changing the scenario script.

Run from the Mobility Pixi environment. The source project and its input caches
must already exist. This probe refuses to rebuild input assets, so it cannot
start a download; a missing input is reported instead.
"""

import ast
import _thread
import gc
import hashlib
import json
import logging
import os
import shutil
import sys
import tempfile
import threading
from dataclasses import replace
from pathlib import Path
from time import perf_counter

import polars as pl
import pandas as pd
import psutil

from mobility.runtime.assets.resolver import AssetResolver, get_current_asset_resolver
from mobility.runtime.scenarios import Scenario, Scenarios
from mobility.runtime.r_integration.r_script_runner import RScriptRunner
from mobility.runtime.temporary import managed_temporary_disk_usage
import mobility.trips.group_day_trips.core.multi_scenario_run as coordinator_module
from mobility.trips.group_day_trips.core.multi_scenario_run import MultiScenarioRunAsset
from mobility.trips.group_day_trips.results.simulation_runner import SimulationRunner
from mobility.trips.group_day_trips.core.performance_trace import (
    SimulationPerformanceTrace, _ACTIVE_TRACE, simulation_step, trace_simulation_step,
)
from mobility.trips.group_day_trips.plans.compact_candidate_steps import CompactCandidateSteps
from mobility.trips.group_day_trips.plans.plan_ids import PLAN_KEY_COLS
from mobility.trips.group_day_trips.plans.plan_updater import PlanUpdater
from mobility.trips.group_day_trips.plans.shared_candidate_pool import SharedCandidatePool
from mobility.trips.group_day_trips.plans.plan_schedule_updater import PlanScheduleUpdater
from mobility.trips.group_day_trips.plans.destination_sequences import DestinationSequences
from mobility.trips.group_day_trips.iterations.iterations import Iterations
import mobility.trips.group_day_trips.plans.mode_sequence_search.mode_sequences as mode_sequences_module
from mobility.trips.group_day_trips.core.parameters import BehaviorChangePhase, BehaviorChangeScope
from mobility.trips.group_day_trips.results.assets.metric import Metric
from mobility.transport.costs.path.path_travel_costs import PathTravelCostsTable
import mobility.transport.costs.path.shared_congested_costs as shared_road_module
from mobility.transport.costs.transport_costs import TransportCosts
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
        "plan_probability_pruning_min_iteration": "plan_pruning_start_step",
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
        if name == "GroupDayTripsOutputParameters" and "MOBILITY_PROBE_CHECKPOINT_DIR" in os.environ:
            node.keywords = [keyword for keyword in node.keywords if keyword.arg != "checkpoint_each_step"]
            node.keywords.append(ast.keyword(arg="checkpoint_each_step", value=ast.Constant(value=True)))
        if (
            isinstance(node.func, ast.Attribute)
            and node.func.attr == "by_scenario_and_step"
            and "MOBILITY_PROBE_SCENARIO_CHANGE_STEP" in os.environ
        ):
            for keyword in node.keywords:
                if keyword.arg == "carbon_tax" and isinstance(keyword.value, ast.Dict):
                    for index, key in enumerate(keyword.value.keys):
                        if isinstance(key, ast.Constant) and key.value == 5:
                            keyword.value.keys[index] = ast.Constant(
                                value=int(os.environ["MOBILITY_PROBE_SCENARIO_CHANGE_STEP"])
                            )
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

if "MOBILITY_PROBE_CHECKPOINT_DIR" in os.environ:
    original_iterations_init = Iterations.__init__

    def isolated_checkpoints(iterations, *, run_inputs_hash, is_weekday, base_folder):
        return original_iterations_init(
            iterations, run_inputs_hash=run_inputs_hash, is_weekday=is_weekday,
            base_folder=Path(os.environ["MOBILITY_PROBE_CHECKPOINT_DIR"]),
        )

    Iterations.__init__ = isolated_checkpoints

if os.environ.get("MOBILITY_PROBE_ONESHOT_ROAD") == "1":
    class OneShotRoadWorker:
        def __init__(self, args):
            pass

        def run(self, args):
            script = Path(shared_road_module.__file__).with_name("prepare_shared_congested_costs.R")
            RScriptRunner(script).run(args)

        def close(self):
            pass

    shared_road_module.SharedRoadWorker = OneShotRoadWorker

if os.environ.get("MOBILITY_PROBE_FORCE_FINAL_COSTS") == "1":
    original_final_requirements = SimulationRunner._final_requirements

    def force_final_costs(requests, last_step):
        _, flows = original_final_requirements(requests, last_step)
        return True, flows

    SimulationRunner._final_requirements = staticmethod(force_final_costs)

if os.environ.get("MOBILITY_PROBE_NO_CATALOGUE_SHARING") == "1":
    original_pool_build = SharedCandidatePool.build

    def build_without_catalogue_sharing(pool):
        result = original_pool_build(pool)
        if result.candidate_catalogue is not None:
            return replace(result, candidate_catalogue=replace(
                result.candidate_catalogue, equivalent_runs=None,
            ))
        return result

    SharedCandidatePool.build = build_without_catalogue_sharing

if (os.environ.get("MOBILITY_PROBE_EVALUATION_GROUPS") == "1"
        or os.environ.get("MOBILITY_PROBE_NO_EVALUATION_SHARING") == "1"):
    original_group_candidate_evaluations = MultiScenarioRunAsset._group_candidate_evaluations

    def inspect_evaluation_groups(coordinator, **kwargs):
        groups = original_group_candidate_evaluations(coordinator, **kwargs)
        print("CANDIDATE_EVALUATION_GROUPS", kwargs["iteration"], [
            [coordinator.runs[run_key].scenario for run_key in group] for group in groups
        ], flush=True)
        if os.environ.get("MOBILITY_PROBE_EVALUATION_INPUTS") == "1":
            pool = kwargs["pool"]
            for key in coordinator.scenarios:
                state = kwargs["states"][key]
                parts = (pool.candidate_step_parts_by_run_key or {}).get(key)
                history = state.candidate_plan_steps
                print("CANDIDATE_EVALUATION_INPUTS", coordinator.runs[key].scenario, {
                    "shared_steps": id(parts[0] if parts else pool.candidate_plan_steps_by_run_key[key]),
                    "active_steps": id(parts[1] if parts else None),
                    "factored_steps": id((pool.scoring_candidate_steps_by_run_key or {}).get(key)),
                    "history": tuple(id(getattr(history, name, None)) for name in ("steps", "membership", "unit_map")),
                    "current_plans": id(state.current_plans),
                    "compact_index": id(kwargs["compact_indices"].get(key)),
                    "contexts": id(kwargs["scoring_setups"][key][0]),
                    "forced_contexts": id(kwargs["forced_contexts"].get(key)),
                }, flush=True)
        if os.environ.get("MOBILITY_PROBE_NO_EVALUATION_SHARING") == "1":
            return [[run_key] for group in groups for run_key in group]
        return groups

    MultiScenarioRunAsset._group_candidate_evaluations = inspect_evaluation_groups

if os.environ.get("MOBILITY_PROBE_POOL_ATTACHMENT_SHARING") == "1":
    original_build_pool_for_attachment = SharedCandidatePool.build

    def inspect_pool_attachments(pool):
        result = original_build_pool_for_attachment(pool)
        groups = {}
        for source in pool.sources:
            factored = (result.scoring_candidate_steps_by_run_key or {}).get(source.run_key)
            if factored is not None:
                groups.setdefault(id(factored), []).append(source.scenario)
        print("POOL_ATTACHMENT_GROUPS", pool.iteration, list(groups.values()), flush=True)
        return result

    SharedCandidatePool.build = inspect_pool_attachments

if os.environ.get("MOBILITY_PROBE_POOL_PROVENANCE") == "1":
    original_build_pool_for_provenance = SharedCandidatePool.build

    def inspect_pool_provenance(pool):
        first = pool.sources[0]
        for source in pool.sources:
            key, first_key = source.run_key, first.run_key
            print("POOL_PROVENANCE", pool.iteration, source.scenario, {
                "previous_state_asset": source.previous_state.inputs_hash == first.previous_state.inputs_hash,
                "destination_asset": source.destination_sequences.inputs_hash == first.destination_sequences.inputs_hash,
                "mode_asset": source.mode_sequences.inputs_hash == first.mode_sequences.inputs_hash,
                "previous_candidates": pool.previous_candidates_by_run_key.get(key) is pool.previous_candidates_by_run_key.get(first_key),
                "destination_index": pool.previous_destination_indices.get(key) is pool.previous_destination_indices.get(first_key),
                "mode_index": pool.previous_mode_indices.get(key) is pool.previous_mode_indices.get(first_key),
                "schedule": pool.previous_time_sequence_steps.get(key) is pool.previous_time_sequence_steps.get(first_key),
            }, flush=True)
        return original_build_pool_for_provenance(pool)

    SharedCandidatePool.build = inspect_pool_provenance

if os.environ.get("MOBILITY_PROBE_POOL_IDENTITY_COST") == "1":
    original_build_pool_for_identity = SharedCandidatePool.build

    def measure_old_pool_identity(pool):
        frames = [
            *pool.previous_time_sequence_steps.values(),
            *pool.previous_candidates_by_run_key.values(),
            *pool.previous_destination_indices.values(),
            *pool.previous_mode_indices.values(),
        ]
        if pool.previous_catalogue is not None:
            frames.extend(getattr(pool.previous_catalogue, name) for name in (
                "plans", "steps", "variants", "membership",
            ))
        tables = []
        for frame in frames:
            if isinstance(frame, CompactCandidateSteps):
                tables.extend((frame.steps, frame.membership, frame.unit_map))
            else:
                tables.append(frame)
        start = perf_counter()
        for table in tables:
            if table is not None:
                digest = hashlib.sha256()
                digest.update(repr(tuple((name, str(dtype)) for name, dtype in table.schema.items())).encode())
                digest.update(table.hash_rows(seed=0).to_numpy().tobytes())
                digest.digest()
        print("OLD_POOL_IDENTITY_COST", pool.iteration,
              "seconds", round(perf_counter() - start, 4),
              "logical_bytes", sum(table.estimated_size() for table in tables if table is not None),
              "tables", sum(table is not None for table in tables), flush=True)
        return original_build_pool_for_identity(pool)

    SharedCandidatePool.build = measure_old_pool_identity

if os.environ.get("MOBILITY_PROBE_SCORING_LOOKUPS") == "1":
    original_build_scoring_lookups = MultiScenarioRunAsset._build_scoring_lookups

    def inspect_scoring_lookups(coordinator, *args, **kwargs):
        lookups = original_build_scoring_lookups(coordinator, *args, **kwargs)
        groups = {}
        for run_key, lookup in lookups.items():
            if lookup is not None:
                groups.setdefault(id(lookup), []).append(coordinator.runs[run_key].scenario)
        print("SCORING_LOOKUP_GROUPS", list(groups.values()), flush=True)
        return lookups

    MultiScenarioRunAsset._build_scoring_lookups = inspect_scoring_lookups

if (os.environ.get("MOBILITY_PROBE_COMPARE_SCENARIO_EVALUATIONS") == "1"
        or os.environ.get("MOBILITY_PROBE_SHARE_EQUAL_EVALUATIONS") == "1"):
    original_evaluate_candidate_pool = PlanUpdater.evaluate_candidate_pool
    evaluation_inputs_by_batch = {}
    evaluation_results_by_batch = {}

    def compare_scenario_inputs(updater, **kwargs):
        active = _ACTIVE_TRACE.get()
        labels = active[2] if active else {}
        key = (labels.get("step"), labels.get("search_batch"))
        factored = kwargs.get("factored_candidate_steps")
        contexts = kwargs.get("scoring_contexts")
        frames = {name: kwargs.get(name) for name in (
            "current_plans", "shared_active_plan_steps", "demand_groups", "survey_plan_steps",
            "destination_saturation", "activity_dur", "home_night_dur", "stay_home_plan",
            "compact_plan_id_index",
        )}
        if factored is not None:
            frames["physical_steps"] = factored.steps.sort(
                [column for column in PLAN_KEY_COLS if column not in ("demand_group_id", "demand_subgroup_id")]
                + ["seq_step_index"]
            )
            frames["membership"] = factored.membership.collect(engine="streaming").sort(
                factored.membership.collect_schema().names()
            )
        if contexts is not None:
            frames["context_object_groups"] = contexts.demand_groups
            frames["context_object_units"] = contexts.unit_map
            frames["context_groups"] = contexts.demand_groups.sort("demand_group_id", "demand_subgroup_id")
            frames["context_units"] = contexts.unit_map.sort("demand_group_id", "demand_subgroup_id")
        signature = (getattr(kwargs["transport_costs"], "inputs_hash", None), frames)
        previous = evaluation_inputs_by_batch.setdefault(key, signature)
        differences = [name for name, frame in frames.items() if not (
            frame is previous[1].get(name) or
            isinstance(frame, pl.DataFrame) and isinstance(previous[1].get(name), pl.DataFrame)
            and frame.equals(previous[1][name])
        )]
        print("SCENARIO_EVALUATION_INPUTS", key, labels.get("scenario"),
              "cost_identity_equal", signature[0] == previous[0], "different_tables", differences,
              "same_objects", [name for name, frame in frames.items() if frame is previous[1].get(name)],
              flush=True)
        if (os.environ.get("MOBILITY_PROBE_SHARE_EQUAL_EVALUATIONS") == "1"
                and key in evaluation_results_by_batch and signature[0] == previous[0]
                and not differences and factored is not None
                and kwargs.get("candidate_plan_steps") is None
                and kwargs.get("shared_active_plan_steps") is None
                and kwargs.get("reachable_scoring_scope") is None
                and kwargs["resolved_activity_parameters"] == previous[2]
                and kwargs["arrival_time_rigidity_by_activity"] == previous[3]
                and kwargs["parameters"].plan_update == previous[4]):
            print("SCENARIO_EVALUATION_REUSED", key, labels.get("scenario"), flush=True)
            return evaluation_results_by_batch[key]
        result = original_evaluate_candidate_pool(updater, **kwargs)
        if key not in evaluation_results_by_batch:
            evaluation_results_by_batch[key] = result
            evaluation_inputs_by_batch[key] = (
                signature[0], frames, kwargs["resolved_activity_parameters"],
                kwargs["arrival_time_rigidity_by_activity"], kwargs["parameters"].plan_update,
            )
        return result

    PlanUpdater.evaluate_candidate_pool = compare_scenario_inputs

if os.environ.get("MOBILITY_PROBE_SCORING_COSTS") == "1":
    original_scoring = PlanUpdater.compute_plan_steps_candidates_utility
    original_profile_costs = TransportCosts.get_profile_costs_by_od_and_mode

    def report_scoring_candidates(updater, **kwargs):
        used = kwargs["candidates"].select("from", "to", "mode").unique().collect(engine="streaming")
        print("SCORING_OD_MODES", used.height, flush=True)
        return original_scoring(updater, **kwargs)

    def report_profile_costs(costs, profiles, metrics, **kwargs):
        result = original_profile_costs(costs, profiles, metrics, **kwargs)
        active = _ACTIVE_TRACE.get()
        labels = active[2] if active else {}
        print("PROFILE_COST_ROWS", labels.get("step"), labels.get("search_batch"),
              labels.get("scenario"), "profiles", len(profiles), "rows", result.height, flush=True)
        return result

    PlanUpdater.compute_plan_steps_candidates_utility = report_scoring_candidates
    TransportCosts.get_profile_costs_by_od_and_mode = report_profile_costs

if os.environ.get("MOBILITY_PROBE_MODE_REUSE_DIAGNOSTIC") == "1":
    original_mode_search = mode_sequences_module.run_rust_mode_sequence_search
    seen_mode_chains = {}

    def report_mode_chains(**kwargs):
        active = _ACTIVE_TRACE.get()
        labels = active[2] if active else {}
        key = (labels.get("step"), labels.get("scenario"))
        chains = kwargs["unique_destination_chains"].select("utility_profile_id", "locations").unique()
        previous = seen_mode_chains.get(key)
        repeated = chains.join(previous, on=["utility_profile_id", "locations"], how="semi").height if previous is not None else 0
        seen_mode_chains[key] = chains if previous is None else pl.concat([previous, chains]).unique()
        print("MODE_CHAIN_REUSE", key, "batch", labels.get("search_batch"),
              "input", kwargs["unique_destination_chains"].height,
              "unique", chains.height, "previously_seen", repeated, flush=True)
        return original_mode_search(**kwargs)

    mode_sequences_module.run_rust_mode_sequence_search = report_mode_chains

if os.environ.get("MOBILITY_PROBE_NO_DEST_PROBABILITY_REUSE") == "1":
    original_destination_init = DestinationSequences.__init__

    def destination_without_probability_reuse(asset, **kwargs):
        original_destination_init(asset, **{**kwargs, "sampling_probabilities": None})

    DestinationSequences.__init__ = destination_without_probability_reuse

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
        graph_manifest = captured / "graphs.json"
        graph_manifest.write_text(Path(args[12]).read_text(encoding="utf-8"), encoding="utf-8")
        captured_args[12] = str(graph_manifest)
        captured_args[-1] = str(captured / "route-requests.parquet")
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
    if os.environ.get("MOBILITY_PROBE_EFFORT_METRICS") == "1":
        metrics_dir = work / "effort-metrics"
        metrics_dir.mkdir(exist_ok=True)
        for run_key, state in next_states.items():
            # These IDs identify modeled plans within this run; they are not
            # compared across runs because their local indices can differ.
            physical_plan = ["activity_seq_id", "time_seq_id", "dest_seq_id", "mode_seq_id"]
            mobile = state.current_plans.filter(pl.col("mode_seq_id") != 0)
            plan_volumes = mobile.group_by(physical_plan).agg(pl.col("n_persons").sum())
            summary = {
                "step": iteration,
                "scenario": coordinator.runs[run_key].scenario,
                "candidate_pool": len(search["pool"].candidate_ids),
                "allocated_unit_plans": mobile.height,
                "mobile_plans": plan_volumes.height,
                "mobile_plans_over_1_person": plan_volumes.filter(pl.col("n_persons") > 1).height,
                "mobile_plans_over_10_persons": plan_volumes.filter(pl.col("n_persons") > 10).height,
                "mobile_persons": mobile["n_persons"].sum(),
                "total_persons": state.current_plans["n_persons"].sum(),
            }
            (metrics_dir / f"step-{iteration}-run-{run_key}.json").write_text(
                json.dumps(summary), encoding="utf-8"
            )
            if os.environ.get("MOBILITY_PROBE_CAPTURE_PRESSURE") == "1":
                state.costs.write_parquet(metrics_dir / f"step-{iteration}-run-{run_key}-costs.parquet")
                state.destination_saturation.write_parquet(
                    metrics_dir / f"step-{iteration}-run-{run_key}-saturation.parquet"
                )
            (
                state.current_plan_steps.filter(pl.col("mode_seq_id") != 0)
                .group_by(["from", "to", "mode", "activity"])
                .agg(pl.col("n_persons").sum().alias("trips"))
                .sort(["from", "to", "mode", "activity"])
                .write_parquet(metrics_dir / f"step-{iteration}-run-{run_key}-flows.parquet")
            )
    if os.environ.get("MOBILITY_PROBE_CAPTURE_STATE") == "1":
        state_dir = work / "state-comparison"
        state_dir.mkdir(exist_ok=True)
        replication = coordinator.runs[next(iter(next_states))].replication
        prefix = f"replication-{replication}-step-{iteration}"
        search["pool"].candidate_ids.write_parquet(state_dir / f"{prefix}-candidate-ids.parquet")
        for run_key, state in next_states.items():
            state.current_plans.sort(PLAN_KEY_COLS).write_parquet(state_dir / f"{prefix}-run-{run_key}-plans.parquet")
            digest = hashlib.sha256(repr(state.rng_state).encode("utf-8")).hexdigest()
            (state_dir / f"{prefix}-run-{run_key}-rng.txt").write_text(digest, encoding="utf-8")
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
    checkpoint_dir = os.environ.get("MOBILITY_PROBE_CHECKPOINT_DIR")
    cache_path = getattr(asset, "cache_path", None)
    if checkpoint_dir and isinstance(cache_path, (str, Path)):
        if Path(cache_path).resolve().is_relative_to(Path(checkpoint_dir).resolve()):
            return original_rebuild(resolver, asset, *args, **kwargs)
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
if os.environ.get("MOBILITY_PROBE_NO_ROAD_CONGESTION") == "1":
    model.parameters.run.n_steps_per_cost_update = 0
if os.environ.get("MOBILITY_PROBE_NO_OPPORTUNITY_CONGESTION") == "1":
    model.parameters.plan_update.enable_opportunity_congestion = False
if "MOBILITY_PROBE_REVISION_PROBABILITY" in os.environ:
    model.parameters.plan_update.transition_revision_probability = float(os.environ["MOBILITY_PROBE_REVISION_PROBABILITY"])
if "MOBILITY_PROBE_DESTINATION_SAMPLING" in os.environ:
    model.parameters.destination_sequences.sampling_method = os.environ["MOBILITY_PROBE_DESTINATION_SAMPLING"]
if "MOBILITY_PROBE_DESTINATION_CUTOFF" in os.environ:
    model.parameters.destination_sequences.dest_prob_cutoff = float(os.environ["MOBILITY_PROBE_DESTINATION_CUTOFF"])
if os.environ.get("MOBILITY_PROBE_DISABLE_TRAVEL_OPTIONS_CACHE") == "1":
    from mobility.trips.group_day_trips.plans.destination_sequences import TravelOptionsCache
    TravelOptionsCache.retain = lambda self, key, table: None
if os.environ.get("MOBILITY_PROBE_TRAVEL_CACHE_IDENTITIES") == "1":
    from mobility.trips.group_day_trips.plans.destination_sequences import TravelOptionsCache
    original_retain = TravelOptionsCache.retain

    def record_travel_cache_identity(self, key, table):
        original_retain(self, key, table)
        print("TRAVEL_CACHE_IDENTITY", json.dumps({
            "run": key[0], "cost": str(key[1]), "profile": str(key[2]),
            "bytes": table.estimated_size(), "retained": key in self.tables,
            "content_digest": hashlib.blake2b(
                table.sort("from", "to").hash_rows(seed=0).to_numpy().tobytes(), digest_size=16,
            ).hexdigest(),
        }), flush=True)

    TravelOptionsCache.retain = record_travel_cache_identity
if os.environ.get("MOBILITY_PROBE_TIMING_FILTER_COUNTS") == "1":
    from mobility.trips.group_day_trips.plans.destination_sequences import DestinationSequences
    original_filter = DestinationSequences._filter_impossible_timing

    def count_timing_filter(choices, rigidity, update_timings):
        filtered = original_filter(choices, rigidity, update_timings)
        before = choices.group_by("activity", "_next_is_direct").len(name="before").collect(engine="streaming")
        after = filtered.group_by("activity", "_next_is_direct").len(name="after").collect(engine="streaming")
        counts = before.join(after, on=["activity", "_next_is_direct"], how="left").with_columns(
            pl.col("after").fill_null(0),
        )
        print("TIMING_FILTER_COUNTS", json.dumps(counts.to_dicts()), flush=True)
        return filtered

    DestinationSequences._filter_impossible_timing = staticmethod(count_timing_filter)
if os.environ.get("MOBILITY_PROBE_TRAVEL_OPTION_COUNTS") == "1":
    from mobility.trips.group_day_trips.plans.destination_sequences import DestinationSequences
    original_travel_options = DestinationSequences._travel_options

    def measure_travel_options(mode_costs):
        started = perf_counter()
        options = original_travel_options(mode_costs)
        print("TRAVEL_OPTION_COUNTS", json.dumps({
            "mode_rows": mode_costs.height, "mode_bytes": mode_costs.estimated_size(),
            "option_rows": options.height, "option_bytes": options.estimated_size(),
            "unknown_time_ods": options["has_unknown_time"].sum() if "has_unknown_time" in options.columns else None,
            "seconds": perf_counter() - started,
        }), flush=True)
        return options

    DestinationSequences._travel_options = staticmethod(measure_travel_options)
if os.environ.get("MOBILITY_PROBE_CAPTURE_TRAVEL_OPTIONS") == "1":
    from mobility.trips.group_day_trips.plans.destination_sequences import DestinationSequences
    original_travel_options = DestinationSequences._travel_options

    def capture_travel_options(mode_costs):
        options = original_travel_options(mode_costs)
        profiles = mode_costs["utility_profile_id"].unique().to_list()
        if len(profiles) != 1:
            raise RuntimeError("Travel-option capture requires one profile at a time.")
        options.write_parquet(work / f"travel-options-profile-{profiles[0]}.parquet")
        return options

    DestinationSequences._travel_options = staticmethod(capture_travel_options)
if os.environ.get("MOBILITY_PROBE_PREVIOUS_LOCATION_COUNTS") == "1":
    from mobility.trips.group_day_trips.plans.destination_sequences import DestinationSequences
    original_sample_destinations = DestinationSequences._sample_sequence_aware_destinations

    def count_previous_locations(**kwargs):
        if kwargs.get("model_previous_arrival"):
            counts = (kwargs["steps_lf"].group_by("_previous_is_first", "_previous_is_anchor")
                      .len().collect(engine="streaming"))
            print("PREVIOUS_LOCATION_COUNTS", json.dumps({
                "anchor_sampling": kwargs.get("anchor_sampling", False), "counts": counts.to_dicts(),
            }), flush=True)
        return original_sample_destinations(**kwargs)

    DestinationSequences._sample_sequence_aware_destinations = staticmethod(count_previous_locations)
if os.environ.get("MOBILITY_PROBE_ANCHOR_COLLECT_TIMES") == "1":
    from mobility.trips.group_day_trips.plans.destination_sequences import DestinationSequences
    original_anchor_sampling = DestinationSequences._spatialize_anchor_activities
    original_collect = pl.LazyFrame.collect
    anchor_profile = {"active": False, "explained": False}

    def timed_anchor_sampling(self, *args, **kwargs):
        anchor_profile["active"] = True
        try:
            return original_anchor_sampling(self, *args, **kwargs)
        finally:
            anchor_profile["active"] = False

    def timed_collect(query, *args, **kwargs):
        if not anchor_profile["active"]:
            return original_collect(query, *args, **kwargs)
        if os.environ.get("MOBILITY_PROBE_EXPLAIN_ANCHOR") == "1" and not anchor_profile["explained"]:
            columns = query.collect_schema().names()
            if "p_ij" in columns and "to" in columns:
                (work / "anchor-weights-plan.txt").write_text(query.explain(optimized=True), encoding="utf-8")
                anchor_profile["explained"] = True
        started = perf_counter()
        result = original_collect(query, *args, **kwargs)
        elapsed = perf_counter() - started
        columns = result.columns
        if "p_ij" in columns and "to" in columns:
            kind = "destination_weights"
        elif any(column.startswith("_race_winner_") for column in columns):
            kind = "draw_winners"
        else:
            kind = "other"
        print("ANCHOR_COLLECT", json.dumps({
            "kind": kind, "seconds": elapsed, "rows": result.height,
            "columns": len(columns), "bytes": result.estimated_size(),
        }), flush=True)
        return result

    DestinationSequences._spatialize_anchor_activities = timed_anchor_sampling
    pl.LazyFrame.collect = timed_collect
if os.environ.get("MOBILITY_PROBE_CACHE_TRAVEL_OPTIONS") == "1":
    from mobility.trips.group_day_trips.plans.destination_sequences import DestinationSequences
    original_travel_options = DestinationSequences._travel_options
    cached_travel_options = {}

    def cache_travel_options(mode_costs):
        active = _ACTIVE_TRACE.get()
        labels = active[2] if active else {}
        profiles = mode_costs["utility_profile_id"].unique().to_list()
        if len(profiles) != 1 or labels.get("scenario") is None or labels.get("step") is None:
            raise RuntimeError("Travel-option probe requires one identified scenario, step and profile.")
        key = (labels["scenario"], labels["step"], profiles[0])
        if key not in cached_travel_options:
            cached_travel_options[key] = original_travel_options(mode_costs)
            print("TRAVEL_OPTION_CACHE", json.dumps({
                "key": key, "entries": len(cached_travel_options),
                "bytes": sum(table.estimated_size() for table in cached_travel_options.values()),
            }), flush=True)
        return cached_travel_options[key]

    DestinationSequences._travel_options = staticmethod(cache_travel_options)
if os.environ.get("MOBILITY_PROBE_EFFORT_METRICS") == "1":
    from effort_diagnostics import install_effort_diagnostics
    install_effort_diagnostics(work / "effort-metrics", model.parameters.plan_update.transition_revision_probability)
if os.environ.get("MOBILITY_PROBE_MNL_BATCHES") == "1":
    from inspect_mnl_batches import install_mnl_batch_probe
    # Retain the normal scheduler but prevent its existing convergence rule
    # from stopping this comparison before the requested batch limit.
    model.parameters.adaptive_search.inner_consecutive_passes = model.parameters.adaptive_search.max_search_batches
    install_mnl_batch_probe(work / "mnl-batches", model.parameters.plan_update.transition_logit_scale)
if os.environ.get("MOBILITY_PROBE_FULL_REPLANNING") == "1":
    model.parameters.behavior_change.phases = None
elif "MOBILITY_PROBE_MODE_FROM_STEP" in os.environ:
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
extra_scenarios = int(os.environ.get("MOBILITY_PROBE_EXTRA_BASELINE_SCENARIOS", "0"))
if extra_scenarios:
    additions = [Scenario(name=f"probe_baseline_{index}") for index in range(1, extra_scenarios + 1)]
    model.scenarios = Scenarios.for_setup(
        Scenarios(list(model.scenarios) + additions), modes=model.modes,
        activities=model.activities, parameters=model.parameters,
    )
    scenario_names.extend(scenario.name for scenario in additions)
replication = int(os.environ.get("MOBILITY_PROBE_REPLICATION", "0"))
replications = (
    [int(value) for value in os.environ["MOBILITY_PROBE_REPLICATIONS"].split(",")]
    if "MOBILITY_PROBE_REPLICATIONS" in os.environ else None
)
print("PROBE_SETTINGS", {
    "scenarios": scenario_names, "replication": replication if replications is None else replications,
    "steps": model.parameters.run.n_steps,
    "activity_sequences": model.parameters.activity_sequences.k_activity_sequences,
    "destination_sequences": model.parameters.destination_sequences.k_destination_sequences,
    "destination_sampling_method": model.parameters.destination_sequences.sampling_method,
    "mode_sequences": model.parameters.mode_sequences.k_mode_sequences,
    "max_batches": model.parameters.adaptive_search.max_search_batches,
    "min_plan_persons": model.parameters.plan_update.min_plan_persons,
    "revision_probability": model.parameters.plan_update.transition_revision_probability,
    "road_congestion": os.environ.get("MOBILITY_PROBE_NO_ROAD_CONGESTION") != "1",
    "opportunity_congestion": os.environ.get("MOBILITY_PROBE_NO_OPPORTUNITY_CONGESTION") != "1",
    "real_scenario_gtfs": os.environ.get("MOBILITY_PROBE_REAL_GTFS") == "1",
    "build_local_transit": os.environ.get("MOBILITY_PROBE_BUILD_LOCAL_TRANSIT") == "1",
    "temporary_disk_limit_gb": float(os.environ["MOBILITY_PROBE_TEMP_GB"])
    if "MOBILITY_PROBE_TEMP_GB" in os.environ else None,
    "memory_limit_gib": os.environ.get("MOBILITY_PROBE_MEMORY_LIMIT_GIB", "none"),
    "reference_expansion": os.environ.get("MOBILITY_PROBE_REFERENCE_EXPAND") == "1",
    "trace": os.environ.get("MOBILITY_PROBE_TRACE", "1") == "1",
    "measure_existing_gtfs_hashes": os.environ.get("MOBILITY_PROBE_MEASURE_HASHES") == "1",
    "mode_from_step": os.environ.get("MOBILITY_PROBE_MODE_FROM_STEP"),
    "full_replanning": os.environ.get("MOBILITY_PROBE_FULL_REPLANNING") == "1",
    "scenario_change_step": os.environ.get("MOBILITY_PROBE_SCENARIO_CHANGE_STEP"),
    "destination_probability_reuse": os.environ.get("MOBILITY_PROBE_NO_DEST_PROBABILITY_REUSE") != "1",
    "destination_probability_cutoff": model.parameters.destination_sequences.dest_prob_cutoff,
    "score_all_candidates": os.environ.get("MOBILITY_PROBE_SCORE_ALL") == "1",
    "late_history_compaction": os.environ.get("MOBILITY_PROBE_LATE_HISTORY") == "1",
}, flush=True)
results = model.results(
    "weekday", scenarios=scenario_names,
    replication=replication if replications is None else None, replications=replications,
)
metric = results.metrics.trip_count(
    by_variable="mode", steps="last" if "MOBILITY_PROBE_CHECKPOINT_DIR" in os.environ else "all"
)
metric.cache_path = work / "trip-count.parquet"
metric.hash_path = work / "trip-count.inputs-hash"
diagnostic_columns = ["scenario", "replication", "outer_step", "unique_pool_size"]
if os.environ.get("MOBILITY_PROBE_EFFORT_METRICS") == "1" or os.environ.get("MOBILITY_PROBE_MNL_BATCHES") == "1":
    diagnostic_columns += ["phase", "search_batch", "flow_change_index", "flow_change_by_mode", "flow_change_by_activity"]
diagnostics = results.tables.adaptive_search_diagnostics(columns=diagnostic_columns)
started = perf_counter()
stop_monitor = threading.Event()
resource_samples = {
    "managed_temporary_peak_bytes": managed_temporary_disk_usage(),
    "peak_rss": 0, "peak_process_tree_rss": 0,
    "process_tree_incomplete": False,
    "logical_cpus": psutil.cpu_count(),
    "samples": [],
    "trace_origins": [],
}

# Match the resource timeline to each trace even when an execution contains
# several independent simulation lanes.
original_trace_init = SimulationPerformanceTrace.__init__


def record_trace_origin(trace, path):
    original_trace_init(trace, path)
    resource_samples["trace_origins"].append({
        "run_id": trace.run_id, "monotonic_seconds": trace.started_at,
    })


SimulationPerformanceTrace.__init__ = record_trace_origin
memory_setting = os.environ.get("MOBILITY_PROBE_MEMORY_LIMIT_GIB", "none")
memory_limit = None if memory_setting.lower() == "none" else float(memory_setting) * 1024**3


def monitor_resources():
    process = psutil.Process()
    processes = {process.pid: process}
    process.cpu_percent(None)
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
        active_ids = {process.pid}
        tree_cpu = process.cpu_percent(None)
        main_cpu = tree_cpu
        for child in children:
            try:
                active_ids.add(child.pid)
                if child.pid not in processes:
                    processes[child.pid] = child
                    child.cpu_percent(None)
                monitored = processes[child.pid]
                tree_rss += monitored.memory_info().rss
                tree_cpu += monitored.cpu_percent(None)
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
        for pid in set(processes) - active_ids:
            del processes[pid]
        resource_samples["peak_process_tree_rss"] = max(resource_samples["peak_process_tree_rss"], tree_rss)
        resource_samples["samples"].append({
            "monotonic_seconds": perf_counter(),
            "python_rss_bytes": rss,
            "process_tree_rss_bytes": tree_rss,
            "python_cpu_percent": main_cpu,
            "process_tree_cpu_percent": tree_cpu,
        })
        if memory_limit is not None and (tree_rss > memory_limit or psutil.virtual_memory().available < 8 * 1024**3):
            print("STOPPING: the probe reached its memory safety limit.", flush=True)
            _thread.interrupt_main()
            return
        if count % 5 == 0:
            resource_samples["managed_temporary_peak_bytes"] = max(
                resource_samples["managed_temporary_peak_bytes"], managed_temporary_disk_usage()
            )


monitor_thread = threading.Thread(target=monitor_resources, daemon=True)
monitor_thread.start()
try:
    requests = {"diagnostics": diagnostics}
    if os.environ.get("MOBILITY_PROBE_DIAGNOSTICS_ONLY") != "1":
        requests["trips"] = metric
    outputs = results.get_all(**requests)
finally:
    stop_monitor.set()
    monitor_thread.join()
    print("PROJECT_MANAGED_TEMPORARY_PEAK_BYTES", resource_samples["managed_temporary_peak_bytes"], flush=True)
    print("SAMPLED_PROCESS_PEAK_BYTES", resource_samples["peak_rss"], flush=True)
    print("SAMPLED_PROCESS_TREE_RSS_PEAK_BYTES", resource_samples["peak_process_tree_rss"], flush=True)
    print("SAMPLED_PROCESS_TREE_INCOMPLETE", resource_samples["process_tree_incomplete"], flush=True)
    (work / "resource-samples.json").write_text(
        json.dumps(resource_samples), encoding="utf-8"
    )
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
if "trips" in outputs:
    print("TRIP_COUNTS", outputs["trips"].to_dicts(), flush=True)
diagnostic_rows = outputs["diagnostics"].collect()
if os.environ.get("MOBILITY_PROBE_EFFORT_METRICS") == "1":
    diagnostic_rows.write_parquet(work / "effort-metrics" / "search-diagnostics.parquet")
if os.environ.get("MOBILITY_PROBE_MNL_BATCHES") == "1":
    diagnostic_rows.write_parquet(work / "mnl-batches" / "search-diagnostics.parquet")
print("CANDIDATE_POOL", diagnostic_rows.select("scenario", "replication", "outer_step", "unique_pool_size").to_dicts(), flush=True)
outputs["diagnostics"].close()
trace_path = work / "trace.jsonl"
if trace_path.exists():
    spans = [json.loads(line) for line in trace_path.read_text(encoding="utf-8").splitlines()]
    roots = [span for span in spans if span["operation"] == "coordinated run"]
    if roots:
        print("PEAK_PROCESS_RSS_BYTES", max(span["rss_peak_bytes"] for span in roots), flush=True)
print("WALL_SECONDS", round(elapsed, 3), flush=True)
print("PROBE_FILES", str(work), flush=True)
