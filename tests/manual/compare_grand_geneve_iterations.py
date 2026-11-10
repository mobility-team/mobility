"""Record a two-step Grand Geneve run for comparison across versions.

Run in each checkout with its package first on PYTHONPATH. Inputs must already
be cached; only the cost calculations needed by the run may be rebuilt. The
memory guard can be disabled with MOBILITY_COMPARE_MEMORY_LIMIT_GIB=none.
"""

import _thread
import ast
import hashlib
import inspect
import json
import os
import sys
import tempfile
import threading
from pathlib import Path
from time import perf_counter

import mobility
import psutil
import polars as pl

print("MOBILITY_SOURCE", mobility.__file__, flush=True)

from mobility.runtime.assets.resolver import AssetResolver
from mobility.transport.costs.path.path_travel_costs import PathTravelCostsTable
from mobility.transport.graphs.congested.congested_path_graph import CongestedPathGraph
from mobility.trips.group_day_trips.core.multi_scenario_run import MultiScenarioRunAsset
from mobility.trips.group_day_trips.iterations.iteration_assets import InitialIterationStateAsset, IterationStateAsset
from mobility.trips.group_day_trips.plans.plan_ids import PLAN_KEY_COLS
from mobility.trips.group_day_trips.plans.plan_updater import PlanUpdater
from mobility.trips.group_day_trips.plans.compact_candidate_steps import CompactCandidateSteps
from mobility.trips.group_day_trips.plans.shared_candidate_pool import SharedCandidatePoolAsset
from mobility.trips.group_day_trips.core.multi_scenario import CANDIDATE_ID_COLS
from mobility.trips.group_day_trips.plans.candidate_plan_steps import CandidatePlanStepsAsset
from mobility.trips.group_day_trips.plans.destination_sequences import DestinationSequences
from mobility.trips.group_day_trips.plans.mode_sequence_search import mode_sequences as mode_module
from mobility.trips.group_day_trips.results.assets.metric import Metric


project = Path(os.environ.get(
    "MOBILITY_GRAND_GENEVE_PROJECT", Path(__file__).resolve().parents[3] / "mobility-grand-geneve"
))
source_path = project / "run_scenarios.py"
source = source_path.read_text(encoding="utf-8-sig").split(
    "# Declare the scenarios to compare before asking for results."
)[0]


class CurrentStepNames(ast.NodeTransformer):
    """Adapt the local scenario script and set a common two-step horizon."""

    names = {
        "n_iterations": "n_steps", "n_iter_per_cost_update": "n_steps_per_cost_update",
        "cache_iteration_events": "cache_step_events", "checkpoint_each_iteration": "checkpoint_each_step",
        "start_iteration": "start_step", "n_warmup_iterations": "n_warmup_steps",
        "plan_probability_pruning_min_iteration": "plan_probability_pruning_min_step",
        "by_scenario_and_iteration": "by_scenario_and_step",
    }

    def visit_Attribute(self, node):
        self.generic_visit(node)
        node.attr = self.names.get(node.attr, node.attr)
        return node

    def visit_Call(self, node):
        self.generic_visit(node)
        for keyword in node.keywords:
            keyword.arg = self.names.get(keyword.arg, keyword.arg)
            if keyword.arg == "n_steps":
                keyword.value = ast.Constant(value=2)
            elif keyword.arg == "additional_gtfs_files":
                keyword.value = ast.Constant(value=None)
        if isinstance(node.func, ast.Attribute) and node.func.attr == "set_params":
            for keyword in node.keywords:
                if keyword.arg == "logging_level":
                    keyword.value = ast.Constant(value="INFO")
            options = {"r_packages": False, "track_project_cache": False, "temporary_disk_limit_gb": 5}
            supported = inspect.signature(mobility.set_params).parameters
            node.keywords.extend(
                ast.keyword(arg=name, value=ast.Constant(value=value))
                for name, value in options.items() if name in supported
            )
        return node


original_rebuild = AssetResolver._rebuild_asset
original_inner_loop = MultiScenarioRunAsset._run_inner_loop
original_state = IterationStateAsset.get
original_initial_state = InitialIterationStateAsset.get
original_pool = SharedCandidatePoolAsset.create_and_get_asset
original_merge = SharedCandidatePoolAsset._merge_candidate_content
original_destination_sequences = DestinationSequences.create_and_get_asset
original_mode_sequences = mode_module.ModeSequences.create_and_get_asset
original_search_inputs = mode_module.build_search_inputs
reported_states = set()
batch_counts = {}
capture = {"step": None, "batch": None, "scenario": None}
stop_after = int(os.environ.get("MOBILITY_COMPARE_STOP_AFTER_STEP", "2"))
stop_after_pool = int(os.environ.get("MOBILITY_COMPARE_STOP_AFTER_POOL", "0"))
stop_after_batch = int(os.environ.get("MOBILITY_COMPARE_STOP_AFTER_BATCH", "0"))


class ComparisonComplete(Exception):
    """Stop before starting an unrequested step in this opt-in diagnostic."""


def capture_pruning_inputs(updater, transitions, *, retained_share):
    """Save selected groups' transition mass before pruning, then stop."""
    groups = [int(value) for value in os.environ["MOBILITY_COMPARE_PRUNING_GROUPS"].split(",")]
    transitions.filter(pl.col("demand_group_id").is_in(groups)).collect(engine="streaming").write_parquet(
        work / "pruning-inputs.parquet"
    )
    print("PRUNING_INPUTS_CAPTURED", groups, "retained_share", retained_share, flush=True)
    raise ComparisonComplete()


if os.environ.get("MOBILITY_COMPARE_PRUNING_GROUPS"):
    PlanUpdater.build_low_probability_target_map = capture_pruning_inputs


def report_initial_state(asset):
    state = original_initial_state(asset)
    for name in ("activity_dur", "home_night_dur", "opportunities", "costs"):
        getattr(state, name).write_parquet(work / f"initial-{name}.parquet")
    print("INITIAL_STATE_CAPTURED", flush=True)
    raise ComparisonComplete()


def rebuild_without_downloads(resolver, asset, *args, **kwargs):
    if not isinstance(asset, (Metric, PathTravelCostsTable, CongestedPathGraph)):
        raise RuntimeError(f"Missing or stale input cache: {type(asset).__name__}")
    return original_rebuild(resolver, asset, *args, **kwargs)


def report_candidate_pool(coordinator, *args, **kwargs):
    if kwargs["iteration"] > stop_after:
        raise ComparisonComplete()
    result = original_inner_loop(coordinator, *args, **kwargs)
    candidates = sorted(result["pool"].candidate_ids)
    digest = hashlib.sha256(json.dumps(candidates, separators=(",", ":")).encode()).hexdigest()
    print("COMPARE", json.dumps({
        "kind": "candidates", "step": kwargs["iteration"],
        "count": len(candidates), "sha256": digest,
    }), flush=True)
    if kwargs["iteration"] == stop_after_pool:
        print("STOPPED_AFTER_CANDIDATE_SEARCH", stop_after_pool, flush=True)
        raise ComparisonComplete()
    return result


def report_candidate_batch(asset):
    batch = batch_counts.get(asset.iteration, 0) + 1
    batch_counts[asset.iteration] = batch
    capture.update(step=asset.iteration, batch=batch)
    if os.environ.get("MOBILITY_COMPARE_GENERATION_INPUTS") == "1" and asset.iteration == 2 and batch == 1:
        for source in asset.sources:
            requests = source.mode_sequences.generation_requests
            if requests is not None:
                requests.write_parquet(work / f"step-2-{source.scenario}-generation-requests.parquet")
    result = original_pool(asset)
    if asset.iteration == 2:
        candidates = sorted(result.candidate_ids)
        digest = hashlib.sha256(json.dumps(candidates, separators=(",", ":")).encode()).hexdigest()
        print("COMPARE_BATCH", json.dumps({
            "step": asset.iteration, "batch": batch, "count": len(candidates), "sha256": digest,
        }), flush=True)
        if os.environ.get("MOBILITY_COMPARE_SAVE_BATCHES") == "1":
            pl.DataFrame(candidates, schema=[("candidate_id_1", pl.UInt64), ("candidate_id_2", pl.UInt64)],
                         orient="row").write_parquet(work / f"step-2-batch-{batch}-ids.parquet")
            if result.candidate_catalogue is not None:
                result.candidate_catalogue.steps.write_parquet(work / f"step-2-batch-{batch}-steps.parquet")
        if batch == stop_after_batch:
            print("STOPPED_AFTER_CANDIDATE_BATCH", batch, flush=True)
            raise ComparisonComplete()
    return result


def capture_candidate_merge(asset, frames):
    """Save the real merge inputs before the high-memory operation begins."""
    if asset.iteration == 2 and batch_counts.get(2) == 1:
        for index, frame in enumerate(frames):
            frame.lazy().sink_parquet(work / f"step-2-merge-input-{index}.parquet", engine="streaming")
        print("STOPPED_BEFORE_CANDIDATE_MERGE", len(frames), flush=True)
        raise ComparisonComplete()
    return original_merge(asset, frames)


def stream_candidate_merge(asset, frames):
    """Use Polars streaming for the same merge during this diagnostic only."""
    keys = CANDIDATE_ID_COLS + [
        "activity_seq_id", "time_seq_id", "destination_sequence_key_1",
        "destination_sequence_key_2", "mode_sequence_key_1", "mode_sequence_key_2",
        "seq_step_index",
    ]
    first_columns = [
        column for column in CandidatePlanStepsAsset.STRUCTURAL_COLUMNS
        if column not in set(keys) | {"dest_seq_id", "mode_seq_id", "iteration"}
    ] + ["reference_time_seq_id"]
    return (
        pl.concat([frame.lazy().select(frames[0].columns) for frame in frames], how="vertical_relaxed")
        .group_by(keys)
        .agg(
            [pl.col(column).first() for column in first_columns]
            + [pl.col("iteration").min(), pl.col("first_seen_iteration").min(),
               pl.col("last_seen_iteration").max(), pl.col("last_active_iteration").max()]
        )
        .sort(keys)
        .collect(engine="streaming")
    )


def report_destination_sequences(asset):
    result = original_destination_sequences(asset)
    if capture["step"] == 2 and capture["batch"] == 1:
        result.write_parquet(work / f"step-2-{asset.scenario}-destinations.parquet")
    return result


def report_search_inputs(*args, **kwargs):
    result = original_search_inputs(*args, **kwargs)
    if capture["step"] == 2 and capture["batch"] == 1 and capture["scenario"] is not None:
        result.leg_mode_costs.write_parquet(work / f"step-2-{capture['scenario']}-mode-costs.parquet")
        if os.environ.get("MOBILITY_COMPARE_STOP_AFTER_MODE_INPUTS") == "1":
            print("STOPPED_AFTER_MODE_INPUTS", capture["scenario"], flush=True)
            raise ComparisonComplete()
    return result


def report_mode_sequences(asset):
    capture["scenario"] = asset.destination_sequences.scenario
    try:
        result = original_mode_sequences(asset)
        if capture["step"] == 2 and capture["batch"] == 1:
            result.write_parquet(work / f"step-2-{capture['scenario']}-modes.parquet")
            if os.environ.get("MOBILITY_COMPARE_STOP_AFTER_MODE_SEARCH") == "1":
                print("STOPPED_AFTER_MODE_SEARCH", capture["scenario"], flush=True)
                raise ComparisonComplete()
        return result
    finally:
        capture["scenario"] = None


def report_state(asset):
    state = original_state(asset)
    key = (asset.iteration, asset.scenario)
    if key in reported_states:
        return state
    reported_states.add(key)
    if os.environ.get("MOBILITY_COMPARE_SAVE_ALLOCATIONS") == "1":
        columns = PLAN_KEY_COLS + [
            name for name in ("plan_id", "n_persons", "utility") if name in state.current_plans.columns
        ]
        state.current_plans.select(columns).write_parquet(work / f"step-{asset.iteration}-{asset.scenario}.parquet")
    if os.environ.get("MOBILITY_COMPARE_SAVE_STATE") == "1":
        for name in ("current_plan_steps", "destination_saturation", "destination_sequence_index",
                     "mode_sequence_index", "time_sequence_steps"):
            value = getattr(state, name)
            if isinstance(value, pl.DataFrame):
                value.write_parquet(work / f"step-{asset.iteration}-{asset.scenario}-{name}.parquet")
        candidates = state.candidate_plan_steps
        if isinstance(candidates, CompactCandidateSteps):
            candidates.steps.write_parquet(work / f"step-{asset.iteration}-{asset.scenario}-candidate-steps.parquet")
            candidates.membership.write_parquet(
                work / f"step-{asset.iteration}-{asset.scenario}-candidate-membership.parquet"
            )
        elif isinstance(candidates, pl.DataFrame):
            candidates.write_parquet(work / f"step-{asset.iteration}-{asset.scenario}-candidate-rows.parquet")
    keys = state.current_plans.select(PLAN_KEY_COLS).hash_rows(seed=0).sort()
    print("COMPARE", json.dumps({
        "kind": "state", "step": asset.iteration, "scenario": asset.scenario,
        "plan_rows": state.current_plans.height,
        "plan_keys_sha256": hashlib.sha256(keys.to_numpy().tobytes()).hexdigest(),
        "rng_sha256": hashlib.sha256(repr(state.rng_state).encode()).hexdigest(),
        "people": float(state.current_plans["n_persons"].sum()),
    }), flush=True)
    return state


AssetResolver._rebuild_asset = rebuild_without_downloads
MultiScenarioRunAsset._run_inner_loop = report_candidate_pool
if (os.environ.get("MOBILITY_COMPARE_BATCH_DETAILS") == "1"
        or os.environ.get("MOBILITY_COMPARE_STOP_AFTER_MODE_INPUTS") == "1"
        or os.environ.get("MOBILITY_COMPARE_STOP_AFTER_MODE_SEARCH") == "1"
        or os.environ.get("MOBILITY_COMPARE_CAPTURE_MERGE") == "1"):
    SharedCandidatePoolAsset.create_and_get_asset = report_candidate_batch
if os.environ.get("MOBILITY_COMPARE_CAPTURE_MERGE") == "1":
    SharedCandidatePoolAsset._merge_candidate_content = capture_candidate_merge
elif os.environ.get("MOBILITY_COMPARE_STREAM_MERGE") == "1":
    SharedCandidatePoolAsset._merge_candidate_content = stream_candidate_merge
if (os.environ.get("MOBILITY_COMPARE_GENERATION_INPUTS") == "1"
        or os.environ.get("MOBILITY_COMPARE_STOP_AFTER_MODE_INPUTS") == "1"
        or os.environ.get("MOBILITY_COMPARE_STOP_AFTER_MODE_SEARCH") == "1"):
    DestinationSequences.create_and_get_asset = report_destination_sequences
    mode_module.ModeSequences.create_and_get_asset = report_mode_sequences
    mode_module.build_search_inputs = report_search_inputs
IterationStateAsset.get = report_state
if os.environ.get("MOBILITY_COMPARE_INITIAL_ONLY") == "1":
    InitialIterationStateAsset.get = report_initial_state
work = Path(tempfile.mkdtemp(prefix="mobility-grand-geneve-comparison-"))
os.environ["MOBILITY_SIMULATION_TRACE"] = str(work / "trace.jsonl")
os.chdir(project)
sys.path.insert(0, str(project))
tree = ast.fix_missing_locations(CurrentStepNames().visit(ast.parse(source)))
namespace = {"__file__": str(source_path), "__name__": "grand_geneve_comparison"}
exec(compile(tree, str(source_path), "exec"), namespace)
results = namespace["pop_group_day_trips"].results(
    "weekday", scenarios=["default", "carbon_tax"], replication=0,
)
stop = threading.Event()
memory_setting = os.environ.get("MOBILITY_COMPARE_MEMORY_LIMIT_GIB", "20")
memory_limit = None if memory_setting.lower() == "none" else float(memory_setting) * 1024**3
memory_observations = {"peak_rss": 0}


def monitor():
    process = psutil.Process()
    while not stop.wait(1):
        rss = process.memory_info().rss
        memory_observations["peak_rss"] = max(memory_observations["peak_rss"], rss)
        if memory_limit is not None and (rss > memory_limit or psutil.virtual_memory().available < 8 * 1024**3):
            print("STOPPING: memory guard reached", flush=True)
            _thread.interrupt_main()
            return


threading.Thread(target=monitor, daemon=True).start()
started = perf_counter()
try:
    requests = {"steps": results.tables.plan_steps(steps=1, columns=["scenario", "step", "mode"])}
    if os.environ.get("MOBILITY_COMPARE_SAVE_REQUESTED") == "1":
        requests["final_steps"] = results.tables.plan_steps(columns=[
            "scenario", "step", "mode", "from", "to", "n_persons", "utility", "cost", "distance",
        ])
        cost_columns = ["scenario", "step", "mode", "from", "to", "cost", "distance"]
        requests["final_costs"] = (
            results.tables.costs(columns=cost_columns)
            if "columns" in inspect.signature(results.tables.costs).parameters
            else results.tables.costs()
        )
    result = results.get_all(**requests)
    for name, table in result.items():
        if os.environ.get("MOBILITY_COMPARE_SAVE_REQUESTED") == "1":
            if isinstance(table, pl.DataFrame):
                table.select(cost_columns if name == "final_costs" else table.columns).write_parquet(
                    work / f"requested-{name}.parquet"
                )
            elif name == "final_costs" and "columns" not in inspect.signature(results.tables.costs).parameters:
                table.select(cost_columns).collect().write_parquet(work / f"requested-{name}.parquet")
            elif hasattr(table, "save"):
                table.save(work / f"requested-{name}.parquet")
            else:
                table.collect().write_parquet(work / f"requested-{name}.parquet")
        if hasattr(table, "close"):
            table.close()
    print("COMPARISON_COMPLETED", flush=True)
except ComparisonComplete:
    if os.environ.get("MOBILITY_COMPARE_INITIAL_ONLY") == "1":
        print("STOPPED_AFTER_INITIAL_STATE", flush=True)
    elif os.environ.get("MOBILITY_COMPARE_STOP_AFTER_MODE_INPUTS") == "1":
        print("MODE_INPUT_COMPARISON_FINISHED", flush=True)
    elif os.environ.get("MOBILITY_COMPARE_STOP_AFTER_MODE_SEARCH") == "1":
        print("MODE_SEARCH_COMPARISON_FINISHED", flush=True)
    elif stop_after_pool or stop_after_batch:
        print("CANDIDATE_COMPARISON_FINISHED", flush=True)
    else:
        print("STOPPED_AFTER_STEP", stop_after, flush=True)
finally:
    stop.set()
    print("WALL_SECONDS", round(perf_counter() - started, 3), flush=True)
    print("SAMPLED_PROCESS_PEAK_GIB", round(memory_observations["peak_rss"] / 1024**3, 3), flush=True)
    print("PROBE_FILES", str(work), flush=True)
