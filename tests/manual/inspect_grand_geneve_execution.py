"""Run a cache-only Grand Genève probe without changing the scenario script.

Run from the Mobility Pixi environment. The source project and its input caches
must already exist. This probe refuses to rebuild input assets, so it cannot
start a download; a missing input is reported instead.
"""

import ast
import _thread
import json
import os
import sys
import tempfile
import threading
from pathlib import Path
from time import perf_counter

import polars as pl
import psutil

from mobility.runtime.assets.resolver import AssetResolver
from mobility.runtime.temporary import managed_temporary_disk_usage
from mobility.trips.group_day_trips.core.multi_scenario_run import MultiScenarioRunAsset
from mobility.trips.group_day_trips.plans.compact_candidate_steps import CompactCandidateSteps
from mobility.trips.group_day_trips.results.assets.metric import Metric
from mobility.transport.costs.path.path_travel_costs import PathTravelCostsTable
from mobility.transport.graphs.congested.congested_path_graph import CongestedPathGraph


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
        for keyword in node.keywords:
            keyword.arg = self.names.get(keyword.arg, keyword.arg)
            if keyword.arg == "additional_gtfs_files":
                # These generated feeds belong to scenarios outside this probe.
                keyword.value = ast.Constant(value=None)
        if isinstance(node.func, ast.Attribute) and node.func.attr == "set_params":
            for keyword in node.keywords:
                if keyword.arg == "logging_level":
                    keyword.value = ast.Constant(value="INFO")
            node.keywords.extend([
                ast.keyword(arg="r_packages", value=ast.Constant(value=False)),
                ast.keyword(arg="track_project_cache", value=ast.Constant(value=False)),
                ast.keyword(arg="temporary_disk_limit_gb", value=ast.Constant(value=5)),
            ])
        return node


original_rebuild = AssetResolver._rebuild_asset
original_inner_loop = MultiScenarioRunAsset._run_inner_loop
original_advance = MultiScenarioRunAsset._advance_cohort_step


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
    return result


def report_live_state(coordinator, iteration, states, **kwargs):
    assets, next_states, search = original_advance(coordinator, iteration, states, **kwargs)
    for run_key, state in next_states.items():
        sizes = {
            name: getattr(state, name).estimated_size()
            for name in ("current_plans", "current_plan_steps", "destination_saturation", "costs")
            if isinstance(getattr(state, name), pl.DataFrame)
        }
        candidates = state.candidate_plan_steps
        if isinstance(candidates, CompactCandidateSteps):
            sizes["candidate_steps"] = candidates.steps.estimated_size()
            sizes["candidate_membership"] = candidates.membership.estimated_size()
        elif isinstance(candidates, pl.DataFrame):
            sizes["candidate_plan_steps"] = candidates.estimated_size()
        print("LIVE_STATE_TABLE_BYTES", iteration, coordinator.runs[run_key].scenario, sizes, flush=True)
    return assets, next_states, search


def rebuild_without_downloads(resolver, asset, *args, **kwargs):
    if not isinstance(asset, (Metric, PathTravelCostsTable, CongestedPathGraph)):
        raise RuntimeError(
            f"Grand Genève probe needs an input cache that is missing or stale: {type(asset).__name__}. "
            "The probe stopped without rebuilding it."
        )
    return original_rebuild(resolver, asset, *args, **kwargs)


AssetResolver._rebuild_asset = rebuild_without_downloads
MultiScenarioRunAsset._run_inner_loop = report_candidate_pool
MultiScenarioRunAsset._advance_cohort_step = report_live_state
work = Path(tempfile.mkdtemp(prefix="mobility-grand-geneve-probe-"))
print("PROBE_FILES", str(work), flush=True)
os.environ["MOBILITY_SIMULATION_TRACE"] = str(work / "trace.jsonl")
os.chdir(project)
sys.path.insert(0, str(project))
tree = ast.fix_missing_locations(CurrentStepNames().visit(ast.parse(source)))
namespace = {"__file__": str(source_path), "__name__": "grand_geneve_probe"}
exec(compile(tree, str(source_path), "exec"), namespace)

model = namespace["pop_group_day_trips"]
results = model.results("weekday", scenarios=["default", "carbon_tax"], replication=0)
metric = results.metrics.trip_count(by_variable="mode", steps="all")
metric.cache_path = work / "trip-count.parquet"
metric.hash_path = work / "trip-count.inputs-hash"
diagnostics = results.tables.adaptive_search_diagnostics(
    columns=["scenario", "outer_step", "unique_pool_size"]
)
started = perf_counter()
stop_monitor = threading.Event()
resource_samples = {"managed_temporary_peak_bytes": managed_temporary_disk_usage(), "peak_rss": 0}
memory_setting = os.environ.get("MOBILITY_PROBE_MEMORY_LIMIT_GIB", "20")
memory_limit = None if memory_setting.lower() == "none" else float(memory_setting) * 1024**3


def monitor_resources():
    process = psutil.Process()
    count = 0
    while not stop_monitor.wait(2):
        count += 1
        rss = process.memory_info().rss
        resource_samples["peak_rss"] = max(resource_samples["peak_rss"], rss)
        if memory_limit is not None and (rss > memory_limit or psutil.virtual_memory().available < 8 * 1024**3):
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
