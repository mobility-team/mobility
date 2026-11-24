"""Report large RSS changes within the production section scorer."""
import inspect
import linecache
import os
import runpy
import sys
import tempfile
from pathlib import Path

import psutil

from mobility.trips.group_day_trips.core.multi_scenario_run import MultiScenarioRunAsset
import mobility.trips.group_day_trips.plans.plan_updater as plan_updater


class Monitor:
    iteration = 0
    previous_line = None
    previous_rss = None


monitor = Monitor()
process = psutil.Process()
advance = MultiScenarioRunAsset._advance_cohort_step
original = plan_updater.score_factored_sections
target = inspect.unwrap(original).__code__


def trace(frame, event, arg):
    if frame.f_code is not target:
        return None
    if (event == "line" and monitor.iteration == 2 and os.environ.get("MOBILITY_CAPTURE_CONTEXT_JOIN") == "1"
            and linecache.getline(target.co_filename, frame.f_lineno).lstrip().startswith("del class_parts")):
        folder = Path(tempfile.mkdtemp(prefix="mobility-context-join-"))
        frame.f_locals["context_plans"].write_parquet(folder / "contexts.parquet")
        frame.f_locals["class_utility"].write_parquet(folder / "utilities.parquet")
        print(f"CONTEXT_JOIN_CAPTURE {folder}", flush=True)
        raise RuntimeError("Context join capture completed")
    rss = process.memory_info().rss
    if monitor.previous_rss is None or abs(rss - monitor.previous_rss) >= 128 * 1024**2:
        line = monitor.previous_line
        code = linecache.getline(target.co_filename, line).strip() if line is not None else "start"
        print(f"SECTION_MEMORY step={monitor.iteration} rss={rss} previous_line={line} code={code}", flush=True)
        monitor.previous_rss = rss
    if event == "line":
        monitor.previous_line = frame.f_lineno
    return trace


def report(*args, **kwargs):
    monitor.previous_line = None
    monitor.previous_rss = None
    sys.settrace(trace)
    try:
        return original(*args, **kwargs)
    finally:
        sys.settrace(None)


def record_iteration(coordinator, iteration, states, **kwargs):
    monitor.iteration = iteration
    return advance(coordinator, iteration, states, **kwargs)


MultiScenarioRunAsset._advance_cohort_step = record_iteration
plan_updater.score_factored_sections = report
runpy.run_path(str(Path(__file__).with_name("inspect_grand_geneve_execution.py")), run_name="__main__")
