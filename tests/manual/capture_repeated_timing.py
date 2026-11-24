"""Capture consecutive real scoring batches before and after timing updates.

Run using the cache-only Geneva probe. Set MOBILITY_TIMING_CAPTURE_STEP to the
outer step to capture; this stops after the second batch of that step. The
capture does extra collection and disk I/O, so it is not a timing benchmark.
"""
import json
import os
import runpy
import tempfile
from pathlib import Path

from mobility.trips.group_day_trips.core.performance_trace import _ACTIVE_TRACE
from mobility.trips.group_day_trips.plans.plan_schedule_updater import PlanScheduleUpdater
from mobility.trips.group_day_trips.plans.plan_updater import PlanUpdater
import mobility.trips.group_day_trips.plans.plan_updater as plan_updater_module


class CaptureComplete(Exception):
    pass


directory = Path(tempfile.mkdtemp(prefix="mobility-repeated-timing-"))
print("REPEATED_TIMING_FILES", directory, flush=True)
target_step = int(os.environ.get("MOBILITY_TIMING_CAPTURE_STEP", "2"))
original_update = PlanScheduleUpdater.update_plan_timings
original_index = plan_updater_module.assign_time_sequence_ids
original_evaluate = PlanUpdater.evaluate_candidate_pool
captured = set()


def labels():
    active = _ACTIVE_TRACE.get()
    return active[2] if active else {}


def capture_update(updater, steps, **kwargs):
    info = labels()
    batch = info.get("search_batch")
    if info.get("step") == target_step and batch in (1, 2) and batch not in captured:
        steps.collect(engine="streaming").write_parquet(directory / f"input-{batch}.parquet")
        (directory / "settings.json").write_text(json.dumps(kwargs), encoding="utf-8")
        print("REPEATED_TIMING_INPUT", batch, flush=True)
    return original_update(updater, steps, **kwargs)


def capture_index(steps, schedule_steps, **kwargs):
    info = labels()
    batch = info.get("search_batch")
    if info.get("step") != target_step or batch not in (1, 2) or batch in captured:
        return original_index(steps, schedule_steps, **kwargs)
    schedule_steps.write_parquet(directory / f"catalogue-before-{batch}.parquet")
    result = original_index(steps, schedule_steps, **kwargs)
    result[0].write_parquet(directory / f"indexed-{batch}.parquet")
    result[1].write_parquet(directory / f"catalogue-after-{batch}.parquet")
    result[2].write_parquet(directory / f"mapping-{batch}.parquet")
    captured.add(batch)
    print("REPEATED_TIMING_INDEXED", batch, flush=True)
    return result


def stop_after_second(updater, *args, **kwargs):
    result = original_evaluate(updater, *args, **kwargs)
    if 2 in captured:
        raise CaptureComplete()
    return result


PlanScheduleUpdater.update_plan_timings = capture_update
plan_updater_module.assign_time_sequence_ids = capture_index
PlanUpdater.evaluate_candidate_pool = stop_after_second
try:
    runpy.run_path(str(Path(__file__).with_name("inspect_grand_geneve_execution.py")), run_name="__main__")
except CaptureComplete:
    print("REPEATED_TIMING_CAPTURE_COMPLETE", flush=True)
