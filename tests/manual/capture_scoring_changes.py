"""Capture costs and opportunity values used by successive outer steps.

Run with the cache-only Geneva probe. This records the first scoring batch of
steps 1–3 and stops after the third capture. It adds disk I/O and is not a
performance benchmark. Only OD/mode/profile rows requested by those batches
are captured, not the complete transport-cost surface.
"""
import os
import runpy
import tempfile
from pathlib import Path

from mobility.transport.costs.transport_costs import TransportCosts
from mobility.trips.group_day_trips.core.performance_trace import _ACTIVE_TRACE
from mobility.trips.group_day_trips.plans.plan_updater import PlanUpdater


class CaptureComplete(Exception):
    pass


directory = Path(tempfile.mkdtemp(prefix="mobility-scoring-changes-"))
print("SCORING_CHANGES_FILES", directory, flush=True)
original_score = PlanUpdater.compute_plan_steps_candidates_utility
original_costs = TransportCosts.get_profile_costs_by_od_and_mode
active = {}
captured = set()


def capture_costs(costs, profiles, metrics, **kwargs):
    result = original_costs(costs, profiles, metrics, **kwargs)
    step = active.get("step")
    if step is not None and step not in captured:
        result.select("utility_profile_id", "from", "to", "mode", "cost", "distance", "time") \
            .write_parquet(directory / f"costs-{step}.parquet")
        captured.add(step)
        print("SCORING_CHANGES_CAPTURED", step, result.height, flush=True)
    return result


def capture_score(updater, **kwargs):
    trace = _ACTIVE_TRACE.get()
    labels = trace[2] if trace else {}
    step, batch = labels.get("step"), labels.get("search_batch")
    if step not in (1, 2, 3) or batch != 1 or step in captured:
        return original_score(updater, **kwargs)
    groups = kwargs["demand_groups"]
    assignments, _ = kwargs["transport_costs"].get_utility_profiles(groups, kwargs["population_segments"] or [])
    assignments.write_parquet(directory / f"profiles-{step}.parquet")
    kwargs["destination_saturation"].write_parquet(directory / f"opportunities-{step}.parquet")
    active["step"] = step
    try:
        result = original_score(updater, **kwargs)
    finally:
        active.clear()
    if step == 3 and len(captured) == 3:
        raise CaptureComplete()
    return result


TransportCosts.get_profile_costs_by_od_and_mode = capture_costs
PlanUpdater.compute_plan_steps_candidates_utility = capture_score
try:
    runpy.run_path(str(Path(__file__).with_name("inspect_grand_geneve_execution.py")), run_name="__main__")
except CaptureComplete:
    print("SCORING_CHANGES_COMPLETE", flush=True)
