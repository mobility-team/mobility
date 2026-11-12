"""Capture late-step scoring inputs from the Grand Geneve diagnostic.

Run from the Mobility Pixi environment. The capture stops after the selected
step and batches, before completing the simulation. It does not change the
model's scoring calculation. Set MOBILITY_SCORING_CAPTURE_STEP (default 4),
MOBILITY_SCORING_CAPTURE_BATCHES (default 2), and optionally
MOBILITY_PROBE_MEMORY_LIMIT_GIB=none. The probe prints its temporary folder.
"""

import os
import runpy
import tempfile
from pathlib import Path

from mobility.trips.group_day_trips.core.performance_trace import _ACTIVE_TRACE
from mobility.trips.group_day_trips.plans.candidate_plan_steps import CandidatePlanStepsAsset
from mobility.trips.group_day_trips.plans.demand_subgroups import DEMAND_UNIT_COLS
from mobility.trips.group_day_trips.plans.plan_updater import PlanUpdater


class CaptureComplete(Exception):
    """Stop once the requested scoring inputs have been written."""


step = int(os.environ.get("MOBILITY_SCORING_CAPTURE_STEP", "4"))
batches = int(os.environ.get("MOBILITY_SCORING_CAPTURE_BATCHES", "2"))
directory = Path(tempfile.mkdtemp(prefix="mobility-scoring-inputs-"))
print("SCORING_CAPTURE_FILES", directory, flush=True)
original = PlanUpdater.compute_plan_steps_candidates_utility
captured = set()


def capture_inputs(updater, **kwargs):
    active = _ACTIVE_TRACE.get()
    labels = active[2] if active else {}
    iteration = labels.get("step")
    batch = labels.get("search_batch")
    scenario = labels.get("scenario")
    key = (batch, scenario)
    if iteration == step and batch is not None and batch <= batches and key not in captured:
        candidates = kwargs["candidates"]
        assignments, _ = kwargs["transport_costs"].get_utility_profiles(
            kwargs["demand_groups"], kwargs["population_segments"] or [],
        )
        columns = [
            name for name in CandidatePlanStepsAsset.STRUCTURAL_COLUMNS
            if name not in DEMAND_UNIT_COLS + ["iteration"]
        ]
        rows = candidates.join(assignments.lazy(), on=DEMAND_UNIT_COLS).select(
            columns + ["utility_profile_id"]
        )
        rows.sink_parquet(directory / f"step-{step}-batch-{batch}-{scenario}.parquet", engine="streaming")
        captured.add(key)
        print("SCORING_INPUTS_CAPTURED", step, batch, scenario, flush=True)
        if len({name for number, name in captured if number == batches}) == 2:
            raise CaptureComplete()
    return original(updater, **kwargs)


PlanUpdater.compute_plan_steps_candidates_utility = capture_inputs
try:
    runpy.run_path(str(Path(__file__).with_name("inspect_grand_geneve_execution.py")), run_name="__main__")
except CaptureComplete:
    print("SCORING_CAPTURE_COMPLETE", flush=True)
