"""Capture real transition inputs for opt-in memory and correctness checks.

The Grand Geneve input caches must already exist. This stops before computing
one scenario's transitions and does not save a simulation checkpoint.
"""

import json
import os
import runpy
import tempfile
from pathlib import Path

from mobility.trips.group_day_trips.core.performance_trace import _ACTIVE_TRACE
from mobility.trips.group_day_trips.plans.plan_updater import PlanUpdater


class CaptureComplete(Exception):
    """Stop after saving the selected transition inputs."""


step = int(os.environ.get("MOBILITY_TRANSITION_CAPTURE_STEP", "4"))
scenario = os.environ.get("MOBILITY_TRANSITION_CAPTURE_SCENARIO", "carbon_tax")
directory = Path(tempfile.mkdtemp(prefix="mobility-transition-inputs-"))
print("TRANSITION_CAPTURE_FILES", directory, flush=True)
original = PlanUpdater.get_transition_probabilities

if os.environ.get("MOBILITY_TRANSITION_CAPTURE_FULL") == "1":
    # Capture all scored choices before the outer-update restriction. The
    # script stops at this step, so it does not advance with these choices.
    PlanUpdater._restrict_transition_choices = staticmethod(
        lambda utilities, origins, evaluation, scope: utilities
    )


def capture(updater, current_plans, possible_plan_utility, possible_plan_steps, behavior_change_scope, **kwargs):
    active = _ACTIVE_TRACE.get()
    labels = active[2] if active else {}
    if labels.get("step") == step and labels.get("scenario") == scenario:
        if kwargs.get("enable_transition_distance_model"):
            raise ValueError("This capture supports transitions without the distance model.")
        print("CAPTURING_TRANSITIONS", step, scenario, behavior_change_scope.value, flush=True)
        current_plans.write_parquet(directory / "current-plans.parquet")
        possible_plan_utility.sink_parquet(directory / "possible-plan-utility.parquet", engine="streaming")
        parameters = {key: value for key, value in kwargs.items() if key != "transport_zones"}
        parameters["behavior_change_scope"] = behavior_change_scope.value
        (directory / "parameters.json").write_text(json.dumps(parameters), encoding="utf-8")
        raise CaptureComplete()
    return original(updater, current_plans, possible_plan_utility, possible_plan_steps, behavior_change_scope, **kwargs)


PlanUpdater.get_transition_probabilities = capture
try:
    runpy.run_path(str(Path(__file__).with_name("inspect_grand_geneve_execution.py")), run_name="__main__")
except CaptureComplete:
    print("TRANSITION_CAPTURE_COMPLETE", flush=True)
