"""Capture scoring inputs for the same 100 contexts as an existing high-effort capture.

Set MOBILITY_COMPONENT_SOURCE to that capture and run with the same Geneva
probe parameters. Stops after the third batch of outer step 2. Extra I/O makes
this unsuitable for measuring simulation runtime.
"""
import json
import os
import runpy
import tempfile
from pathlib import Path

import polars as pl

from mobility.trips.group_day_trips.core.performance_trace import _ACTIVE_TRACE
from mobility.trips.group_day_trips.plans.plan_schedule_updater import PlanScheduleUpdater
from mobility.trips.group_day_trips.plans.plan_updater import PlanUpdater
import mobility.trips.group_day_trips.plans.plan_updater as plan_updater_module


class CaptureComplete(Exception):
    pass


source = Path(os.environ["MOBILITY_COMPONENT_SOURCE"])
utility = pl.read_parquet(source / "candidate-utility.parquet").filter(
    pl.col("is_feasible") & (pl.col("mode_seq_id") != 0)
)
contexts = pl.read_parquet(source / "contexts.parquet")
unit = ["demand_group_id", "demand_subgroup_id"]
counts = utility.group_by(unit).len(name="candidates").filter(pl.col("candidates") >= 2)
choices = contexts.join(counts, on=unit).sort("country", "n_persons", "demand_group_id")
selected = []
for country in choices.partition_by("country", maintain_order=True):
    n = min(50, country.height)
    selected += [country["demand_group_id"][(2 * i + 1) * country.height // (2 * n)] for i in range(n)]
directory = Path(tempfile.mkdtemp(prefix="mobility-high-components-"))
print("HIGH_COMPONENT_CAPTURE", directory, "contexts", len(selected), flush=True)
original_update = PlanScheduleUpdater.update_plan_timings
original_index = plan_updater_module.assign_time_sequence_ids
original_evaluate = PlanUpdater.evaluate_candidate_pool
captured = False


def target():
    trace = _ACTIVE_TRACE.get()
    labels = trace[2] if trace else {}
    return labels.get("step") == 2 and labels.get("search_batch") == 3


def capture_update(updater, steps, **kwargs):
    if target():
        steps.filter(pl.col("demand_group_id").is_in(selected)).collect(engine="streaming").write_parquet(
            directory / "input-2.parquet"
        )
        (directory / "settings.json").write_text(json.dumps(kwargs), encoding="utf-8")
    return original_update(updater, steps, **kwargs)


def capture_index(steps, schedule_steps, **kwargs):
    global captured
    result = original_index(steps, schedule_steps, **kwargs)
    if target():
        result[0].filter(pl.col("demand_group_id").is_in(selected)).write_parquet(directory / "indexed-2.parquet")
        captured = True
        print("HIGH_COMPONENT_INPUTS_CAPTURED", flush=True)
    return result


def stop_after_capture(updater, *args, **kwargs):
    result = original_evaluate(updater, *args, **kwargs)
    if captured:
        raise CaptureComplete()
    return result


PlanScheduleUpdater.update_plan_timings = capture_update
plan_updater_module.assign_time_sequence_ids = capture_index
PlanUpdater.evaluate_candidate_pool = stop_after_capture
try:
    runpy.run_path(str(Path(__file__).with_name("inspect_grand_geneve_execution.py")), run_name="__main__")
except CaptureComplete:
    print("HIGH_COMPONENT_CAPTURE_COMPLETE", flush=True)
