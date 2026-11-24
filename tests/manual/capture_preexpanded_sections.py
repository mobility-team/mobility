"""Capture shared physical candidates before context-step expansion.

This diagnostic observes the existing evaluator without changing its result.
It stops after the selected batch of outer step 2. Data collection and tracing
make the run unsuitable for timing comparisons.
"""
import json
import os
import runpy
import sys
import tempfile
from pathlib import Path

from mobility.trips.group_day_trips.core.performance_trace import _ACTIVE_TRACE
from mobility.trips.group_day_trips.plans.plan_schedule_updater import PlanScheduleUpdater
from mobility.trips.group_day_trips.plans.plan_updater import PlanUpdater
from mobility.trips.group_day_trips.plans.scoring_contexts import ScoringContexts
import mobility.trips.group_day_trips.plans.plan_updater as plan_updater_module


class CaptureComplete(Exception):
    pass


folder = Path(tempfile.mkdtemp(prefix="mobility-preexpanded-sections-"))
target_batch = int(os.environ.get("MOBILITY_SECTION_CAPTURE_BATCH", "2"))
print("PREEXPANDED_CAPTURE", folder, flush=True)
original_collapse = ScoringContexts.collapse_factored_step_rows
original_score = PlanUpdater.compute_plan_steps_candidates_utility
original_update = PlanScheduleUpdater.update_plan_timings
original_index = plan_updater_module.assign_time_sequence_ids
original_evaluate = PlanUpdater.evaluate_candidate_pool
target_code = original_collapse.__wrapped__.__code__
captured = set()


def target():
    trace = _ACTIVE_TRACE.get()
    labels = trace[2] if trace else {}
    return labels.get("step") == 2 and labels.get("search_batch") == target_batch


def capture_collapse(contexts, *args, **kwargs):
    if not target():
        return original_collapse(contexts, *args, **kwargs)

    def observe(frame, event, result):
        if event == "return" and frame.f_code is target_code:
            sys.setprofile(None)
            if result is None:
                print("PREEXPANDED_FALLBACK", flush=True)
                return
            physical = frame.f_locals["steps"]
            members = frame.f_locals["grouped"].drop("_country_count", "_csp_count")
            physical.write_parquet(folder / "physical.parquet")
            members.write_parquet(folder / "members.parquet")
            result.write_parquet(folder / "expanded-input.parquet")
            captured.add("candidates")
            print("PREEXPANDED_CANDIDATES", physical.height, members.height, result.height, flush=True)

    sys.setprofile(observe)
    try:
        return original_collapse(contexts, *args, **kwargs)
    finally:
        sys.setprofile(None)


def capture_score(updater, **kwargs):
    result = original_score(updater, **kwargs)
    if target():
        lookup = kwargs.get("scoring_lookup")
        if lookup is None or lookup.scores is None:
            raise ValueError("The shared scoring lookup was not retained for this batch.")
        lookup.scores.write_parquet(folder / "step-scores.parquet")
        captured.add("scores")
        print("PREEXPANDED_SCORE_LOOKUP", lookup.scores.height, flush=True)
    return result


def capture_update(updater, steps, **kwargs):
    if target():
        steps.collect(engine="streaming").write_parquet(folder / "expanded-scored.parquet")
        (folder / "settings.json").write_text(json.dumps(kwargs), encoding="utf-8")
        captured.add("timing_input")
    return original_update(updater, steps, **kwargs)


def capture_index(steps, schedule_steps, **kwargs):
    result = original_index(steps, schedule_steps, **kwargs)
    if target():
        result[0].write_parquet(folder / "modeled-steps.parquet")
        result[2].write_parquet(folder / "modeled-mapping.parquet")
        captured.add("timing_output")
    return result


def capture_evaluation(updater, *args, **kwargs):
    result = original_evaluate(updater, *args, **kwargs)
    if target():
        if captured != {"candidates", "scores", "timing_input", "timing_output"}:
            raise ValueError(f"Incomplete pre-expansion capture: {captured}")
        result.possible_plan_utility.write_parquet(folder / "utilities.parquet")
        result.scoring_contexts.demand_groups.write_parquet(folder / "contexts.parquet")
        kwargs["home_night_dur"].write_parquet(folder / "home-durations.parquet")
        print("PREEXPANDED_CAPTURE_COMPLETE", flush=True)
        raise CaptureComplete()
    return result


ScoringContexts.collapse_factored_step_rows = capture_collapse
PlanUpdater.compute_plan_steps_candidates_utility = capture_score
PlanScheduleUpdater.update_plan_timings = capture_update
plan_updater_module.assign_time_sequence_ids = capture_index
PlanUpdater.evaluate_candidate_pool = capture_evaluation
try:
    runpy.run_path(str(Path(__file__).with_name("inspect_grand_geneve_execution.py")), run_name="__main__")
except CaptureComplete:
    pass
