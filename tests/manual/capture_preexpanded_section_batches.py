"""Capture shared physical candidates before context-step expansion.

This diagnostic observes the existing evaluator without changing its result.
It captures two consecutive batches of outer step 2 and then stops. Data collection and tracing
make the run unsuitable for timing comparisons.
"""
import json
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


class CaptureScheduleUpdater(PlanScheduleUpdater):
    """Capture context-level timing rather than the shared-timing fast path."""


folder = Path(tempfile.mkdtemp(prefix="mobility-preexpanded-section-batches-"))
for batch in (1, 2):
    (folder / f"batch-{batch}").mkdir()
print("PREEXPANDED_CAPTURE", folder, flush=True)
original_collapse = ScoringContexts.collapse_factored_step_rows
original_score = PlanUpdater.compute_plan_steps_candidates_utility
original_update = PlanScheduleUpdater.update_plan_timings
original_index = plan_updater_module.assign_time_sequence_ids
original_evaluate = PlanUpdater.evaluate_candidate_pool
original_init = PlanUpdater.__init__
target_code = original_collapse.__wrapped__.__code__
captured = {1: set(), 2: set()}


def capture_init(updater):
    original_init(updater)
    updater.schedule_updater = CaptureScheduleUpdater()


def target():
    trace = _ACTIVE_TRACE.get()
    labels = trace[2] if trace else {}
    return labels.get("search_batch") if labels.get("step") == 2 and labels.get("search_batch") in (1, 2) else None


def capture_collapse(contexts, *args, **kwargs):
    if not target():
        return original_collapse(contexts, *args, **kwargs)

    def observe(frame, event, result):
        if event == "return" and frame.f_code is target_code:
            sys.setprofile(None)
            if result is None:
                raise ValueError("The factored candidate path fell back to expanded rows.")
            physical = frame.f_locals["steps"]
            members = frame.f_locals["grouped"].drop("_country_count", "_csp_count", strict=False)
            physical.write_parquet(folder / f"batch-{target()}" / "physical.parquet")
            members.write_parquet(folder / f"batch-{target()}" / "members.parquet")
            expanded = result[0] if isinstance(result, tuple) else result
            expanded.write_parquet(folder / f"batch-{target()}" / "expanded-input.parquet")
            captured[target()].add("candidates")
            print("PREEXPANDED_CANDIDATES", physical.height, members.height, expanded.height, flush=True)

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
        lookup.scores.write_parquet(folder / f"batch-{target()}" / "step-scores.parquet")
        captured[target()].add("scores")
        print("PREEXPANDED_SCORE_LOOKUP", lookup.scores.height, flush=True)
    return result


def capture_update(updater, steps, **kwargs):
    if target():
        steps.collect(engine="streaming").write_parquet(folder / f"batch-{target()}" / "expanded-scored.parquet")
        (folder / f"batch-{target()}" / "settings.json").write_text(json.dumps(kwargs), encoding="utf-8")
        captured[target()].add("timing_input")
    return original_update(updater, steps, **kwargs)


def capture_index(steps, schedule_steps, **kwargs):
    if target():
        schedule_steps.write_parquet(folder / f"batch-{target()}" / "schedule-catalogue.parquet")
    result = original_index(steps, schedule_steps, **kwargs)
    if target():
        result[0].write_parquet(folder / f"batch-{target()}" / "modeled-steps.parquet")
        result[2].write_parquet(folder / f"batch-{target()}" / "modeled-mapping.parquet")
        captured[target()].add("timing_output")
    return result


def capture_evaluation(updater, *args, **kwargs):
    result = original_evaluate(updater, *args, **kwargs)
    if target():
        if captured[target()] != {"candidates", "scores", "timing_input", "timing_output"}:
            raise ValueError(f"Incomplete pre-expansion capture: {captured}")
        result.possible_plan_utility.write_parquet(folder / f"batch-{target()}" / "utilities.parquet")
        result.scoring_contexts.demand_groups.write_parquet(folder / f"batch-{target()}" / "contexts.parquet")
        kwargs["home_night_dur"].write_parquet(folder / f"batch-{target()}" / "home-durations.parquet")
        print("PREEXPANDED_BATCH_COMPLETE", target(), flush=True)
        if target() == 2:
            raise CaptureComplete()
    return result


ScoringContexts.collapse_factored_step_rows = capture_collapse
PlanUpdater.__init__ = capture_init
PlanUpdater.compute_plan_steps_candidates_utility = capture_score
PlanScheduleUpdater.update_plan_timings = capture_update
plan_updater_module.assign_time_sequence_ids = capture_index
PlanUpdater.evaluate_candidate_pool = capture_evaluation
try:
    runpy.run_path(str(Path(__file__).with_name("inspect_grand_geneve_execution.py")), run_name="__main__")
except CaptureComplete:
    pass
