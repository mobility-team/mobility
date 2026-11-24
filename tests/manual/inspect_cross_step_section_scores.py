"""Measure how many repeated section scores actually remain unchanged."""
import runpy
from pathlib import Path

import polars as pl

from mobility.trips.group_day_trips.core.multi_scenario_run import MultiScenarioRunAsset
import mobility.trips.group_day_trips.plans.plan_updater as plan_updater


class Capture:
    iteration = 0
    previous_scores = None
    reported = False


capture = Capture()
advance = MultiScenarioRunAsset._advance_cohort_step
score = plan_updater.score_factored_sections


def record_iteration(coordinator, iteration, states, **kwargs):
    capture.iteration = iteration
    return advance(coordinator, iteration, states, **kwargs)


def compare_scores(*args, **kwargs):
    result = score(*args, **kwargs)
    if capture.iteration == 1:
        capture.previous_scores = result.section_scores
    elif capture.iteration == 2 and not capture.reported and capture.previous_scores is not None:
        keys = ["country", "csp", "utility_profile_id", "_section_id"]
        values = ["_utilities", "_feasible", "_missing", "_invalid"]
        common = result.section_scores.join(capture.previous_scores, on=keys, how="inner")
        unchanged = common.filter(pl.all_horizontal([
            pl.col(name).eq_missing(pl.col(name + "_right")) for name in values
        ]))
        print(f"CROSS_STEP_SCORES previous={capture.previous_scores.height} "
              f"current={result.section_scores.height} common={common.height} "
              f"unchanged={unchanged.height}", flush=True)
        capture.reported = True
        capture.previous_scores = None
    return result


MultiScenarioRunAsset._advance_cohort_step = record_iteration
plan_updater.score_factored_sections = compare_scores
runpy.run_path(str(Path(__file__).with_name("inspect_grand_geneve_execution.py")), run_name="__main__")
