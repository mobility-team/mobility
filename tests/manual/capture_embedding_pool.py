"""Capture candidate and allocated plans for a plan-similarity study.

Run with the cache-only Geneva probe. This captures the second inner batch of
outer step 2, plus its resulting allocated plans. Set
MOBILITY_EMBEDDING_CAPTURE_BATCH to capture another batch. It does not alter choices;
the extra collections and files make it unsuitable for timing measurements.
"""
import os
import runpy
import tempfile
from pathlib import Path

from mobility.trips.group_day_trips.core.multi_scenario_run import MultiScenarioRunAsset
from mobility.trips.group_day_trips.core.performance_trace import _ACTIVE_TRACE
from mobility.trips.group_day_trips.plans.plan_updater import PlanUpdater


directory = Path(tempfile.mkdtemp(prefix="mobility-embedding-pool-"))
print("EMBEDDING_CAPTURE_FILES", directory, flush=True)
original_evaluate = PlanUpdater.evaluate_candidate_pool
original_advance = MultiScenarioRunAsset._advance_cohort_step
capture_batch = int(os.environ.get("MOBILITY_EMBEDDING_CAPTURE_BATCH", "2"))


def capture_evaluation(updater, *args, **kwargs):
    result = original_evaluate(updater, *args, **kwargs)
    active = _ACTIVE_TRACE.get()
    labels = active[2] if active else {}
    if labels.get("step") == 2 and labels.get("search_batch") == capture_batch:
        if result.scoring_contexts is None:
            raise ValueError("The embedding capture requires compact scoring contexts.")
        result.possible_plan_steps.lazy().select(
            "plan_id", "seq_step_index", "activity", "from", "to", "mode",
            "departure_time", "arrival_time", "next_departure_time",
        ).collect(engine="streaming").write_parquet(directory / "candidate-steps.parquet")
        result.possible_plan_utility.lazy().collect(engine="streaming").write_parquet(directory / "candidate-utility.parquet")
        result.scoring_contexts.demand_groups.write_parquet(directory / "contexts.parquet")
        result.scoring_contexts.unit_map.write_parquet(directory / "unit-map.parquet")
        print("EMBEDDING_CANDIDATES_CAPTURED", flush=True)
    return result


def capture_allocation(coordinator, iteration, states, **kwargs):
    result = original_advance(coordinator, iteration, states, **kwargs)
    if iteration == 2:
        _, next_states, _ = result
        if len(next_states) != 1:
            raise ValueError("The embedding capture requires one scenario.")
        state = next(iter(next_states.values()))
        state.current_plans.write_parquet(directory / "allocated-plans.parquet")
        print("EMBEDDING_ALLOCATION_CAPTURED", flush=True)
    return result


PlanUpdater.evaluate_candidate_pool = capture_evaluation
MultiScenarioRunAsset._advance_cohort_step = capture_allocation
runpy.run_path(str(Path(__file__).with_name("inspect_grand_geneve_execution.py")), run_name="__main__")
