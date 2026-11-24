"""Check whether successive inner batches retain the same schedule catalogue."""
import runpy
from pathlib import Path

import mobility.trips.group_day_trips.plans.plan_updater as plan_updater


original = plan_updater.time_factored_candidates
previous = []


def report(*args, **kwargs):
    cache = kwargs.get("index_cache")
    schedules = args[4]
    print(f"TIMING_CATALOGUE_REUSE present={cache is not None and cache.schedules is not None} "
          f"same_object={cache is not None and cache.schedules is schedules} "
          f"same_previous={bool(previous) and previous[-1] is schedules}", flush=True)
    result = original(*args, **kwargs)
    previous.append(result.time_sequence_steps)
    return result


plan_updater.time_factored_candidates = report
runpy.run_path(str(Path(__file__).with_name("inspect_grand_geneve_execution.py")), run_name="__main__")
