"""Report section reuse while running the normal Geneva diagnostic."""
import runpy
from pathlib import Path

import polars as pl

import mobility.trips.group_day_trips.plans.plan_updater as plan_updater


original_score = plan_updater.score_factored_sections


def report_sections(*args, **kwargs):
    cache = kwargs.get("cache")
    retained = cache.sections.height if cache is not None and cache.sections is not None else 0
    result = original_score(*args, **kwargs)
    references = result.plan_sections
    reused = references.filter(pl.col("_section_id") < retained).height
    print(f"SECTION_REUSE retained={retained} references={references.height} reused={reused}", flush=True)
    return result


plan_updater.score_factored_sections = report_sections
runpy.run_path(str(Path(__file__).with_name("inspect_grand_geneve_execution.py")), run_name="__main__")
