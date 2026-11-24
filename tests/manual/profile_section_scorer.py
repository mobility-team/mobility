"""Time lines in the section scorer using captured inner batches."""
import inspect
import linecache
import runpy
import sys
from collections import defaultdict
from pathlib import Path
from time import perf_counter

import mobility.trips.group_day_trips.plans.factored_plan_scoring as scoring


original = scoring.score_factored_sections
target = inspect.unwrap(original).__code__
totals = defaultdict(float)
last = {}


def trace(frame, event, arg):
    if frame.f_code is not target:
        return None
    now = perf_counter()
    old = last.pop(id(frame), None)
    if old is not None:
        totals[old[0]] += now - old[1]
    if event == "line":
        last[id(frame)] = (frame.f_lineno, now)
    return trace


def profile(*args, **kwargs):
    sys.settrace(trace)
    try:
        return original(*args, **kwargs)
    finally:
        sys.settrace(None)


scoring.score_factored_sections = profile
runpy.run_path(str(Path(__file__).with_name("check_section_scoring_batches.py")), run_name="__main__")
for line, seconds in sorted(totals.items(), key=lambda item: -item[1])[:20]:
    print(f"SECTION_PROFILE seconds={seconds:.3f} line={line} "
          f"code={linecache.getline(target.co_filename, line).strip()}", flush=True)
