"""Run the Geneva probe without sectioned candidate storage for comparison."""
import runpy
from pathlib import Path

from mobility.trips.group_day_trips.plans.compact_candidate_steps import CompactCandidateSteps

original = CompactCandidateSteps.from_frame.__func__


def without_sections(cls, frame, *, prefer_compact=False, rigid_activities=None):
    return original(cls, frame, prefer_compact=prefer_compact, rigid_activities=None)


CompactCandidateSteps.from_frame = classmethod(without_sections)
runpy.run_path(str(Path(__file__).with_name("inspect_grand_geneve_execution.py")), run_name="__main__")
