"""Compare section scoring with the existing expanded candidate scorer."""
import runpy
from pathlib import Path

from mobility.trips.group_day_trips.plans.plan_updater import PlanUpdater


PlanUpdater._evaluate_shared_sections = lambda self, **kwargs: None
runpy.run_path(str(Path(__file__).with_name("inspect_grand_geneve_execution.py")), run_name="__main__")
