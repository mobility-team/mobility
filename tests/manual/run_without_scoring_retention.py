"""Exercise compact scoring when no lookup can be retained between batches."""
import runpy
from pathlib import Path

from mobility.trips.group_day_trips.core.multi_scenario_run import MultiScenarioRunAsset


original = MultiScenarioRunAsset._build_scoring_lookups


def disable_retention(coordinator, *args):
    args[-1].max_bytes = 0
    return original(coordinator, *args)


MultiScenarioRunAsset._build_scoring_lookups = disable_retention
runpy.run_path(str(Path(__file__).with_name("inspect_grand_geneve_execution.py")), run_name="__main__")
