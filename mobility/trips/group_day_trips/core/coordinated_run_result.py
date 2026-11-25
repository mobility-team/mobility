from dataclasses import dataclass

import polars as pl

from .run_state import RunState
from mobility.runtime.assets.resolver import get_current_asset_resolver


@dataclass(frozen=True)
class SimulationRequirements:
    """The steps and final calculations requested by result consumers."""

    steps: frozenset[int] | None
    final_outputs: bool
    mnl_flows: bool
    flow_change: bool


@dataclass(frozen=True)
class CoordinatedRunResult:
    """Per-run outer state, behavior and latest inner estimates."""

    final_states_by_run_key: dict[int, RunState]
    mnl_od_mode_flows_by_run_key: dict[int, pl.DataFrame]
    current_iteration: int = 0
    current_states_by_run_key: dict[int, RunState] | None = None


def current_coordinated_result(coordinator) -> CoordinatedRunResult | None:
    """Return the step currently supplied to this execution's consumers."""
    resolver = get_current_asset_resolver()
    if resolver is not None and getattr(resolver, "simulation_coordinator", None) is coordinator:
        return getattr(resolver, "simulation_step_result", None)
    return None
