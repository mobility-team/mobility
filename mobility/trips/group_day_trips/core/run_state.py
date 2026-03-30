from dataclasses import dataclass

import polars as pl

from mobility.transport.costs.congestion_state import CongestionState


@dataclass
class RunState:
    chains_by_activity: pl.DataFrame
    chains: pl.DataFrame
    demand_groups: pl.DataFrame
    activity_dur: pl.DataFrame
    home_night_dur: pl.DataFrame
    stay_home_plan: pl.DataFrame
    opportunities: pl.DataFrame
    current_plans: pl.DataFrame
    remaining_opportunities: pl.DataFrame
    costs: pl.DataFrame
    congestion_state: CongestionState | None
    start_iteration: int
    current_plan_steps: pl.DataFrame | None = None
