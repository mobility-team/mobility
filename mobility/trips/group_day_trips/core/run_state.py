from dataclasses import dataclass

import polars as pl


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
    candidate_plan_steps: pl.DataFrame | None
    destination_saturation: pl.DataFrame
    costs: pl.DataFrame
    start_iteration: int
    current_plan_steps: pl.DataFrame | None = None
