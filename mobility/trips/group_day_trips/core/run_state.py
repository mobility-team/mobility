from dataclasses import dataclass

import polars as pl


@dataclass
class RunState:
    survey_plans: pl.DataFrame
    survey_plan_steps: pl.DataFrame
    demand_groups: pl.DataFrame
    activity_dur: pl.DataFrame
    home_night_dur: pl.DataFrame
    stay_home_plan: pl.DataFrame
    opportunities: pl.DataFrame
    current_plans: pl.DataFrame
    candidate_plan_steps: pl.DataFrame | None
    plan_id_index: pl.DataFrame
    destination_saturation: pl.DataFrame
    costs: pl.DataFrame
    start_iteration: int
    current_plan_steps: pl.DataFrame | None = None
    rng_state: object | None = None
    destination_sequence_index: pl.DataFrame | None = None
    mode_sequence_index: pl.DataFrame | None = None
    adaptive_search_diagnostics: pl.DataFrame | None = None
    time_sequence_steps: pl.DataFrame | None = None

    @property
    def schedule_steps(self) -> pl.DataFrame:
        """Return survey schedules together with registered modeled timings."""
        return self.survey_plan_steps if self.time_sequence_steps is None else self.time_sequence_steps
