from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import polars as pl

if TYPE_CHECKING:
    from mobility.trips.group_day_trips.plans.compact_candidate_steps import CompactCandidateSteps


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
    candidate_plan_steps: pl.DataFrame | CompactCandidateSteps | None
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
