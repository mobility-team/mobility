from __future__ import annotations

from typing import TYPE_CHECKING, Literal

import polars as pl

from ..transitions.transition_metrics import state_waterfall as _state_waterfall

if TYPE_CHECKING:
    from .results import RunResults


class RunTransitions:
    """Grouped access to transition-dynamics diagnostics."""

    def __init__(self, results: "RunResults") -> None:
        self.results = results

    def state_waterfall(
        self,
        quantity: Literal["distance", "utility", "travel_time", "trip_count"],
        plot: bool = True,
        top_n: int = 5,
        demand_group_ids: list[int] | None = None,
    ) -> tuple[pl.DataFrame, pl.DataFrame]:
        """Run one state-pair waterfall metric for this run."""
        return _state_waterfall(
            transitions=self.results.transition_events,
            quantity=quantity,
            demand_groups=self.results.demand_groups,
            transport_zones=self.results.transport_zones,
            plot=plot,
            top_n=top_n,
            demand_group_ids=demand_group_ids,
        )
