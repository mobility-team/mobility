from __future__ import annotations

import polars as pl


class ObservedPlanSteps:
    """Light wrapper exposing observed final plan steps through a `.get()` API."""

    def __init__(self, plan_steps: pl.LazyFrame) -> None:
        self.plan_steps = plan_steps

    def get(self) -> pl.LazyFrame:
        return self.plan_steps
