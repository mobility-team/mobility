from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl

from ..evaluation.model_entropy import ModelEntropy
from ..evaluation.model_loss import ModelLoss

if TYPE_CHECKING:
    from .results import RunResults


class RunDiagnostics:
    """Grouped access to model-fit and convergence diagnostics.

    This namespace exposes helpers that compare model outputs with survey-based
    reference data, plus the compact per-iteration diagnostics history written
    during the run.
    """

    def __init__(self, results: "RunResults") -> None:
        """Bind diagnostics helpers to one run-results object."""
        self.results = results

    def loss(self) -> ModelLoss:
        """Return the calibration-loss helper for this run."""
        return ModelLoss(
            expected_plan_steps=self.results.expected_calibration_plan_steps,
            observed_plan_steps=self.results.observed_calibration_plan_steps,
            history=self.results.iteration_metrics_store,
        )

    def entropy(self) -> ModelEntropy:
        """Return the plan-signature entropy helper for this run."""
        return ModelEntropy(
            expected_plan_steps=self.results.expected_entropy_plan_steps,
            observed_plan_steps=self.results.observed_entropy_plan_steps,
            history=self.results.iteration_metrics_store,
        )

    def iteration_metrics(self) -> pl.DataFrame:
        """Return the persisted per-iteration diagnostics table for this run."""
        return self.results.iteration_metrics_store.get()
