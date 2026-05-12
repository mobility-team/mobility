from __future__ import annotations

import polars as pl

from .calibration_plan_steps import CALIBRATION_PLAN_STEP_COLUMNS


class ModelLoss:
    """Compare observed and expected calibration marginals for one run.

    The loss is built from aggregated trip-count, distance, and travel-time
    marginals over canonical calibration plan steps. It can be used either on
    the final observed run outputs or on any intermediate plan-step table.
    """

    def __init__(
        self,
        *,
        expected_plan_steps,
        observed_plan_steps=None,
        history=None,
        epsilon: float = 1e-9,
    ) -> None:
        """Attach expected data, optional observed data, and optional history storage."""
        self.expected_plan_steps = expected_plan_steps
        self.observed_plan_steps = observed_plan_steps
        self.history_store = history
        self.epsilon = epsilon
        self._expected_marginals = None

    def expected_marginals(self) -> pl.DataFrame:
        """Return the expected survey-derived calibration marginals."""
        if self._expected_marginals is None:
            self._expected_marginals = self._aggregate(self.expected_plan_steps.get())
        return self._expected_marginals

    def observed_marginals(self) -> pl.DataFrame:
        """Return the observed calibration marginals for the attached run."""
        if self.observed_plan_steps is None:
            raise ValueError("No observed calibration plan steps are attached to this ModelLoss instance.")
        return self._aggregate(self.observed_plan_steps.get())

    def comparison(self, plan_steps=None) -> pl.DataFrame:
        """Return observed-versus-expected marginal comparisons for one plan-step table."""
        observed = self._aggregate(plan_steps) if plan_steps is not None else self.observed_marginals()
        expected = self.expected_marginals()
        return (
            observed.join(
                expected,
                on=["activity", "distance_bin", "mode", "metric"],
                how="full",
                coalesce=True,
                suffix="_expected",
            )
            .with_columns(
                value=pl.col("value").fill_null(0.0),
                value_expected=pl.col("value_expected").fill_null(0.0),
            )
            .with_columns(
                delta=pl.col("value") - pl.col("value_expected"),
                relative_error=(pl.col("value") - pl.col("value_expected")) / pl.col("value_expected").clip(self.epsilon),
                squared_error=(pl.col("value") - pl.col("value_expected")).pow(2),
                metric_scale=pl.col("value_expected").sum().over("metric").clip(self.epsilon),
            )
            .with_columns(
                normalized_squared_error=pl.col("squared_error") / pl.col("metric_scale").pow(2)
            )
            .rename(
                {
                    "value": "observed_value",
                    "value_expected": "expected_value",
                }
            )
            .sort(["metric", "activity", "distance_bin", "mode"])
        )

    def metric_losses(self, plan_steps=None) -> pl.DataFrame:
        """Summarize normalized loss separately for trip count, distance, and time."""
        return (
            self.comparison(plan_steps=plan_steps)
            .group_by("metric")
            .agg(
                loss=pl.col("normalized_squared_error").sum(),
                observed_total=pl.col("observed_value").sum(),
                expected_total=pl.col("expected_value").sum(),
            )
            .sort("metric")
        )

    def total_loss(self, plan_steps=None) -> float:
        """Return the total calibration loss across all component metrics."""
        metric_losses = self.metric_losses(plan_steps=plan_steps)
        if metric_losses.height == 0:
            return 0.0
        return float(metric_losses["loss"].sum())

    def summary(self) -> pl.DataFrame:
        """Return metric-level losses together with the total loss for the attached run."""
        metric_losses = self.metric_losses()
        total_loss = self.total_loss()
        return metric_losses.with_columns(total_loss=pl.lit(total_loss, dtype=pl.Float64))

    def history(self) -> pl.DataFrame:
        """Return the persisted loss history extracted from iteration diagnostics."""
        if self.history_store is None:
            raise ValueError("No model loss history is attached to this ModelLoss instance.")
        return self.history_store.get().select(
            ["iteration", "total_loss", "distance_loss", "n_trips_loss", "time_loss"]
        )

    def history_row(self, *, iteration: int, plan_steps) -> dict[str, float]:
        """Build one persisted loss-history row for a given iteration state."""
        metric_losses = self.metric_losses(plan_steps=plan_steps)
        metric_loss_map = {
            row["metric"]: row["loss"]
            for row in metric_losses.select(["metric", "loss"]).to_dicts()
        }
        return {
            "iteration": iteration,
            "total_loss": float(metric_losses["loss"].sum()) if metric_losses.height > 0 else 0.0,
            "distance_loss": float(metric_loss_map.get("distance", 0.0)),
            "n_trips_loss": float(metric_loss_map.get("n_trips", 0.0)),
            "time_loss": float(metric_loss_map.get("time", 0.0)),
        }

    def _aggregate(self, plan_steps: pl.LazyFrame | pl.DataFrame) -> pl.DataFrame:
        """Aggregate canonical calibration plan steps into loss marginals."""
        if isinstance(plan_steps, pl.DataFrame):
            plan_steps = plan_steps.lazy()
        available_columns = set(plan_steps.collect_schema().names())
        missing_columns = [col for col in CALIBRATION_PLAN_STEP_COLUMNS if col not in available_columns]
        if missing_columns:
            raise ValueError(
                "ModelLoss expects canonical calibration plan steps. "
                f"Missing columns: {missing_columns}."
            )
        aggregated = (
            plan_steps
            .group_by(["activity", "distance_bin", "mode"])
            .agg(
                n_trips=pl.col("n_persons").sum(),
                distance=(pl.col("distance") * pl.col("n_persons")).sum(),
                time=(pl.col("time") * pl.col("n_persons")).sum(),
            )
            .unpivot(
                index=["activity", "distance_bin", "mode"],
                on=["n_trips", "distance", "time"],
                variable_name="metric",
                value_name="value",
            )
            .collect(engine="streaming")
        )

        return aggregated.with_columns(value=pl.col("value").cast(pl.Float64))
