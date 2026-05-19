from __future__ import annotations

import polars as pl

from .calibration_plan_steps import CALIBRATION_PLAN_STEP_COLUMNS

LOSS_GROUP_COLUMNS = ["activity", "distance_bin", "time_bin", "mode"]
MARGINAL_LOSS_GROUPS = {
    "activity_loss": ["activity"],
    "distance_bin_loss": ["distance_bin"],
    "time_bin_loss": ["time_bin"],
    "mode_loss": ["mode"],
}


class ModelLoss:
    """Compare observed and expected calibration distributions for one run.

    The loss is built from the trip-count shares of each canonical calibration
    step group. Distance and time enter the comparison through their bins, so
    the loss stays a single, plain distribution over modelled trip states.
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

    def comparison(self, plan_steps=None, group_columns: list[str] | None = None) -> pl.DataFrame:
        """Return observed-versus-expected distribution comparisons for one plan-step table."""
        observed = self._aggregate(plan_steps) if plan_steps is not None else self.observed_marginals()
        expected = self.expected_marginals()
        group_columns = group_columns or LOSS_GROUP_COLUMNS
        observed = self._collapse_to_group(observed, group_columns)
        expected = self._collapse_to_group(expected, group_columns)
        return (
            observed.join(
                expected,
                on=[*group_columns, "metric"],
                how="full",
                coalesce=True,
                suffix="_expected",
            )
            .with_columns(
                value=pl.col("value").fill_null(0.0),
                value_expected=pl.col("value_expected").fill_null(0.0),
            )
            .with_columns(
                observed_total=pl.col("value").sum().over("metric"),
                expected_total=pl.col("value_expected").sum().over("metric"),
                delta=pl.col("value") - pl.col("value_expected"),
                relative_error=(pl.col("value") - pl.col("value_expected")) / pl.col("value_expected").clip(self.epsilon),
            )
            .with_columns(
                observed_share=pl.col("value") / pl.col("observed_total").clip(self.epsilon),
                expected_share=pl.col("value_expected") / pl.col("expected_total").clip(self.epsilon),
            )
            .with_columns(
                share_delta=pl.col("observed_share") - pl.col("expected_share"),
                normalized_squared_error=(pl.col("observed_share") - pl.col("expected_share")).pow(2),
            )
            .rename(
                {
                    "value": "observed_value",
                    "value_expected": "expected_value",
                }
            )
            .sort(["metric", *group_columns])
        )

    def metric_losses(self, plan_steps=None, group_columns: list[str] | None = None) -> pl.DataFrame:
        """Summarize the trip-state distribution loss."""
        return (
            self.comparison(plan_steps=plan_steps, group_columns=group_columns)
            .group_by("metric")
            .agg(
                loss=pl.col("normalized_squared_error").sum(),
                observed_total=pl.col("observed_total").first(),
                expected_total=pl.col("expected_total").first(),
            )
            .sort("metric")
        )

    def total_loss(self, plan_steps=None) -> float:
        """Return the total calibration loss across all component metrics."""
        metric_losses = self.metric_losses(plan_steps=plan_steps)
        if metric_losses.height == 0:
            return 0.0
        return float(metric_losses["loss"].sum())

    def marginal_losses(self, plan_steps=None) -> dict[str, float]:
        """Return one distribution loss per calibration dimension."""
        return {
            loss_name: self.total_loss_for_group(plan_steps=plan_steps, group_columns=group_columns)
            for loss_name, group_columns in MARGINAL_LOSS_GROUPS.items()
        }

    def total_loss_for_group(self, *, plan_steps=None, group_columns: list[str]) -> float:
        """Return the distribution loss after collapsing to one set of columns."""
        metric_losses = self.metric_losses(plan_steps=plan_steps, group_columns=group_columns)
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
        history = self.history_store.get()
        columns = ["iteration", "total_loss", "activity_loss", "distance_bin_loss", "time_bin_loss", "mode_loss"]
        if "trip_count_loss" in history.columns:
            columns.insert(2, "trip_count_loss")
        return history.select(columns)

    def history_row(self, *, iteration: int, plan_steps) -> dict[str, float]:
        """Build one persisted loss-history row for a given iteration state."""
        metric_losses = self.metric_losses(plan_steps=plan_steps)
        return {
            "iteration": iteration,
            "total_loss": float(metric_losses["loss"].sum()) if metric_losses.height > 0 else 0.0,
            **self.marginal_losses(plan_steps=plan_steps),
        }

    @staticmethod
    def _collapse_to_group(marginals: pl.DataFrame, group_columns: list[str]) -> pl.DataFrame:
        """Collapse full trip-state marginals to one diagnostic grouping."""
        return (
            marginals
            .group_by([*group_columns, "metric"])
            .agg(value=pl.col("value").sum())
        )

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
            .filter(pl.col("mode") != "stay_home")
            .group_by(["activity", "distance_bin", "time_bin", "mode"])
            .agg(
                n_trips=pl.col("n_persons").sum(),
            )
            .unpivot(
                index=["activity", "distance_bin", "time_bin", "mode"],
                on=["n_trips"],
                variable_name="metric",
                value_name="value",
            )
            .collect(engine="streaming")
        )

        return aggregated.with_columns(value=pl.col("value").cast(pl.Float64))
