from __future__ import annotations

import polars as pl

from .calibration_plan_steps import to_calibration_plan_steps


class IterationMetricsHistory:
    """Helper around the compact per-iteration diagnostics table.

    This table is the run-level convergence summary persisted to disk. It keeps
    one row per iteration with loss, entropy, utility, and main travel-volume
    indicators.
    """

    def __init__(self, history: pl.LazyFrame) -> None:
        """Wrap the lazy parquet reader storing iteration diagnostics."""
        self.history = history

    @staticmethod
    def schema() -> dict[str, pl.DataType]:
        """Return the canonical schema for persisted iteration diagnostics."""
        return {
            "iteration": pl.UInt16,
            "total_loss": pl.Float64,
            "trip_count_loss": pl.Float64,
            "activity_loss": pl.Float64,
            "distance_bin_loss": pl.Float64,
            "time_bin_loss": pl.Float64,
            "mode_loss": pl.Float64,
            "observed_entropy": pl.Float64,
            "mean_utility": pl.Float64,
            "mean_trip_count": pl.Float64,
            "mean_travel_time": pl.Float64,
            "mean_travel_distance": pl.Float64,
            "excess_occupation_share": pl.Float64,
        }

    @staticmethod
    def empty() -> pl.DataFrame:
        """Return an empty iteration-metrics dataframe with the canonical schema."""
        return pl.DataFrame(schema=IterationMetricsHistory.schema())

    @staticmethod
    def from_records(records: list[dict[str, float]]) -> pl.DataFrame:
        """Build a persisted iteration-metrics dataframe from Python records."""
        if not records:
            return IterationMetricsHistory.empty()

        frame = pl.DataFrame(records)
        if "excess_occupation_share" not in frame.columns:
            frame = frame.with_columns(excess_occupation_share=pl.lit(0.0))

        return frame.select(
            [
                pl.col("iteration").cast(pl.UInt16),
                pl.col("total_loss").cast(pl.Float64),
                pl.col("trip_count_loss").cast(pl.Float64),
                pl.col("activity_loss").cast(pl.Float64),
                pl.col("distance_bin_loss").cast(pl.Float64),
                pl.col("time_bin_loss").cast(pl.Float64),
                pl.col("mode_loss").cast(pl.Float64),
                pl.col("observed_entropy").cast(pl.Float64),
                pl.col("mean_utility").cast(pl.Float64),
                pl.col("mean_trip_count").cast(pl.Float64),
                pl.col("mean_travel_time").cast(pl.Float64),
                pl.col("mean_travel_distance").cast(pl.Float64),
                pl.col("excess_occupation_share").cast(pl.Float64),
            ]
        )

    def get(self) -> pl.DataFrame:
        """Return the persisted iteration-metrics table sorted by iteration."""
        return self.history.collect(engine="streaming").sort("iteration")


class IterationMetricsBuilder:
    """Build one compact iteration-diagnostics row from the current model state.

    The builder combines detailed diagnostic helpers (`ModelLoss` and
    `ModelEntropy`) with simple run-level means such as utility, trips, travel
    time, and travel distance.
    """

    def __init__(self, *, model_loss, model_trip_count_loss, model_entropy) -> None:
        """Attach the detailed diagnostic helpers used to score each iteration."""
        self.model_loss = model_loss
        self.model_trip_count_loss = model_trip_count_loss
        self.model_entropy = model_entropy

    def history_row(
        self,
        *,
        iteration: int,
        current_plans: pl.DataFrame,
        current_plan_steps: pl.DataFrame,
        destination_saturation: pl.DataFrame | None = None,
    ) -> dict[str, float]:
        """Compute one persisted diagnostics row for a single model iteration."""
        loss_row = self.model_loss.history_row(
            iteration=iteration,
            plan_steps=to_calibration_plan_steps(current_plan_steps.lazy()),
        )
        trip_count_loss_row = self.model_trip_count_loss.history_row(
            iteration=iteration,
            plan_steps=current_plan_steps.lazy(),
        )
        entropy_row = self.model_entropy.history_row(
            iteration=iteration,
            plan_steps=current_plan_steps.lazy(),
        )
        total_loss = float(loss_row["total_loss"]) + float(trip_count_loss_row["trip_count_loss"])
        return {
            **loss_row,
            "total_loss": total_loss,
            "trip_count_loss": float(trip_count_loss_row["trip_count_loss"]),
            "observed_entropy": float(entropy_row["observed_entropy"]),
            "mean_utility": self._mean_plan_utility(current_plans),
            "mean_trip_count": self._mean_trip_count(current_plans, current_plan_steps),
            "mean_travel_time": self._mean_travel_metric(current_plans, current_plan_steps, "time"),
            "mean_travel_distance": self._mean_travel_metric(current_plans, current_plan_steps, "distance"),
            "excess_occupation_share": self._excess_occupation_share(destination_saturation),
        }

    def rebuild_history(
        self,
        *,
        iterations,
        resume_from_iteration: int | None,
    ) -> list[dict[str, float]]:
        """Rebuild persisted iteration diagnostics from saved iteration states when resuming."""
        if resume_from_iteration is None:
            return []

        iteration_metrics_records: list[dict[str, float]] = []
        for iteration_index in range(1, resume_from_iteration + 1):
            saved_state = iterations.iteration(iteration_index).load_state()
            iteration_metrics_records.append(
                self.history_row(
                    iteration=iteration_index,
                    current_plans=saved_state.current_plans,
                    current_plan_steps=saved_state.current_plan_steps,
                    destination_saturation=saved_state.destination_saturation,
                )
            )

        return iteration_metrics_records

    @staticmethod
    def _mean_plan_utility(current_plans: pl.DataFrame) -> float:
        """Return the population-weighted mean utility of the current plan mix."""
        if current_plans.height == 0:
            return 0.0

        value = current_plans.select(
            (
                (pl.col("utility") * pl.col("n_persons")).sum()
                / pl.col("n_persons").sum().clip(1e-12)
            ).alias("mean_utility")
        ).item()
        return float(value or 0.0)

    @staticmethod
    def _excess_occupation_share(destination_saturation: pl.DataFrame | None) -> float:
        """Return the share of occupied activity duration above soft capacity."""
        if destination_saturation is None or destination_saturation.height == 0:
            return 0.0
        required = {
            "opportunity_occupation",
            "opportunity_capacity",
        }
        if required.issubset(set(destination_saturation.columns)) is False:
            return 0.0

        soft_capacity_factor = (
            pl.col("destination_soft_capacity_factor")
            if "destination_soft_capacity_factor" in destination_saturation.columns
            else pl.lit(1.25)
        )
        value = destination_saturation.select(
            (
                (
                    pl.col("opportunity_occupation")
                    - soft_capacity_factor * pl.col("opportunity_capacity")
                )
                .clip(0.0)
                .sum()
                / pl.col("opportunity_occupation").sum().clip(1e-12)
            ).alias("excess_occupation_share")
        ).item()
        return float(value or 0.0)

    @staticmethod
    def _mean_trip_count(current_plans: pl.DataFrame, current_plan_steps: pl.DataFrame) -> float:
        """Return the mean number of travelled trips per person."""
        mobile_steps = current_plan_steps.filter(pl.col("activity_seq_id") != 0)
        if mobile_steps.height == 0:
            return 0.0

        total_persons = current_plans.select(pl.col("n_persons").sum()).item()
        if total_persons is None or float(total_persons) == 0.0:
            return 0.0

        value = mobile_steps.select(
            (
                pl.col("n_persons").sum()
                / pl.lit(float(total_persons))
            ).alias("mean_trip_count")
        ).item()
        return float(value or 0.0)

    @staticmethod
    def _mean_travel_metric(current_plans: pl.DataFrame, current_plan_steps: pl.DataFrame, metric: str) -> float:
        """Return one mean travelled metric per person across mobile plan steps."""
        mobile_steps = current_plan_steps.filter(pl.col("activity_seq_id") != 0)
        if mobile_steps.height == 0:
            return 0.0

        total_persons = current_plans.select(pl.col("n_persons").sum()).item()
        if total_persons is None or float(total_persons) == 0.0:
            return 0.0

        value = mobile_steps.select(
            (
                (pl.col(metric) * pl.col("n_persons")).sum()
                / pl.lit(float(total_persons))
            ).alias(metric)
        ).item()
        return float(value or 0.0)
