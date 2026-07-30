from __future__ import annotations

import polars as pl

from .trip_pattern_distribution import get_survey_immobility_probabilities

TRIP_COUNT_BINS = ["0", "1", "2", "3", "4", "5+"]


def build_trip_count_distribution(
    plan_steps: pl.LazyFrame,
    *,
    immobility_probabilities: pl.DataFrame | None = None,
    epsilon: float = 1e-12,
) -> pl.DataFrame:
    """Build a weighted distribution of the number of trips per person.

    When survey immobility probabilities are given, the input is treated as the
    cleaned mobile survey plans. Mobile weights are scaled by the mobile share,
    and a zero-trip mass is added by country and socio-professional group.
    """
    available_columns = set(plan_steps.collect_schema().names())
    required_columns = {"activity_seq_id", "n_persons"}
    if not required_columns.issubset(available_columns):
        missing = sorted(required_columns - available_columns)
        raise ValueError(
            "Trip-count loss expects raw ordered plan steps. "
            f"Missing columns: {missing}."
        )

    candidate_columns = [
        "country",
        "demand_group_id",
        "home_zone_id",
        "city_category",
        "csp",
        "n_cars",
        "activity_seq_id",
        "time_seq_id",
        "dest_seq_id",
        "mode_seq_id",
    ]
    plan_key_cols = [col for col in candidate_columns if col in available_columns]

    mobile_counts = (
        plan_steps
        .filter(pl.col("activity_seq_id") != 0)
        .select(plan_key_cols + ["n_persons"])
        .group_by(plan_key_cols)
        .agg(
            n_persons=pl.col("n_persons").first(),
            trip_count=pl.len(),
        )
    )

    if immobility_probabilities is not None:
        if not {"country", "csp"}.issubset(set(plan_key_cols)):
            raise ValueError(
                "Survey-side immobility rescaling requires plan steps with 'country' and 'csp' columns."
            )
        segment_cols = ["country", "csp"]
        mobile_counts = mobile_counts.with_columns(
            country=pl.col("country").cast(pl.String),
            csp=pl.col("csp").cast(pl.String),
        )
        segment_totals = (
            mobile_counts
            .group_by(segment_cols)
            .agg(n_persons_segment=pl.col("n_persons").sum())
            .join(
                immobility_probabilities.lazy().with_columns(
                    country=pl.col("country").cast(pl.String),
                    csp=pl.col("csp").cast(pl.String),
                ),
                on=segment_cols,
                how="left",
            )
            .with_columns(p_immobility=pl.col("p_immobility").fill_null(0.0))
        )
        mobile_counts = (
            mobile_counts
            .join(segment_totals, on=segment_cols, how="left")
            .with_columns(n_persons=pl.col("n_persons") * (1.0 - pl.col("p_immobility")))
            .select(["trip_count", "n_persons"])
        )
        stay_home_counts = (
            segment_totals
            .with_columns(
                trip_count=pl.lit(0, dtype=pl.UInt32),
                n_persons=pl.col("n_persons_segment") * pl.col("p_immobility"),
            )
            .select(["trip_count", "n_persons"])
        )
        all_counts = pl.concat([mobile_counts, stay_home_counts], how="vertical_relaxed")
    else:
        stay_home_counts = (
            plan_steps
            .filter(pl.col("activity_seq_id") == 0)
            .select(plan_key_cols + ["n_persons"])
            .group_by(plan_key_cols)
            .agg(n_persons=pl.col("n_persons").first())
            .with_columns(trip_count=pl.lit(0, dtype=pl.UInt32))
            .select(["trip_count", "n_persons"])
        )
        all_counts = pl.concat(
            [
                mobile_counts.select(["trip_count", "n_persons"]),
                stay_home_counts,
            ],
            how="vertical_relaxed",
        )

    distribution = (
        all_counts
        .with_columns(trip_count_bin=_trip_count_bin_expr("trip_count"))
        .group_by("trip_count_bin")
        .agg(n_persons=pl.col("n_persons").sum())
        .with_columns(probability=pl.col("n_persons") / pl.col("n_persons").sum().clip(epsilon))
        .collect(engine="streaming")
    )
    return (
        _all_trip_count_bins()
        .join(distribution, on="trip_count_bin", how="left")
        .with_columns(
            n_persons=pl.col("n_persons").fill_null(0.0).cast(pl.Float64),
            probability=pl.col("probability").fill_null(0.0).cast(pl.Float64),
        )
    )


class ModelTripCountLoss:
    """Compare the observed and expected distributions of trips per person."""

    def __init__(
        self,
        *,
        expected_plan_steps: pl.LazyFrame,
        surveys=None,
        is_weekday: bool = True,
        observed_plan_steps: pl.LazyFrame | None = None,
        history=None,
        epsilon: float = 1e-9,
    ) -> None:
        """Attach expected data, optional observed data, and optional history storage."""
        if not isinstance(expected_plan_steps, pl.LazyFrame):
            raise TypeError("ModelTripCountLoss expects lazy expected plan steps.")
        if observed_plan_steps is not None and not isinstance(observed_plan_steps, pl.LazyFrame):
            raise TypeError("ModelTripCountLoss expects lazy observed plan steps.")
        self.expected_plan_steps = expected_plan_steps
        self.surveys = surveys
        self.is_weekday = is_weekday
        self.observed_plan_steps = observed_plan_steps
        self.history_store = history
        self.epsilon = epsilon
        self._expected_distribution = None

    def expected_distribution(self) -> pl.DataFrame:
        """Return the survey-derived trip-count distribution."""
        if self._expected_distribution is None:
            immobility = (
                get_survey_immobility_probabilities(self.surveys, is_weekday=self.is_weekday)
                if self.surveys is not None
                else None
            )
            self._expected_distribution = build_trip_count_distribution(
                self.expected_plan_steps,
                immobility_probabilities=immobility,
            )
        return self._expected_distribution

    def observed_distribution(self) -> pl.DataFrame:
        """Return the observed trip-count distribution for the attached run."""
        if self.observed_plan_steps is None:
            raise ValueError("No observed plan steps are attached to this ModelTripCountLoss instance.")
        return build_trip_count_distribution(self.observed_plan_steps)

    def comparison(self, plan_steps=None) -> pl.DataFrame:
        """Return observed-versus-expected trip-count distribution comparisons."""
        observed = (
            build_trip_count_distribution(plan_steps)
            if plan_steps is not None
            else self.observed_distribution()
        )
        expected = self.expected_distribution()
        return (
            observed
            .select(["trip_count_bin", pl.col("n_persons").alias("observed_value")])
            .join(
                expected.select(["trip_count_bin", pl.col("n_persons").alias("expected_value")]),
                on="trip_count_bin",
                how="full",
                coalesce=True,
            )
            .with_columns(
                observed_value=pl.col("observed_value").fill_null(0.0),
                expected_value=pl.col("expected_value").fill_null(0.0),
            )
            .with_columns(
                observed_total=pl.col("observed_value").sum(),
                expected_total=pl.col("expected_value").sum(),
                delta=pl.col("observed_value") - pl.col("expected_value"),
                relative_error=(pl.col("observed_value") - pl.col("expected_value"))
                / pl.col("expected_value").clip(self.epsilon),
            )
            .with_columns(
                observed_share=pl.col("observed_value") / pl.col("observed_total").clip(self.epsilon),
                expected_share=pl.col("expected_value") / pl.col("expected_total").clip(self.epsilon),
            )
            .with_columns(
                share_delta=pl.col("observed_share") - pl.col("expected_share"),
                normalized_squared_error=(pl.col("observed_share") - pl.col("expected_share")).pow(2),
            )
            .sort("trip_count_bin")
        )

    def total_loss(self, plan_steps=None) -> float:
        """Return the trip-count distribution loss."""
        comparison = self.comparison(plan_steps=plan_steps)
        if comparison.height == 0:
            return 0.0
        return float(comparison["normalized_squared_error"].sum())

    def summary(self, plan_steps=None) -> pl.DataFrame:
        """Return the trip-count loss and population totals."""
        comparison = self.comparison(plan_steps=plan_steps)
        return pl.DataFrame(
            {
                "trip_count_loss": [float(comparison["normalized_squared_error"].sum())],
                "observed_total": [float(comparison["observed_total"].max() or 0.0)],
                "expected_total": [float(comparison["expected_total"].max() or 0.0)],
            }
        )

    def history(self) -> pl.DataFrame:
        """Return the persisted trip-count loss history."""
        if self.history_store is None:
            raise ValueError("No model trip-count loss history is attached to this instance.")
        return self.history_store.get().select(["iteration", "trip_count_loss"])

    def history_row(self, *, iteration: int, plan_steps) -> dict[str, float]:
        """Build one persisted trip-count loss row for a given iteration state."""
        return {
            "iteration": iteration,
            "trip_count_loss": self.total_loss(plan_steps=plan_steps),
        }


def _trip_count_bin_expr(column: str) -> pl.Expr:
    """Return the small set of trip-count classes used by diagnostics."""
    return (
        pl.when(pl.col(column) >= 5)
        .then(pl.lit("5+"))
        .otherwise(pl.col(column).cast(pl.UInt32).cast(pl.String))
    )


def _all_trip_count_bins() -> pl.DataFrame:
    """Return all trip-count bins so missing classes get explicit zero mass."""
    return pl.DataFrame({"trip_count_bin": TRIP_COUNT_BINS})
