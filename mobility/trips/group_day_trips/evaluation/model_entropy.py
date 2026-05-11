from __future__ import annotations

import polars as pl

from .trip_pattern_distribution import build_trip_pattern_distribution


class ModelEntropy:
    """Compare expected and observed diversity of daily trip patterns.

    The entropy diagnostic is based on whole-day signatures built from the
    sequence of `(activity, distance_bin, mode)` trip descriptors for each
    person group.
    """

    def __init__(
        self,
        *,
        expected_plan_steps,
        observed_plan_steps=None,
        history=None,
        epsilon: float = 1e-12,
    ) -> None:
        """Attach expected data, optional observed data, and optional history storage."""
        self.expected_plan_steps = expected_plan_steps
        self.observed_plan_steps = observed_plan_steps
        self.history_store = history
        self.epsilon = epsilon
        self._expected_distribution = None
        self._observed_distribution = None
        self._expected_entropy = None

    def expected_distribution(self) -> pl.DataFrame:
        """Return the expected survey-derived trip-pattern distribution."""
        if self._expected_distribution is None:
            self._expected_distribution = self.expected_plan_steps.get().collect(engine="streaming")
        return self._expected_distribution

    def observed_distribution(self) -> pl.DataFrame:
        """Return the observed trip-pattern distribution for the attached run."""
        if self.observed_plan_steps is None:
            raise ValueError("No observed plan steps are attached to this ModelEntropy instance.")
        if self._observed_distribution is None:
            self._observed_distribution = self.observed_plan_steps.get().collect(engine="streaming")
        return self._observed_distribution

    def comparison(self, plan_steps=None) -> pl.DataFrame:
        """Compare observed and expected probabilities for each trip pattern."""
        observed = build_trip_pattern_distribution(plan_steps, epsilon=self.epsilon) if plan_steps is not None else self.observed_distribution()
        expected = self.expected_distribution()
        return (
            observed.join(
                expected,
                on="trip_pattern",
                how="full",
                coalesce=True,
                suffix="_expected",
            )
            .with_columns(
                n_persons=pl.col("n_persons").fill_null(0.0),
                n_persons_expected=pl.col("n_persons_expected").fill_null(0.0),
                probability=pl.col("probability").fill_null(0.0),
                probability_expected=pl.col("probability_expected").fill_null(0.0),
            )
            .with_columns(
                probability_gap=pl.col("probability") - pl.col("probability_expected"),
                n_persons_gap=pl.col("n_persons") - pl.col("n_persons_expected"),
            )
            .sort("probability_gap", descending=True)
        )

    def expected_entropy(self) -> float:
        """Return the entropy of the expected survey-derived trip-pattern distribution."""
        if self._expected_entropy is None:
            self._expected_entropy = self._entropy_from_distribution(self.expected_distribution())
        return self._expected_entropy

    def observed_entropy(self, plan_steps=None) -> float:
        """Return the entropy of one observed or intermediate trip-pattern distribution."""
        distribution = build_trip_pattern_distribution(plan_steps, epsilon=self.epsilon) if plan_steps is not None else self.observed_distribution()
        return self._entropy_from_distribution(distribution)

    def summary(self, plan_steps=None) -> pl.DataFrame:
        """Return observed entropy, expected entropy, and their difference."""
        expected_entropy = self.expected_entropy()
        observed_entropy = self.observed_entropy(plan_steps=plan_steps)
        return pl.DataFrame(
            {
                "observed_entropy": [observed_entropy],
                "expected_entropy": [expected_entropy],
                "entropy_gap": [observed_entropy - expected_entropy],
            }
        )

    def history(self) -> pl.DataFrame:
        """Return the persisted entropy history extracted from iteration diagnostics."""
        if self.history_store is None:
            raise ValueError("No model entropy history is attached to this ModelEntropy instance.")
        return self.history_store.get().select(["iteration", "observed_entropy"])

    def history_row(self, *, iteration: int, plan_steps) -> dict[str, float]:
        """Build one persisted entropy-history row for a given iteration state."""
        observed_entropy = self.observed_entropy(plan_steps=plan_steps)
        return {
            "iteration": iteration,
            "observed_entropy": observed_entropy,
        }

    def _entropy_from_distribution(self, distribution: pl.DataFrame) -> float:
        """Compute Shannon entropy from a trip-pattern probability distribution."""
        if distribution.height == 0:
            return 0.0
        entropy = distribution.select(
            -(pl.col("probability") * pl.col("probability").clip(self.epsilon).log()).sum()
        ).item()
        return float(entropy or 0.0)
