from __future__ import annotations

import polars as pl


class ModelEntropyHistory:
    """Small helper around persisted iteration entropy summaries."""

    def __init__(self, history: pl.LazyFrame) -> None:
        self.history = history

    @staticmethod
    def empty() -> pl.DataFrame:
        return pl.DataFrame(
            schema={
                "iteration": pl.UInt16,
                "observed_entropy": pl.Float64,
                "expected_entropy": pl.Float64,
                "entropy_gap": pl.Float64,
            }
        )

    @staticmethod
    def from_records(records: list[dict[str, float]]) -> pl.DataFrame:
        if not records:
            return ModelEntropyHistory.empty()
        return pl.DataFrame(records).select(
            [
                pl.col("iteration").cast(pl.UInt16),
                pl.col("observed_entropy").cast(pl.Float64),
                pl.col("expected_entropy").cast(pl.Float64),
                pl.col("entropy_gap").cast(pl.Float64),
            ]
        )

    def get(self) -> pl.DataFrame:
        return self.history.collect(engine="streaming").sort("iteration")
