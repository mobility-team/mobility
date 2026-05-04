from __future__ import annotations

import polars as pl


class ModelLossHistory:
    """Small helper around persisted iteration loss summaries."""

    def __init__(self, history: pl.LazyFrame) -> None:
        self.history = history

    @staticmethod
    def empty() -> pl.DataFrame:
        return pl.DataFrame(
            schema={
                "iteration": pl.UInt16,
                "total_loss": pl.Float64,
                "distance_loss": pl.Float64,
                "n_trips_loss": pl.Float64,
                "time_loss": pl.Float64,
            }
        )

    @staticmethod
    def from_records(records: list[dict[str, float]]) -> pl.DataFrame:
        if not records:
            return ModelLossHistory.empty()
        return pl.DataFrame(records).select(
            [
                pl.col("iteration").cast(pl.UInt16),
                pl.col("total_loss").cast(pl.Float64),
                pl.col("distance_loss").cast(pl.Float64),
                pl.col("n_trips_loss").cast(pl.Float64),
                pl.col("time_loss").cast(pl.Float64),
            ]
        )

    def get(self) -> pl.DataFrame:
        return self.history.collect(engine="streaming").sort("iteration")
