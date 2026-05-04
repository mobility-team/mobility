from __future__ import annotations

import os
import pathlib

import polars as pl

from mobility.runtime.assets.file_asset import FileAsset

DISTANCE_BIN_BREAKS = [0.0, 1.0, 5.0, 10.0, 20.0, 40.0, 80.0, 1e6]
CALIBRATION_PLAN_STEP_COLUMNS = ["activity", "distance_bin", "mode", "distance", "time", "n_persons"]


def _as_lazyframe(plan_steps: pl.LazyFrame | pl.DataFrame) -> pl.LazyFrame:
    """Return a lazy frame regardless of the input plan-step container."""
    if isinstance(plan_steps, pl.DataFrame):
        return plan_steps.lazy()
    return plan_steps


def distance_bin_expr(column: str = "distance") -> pl.Expr:
    """Return the canonical distance-bin expression shared by calibration diagnostics."""
    return pl.col(column).cast(pl.Float64).cut(DISTANCE_BIN_BREAKS, left_closed=True).cast(pl.String)


def to_calibration_plan_steps(
    plan_steps: pl.LazyFrame | pl.DataFrame,
) -> pl.LazyFrame:
    """Convert raw plan steps to the canonical calibration-step schema."""
    plan_steps = _as_lazyframe(plan_steps)
    return (
        plan_steps
        .filter(pl.col("activity_seq_id") != 0)
        .select(["activity", "mode", "distance", "time", "n_persons"])
        .with_columns(
            activity=pl.col("activity").cast(pl.String),
            mode=pl.col("mode").cast(pl.String),
            distance=pl.col("distance").cast(pl.Float64),
            time=pl.col("time").cast(pl.Float64),
            n_persons=pl.col("n_persons").cast(pl.Float64),
            distance_bin=distance_bin_expr("distance"),
        )
        .select(CALIBRATION_PLAN_STEP_COLUMNS)
    )


class PopulationWeightedCalibrationPlanSteps(FileAsset):
    """Persist survey-weighted plan steps prepared for calibration."""

    def __init__(
        self,
        *,
        population_weighted_plan_steps: FileAsset,
        is_weekday: bool,
    ) -> None:
        project_folder = pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"])
        cache_path = (
            project_folder
            / "group_day_trips"
            / f"calibration_expected_plan_steps_{'weekday' if is_weekday else 'weekend'}.parquet"
        )
        inputs = {
            "version": 1,
            "population_weighted_plan_steps": population_weighted_plan_steps,
            "is_weekday": is_weekday,
        }
        self.population_weighted_plan_steps = population_weighted_plan_steps
        self.is_weekday = is_weekday
        super().__init__(inputs, cache_path)

    def get_cached_asset(self) -> pl.LazyFrame:
        return pl.scan_parquet(self.cache_path)

    def create_and_get_asset(self) -> pl.LazyFrame:
        standardized = to_calibration_plan_steps(
            self.population_weighted_plan_steps.get().rename({"travel_time": "time"})
        ).collect(engine="streaming")
        standardized.write_parquet(self.cache_path)
        return self.get_cached_asset()


class ObservedCalibrationPlanSteps(FileAsset):
    """Persist simulated final plan steps prepared for calibration."""

    def __init__(
        self,
        *,
        run: FileAsset,
        is_weekday: bool,
    ) -> None:
        project_folder = pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"])
        cache_path = (
            project_folder
            / "group_day_trips"
            / f"calibration_observed_plan_steps_{'weekday' if is_weekday else 'weekend'}.parquet"
        )
        inputs = {
            "version": 1,
            "run": run,
            "is_weekday": is_weekday,
        }
        self.run = run
        self.is_weekday = is_weekday
        super().__init__(inputs, cache_path)

    def get_cached_asset(self) -> pl.LazyFrame:
        return pl.scan_parquet(self.cache_path)

    def create_and_get_asset(self) -> pl.LazyFrame:
        standardized = to_calibration_plan_steps(
            self.run.get_cached_asset()["plan_steps"]
        ).collect(engine="streaming")
        standardized.write_parquet(self.cache_path)
        return self.get_cached_asset()
