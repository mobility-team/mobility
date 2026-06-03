from __future__ import annotations

import os
import pathlib

import numpy as np
import polars as pl

from mobility.runtime.assets.file_asset import FileAsset

from .person_metrics import RESULT_COLUMNS, SCOPE_COLUMNS


class ActivityDurationDistribution(FileAsset):
    """Persist model-vs-survey activity duration distributions."""

    def __init__(
        self,
        *,
        plan_steps: FileAsset,
        reference_plan_steps: FileAsset | None = None,
        scenarios: list[str],
        day_type: str,
        iterations: list[int],
        replications: list[int],
        bin_width_minutes: int,
    ) -> None:
        if bin_width_minutes <= 0:
            raise ValueError("bin_width_minutes must be positive.")

        self.plan_steps = plan_steps
        self.reference_plan_steps = reference_plan_steps
        self.scenarios = list(scenarios)
        self.day_type = day_type
        self.iterations = list(iterations)
        self.replications = list(replications)
        self.bin_width_minutes = bin_width_minutes

        project_folder = pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"])
        cache_path = (
            project_folder
            / "group_day_trips"
            / "results"
            / f"activity_duration_distribution_{bin_width_minutes}min.parquet"
        )
        super().__init__(
            {
                "version": 1,
                "plan_steps": plan_steps,
                "reference_plan_steps": reference_plan_steps,
                "scenarios": tuple(scenarios),
                "day_type": day_type,
                "iterations": tuple(iterations),
                "replications": tuple(replications),
                "bin_width_minutes": bin_width_minutes,
            },
            cache_path,
        )

    def get_cached_asset(self) -> pl.DataFrame:
        """Return the cached distribution table."""
        return pl.read_parquet(self.cache_path)

    def create_and_get_asset(self) -> pl.DataFrame:
        """Build and cache the distribution table."""
        duration_tables = [_activity_duration_samples(self.plan_steps.get(), source="model")]
        if self.reference_plan_steps is not None:
            survey_steps = _with_result_scope(
                self.reference_plan_steps.get(),
                scenarios=self.scenarios,
                day_type=self.day_type,
                iterations=self.iterations,
                replications=self.replications,
            )
            duration_tables.append(_activity_duration_samples(survey_steps, source="survey"))
        durations = pl.concat(duration_tables, how="vertical_relaxed")
        bin_width_hours = self.bin_width_minutes / 60.0
        distribution = (
            durations.lazy()
            .with_columns(
                duration_bin_start=(pl.col("duration") / bin_width_hours).floor() * bin_width_hours,
            )
            .with_columns(
                duration_bin_end=pl.col("duration_bin_start") + bin_width_hours,
                duration_bin_mid=pl.col("duration_bin_start") + bin_width_hours / 2.0,
                duration_label=pl.format(
                    "{}-{} h",
                    pl.col("duration_bin_start").round(2),
                    (pl.col("duration_bin_start") + bin_width_hours).round(2),
                ),
            )
            .group_by(
                SCOPE_COLUMNS
                + [
                    "source",
                    "activity",
                    "duration_bin_start",
                    "duration_bin_end",
                    "duration_bin_mid",
                    "duration_label",
                ]
            )
            .agg(
                weighted_visits=pl.col("weight").sum(),
                person_hours=(pl.col("duration") * pl.col("weight")).sum(),
            )
            .with_columns(
                probability=pl.col("weighted_visits")
                / pl.col("weighted_visits").sum().over(SCOPE_COLUMNS + ["source", "activity"])
            )
        )
        output_columns = RESULT_COLUMNS + [
            "source",
            "activity",
            "duration_bin_start",
            "duration_bin_end",
            "duration_bin_mid",
            "duration_label",
        ]
        result = (
            distribution
            .group_by(output_columns)
            .agg(
                pl.col("weighted_visits").mean().alias("weighted_visits"),
                pl.col("person_hours").mean().alias("person_hours"),
                pl.col("probability").mean().alias("probability"),
                pl.col("probability").std().alias("probability_std"),
                pl.col("replication").n_unique().cast(pl.UInt32).alias("n_replications"),
            )
            .sort(output_columns)
            .collect(engine="streaming")
        )

        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        result.write_parquet(self.cache_path)
        return result


class ActivityTimeSeries(FileAsset):
    """Persist model-vs-survey activity occupancy over time."""

    def __init__(
        self,
        *,
        plan_steps: FileAsset,
        demand_groups: FileAsset,
        reference_plan_steps: FileAsset | None = None,
        scenarios: list[str],
        day_type: str,
        iterations: list[int],
        replications: list[int],
        interval_minutes: int,
    ) -> None:
        if interval_minutes <= 0 or 1440 % interval_minutes != 0:
            raise ValueError("interval_minutes must be a positive divisor of 1440.")

        self.plan_steps = plan_steps
        self.demand_groups = demand_groups
        self.reference_plan_steps = reference_plan_steps
        self.scenarios = list(scenarios)
        self.day_type = day_type
        self.iterations = list(iterations)
        self.replications = list(replications)
        self.interval_minutes = interval_minutes

        project_folder = pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"])
        cache_path = (
            project_folder
            / "group_day_trips"
            / "results"
            / f"activity_time_series_{interval_minutes}min.parquet"
        )
        super().__init__(
            {
                "version": 1,
                "plan_steps": plan_steps,
                "demand_groups": demand_groups,
                "reference_plan_steps": reference_plan_steps,
                "scenarios": tuple(scenarios),
                "day_type": day_type,
                "iterations": tuple(iterations),
                "replications": tuple(replications),
                "interval_minutes": interval_minutes,
            },
            cache_path,
        )

    def get_cached_asset(self) -> pl.DataFrame:
        """Return the cached time series table."""
        return pl.read_parquet(self.cache_path)

    def create_and_get_asset(self) -> pl.DataFrame:
        """Build and cache the time series table."""
        bins = _time_bins(interval_minutes=self.interval_minutes)
        total_population = (
            self.demand_groups.get()
            .group_by(SCOPE_COLUMNS)
            .agg(total_population=pl.col("n_persons").cast(pl.Float64).sum())
        )
        model_series = _add_residual_home_time(
            _time_series_from_plan_steps(self.plan_steps.get(), interval_minutes=self.interval_minutes),
            bins=bins,
            total_population=total_population,
        ).with_columns(source=pl.lit("model"))
        series_tables = [model_series]
        if self.reference_plan_steps is not None:
            survey_steps = _with_result_scope(
                self.reference_plan_steps.get(),
                scenarios=self.scenarios,
                day_type=self.day_type,
                iterations=self.iterations,
                replications=self.replications,
            )
            survey_series = _add_residual_home_time(
                _time_series_from_plan_steps(survey_steps, interval_minutes=self.interval_minutes),
                bins=bins,
                total_population=total_population,
            ).with_columns(source=pl.lit("survey"))
            series_tables.append(survey_series)
        per_replication = pl.concat(series_tables, how="vertical_relaxed")
        output_columns = RESULT_COLUMNS + ["source", "time_bin_start", "time_label", "label"]
        result = (
            per_replication
            .lazy()
            .group_by(output_columns)
            .agg(
                pl.col("n_persons").mean().alias("n_persons"),
                pl.col("n_persons").std().alias("n_persons_std"),
                pl.col("replication").n_unique().cast(pl.UInt32).alias("n_replications"),
            )
            .sort(output_columns)
            .collect(engine="streaming")
        )

        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        result.write_parquet(self.cache_path)
        return result


def _with_result_scope(
    plan_steps: pl.LazyFrame,
    *,
    scenarios: list[str],
    day_type: str,
    iterations: list[int],
    replications: list[int],
) -> pl.LazyFrame:
    """Duplicate a reference table for each result scope."""
    scoped_tables = []
    for scenario in scenarios:
        for iteration in iterations:
            for replication in replications:
                scoped_tables.append(
                    plan_steps.with_columns(
                        scenario=pl.lit(scenario, dtype=pl.String),
                        day_type=pl.lit(day_type, dtype=pl.String),
                        iteration=pl.lit(iteration, dtype=pl.Int32),
                        replication=pl.lit(replication, dtype=pl.Int64),
                    )
                )
    return pl.concat(scoped_tables, how="vertical_relaxed")


def _activity_duration_samples(plan_steps: pl.LazyFrame, *, source: str) -> pl.DataFrame:
    """Extract weighted activity-duration samples from plan steps."""
    column_names = set(plan_steps.collect_schema().names())
    required_columns = set(SCOPE_COLUMNS + ["activity", "n_persons"])
    missing_columns = required_columns.difference(column_names)
    if missing_columns:
        raise ValueError(f"Plan steps are missing duration-distribution columns: {sorted(missing_columns)}.")

    if "duration_per_pers" in column_names:
        duration_expr = pl.col("duration_per_pers").cast(pl.Float64)
    elif {"duration", "n_persons"} <= column_names:
        duration_expr = pl.col("duration").cast(pl.Float64) / pl.col("n_persons").cast(pl.Float64)
    else:
        raise ValueError("Plan steps need either duration_per_pers or duration and n_persons.")

    samples = plan_steps.with_columns(
        source=pl.lit(source),
        activity=pl.col("activity").cast(pl.String),
        duration=duration_expr,
        weight=pl.col("n_persons").cast(pl.Float64),
    )
    samples = _filter_terminal_home_steps(samples, column_names)
    return (
        samples
        .filter(
            pl.col("duration").is_not_null()
            & pl.col("duration").is_finite()
            & (pl.col("duration") >= 0.0)
            & pl.col("weight").is_not_null()
            & pl.col("weight").is_finite()
            & (pl.col("weight") > 0.0)
        )
        .select(SCOPE_COLUMNS + ["source", "activity", "duration", "weight"])
        .collect(engine="streaming")
    )


def _filter_terminal_home_steps(plan_steps: pl.LazyFrame, column_names: set[str]) -> pl.LazyFrame:
    """Remove the final home stay and keep home stops reached during the day."""
    if "seq_step_index" in column_names:
        plan_key_candidates = [
            "country",
            "home_zone_id",
            "city_category",
            "csp",
            "n_cars",
            "activity_seq_id",
            "time_seq_id",
            "dest_seq_id",
            "mode_seq_id",
        ]
        plan_keys = [column for column in plan_key_candidates if column in column_names]
        if plan_keys:
            return (
                plan_steps
                .with_columns(is_terminal_step=pl.col("seq_step_index") == pl.col("seq_step_index").max().over(plan_keys))
                .filter(~((pl.col("activity") == "home") & pl.col("is_terminal_step")))
                .drop("is_terminal_step")
            )

    if "next_departure_time" in column_names:
        return plan_steps.filter(~((pl.col("activity") == "home") & (pl.col("next_departure_time") >= 24.0)))

    return plan_steps


def _time_series_from_plan_steps(
    plan_steps: pl.LazyFrame,
    *,
    interval_minutes: int,
) -> pl.DataFrame:
    """Aggregate explicit states stored in plan steps over time bins."""
    bins = _time_bins(interval_minutes=interval_minutes)
    bin_duration_hours = interval_minutes / 60.0
    n_bins = bins.height
    required_columns = set(
        SCOPE_COLUMNS
        + [
            "activity",
            "mode",
            "n_persons",
            "departure_time",
            "arrival_time",
            "next_departure_time",
        ]
    )
    missing_columns = required_columns.difference(plan_steps.collect_schema().names())
    if missing_columns:
        raise ValueError(f"Plan steps are missing activity-time-series columns: {sorted(missing_columns)}.")

    plan_steps = plan_steps.select(list(required_columns)).with_columns(
        activity=pl.col("activity").cast(pl.String),
        mode=pl.col("mode").cast(pl.String),
    )
    activity_intervals = plan_steps.select(
        SCOPE_COLUMNS
        + [
            pl.col("activity").alias("label"),
            pl.col("arrival_time").alias("interval_start"),
            pl.col("next_departure_time").alias("interval_end"),
            "n_persons",
        ]
    )
    transit_intervals = plan_steps.select(
        SCOPE_COLUMNS
        + [
            pl.format("in_transit:{}", pl.col("mode")).alias("label"),
            pl.col("departure_time").alias("interval_start"),
            pl.col("arrival_time").alias("interval_end"),
            "n_persons",
        ]
    )
    intervals = pl.concat([activity_intervals, transit_intervals], how="vertical_relaxed")
    return (
        intervals.lazy()
        .filter(pl.col("interval_end") > pl.col("interval_start"))
        .with_columns(
            start_bin_index=(
                (pl.col("interval_start") / pl.lit(bin_duration_hours))
                .floor()
                .clip(0, n_bins - 1)
                .cast(pl.UInt32)
            ),
            end_bin_index=(
                (pl.col("interval_end") / pl.lit(bin_duration_hours))
                .ceil()
                .clip(0, n_bins)
                .cast(pl.UInt32)
            ),
        )
        .with_columns(bin_index=pl.int_ranges("start_bin_index", "end_bin_index", step=1, dtype=pl.UInt32))
        .explode("bin_index")
        .join(bins.with_row_index("bin_index").lazy(), on="bin_index", how="inner")
        .with_columns(
            overlap_start=pl.max_horizontal("interval_start", "time_bin_start"),
            overlap_end=pl.min_horizontal("interval_end", "time_bin_end"),
        )
        .with_columns(
            overlap_hours=(pl.col("overlap_end") - pl.col("overlap_start")).clip(0.0, bin_duration_hours),
        )
        .filter(pl.col("overlap_hours") > 0.0)
        .with_columns(
            n_persons=(pl.col("n_persons") * pl.col("overlap_hours") / pl.lit(bin_duration_hours)),
        )
        .group_by(SCOPE_COLUMNS + ["time_bin_start", "time_label", "label"])
        .agg(n_persons=pl.col("n_persons").sum())
        .sort(SCOPE_COLUMNS + ["time_bin_start", "label"])
        .collect(engine="streaming")
    )


def _add_residual_home_time(
    time_series: pl.DataFrame,
    *,
    bins: pl.DataFrame,
    total_population: pl.LazyFrame,
) -> pl.DataFrame:
    """Fill missing occupancy in each bin as home time."""
    scope_bins = (
        total_population
        .select(SCOPE_COLUMNS + ["total_population"])
        .join(bins.lazy(), how="cross")
    )
    residual_home = (
        scope_bins
        .join(
            time_series.lazy()
            .group_by(SCOPE_COLUMNS + ["time_bin_start", "time_label"])
            .agg(stacked_total=pl.col("n_persons").sum()),
            on=SCOPE_COLUMNS + ["time_bin_start", "time_label"],
            how="left",
        )
        .with_columns(
            stacked_total=pl.col("stacked_total").fill_null(0.0),
            label=pl.lit("home"),
            n_persons=pl.col("total_population") - pl.col("stacked_total"),
        )
        .filter(pl.col("n_persons") != 0.0)
        .select(SCOPE_COLUMNS + ["time_bin_start", "time_label", "label", "n_persons"])
        .collect(engine="streaming")
    )
    return (
        pl.concat([time_series, residual_home], how="vertical_relaxed")
        .group_by(SCOPE_COLUMNS + ["time_bin_start", "time_label", "label"])
        .agg(n_persons=pl.col("n_persons").sum())
        .sort(SCOPE_COLUMNS + ["time_bin_start", "label"])
    )


def _time_bins(*, interval_minutes: int) -> pl.DataFrame:
    """Return one full-day time-bin table."""
    bin_duration_hours = interval_minutes / 60.0
    bin_starts = np.arange(0.0, 24.0, bin_duration_hours, dtype=float)
    return pl.DataFrame(
        {
            "time_bin_start": bin_starts,
            "time_bin_end": bin_starts + bin_duration_hours,
            "time_label": [
                f"{int(hour):02d}:{int(round((hour * 60.0) % 60.0)):02d}"
                for hour in bin_starts
            ],
        }
    )
