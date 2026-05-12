from __future__ import annotations

import os
import pathlib

import polars as pl

from mobility.runtime.assets.file_asset import FileAsset

from .calibration_plan_steps import _as_lazyframe, distance_bin_expr


def get_survey_immobility_probabilities(surveys, *, is_weekday: bool) -> pl.DataFrame:
    """Return survey immobility probabilities keyed by (country, csp)."""
    column_name = "immobility_weekday" if is_weekday else "immobility_weekend"
    return (
        pl.concat(
            [
                pl.DataFrame(survey.get()["p_immobility"].reset_index()).with_columns(
                    country=pl.lit(survey.inputs["parameters"].country, pl.String())
                )
                for survey in surveys
            ]
        )
        .with_columns(
            country=pl.col("country").cast(pl.String),
            csp=pl.col("csp").cast(pl.String),
            p_immobility=pl.col(column_name).cast(pl.Float64),
        )
        .select(["country", "csp", "p_immobility"])
    )


def build_trip_pattern_distribution(
    plan_steps: pl.LazyFrame | pl.DataFrame,
    *,
    epsilon: float = 1e-12,
    immobility_probabilities: pl.DataFrame | None = None,
) -> pl.DataFrame:
    """Build a weighted trip-pattern distribution from raw ordered plan steps."""
    plan_steps = _as_lazyframe(plan_steps)
    available_columns = set(plan_steps.collect_schema().names())
    candidate_columns = [
        "country",
        "demand_group_id",
        "home_zone_id",
        "csp",
        "n_cars",
        "activity_seq_id",
        "time_seq_id",
        "dest_seq_id",
        "mode_seq_id",
    ]
    plan_key_cols = [col for col in candidate_columns if col in available_columns]
    required_columns = {"activity_seq_id", "time_seq_id", "seq_step_index", "activity", "distance", "mode", "n_persons"}
    if not required_columns.issubset(available_columns):
        missing = sorted(required_columns - available_columns)
        raise ValueError(
            "Trip-pattern distribution expects raw ordered plan steps. "
            f"Missing columns: {missing}."
        )

    mobile_weighted_plans = (
        plan_steps
        .filter(pl.col("activity_seq_id") != 0)
        .select(plan_key_cols + ["n_persons"])
        .group_by(plan_key_cols)
        .agg(n_persons=pl.col("n_persons").first())
    )
    stay_home_weighted_plans = (
        plan_steps
        .filter(pl.col("activity_seq_id") == 0)
        .select(plan_key_cols + ["n_persons"])
        .group_by(plan_key_cols)
        .agg(n_persons=pl.col("n_persons").first())
        .with_columns(trip_pattern=pl.lit("stay_home"))
    )
    mobile_trip_pattern_lookup = (
        plan_steps
        .filter(pl.col("activity_seq_id") != 0)
        .select(plan_key_cols + ["seq_step_index", "activity", "distance", "mode"])
        .with_columns(
            activity=pl.col("activity").cast(pl.String),
            mode=pl.col("mode").cast(pl.String),
            distance_bin=distance_bin_expr("distance"),
        )
        .with_columns(
            trip_pattern_token=pl.concat_str(
                [pl.col("activity"), pl.col("distance_bin"), pl.col("mode")],
                separator="|",
            )
        )
        .group_by(plan_key_cols)
        .agg(trip_pattern=pl.col("trip_pattern_token").sort_by("seq_step_index").str.join("||"))
    )
    mobile_trip_patterns = mobile_weighted_plans.join(
        mobile_trip_pattern_lookup,
        on=plan_key_cols,
        how="inner",
    )

    if immobility_probabilities is not None:
        if not {"country", "csp"}.issubset(set(plan_key_cols)):
            raise ValueError(
                "Survey-side immobility rescaling requires plan steps with 'country' and 'csp' columns."
            )
        segment_cols = ["country", "csp"]
        mobile_trip_patterns = mobile_trip_patterns.with_columns(
            country=pl.col("country").cast(pl.String),
            csp=pl.col("csp").cast(pl.String),
        )
        segment_totals = (
            mobile_trip_patterns
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
        mobile_trip_patterns = (
            mobile_trip_patterns
            .join(segment_totals, on=segment_cols, how="left")
            .with_columns(n_persons=pl.col("n_persons") * (1.0 - pl.col("p_immobility")))
            .select(["trip_pattern", "n_persons"])
        )
        stay_home_patterns = (
            segment_totals
            .with_columns(
                trip_pattern=pl.lit("stay_home"),
                n_persons=pl.col("n_persons_segment") * pl.col("p_immobility"),
            )
            .select(["trip_pattern", "n_persons"])
        )
        all_plan_patterns = pl.concat(
            [
                mobile_trip_patterns,
                stay_home_patterns,
            ],
            how="vertical_relaxed",
        )
    else:
        all_plan_patterns = pl.concat(
            [
                mobile_trip_patterns.select(["trip_pattern", "n_persons"]),
                stay_home_weighted_plans.select(["trip_pattern", "n_persons"]),
            ],
            how="vertical_relaxed",
        )

    distribution = (
        all_plan_patterns
        .group_by("trip_pattern")
        .agg(n_persons=pl.col("n_persons").sum())
        .with_columns(
            probability=pl.col("n_persons") / pl.col("n_persons").sum().clip(epsilon),
        )
        .collect(engine="streaming")
        .sort("probability", descending=True)
    )
    return distribution.with_columns(
        n_persons=pl.col("n_persons").cast(pl.Float64),
        probability=pl.col("probability").cast(pl.Float64),
    )


class PopulationWeightedTripPatternDistribution(FileAsset):
    """Persist the expected survey-weighted trip-pattern distribution."""

    def __init__(self, *, population_weighted_plan_steps: FileAsset, surveys, is_weekday: bool) -> None:
        project_folder = pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"])
        cache_path = (
            project_folder
            / "group_day_trips"
            / f"expected_trip_pattern_distribution_{'weekday' if is_weekday else 'weekend'}.parquet"
        )
        inputs = {
            "version": 2,
            "population_weighted_plan_steps": population_weighted_plan_steps,
            "surveys": surveys,
            "is_weekday": is_weekday,
        }
        self.population_weighted_plan_steps = population_weighted_plan_steps
        self.surveys = surveys
        self.is_weekday = is_weekday
        super().__init__(inputs, cache_path)

    def get_cached_asset(self) -> pl.LazyFrame:
        return pl.scan_parquet(self.cache_path)

    def create_and_get_asset(self) -> pl.LazyFrame:
        distribution = build_trip_pattern_distribution(
            self.population_weighted_plan_steps.get(),
            immobility_probabilities=get_survey_immobility_probabilities(
                self.surveys,
                is_weekday=self.is_weekday,
            ),
        )
        distribution.write_parquet(self.cache_path)
        return self.get_cached_asset()


class ObservedTripPatternDistribution(FileAsset):
    """Persist the observed final trip-pattern distribution."""

    def __init__(self, *, run: FileAsset, is_weekday: bool) -> None:
        project_folder = pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"])
        cache_path = (
            project_folder
            / "group_day_trips"
            / f"observed_trip_pattern_distribution_{'weekday' if is_weekday else 'weekend'}.parquet"
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
        distribution = build_trip_pattern_distribution(self.run.get_cached_asset()["plan_steps"])
        distribution.write_parquet(self.cache_path)
        return self.get_cached_asset()
