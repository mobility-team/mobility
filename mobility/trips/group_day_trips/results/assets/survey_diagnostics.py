from __future__ import annotations

import os
import pathlib

import polars as pl

from mobility.runtime.assets.file_asset import FileAsset
from mobility.trips.group_day_trips.evaluation.calibration_plan_steps import (
    distance_bin_expr,
    time_bin_expr,
)

from .person_metrics import RESULT_COLUMNS, SCOPE_COLUMNS

JOINT_DIMENSIONS = ["country", "activity", "mode", "distance_bin", "time_bin"]


def _with_string_dimensions(table: pl.DataFrame | pl.LazyFrame) -> pl.DataFrame | pl.LazyFrame:
    """Cast survey-reference dimensions to strings before joins and caches."""
    if isinstance(table, pl.LazyFrame):
        columns = table.collect_schema().names()
    else:
        columns = table.columns
    expressions = [
        pl.col(column).cast(pl.String)
        for column in JOINT_DIMENSIONS
        if column in columns
    ]
    if not expressions:
        return table
    return table.with_columns(expressions)


class SurveyReferenceComparison(FileAsset):
    """Persist the joint model-vs-survey comparison table."""

    def __init__(
        self,
        *,
        plan_steps: FileAsset,
        reference_plan_steps: FileAsset,
        transport_zones: FileAsset,
    ) -> None:
        self.plan_steps = plan_steps
        self.reference_plan_steps = reference_plan_steps
        self.transport_zones = transport_zones

        project_folder = pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"])
        cache_path = (
            project_folder
            / "group_day_trips"
            / "results"
            / "survey_reference_comparison.parquet"
        )
        inputs = {
            "version": 3,
            "plan_steps": plan_steps,
            "reference_plan_steps": reference_plan_steps,
            "transport_zones": transport_zones,
        }
        super().__init__(inputs, cache_path)

    def get_cached_asset(self) -> pl.LazyFrame:
        """Return the cached joint comparison as a lazy parquet scan."""
        return _with_string_dimensions(pl.scan_parquet(self.cache_path))

    def create_and_get_asset(self) -> pl.LazyFrame:
        """Build and cache the joint comparison table."""
        plan_steps = self._model_plan_steps()
        reference_plan_steps = self._reference_plan_steps()

        model_values = self._indicator_values(
            plan_steps,
            group_columns=SCOPE_COLUMNS + JOINT_DIMENSIONS,
            value_column="model_value",
        )
        reference_values = self._indicator_values(
            reference_plan_steps,
            group_columns=JOINT_DIMENSIONS,
            value_column="reference_value",
        )
        scope = plan_steps.select(SCOPE_COLUMNS).unique()
        reference_by_scope = scope.join(reference_values, how="cross")

        comparison = (
            model_values
            .join(
                reference_by_scope,
                on=SCOPE_COLUMNS + JOINT_DIMENSIONS + ["indicator"],
                how="full",
                coalesce=True,
            )
            .with_columns(
                reference_source=pl.lit("survey"),
                country=pl.col("country").cast(pl.String),
                activity=pl.col("activity").cast(pl.String),
                mode=pl.col("mode").cast(pl.String),
                distance_bin=pl.col("distance_bin").cast(pl.String),
                time_bin=pl.col("time_bin").cast(pl.String),
                model_value=pl.col("model_value").fill_null(0.0),
                reference_value=pl.col("reference_value").fill_null(0.0),
            )
            .select(
                [
                    "reference_source",
                    *SCOPE_COLUMNS,
                    *JOINT_DIMENSIONS,
                    "indicator",
                    "model_value",
                    "reference_value",
                ]
            )
        )

        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        comparison.collect(engine="streaming").write_parquet(self.cache_path)
        return self.get_cached_asset()

    def _model_plan_steps(self) -> pl.LazyFrame:
        """Return model plan steps with country and calibration bins."""
        plan_steps = self.plan_steps.get()
        required_columns = {
            "activity_seq_id",
            "home_zone_id",
            "activity",
            "mode",
            "distance",
            "time",
            "n_persons",
            *SCOPE_COLUMNS,
        }
        missing_columns = required_columns.difference(plan_steps.collect_schema().names())
        if missing_columns:
            raise ValueError(
                "Survey diagnostics model plan steps are missing columns: "
                f"{sorted(missing_columns)}."
            )

        transport_zones = self._transport_zone_countries()
        return (
            plan_steps
            .filter(pl.col("activity_seq_id") != 0)
            .rename({"home_zone_id": "transport_zone_id"})
            .join(transport_zones, on="transport_zone_id", how="inner")
            .with_columns(
                country=pl.col("country").cast(pl.String),
                activity=pl.col("activity").cast(pl.String),
                mode=pl.col("mode").cast(pl.String),
                distance=pl.col("distance").cast(pl.Float64),
                time=pl.col("time").cast(pl.Float64),
                n_persons=pl.col("n_persons").cast(pl.Float64),
                distance_bin=distance_bin_expr("distance"),
                time_bin=time_bin_expr("time"),
            )
        )

    def _reference_plan_steps(self) -> pl.LazyFrame:
        """Return survey-weighted reference plan steps with calibration bins."""
        reference_plan_steps = self.reference_plan_steps.get()
        if "travel_time" in reference_plan_steps.collect_schema().names():
            reference_plan_steps = reference_plan_steps.rename({"travel_time": "time"})
        required_columns = {
            "activity_seq_id",
            "home_zone_id",
            "country",
            "activity",
            "mode",
            "distance",
            "time",
            "n_persons",
        }
        missing_columns = required_columns.difference(reference_plan_steps.collect_schema().names())
        if missing_columns:
            raise ValueError(
                "Survey diagnostics reference plan steps are missing columns: "
                f"{sorted(missing_columns)}."
            )

        inner_zones = self._transport_zone_countries().select("transport_zone_id")
        return (
            reference_plan_steps
            .filter(pl.col("activity_seq_id") != 0)
            .rename({"home_zone_id": "transport_zone_id"})
            .join(inner_zones, on="transport_zone_id", how="inner")
            .with_columns(
                country=pl.col("country").cast(pl.String),
                activity=pl.col("activity").cast(pl.String),
                mode=pl.col("mode").cast(pl.String),
                distance=pl.col("distance").cast(pl.Float64),
                time=pl.col("time").cast(pl.Float64),
                n_persons=pl.col("n_persons").cast(pl.Float64),
                distance_bin=distance_bin_expr("distance"),
                time_bin=time_bin_expr("time"),
            )
        )

    def _transport_zone_countries(self) -> pl.LazyFrame:
        """Return inner transport zones and their country."""
        transport_zones = self.transport_zones.get().drop("geometry", axis=1, errors="ignore")
        study_area = self.transport_zones.study_area.get().drop("geometry", axis=1, errors="ignore")
        return (
            pl.DataFrame(transport_zones)
            .lazy()
            .filter(pl.col("is_inner_zone"))
            .select(["transport_zone_id", "local_admin_unit_id"])
            .join(
                pl.DataFrame(study_area)
                .lazy()
                .select([
                    "local_admin_unit_id",
                    pl.col("country").cast(pl.String),
                ]),
                on="local_admin_unit_id",
                how="left",
            )
            .select(["transport_zone_id", "country"])
        )

    @staticmethod
    def _indicator_values(
        plan_steps: pl.LazyFrame,
        *,
        group_columns: list[str],
        value_column: str,
    ) -> pl.LazyFrame:
        """Aggregate trip count, travel time, and travel distance."""
        return (
            plan_steps
            .group_by(group_columns)
            .agg(
                trip_count=pl.col("n_persons").sum(),
                travel_time=(pl.col("time") * pl.col("n_persons")).sum(),
                travel_distance=(pl.col("distance") * pl.col("n_persons")).sum(),
            )
            .unpivot(
                index=group_columns,
                on=["trip_count", "travel_time", "travel_distance"],
                variable_name="indicator",
                value_name=value_column,
            )
        )


class SurveyReferenceMarginal(FileAsset):
    """Persist one marginal view of the survey reference comparison."""

    def __init__(
        self,
        *,
        comparison: FileAsset,
        marginal_columns: list[str],
    ) -> None:
        self.comparison = comparison
        self.marginal_columns = list(marginal_columns)

        project_folder = pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"])
        marginal_part = "overall" if not self.marginal_columns else "-".join(self.marginal_columns)
        cache_path = (
            project_folder
            / "group_day_trips"
            / "results"
            / f"survey_reference_{marginal_part}.parquet"
        )
        inputs = {
            "version": 3,
            "comparison": comparison,
            "marginal_columns": tuple(self.marginal_columns),
        }
        super().__init__(inputs, cache_path)

    def get_cached_asset(self) -> pl.DataFrame:
        """Return the cached marginal comparison."""
        return _with_string_dimensions(pl.read_parquet(self.cache_path))

    def create_and_get_asset(self) -> pl.DataFrame:
        """Build and cache the marginal comparison."""
        comparison = self.comparison.get()
        group_columns = ["country"] + list(self.marginal_columns)
        for column in self.marginal_columns:
            if column not in JOINT_DIMENSIONS:
                raise ValueError(
                    "Survey diagnostic marginals should use one of: "
                    f"{JOINT_DIMENSIONS}. Received `{column}`."
                )

        per_replication = (
            comparison
            .group_by(["reference_source", *SCOPE_COLUMNS, *group_columns, "indicator"])
            .agg(
                model_value=pl.col("model_value").sum(),
                reference_value=pl.col("reference_value").sum(),
            )
            .with_columns(
                gap=pl.col("model_value") - pl.col("reference_value"),
                relative_gap=(
                    (pl.col("model_value") - pl.col("reference_value"))
                    / pl.col("reference_value").abs().clip(1e-12)
                ),
            )
        )
        output_columns = ["reference_source", *RESULT_COLUMNS, *group_columns, "indicator"]
        marginal = (
            per_replication
            .pipe(_with_string_dimensions)
            .group_by(output_columns)
            .agg(
                pl.col("model_value").mean(),
                pl.col("model_value").std().alias("model_value_std"),
                pl.col("reference_value").mean(),
                pl.col("gap").mean(),
                pl.col("gap").std().alias("gap_std"),
                pl.col("relative_gap").mean(),
                pl.col("replication").n_unique().cast(pl.UInt32).alias("n_replications"),
            )
            .sort(output_columns)
            .collect(engine="streaming")
        )

        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        marginal.write_parquet(self.cache_path)
        return marginal
