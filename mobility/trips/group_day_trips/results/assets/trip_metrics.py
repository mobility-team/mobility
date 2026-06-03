from __future__ import annotations

import os
import pathlib

import polars as pl

from mobility.runtime.assets.file_asset import FileAsset

from .person_metrics import (
    DEMAND_GROUP_COLUMNS,
    RESULT_COLUMNS,
    SCOPE_COLUMNS,
    add_demand_group_columns,
    complete_missing_replication_groups,
    filter_demand_groups_to_inner_zone_residents,
    filter_plan_steps_to_inner_zone_residents,
    with_analysis_dimensions,
)


class TripCountMetric(FileAsset):
    """Persist weighted trip counts across selected replications."""

    def __init__(
        self,
        *,
        plan_steps: FileAsset,
        demand_groups: FileAsset,
        transport_zones,
        output_column: str,
        group_columns: list[str],
        per_person: bool,
        inner_zone_residents_only: bool = False,
    ) -> None:
        self.plan_steps = plan_steps
        self.demand_groups = demand_groups
        self.transport_zones = transport_zones
        self.output_column = output_column
        self.group_columns = list(group_columns)
        self.per_person = per_person
        self.inner_zone_residents_only = inner_zone_residents_only

        project_folder = pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"])
        group_part = "all" if not self.group_columns else "-".join(self.group_columns)
        scope_part = "inner-zone-residents" if inner_zone_residents_only else "all-residents"
        cache_path = (
            project_folder
            / "group_day_trips"
            / "results"
            / f"{output_column}_by_{group_part}_{scope_part}.parquet"
        )
        inputs = {
            "version": 4,
            "plan_steps": plan_steps,
            "demand_groups": demand_groups,
            "transport_zones": transport_zones,
            "output_column": output_column,
            "group_columns": tuple(self.group_columns),
            "per_person": per_person,
            "inner_zone_residents_only": inner_zone_residents_only,
        }
        super().__init__(inputs, cache_path)

    def get_cached_asset(self) -> pl.DataFrame:
        """Return the cached metric table."""
        return pl.read_parquet(self.cache_path)

    def create_and_get_asset(self) -> pl.DataFrame:
        """Build and cache the trip-count table."""
        needed_demand_columns = [
            column
            for column in self.group_columns
            if column in DEMAND_GROUP_COLUMNS
        ]
        if self.inner_zone_residents_only and "home_zone_id" not in needed_demand_columns:
            needed_demand_columns.append("home_zone_id")
        plan_steps = with_analysis_dimensions(
            add_demand_group_columns(
                self.plan_steps.get(),
                self.demand_groups.get(),
                needed_demand_columns,
            ),
            self.group_columns,
        )
        if self.inner_zone_residents_only:
            plan_steps = filter_plan_steps_to_inner_zone_residents(
                plan_steps,
                self.transport_zones,
            )
        group_columns = SCOPE_COLUMNS + list(self.group_columns)
        required_plan_columns = set(group_columns + ["activity_seq_id", "n_persons"])
        missing_plan_columns = required_plan_columns.difference(plan_steps.collect_schema().names())
        if missing_plan_columns:
            raise ValueError(
                "TripCountMetric plan steps are missing columns: "
                f"{sorted(missing_plan_columns)}."
            )

        per_replication = (
            plan_steps
            .filter(pl.col("activity_seq_id") != 0)
            .group_by(group_columns)
            .agg(value=pl.col("n_persons").cast(pl.Float64).sum())
        )
        per_replication = complete_missing_replication_groups(
            per_replication,
            group_columns=self.group_columns,
            value_column="value",
        )
        if self.per_person:
            per_replication = self._divide_by_population(per_replication)

        output_columns = RESULT_COLUMNS + list(self.group_columns)
        metric = (
            per_replication
            .group_by(output_columns)
            .agg(
                pl.col("value").mean().alias(self.output_column),
                pl.col("value").std().alias(f"{self.output_column}_std"),
                pl.col("replication").n_unique().cast(pl.UInt32).alias("n_replications"),
            )
            .sort(output_columns)
            .collect(engine="streaming")
        )

        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        metric.write_parquet(self.cache_path)
        return metric

    def _divide_by_population(self, trips: pl.LazyFrame) -> pl.LazyFrame:
        """Divide trip counts by the matching population denominator."""
        demand_groups = self.demand_groups.get()
        if self.inner_zone_residents_only:
            demand_groups = filter_demand_groups_to_inner_zone_residents(
                demand_groups,
                self.transport_zones,
            )
        demand_schema = demand_groups.collect_schema().names()
        population_group_columns = [
            column
            for column in self.group_columns
            if column in demand_schema
        ]
        denominator_columns = SCOPE_COLUMNS + population_group_columns
        missing_demand_columns = set(denominator_columns + ["n_persons"]).difference(demand_schema)
        if missing_demand_columns:
            raise ValueError(
                "TripCountMetric demand groups are missing columns: "
                f"{sorted(missing_demand_columns)}."
            )

        population = (
            demand_groups
            .group_by(denominator_columns)
            .agg(n_persons=pl.col("n_persons").cast(pl.Float64).sum())
        )
        return (
            trips
            .join(population, on=denominator_columns, how="left")
            .with_columns(value=pl.col("value") / pl.col("n_persons").clip(1e-12))
            .select(denominator_columns + [
                column
                for column in self.group_columns
                if column not in population_group_columns
            ] + ["value"])
        )
