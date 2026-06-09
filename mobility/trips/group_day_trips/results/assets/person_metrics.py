from __future__ import annotations

import polars as pl
from mobility.trips.group_day_trips.evaluation.calibration_plan_steps import (
    distance_bin_expr,
    time_bin_expr,
)


RESULT_COLUMNS = ["scenario", "sensitivity_case", "day_type", "iteration"]
SCOPE_COLUMNS = RESULT_COLUMNS + ["replication"]
DEMAND_GROUP_COLUMNS = ["home_zone_id", "csp", "n_cars"]


def add_demand_group_columns(
    plan_steps: pl.LazyFrame,
    demand_groups: pl.LazyFrame,
    columns: list[str],
) -> pl.LazyFrame:
    """Add demand-group columns, such as home zone, when plan steps only carry ids."""
    plan_schema = plan_steps.collect_schema().names()
    demand_schema = demand_groups.collect_schema().names()
    missing_columns = [
        column
        for column in columns
        if column not in plan_schema and column in demand_schema
    ]
    if not missing_columns:
        return plan_steps

    join_columns = SCOPE_COLUMNS + ["demand_group_id"]
    missing_join_columns = [
        column
        for column in join_columns
        if column not in plan_schema or column not in demand_schema
    ]
    if missing_join_columns:
        return plan_steps

    return plan_steps.join(
        demand_groups
        .select(join_columns + missing_columns)
        .unique(),
        on=join_columns,
        how="left",
    )


def with_analysis_dimensions(plan_steps: pl.LazyFrame, group_columns: list[str]) -> pl.LazyFrame:
    """Add and normalize optional analysis dimensions requested by a result table."""
    schema = plan_steps.collect_schema().names()
    expressions: list[pl.Expr] = []

    if "home_zone_id" in group_columns and "home_zone_id" in schema:
        expressions.append(pl.col("home_zone_id").cast(pl.String))
    if "origin_zone_id" in group_columns and "from" in schema:
        expressions.append(pl.col("from").cast(pl.String).alias("origin_zone_id"))
    if "destination_zone_id" in group_columns and "to" in schema:
        expressions.append(pl.col("to").cast(pl.String).alias("destination_zone_id"))

    for column in ("activity", "mode"):
        if column in group_columns and column in schema:
            expressions.append(pl.col(column).cast(pl.String))

    if "distance_bin" in group_columns:
        if "distance_bin" in schema:
            expressions.append(pl.col("distance_bin").cast(pl.String))
        elif "distance" in schema:
            expressions.append(distance_bin_expr("distance").alias("distance_bin"))

    if "time_bin" in group_columns:
        if "time_bin" in schema:
            expressions.append(pl.col("time_bin").cast(pl.String))
        elif "time" in schema:
            expressions.append(time_bin_expr("time").alias("time_bin"))

    if not expressions:
        return plan_steps
    return plan_steps.with_columns(expressions)


def inner_zone_ids(transport_zones) -> pl.DataFrame:
    """Return transport-zone ids marked as inner zones."""
    return (
        pl.DataFrame(transport_zones.get().drop("geometry", axis=1, errors="ignore"))
        .filter(pl.col("is_inner_zone"))
        .select(pl.col("transport_zone_id").cast(pl.String))
        .unique()
    )


def filter_plan_steps_to_inner_zone_residents(
    plan_steps: pl.LazyFrame,
    transport_zones,
) -> pl.LazyFrame:
    """Keep only plan steps whose home zone is an inner zone."""
    return (
        plan_steps
        .with_columns(pl.col("home_zone_id").cast(pl.String))
        .join(
            inner_zone_ids(transport_zones).lazy(),
            left_on="home_zone_id",
            right_on="transport_zone_id",
            how="inner",
        )
    )


def filter_demand_groups_to_inner_zone_residents(
    demand_groups: pl.LazyFrame,
    transport_zones,
) -> pl.LazyFrame:
    """Keep only demand groups whose home zone is an inner zone."""
    return (
        demand_groups
        .with_columns(pl.col("home_zone_id").cast(pl.String))
        .join(
            inner_zone_ids(transport_zones).lazy(),
            left_on="home_zone_id",
            right_on="transport_zone_id",
            how="inner",
        )
    )


def complete_missing_replication_groups(
    values: pl.LazyFrame,
    *,
    group_columns: list[str],
    value_column: str,
) -> pl.LazyFrame:
    """Fill missing seed/group rows with zero before averaging over seeds."""
    if not group_columns:
        return values
    scope_index = values.select(SCOPE_COLUMNS).unique()
    group_index_columns = RESULT_COLUMNS + list(group_columns)
    group_index = values.select(group_index_columns).unique()
    full_index = scope_index.join(group_index, on=RESULT_COLUMNS, how="inner")
    return (
        full_index
        .join(values, on=SCOPE_COLUMNS + list(group_columns), how="left")
        .with_columns(pl.col(value_column).fill_null(0.0))
    )
