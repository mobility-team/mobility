from __future__ import annotations

import polars as pl


DEMAND_UNIT_COLS = ["demand_group_id", "demand_subgroup_id"]


def with_demand_subgroup_id(frame: pl.DataFrame | pl.LazyFrame | None) -> pl.DataFrame | pl.LazyFrame | None:
    """Return a frame with the default demand subgroup column."""
    if frame is None:
        return None
    schema = frame.schema if isinstance(frame, pl.DataFrame) else frame.collect_schema()
    if "demand_subgroup_id" in schema:
        return frame
    return frame.with_columns(pl.lit(0, dtype=pl.UInt32).alias("demand_subgroup_id"))


def demand_unit_hash(other_columns: list[str], *, seed: int) -> pl.Expr:
    """Return a stable hash that keeps subgroup 0 on the old random stream."""
    old_key = ["demand_group_id"] + other_columns
    new_key = DEMAND_UNIT_COLS + other_columns
    return (
        pl.when(pl.col("demand_subgroup_id") == 0)
        .then(pl.struct(old_key).hash(seed=seed))
        .otherwise(pl.struct(new_key).hash(seed=seed))
    )


def split_large_demand_groups(
    demand_groups: pl.DataFrame,
    *,
    max_persons_per_demand_subgroup: int | None,
) -> pl.DataFrame:
    """Split high-weight demand groups into deterministic demand subgroups.

    When a maximum subgroup size is configured, each demand group is expanded
    into ``ceil(n_persons / max_persons_per_demand_subgroup)`` rows. The
    represented persons are divided evenly across those rows, so the total
    population weight is unchanged.

    The split happens before activity, destination, and mode sampling. For
    example, a demand group representing 120 persons with a maximum subgroup
    size of 50 becomes three 40-person subgroups that can draw different day
    plans, instead of one 120-person group sharing a single sampled plan.
    """
    demand_groups = with_demand_subgroup_id(demand_groups)
    if max_persons_per_demand_subgroup is None:
        return demand_groups

    max_persons = int(max_persons_per_demand_subgroup)
    return (
        demand_groups
        .with_columns(
            n_subgroups=(
                pl.col("n_persons")
                .cast(pl.Float64)
                .truediv(pl.lit(float(max_persons)))
                .ceil()
                .clip(1)
                .cast(pl.UInt32)
            )
        )
        .with_columns(
            demand_subgroup_id=pl.int_ranges(0, pl.col("n_subgroups")).cast(pl.List(pl.UInt32)),
            n_persons=pl.col("n_persons").cast(pl.Float64) / pl.col("n_subgroups").cast(pl.Float64),
        )
        .explode("demand_subgroup_id")
        .drop("n_subgroups")
    )
