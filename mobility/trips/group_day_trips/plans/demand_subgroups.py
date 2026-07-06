from __future__ import annotations

import polars as pl


DEMAND_UNIT_COLS = ["demand_group_id", "demand_subgroup_id"]
DEMAND_UNIT_SCHEMA = {
    "demand_group_id": pl.UInt32,
    "demand_subgroup_id": pl.UInt32,
}


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
    """Split high-weight demand groups into deterministic stochastic subgroups."""
    if "demand_subgroup_id" in demand_groups.columns:
        raise ValueError(
            "`split_large_demand_groups()` expects raw demand groups without "
            "`demand_subgroup_id` because it owns subgroup creation."
        )

    demand_groups = demand_groups.with_columns(
        demand_subgroup_id=pl.lit(0, dtype=pl.UInt32)
    )
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
