from __future__ import annotations

import polars as pl

from mobility.runtime.population_segments import PopulationSegment

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
    """Split high-weight demand groups into deterministic demand subgroups.

    This function owns subgroup creation. It starts each raw demand group at
    subgroup 0, then expands large groups when a maximum subgroup size is set.
    The represented persons are divided evenly across the generated subgroups,
    so the total population weight is unchanged.
    """
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


def split_demand_groups(
    demand_groups: pl.DataFrame,
    *,
    population_segments: list[PopulationSegment],
    max_persons_per_demand_subgroup: int | None,
) -> pl.DataFrame:
    """Create reusable segment shares, then enforce the subgroup-size limit."""
    if "demand_subgroup_id" in demand_groups.columns:
        raise ValueError(
            "`split_demand_groups()` expects raw demand groups because it owns "
            "subgroup creation."
        )

    result = demand_groups.with_columns(
        population_segments=pl.lit([], dtype=pl.List(pl.String)),
        _segment_order=pl.lit("", dtype=pl.String),
    )

    for segment in population_segments:
        missing = sorted(set(segment.selector) - set(result.columns))
        if missing:
            raise ValueError(
                f"Population segment '{segment.name}' uses unavailable demand-group "
                f"columns: {', '.join(missing)}."
            )

        matches = pl.lit(True)
        for column, values in segment.selector.items():
            matches = matches & pl.col(column).cast(pl.String).is_in(
                [str(value) for value in values]
            )

        selected_segments = pl.concat_list(
            "population_segments",
            pl.lit([segment.name], dtype=pl.List(pl.String)),
        )
        if segment.share == 1.0:
            result = result.with_columns(
                population_segments=pl.when(matches)
                .then(selected_segments)
                .otherwise(pl.col("population_segments"))
            )
            continue

        selected = (
            result.filter(matches)
            .with_columns(
                n_persons=pl.col("n_persons") * segment.share,
                population_segments=selected_segments,
                _segment_order=pl.col("_segment_order") + pl.lit("0"),
            )
        )
        complement = result.with_columns(
            n_persons=pl.when(matches)
            .then(pl.col("n_persons") * (1.0 - segment.share))
            .otherwise(pl.col("n_persons")),
            _segment_order=pl.when(matches)
            .then(pl.col("_segment_order") + pl.lit("1"))
            .otherwise(pl.col("_segment_order")),
        )
        result = pl.concat([selected, complement], how="vertical").filter(
            pl.col("n_persons") > 0.0
        )

    result = result.sort(["demand_group_id", "_segment_order"])
    if max_persons_per_demand_subgroup is not None:
        max_persons = float(max_persons_per_demand_subgroup)
        result = (
            result.with_columns(
                _size_parts=(pl.col("n_persons") / max_persons)
                .ceil()
                .clip(1)
                .cast(pl.UInt32)
            )
            .with_columns(
                _size_part=pl.int_ranges(0, pl.col("_size_parts")),
                n_persons=pl.col("n_persons") / pl.col("_size_parts"),
            )
            .explode("_size_part")
            .drop("_size_parts")
        )
    else:
        result = result.with_columns(_size_part=pl.lit(0, dtype=pl.UInt32))

    return (
        result.sort(["demand_group_id", "_segment_order", "_size_part"])
        .with_columns(
            demand_subgroup_id=(
                pl.col("demand_group_id").cum_count().over("demand_group_id") - 1
            ).cast(pl.UInt32)
        )
        .drop("_segment_order", "_size_part")
    )
