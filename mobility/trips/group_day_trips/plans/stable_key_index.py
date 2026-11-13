from __future__ import annotations

import pathlib
from dataclasses import dataclass
from typing import Any

import polars as pl

from mobility.trips.group_day_trips.core.performance_trace import trace_simulation_step


def add_ordered_key_hashes(
    rows: pl.DataFrame,
    *,
    group_cols: list[str],
    order_col: str,
    step_content: pl.Expr,
    key_prefix: str,
) -> pl.DataFrame:
    """Build a compact, integer key for each ordered sequence of step content.

    Two independently seeded 64-bit hashes make accidental collisions
    negligible while keeping sequence joins on fixed-width integers. The
    original values stay in their typed columns; no delimited text key is
    assembled.
    """
    step_hash_1 = f"__{key_prefix}_step_hash_1"
    step_hash_2 = f"__{key_prefix}_step_hash_2"
    sequence_hashes_1 = f"__{key_prefix}_sequence_hashes_1"
    sequence_hashes_2 = f"__{key_prefix}_sequence_hashes_2"
    key_col_1 = f"{key_prefix}_1"
    key_col_2 = f"{key_prefix}_2"
    ordered_step_content = pl.struct(
        pl.col(order_col),
        step_content.alias("step_content"),
    )

    return (
        rows.with_columns(
            ordered_step_content.hash(seed=0).alias(step_hash_1),
            ordered_step_content.hash(seed=1).alias(step_hash_2),
        )
        .group_by(group_cols)
        .agg(
            pl.col(step_hash_1).sort_by(order_col).alias(sequence_hashes_1),
            pl.col(step_hash_2).sort_by(order_col).alias(sequence_hashes_2),
        )
        .with_columns(
            pl.col(sequence_hashes_1)
            .hash(seed=2)
            .alias(key_col_1),
            pl.col(sequence_hashes_2)
            .hash(seed=3)
            .alias(key_col_2),
        )
        .drop(sequence_hashes_1, sequence_hashes_2)
    )


@dataclass(frozen=True)
class StableKeyIndex:
    """Maintain stable integer ids for sorted key columns.

    The model uses small integer ids for destination, mode, and plan keys. The
    ids must be stable across iterations, so each iteration receives the previous
    index, appends unseen keys in sorted order, and writes the updated index.
    """

    key_cols: list[str]
    index_col: str
    first_new_id: int = 1

    @trace_simulation_step("extend stable index")
    def extend(
        self,
        rows: pl.DataFrame | pl.LazyFrame,
        previous_index: pl.DataFrame | None,
        *,
        additional_keys: pl.DataFrame | None = None,
    ) -> tuple[pl.DataFrame | pl.LazyFrame, pl.DataFrame]:
        """Attach ids to rows and return the updated key-index table."""
        input_was_lazy = isinstance(rows, pl.LazyFrame)
        # Keep the potentially much wider input lazy until its caller needs
        # it. Existing keys need not be sorted again: only unseen keys receive
        # new IDs, in their usual sorted order.
        key_query = rows.select(self.key_cols).unique()
        key_frame = key_query.collect(engine="streaming") if input_was_lazy else key_query
        if additional_keys is not None:
            # Assign IDs to the first set before the additional keys, just as
            # two successive extensions would, while validating the index once.
            extra = additional_keys.select(self.key_cols).unique().join(
                key_frame, on=self.key_cols, how="anti"
            )
            key_frame = pl.concat([
                key_frame.with_columns(_key_order=pl.lit(0, dtype=pl.UInt8)),
                extra.with_columns(_key_order=pl.lit(1, dtype=pl.UInt8)),
            ])
        if key_frame.select(self.key_cols).null_count().row(0) != tuple([0] * len(self.key_cols)):
            raise ValueError(f"Index keys cannot contain nulls: {self.key_cols}")

        if previous_index is None:
            previous_index = self._empty_from(key_frame)
        else:
            previous_index = previous_index.select(self.key_cols + [self.index_col])
            self.validate(previous_index)

        missing_keys = key_frame.join(
            previous_index.select(self.key_cols),
            on=self.key_cols,
            how="anti",
        ).sort((["_key_order"] if additional_keys is not None else []) + self.key_cols)
        if additional_keys is not None:
            missing_keys = missing_keys.drop("_key_order")

        max_index = previous_index[self.index_col].max()
        next_index = self.first_new_id if max_index is None else int(max_index) + 1
        missing_index = (
            missing_keys
            .with_row_index("__new_index")
            .with_columns(
                (pl.col("__new_index") + next_index)
                .cast(pl.UInt32)
                .alias(self.index_col)
            )
            .drop("__new_index")
        )
        updated_index = (
            pl.concat([previous_index, missing_index], how="vertical_relaxed")
            .with_columns(pl.col(self.index_col).cast(pl.UInt32))
            .sort(self.index_col)
        )
        # A validated UInt32 index stays unique: missing_keys is distinct and
        # excludes existing keys, while its new IDs are consecutive above the
        # previous maximum. Keep the full check for other input types (casting
        # can merge distinct values) and when the assigned IDs may overflow.
        if (
            previous_index.schema[self.index_col] != pl.UInt32
            or next_index + missing_keys.height > 2**32
        ):
            self.validate(updated_index)

        if input_was_lazy:
            indexed_rows = rows.drop(self.index_col, strict=False).join(
                updated_index.lazy(), on=self.key_cols, how="inner"
            )
        else:
            indexed_rows = rows.drop(self.index_col, strict=False).join(
                updated_index, on=self.key_cols, how="inner"
            )
        return indexed_rows, updated_index

    def extend_and_cache(
        self,
        rows: pl.DataFrame | pl.LazyFrame,
        *,
        previous_asset: Any,
        index_path: pathlib.Path,
    ) -> tuple[pl.DataFrame | pl.LazyFrame, pl.DataFrame]:
        """Extend the index from a previous asset and write the updated table."""
        previous_index = self.read_from_asset(previous_asset)
        indexed_rows, updated_index = self.extend(rows, previous_index)
        self.write(updated_index, index_path)
        return indexed_rows, updated_index

    def read_from_asset(self, previous_asset: Any) -> pl.DataFrame | None:
        """Read the previous index after ensuring the upstream asset exists."""
        if previous_asset is None:
            return None
        previous_asset.get()
        return previous_asset.get_index()

    def write(self, index: pl.DataFrame, index_path: pathlib.Path) -> None:
        """Write an index table to disk."""
        pathlib.Path(index_path).parent.mkdir(parents=True, exist_ok=True)
        index.write_parquet(index_path)

    def validate(self, index: pl.DataFrame) -> None:
        """Fail fast when an index contains duplicate keys or duplicate ids."""
        if index.select(self.key_cols).n_unique() != index.height:
            raise ValueError(
                f"Index '{self.index_col}' contains duplicate keys for columns {self.key_cols}."
            )
        if index[self.index_col].n_unique() != index.height:
            raise ValueError(f"Index '{self.index_col}' contains duplicate ids.")

    def _empty_from(self, rows: pl.DataFrame) -> pl.DataFrame:
        """Create an empty index table with the same key dtypes as input rows."""
        return (
            rows.select(self.key_cols)
            .head(0)
            .with_columns(pl.lit(None, dtype=pl.UInt32).alias(self.index_col))
            .head(0)
        )
