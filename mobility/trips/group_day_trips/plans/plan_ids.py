from __future__ import annotations

import pathlib

import polars as pl

from .sequence_index import add_plan_index


PLAN_KEY_COLS = ["demand_group_id", "activity_seq_id", "dest_seq_id", "mode_seq_id"]


def add_plan_id(
    frame: pl.DataFrame | pl.LazyFrame,
    *,
    index_folder: pathlib.Path,
) -> pl.DataFrame | pl.LazyFrame:
    """Annotate one stable persisted plan_id per unique full plan state."""
    return add_plan_index(
        frame,
        index_folder=index_folder,
        index_col_name="plan_id",
        key_cols=PLAN_KEY_COLS,
    )
