from __future__ import annotations

import polars as pl

from .stable_key_index import StableKeyIndex


PLAN_KEY_COLS = ["demand_group_id", "activity_seq_id", "time_seq_id", "dest_seq_id", "mode_seq_id"]


def add_plan_id(
    frame: pl.DataFrame | pl.LazyFrame,
    *,
    previous_index: pl.DataFrame | None,
) -> tuple[pl.DataFrame | pl.LazyFrame, pl.DataFrame]:
    """Annotate one stable plan_id per unique full plan state.

    The plan index is cached by the iteration state asset. This keeps it in the
    normal FileAsset DAG instead of mutating one shared index file as a hidden
    side effect.
    """
    return StableKeyIndex(
        key_cols=PLAN_KEY_COLS,
        index_col="plan_id",
        first_new_id=0,
    ).extend(frame, previous_index)
