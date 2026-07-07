from __future__ import annotations

import polars as pl

from .stable_key_index import StableKeyIndex
from .demand_subgroups import DEMAND_UNIT_COLS, DEMAND_UNIT_SCHEMA


PLAN_KEY_COLS = DEMAND_UNIT_COLS + ["activity_seq_id", "time_seq_id", "dest_seq_id", "mode_seq_id"]
PLAN_KEY_SCHEMA = {
    **DEMAND_UNIT_SCHEMA,
    "activity_seq_id": pl.UInt32,
    "time_seq_id": pl.UInt32,
    "dest_seq_id": pl.UInt32,
    "mode_seq_id": pl.UInt32,
}
PLAN_STEP_KEY_SCHEMA = {
    **PLAN_KEY_SCHEMA,
    "seq_step_index": pl.UInt8,
}


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
