"""Give modeled schedules stable ids while keeping their survey reference."""

from dataclasses import dataclass

import polars as pl

from mobility.trips.group_day_trips.core.performance_trace import trace_simulation_step

from .plan_ids import PLAN_KEY_COLS
from .stable_key_index import StableKeyIndex, add_ordered_key_hashes


@dataclass
class TimeSequenceIndexCache:
    """Reuse an index only for the exact schedule table that produced it."""

    schedules: pl.DataFrame | None = None
    index: pl.DataFrame | None = None


@trace_simulation_step("index time sequences")
def assign_time_sequence_ids(
    steps: pl.DataFrame,
    schedule_steps: pl.DataFrame,
    *,
    index_cache: TimeSequenceIndexCache | None = None,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """Index complete timed schedules at the same second precision as surveys.

    The returned mapping links each incoming plan key to its new time id.
    The schedule table includes original survey schedules and modeled variants.
    """
    time_columns = ["departure_time", "arrival_time", "next_departure_time", "duration_per_pers"]
    candidate_id_columns = [
        column for column in ("candidate_id_1", "candidate_id_2") if column in steps.columns
    ]
    group_columns = PLAN_KEY_COLS + candidate_id_columns
    catalog = schedule_steps.with_columns(
        duration_per_pers=((pl.col("duration_per_pers").cast(pl.Float64) * 3600.0).round(0) / 3600.0)
        .cast(schedule_steps.schema["duration_per_pers"])
    )
    if "reference_time_seq_id" not in catalog.columns:
        catalog = catalog.with_columns(reference_time_seq_id=pl.col("time_seq_id"))

    # Include duration as well as clock times: modeled duration floors and
    # rounding can leave different scored durations at the same clock times.
    # Ordered integer hashes keep these joins compact without building text
    # representations for every schedule step.
    # Hash seconds directly instead of keeping four additional full-length
    # columns alongside the scored steps during schedule indexing.
    step_content = pl.struct(
        pl.col("activity").cast(pl.String),
        *[
            (pl.col(column).cast(pl.Float64) * 3600.0)
            .round(0).cast(pl.Int64).alias(f"_{column}_seconds")
            for column in time_columns
        ],
    )
    key_columns = ["time_key_1", "time_key_2"]
    if index_cache is not None and index_cache.schedules is schedule_steps and index_cache.index is not None:
        previous_index = index_cache.index
    else:
        previous_index = add_ordered_key_hashes(
            catalog.select(["time_seq_id", "seq_step_index", "activity"] + time_columns),
            group_cols=["time_seq_id"],
            order_col="seq_step_index",
            step_content=step_content,
            key_prefix="time_key",
        )
    schedules = add_ordered_key_hashes(
        steps.select(group_columns + ["seq_step_index", "activity"] + time_columns),
        group_cols=group_columns,
        order_col="seq_step_index",
        step_content=step_content,
        key_prefix="time_key",
    ).rename({"time_seq_id": "previous_time_seq_id"})
    schedules, updated_index = StableKeyIndex(key_columns, "time_seq_id").extend(
        schedules,
        previous_index,
    )
    mapping = schedules.drop(key_columns)

    # Carry the survey reference across repeated timing changes. A shared plan
    # can arrive from another scenario with different local time ids.
    reference_was_present = "reference_time_seq_id" in steps.columns
    if not reference_was_present:
        steps = steps.join(
            catalog.select("activity_seq_id", "time_seq_id", "reference_time_seq_id").unique(),
            on=["activity_seq_id", "time_seq_id"],
            how="left",
        )
    if steps["reference_time_seq_id"].null_count():
        raise ValueError("A modeled schedule has no matching survey reference.")
    updated = (
        steps.rename({"time_seq_id": "previous_time_seq_id"})
        .join(mapping, on=["previous_time_seq_id" if col == "time_seq_id" else col for col in group_columns])
        .drop("previous_time_seq_id")
        .with_columns([
            ((pl.col(column).cast(pl.Float64) * 3600.0).round(0) / 3600.0)
            .cast(steps.schema[column]).alias(column)
            for column in time_columns
        ])
    )

    # Store one compact schedule, independent of zone, destination, and mode.
    # Eligibility lookup needs the activity flags as well as the timed steps.
    # StableKeyIndex assigns new ids above the previous maximum. Use that
    # boundary to select new schedules without joining every modeled step to
    # the complete catalogue. Only schedule columns can be stored there.
    schedule_columns = [
        column for column in catalog.columns
        if column in updated.columns and column != "step_count"
    ]
    previous_max_id = previous_index["time_seq_id"].max()
    previous_max_id = int(previous_max_id) if previous_max_id is not None else -1
    new_schedules = (
        updated.select(schedule_columns)
        .filter(pl.col("time_seq_id") > previous_max_id)
        .join(catalog.select("activity", "is_anchor").unique(), on="activity", how="left")
        .with_columns(step_count=pl.col("seq_step_index").max().over("time_seq_id"))
        .select(catalog.columns)
        .sort(["time_seq_id", "reference_time_seq_id", "seq_step_index"])
        .unique(subset=["time_seq_id", "seq_step_index"], keep="first")
    )
    catalog = pl.concat([catalog, new_schedules], how="vertical_relaxed").sort(
        ["time_seq_id", "seq_step_index"]
    )
    if index_cache is not None:
        index_cache.schedules = catalog
        index_cache.index = updated_index
    if not reference_was_present:
        updated = updated.drop("reference_time_seq_id")
    return updated, catalog, mapping
