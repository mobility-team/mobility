"""Shared transition-events schema and column list.

Used by:
- `StateUpdater` when producing transition events.
- `PopulationTrips` when writing empty transition tables.
- `transition_metrics` when validating cached transitions.
"""

import polars as pl


TRANSITION_EVENT_SCHEMA: dict[str, pl.DataType] = {
    "iteration": pl.UInt32,
    "demand_group_id": pl.UInt32,
    "motive_seq_id": pl.UInt32,
    "dest_seq_id": pl.UInt32,
    "mode_seq_id": pl.UInt32,
    "motive_seq_id_trans": pl.UInt32,
    "dest_seq_id_trans": pl.UInt32,
    "mode_seq_id_trans": pl.UInt32,
    "n_persons_moved": pl.Float64,
    "utility_prev_from": pl.Float64,
    "utility_prev_to": pl.Float64,
    "utility_from": pl.Float64,
    "utility_to": pl.Float64,
    "is_self_transition": pl.Boolean,
    "trip_count_from": pl.Float64,
    "activity_time_from": pl.Float64,
    "travel_time_from": pl.Float64,
    "distance_from": pl.Float64,
    "steps_from": pl.String,
    "trip_count_to": pl.Float64,
    "activity_time_to": pl.Float64,
    "travel_time_to": pl.Float64,
    "distance_to": pl.Float64,
    "steps_to": pl.String,
}


TRANSITION_EVENT_COLUMNS: list[str] = list(TRANSITION_EVENT_SCHEMA.keys())

