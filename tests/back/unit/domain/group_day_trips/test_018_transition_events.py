import polars as pl

from mobility.trips.group_day_trips.transitions.transition_events import (
    add_transition_plan_details,
)
from mobility.trips.group_day_trips.transitions.transition_schema import (
    TRANSITION_EVENT_SCHEMA,
)


def test_transition_plan_details_match_cache_schema_with_float32_activity_times():
    transition_events = pl.DataFrame(
        {
            "iteration": [1],
            "demand_group_id": [1],
            "demand_subgroup_id": [0],
            "activity_seq_id": [10],
            "time_seq_id": [100],
            "dest_seq_id": [1000],
            "mode_seq_id": [10000],
            "activity_seq_id_trans": [11],
            "time_seq_id_trans": [101],
            "dest_seq_id_trans": [1001],
            "mode_seq_id_trans": [10001],
            "n_persons_moved": [3.0],
            "utility_prev_from": [1.0],
            "utility_prev_to": [1.5],
            "utility_from": [2.0],
            "utility_to": [2.5],
            "tau_transition": [0.1],
            "q_transition": [0.2],
            "adjustment_factor": [1.0],
            "is_self_transition": [False],
        },
        schema={
            "iteration": pl.UInt32,
            "demand_group_id": pl.UInt32,
            "demand_subgroup_id": pl.UInt32,
            "activity_seq_id": pl.UInt32,
            "time_seq_id": pl.UInt32,
            "dest_seq_id": pl.UInt32,
            "mode_seq_id": pl.UInt32,
            "activity_seq_id_trans": pl.UInt32,
            "time_seq_id_trans": pl.UInt32,
            "dest_seq_id_trans": pl.UInt32,
            "mode_seq_id_trans": pl.UInt32,
            "n_persons_moved": pl.Float64,
            "utility_prev_from": pl.Float64,
            "utility_prev_to": pl.Float64,
            "utility_from": pl.Float64,
            "utility_to": pl.Float64,
            "tau_transition": pl.Float64,
            "q_transition": pl.Float64,
            "adjustment_factor": pl.Float64,
            "is_self_transition": pl.Boolean,
        },
    ).lazy()
    possible_plan_steps = pl.DataFrame(
        {
            "demand_group_id": [1, 1],
            "demand_subgroup_id": [0, 0],
            "activity_seq_id": [10, 11],
            "time_seq_id": [100, 101],
            "dest_seq_id": [1000, 1001],
            "mode_seq_id": [10000, 10001],
            "seq_step_index": [0, 0],
            "to": [100, 101],
            "activity": ["work", "shop"],
            "mode": ["car", "walk"],
            "duration_per_pers": [8.0, 2.5],
            "time": [0.5, 0.25],
            "distance": [12.0, 1.0],
        },
        schema={
            "demand_group_id": pl.UInt32,
            "demand_subgroup_id": pl.UInt32,
            "activity_seq_id": pl.UInt32,
            "time_seq_id": pl.UInt32,
            "dest_seq_id": pl.UInt32,
            "mode_seq_id": pl.UInt32,
            "seq_step_index": pl.UInt32,
            "to": pl.Int32,
            "activity": pl.Utf8,
            "mode": pl.Utf8,
            "duration_per_pers": pl.Float32,
            "time": pl.Float64,
            "distance": pl.Float64,
        },
    ).lazy()

    with_details = add_transition_plan_details(transition_events, possible_plan_steps)

    assert dict(with_details.collect_schema()) == TRANSITION_EVENT_SCHEMA
