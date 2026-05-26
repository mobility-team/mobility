import logging
from types import SimpleNamespace

import pandas as pd
import polars as pl

from mobility.trips.group_day_trips.plans.debug_logs import (
    log_destination_sequence_diagnostics,
    log_step_dropout_diagnostics,
)
from mobility.trips.group_day_trips.plans.mode_sequence_search.debug_logs import (
    log_location_chain_diagnostics,
)


def test_debug_diagnostics_do_not_collect_inputs_when_debug_is_disabled(caplog):
    with caplog.at_level(logging.INFO):
        log_destination_sequence_diagnostics(
            iteration=1,
            source_activity_sequences=pl.DataFrame(),
            destination_sequences=pl.DataFrame(),
        )
        log_step_dropout_diagnostics(
            seq_step_index=1,
            chains_step=pl.DataFrame(),
            costs=pl.DataFrame(),
            non_anchor_candidates=pl.DataFrame().lazy(),
            candidates_with_origin_costs=pl.DataFrame().lazy(),
            candidates_with_costs=pl.DataFrame().lazy(),
            sampled_non_anchor_steps=pl.DataFrame().lazy(),
            chain_key_cols=[],
            transport_zones=None,
        )
        log_location_chain_diagnostics(
            iteration=1,
            destination_steps=pl.DataFrame(),
            trip_chains=pl.DataFrame(),
            unique_destination_chains=pl.DataFrame(),
        )


def test_log_destination_sequence_diagnostics_reports_one_step_chains(caplog):
    source_activity_sequences = pl.DataFrame(
        {
            "demand_group_id": [1, 1, 2],
            "activity_seq_id": [10, 10, 20],
            "time_seq_id": [100, 100, 200],
            "seq_step_index": [0, 1, 0],
            "activity": ["home", "work", "home"],
        }
    )
    destination_sequences = pl.DataFrame(
        {
            "demand_group_id": [1, 1, 2],
            "activity_seq_id": [10, 10, 20],
            "time_seq_id": [100, 100, 200],
            "dest_seq_id": [1000, 1000, 2000],
            "seq_step_index": [0, 1, 0],
            "from": [1, 2, 3],
            "to": [2, 1, 4],
        }
    )

    with caplog.at_level(logging.DEBUG):
        log_destination_sequence_diagnostics(
            iteration=7,
            source_activity_sequences=source_activity_sequences,
            destination_sequences=destination_sequences,
        )

    assert "Destination sequence step counts at iteration 7" in caplog.text
    assert "Destination sequences contain 1 one-step chains at iteration 7" in caplog.text
    assert "Activity-sequence source rows behind one-step destination chains at iteration 7" in caplog.text
    assert "'dest_seq_id': 2000" in caplog.text
    assert "'activity': 'home'" in caplog.text


def test_log_destination_sequence_diagnostics_skips_warning_for_complete_chains(caplog):
    destination_sequences = pl.DataFrame(
        {
            "demand_group_id": [1, 1],
            "activity_seq_id": [10, 10],
            "time_seq_id": [100, 100],
            "dest_seq_id": [1000, 1000],
            "seq_step_index": [0, 1],
            "from": [1, 2],
            "to": [2, 1],
        }
    )

    with caplog.at_level(logging.DEBUG):
        log_destination_sequence_diagnostics(
            iteration=7,
            source_activity_sequences=pl.DataFrame(),
            destination_sequences=destination_sequences,
        )

    assert "Destination sequence step counts at iteration 7" in caplog.text
    assert "Destination sequences contain" not in caplog.text


def test_log_step_dropout_diagnostics_reports_each_dropout_stage(caplog):
    chain_key_cols = ["demand_group_id", "activity_seq_id", "time_seq_id", "dest_draw_id"]
    chains_step = pl.DataFrame(
        {
            "demand_group_id": [1, 2, 3, 4, 5],
            "activity_seq_id": [10, 20, 30, 40, 50],
            "time_seq_id": [100, 200, 300, 400, 500],
            "dest_draw_id": [1000, 2000, 3000, 4000, 5000],
            "activity": ["shop", "shop", "shop", "shop", "home"],
            "seq_step_index": [2, 2, 2, 2, 2],
            "from": [10, 20, 30, 40, 50],
            "anchor_to": [110, 120, 130, 140, 150],
            "is_anchor": [False, False, False, False, True],
        }
    )
    non_anchor_candidates = pl.DataFrame(
        {
            "demand_group_id": [2, 3, 4],
            "activity_seq_id": [20, 30, 40],
            "time_seq_id": [200, 300, 400],
            "dest_draw_id": [2000, 3000, 4000],
            "activity": ["shop", "shop", "shop"],
            "seq_step_index": [2, 2, 2],
            "from": [20, 30, 40],
            "to": [220, 230, 240],
            "anchor_to": [120, 130, 140],
            "p_ij": [0.2, 0.3, 0.4],
        }
    )
    candidates_with_origin_costs = non_anchor_candidates.filter(pl.col("demand_group_id").is_in([3, 4]))
    candidates_with_costs = non_anchor_candidates.filter(pl.col("demand_group_id") == 4)
    sampled_non_anchor_steps = non_anchor_candidates.head(0)
    costs = pl.DataFrame(
        {
            "from": [30, 230, 130, 240],
            "to": [230, 130, 230, 140],
            "cost": [1.0, 2.0, 3.0, 4.0],
        }
    )
    transport_zones = SimpleNamespace(
        get=lambda: pd.DataFrame(
            {
                "transport_zone_id": [130, 140, 230, 240],
                "x": [0.0, 3000.0, 3000.0, 6000.0],
                "y": [0.0, 4000.0, 4000.0, 8000.0],
                "geometry": [None, None, None, None],
            }
        )
    )

    with caplog.at_level(logging.DEBUG):
        log_step_dropout_diagnostics(
            seq_step_index=2,
            chains_step=chains_step,
            costs=costs,
            non_anchor_candidates=non_anchor_candidates.lazy(),
            candidates_with_origin_costs=candidates_with_origin_costs.lazy(),
            candidates_with_costs=candidates_with_costs.lazy(),
            sampled_non_anchor_steps=sampled_non_anchor_steps.lazy(),
            chain_key_cols=chain_key_cols,
            transport_zones=transport_zones,
        )

    assert "Destination spatialization dropouts at step 2" in caplog.text
    assert "after_probability_missing=1" in caplog.text
    assert "after_origin_cost_missing=1" in caplog.text
    assert "after_anchor_cost_missing=1" in caplog.text
    assert "after_sampling_missing=1" in caplog.text
    assert "candidate_anchor_beeline_km" in caplog.text
    assert "'reverse_anchor_cost_rows': [{'from': 130, 'to': 230, 'cost': 3.0}]" in caplog.text


def test_log_step_dropout_diagnostics_skips_when_no_non_anchor_or_dropout(caplog):
    chain_key_cols = ["demand_group_id", "activity_seq_id", "time_seq_id", "dest_draw_id"]
    anchor_only = pl.DataFrame(
        {
            "demand_group_id": [1],
            "activity_seq_id": [10],
            "time_seq_id": [100],
            "dest_draw_id": [1000],
            "activity": ["home"],
            "seq_step_index": [1],
            "from": [10],
            "anchor_to": [20],
            "is_anchor": [True],
        }
    )
    surviving_candidate = pl.DataFrame(
        {
            "demand_group_id": [2],
            "activity_seq_id": [20],
            "time_seq_id": [200],
            "dest_draw_id": [2000],
            "activity": ["shop"],
            "seq_step_index": [1],
            "from": [20],
            "to": [30],
            "anchor_to": [40],
            "p_ij": [1.0],
            "is_anchor": [False],
        }
    )

    with caplog.at_level(logging.DEBUG):
        log_step_dropout_diagnostics(
            seq_step_index=1,
            chains_step=anchor_only,
            costs=pl.DataFrame(),
            non_anchor_candidates=pl.DataFrame().lazy(),
            candidates_with_origin_costs=pl.DataFrame().lazy(),
            candidates_with_costs=pl.DataFrame().lazy(),
            sampled_non_anchor_steps=pl.DataFrame().lazy(),
            chain_key_cols=chain_key_cols,
            transport_zones=None,
        )
        log_step_dropout_diagnostics(
            seq_step_index=1,
            chains_step=surviving_candidate,
            costs=pl.DataFrame(),
            non_anchor_candidates=surviving_candidate.lazy(),
            candidates_with_origin_costs=surviving_candidate.lazy(),
            candidates_with_costs=surviving_candidate.lazy(),
            sampled_non_anchor_steps=surviving_candidate.lazy(),
            chain_key_cols=chain_key_cols,
            transport_zones=None,
        )

    assert "Destination spatialization dropouts" not in caplog.text


def test_log_step_dropout_diagnostics_can_trace_costs_without_transport_zones(caplog):
    chain_key_cols = ["demand_group_id", "activity_seq_id", "time_seq_id", "dest_draw_id"]
    chains_step = pl.DataFrame(
        {
            "demand_group_id": [1],
            "activity_seq_id": [10],
            "time_seq_id": [100],
            "dest_draw_id": [1000],
            "activity": ["shop"],
            "seq_step_index": [1],
            "from": [10],
            "anchor_to": [30],
            "is_anchor": [False],
        }
    )
    non_anchor_candidates = pl.DataFrame(
        {
            "demand_group_id": [1],
            "activity_seq_id": [10],
            "time_seq_id": [100],
            "dest_draw_id": [1000],
            "activity": ["shop"],
            "seq_step_index": [1],
            "from": [10],
            "to": [20],
            "anchor_to": [30],
            "p_ij": [1.0],
        }
    )

    with caplog.at_level(logging.DEBUG):
        log_step_dropout_diagnostics(
            seq_step_index=1,
            chains_step=chains_step,
            costs=pl.DataFrame({"from": [10], "to": [20], "cost": [1.0]}),
            non_anchor_candidates=non_anchor_candidates.lazy(),
            candidates_with_origin_costs=pl.DataFrame(schema=non_anchor_candidates.schema).lazy(),
            candidates_with_costs=pl.DataFrame(schema=non_anchor_candidates.schema).lazy(),
            sampled_non_anchor_steps=pl.DataFrame(schema=non_anchor_candidates.schema).lazy(),
            chain_key_cols=chain_key_cols,
            transport_zones=None,
        )

    assert "Destination spatialization dropouts at step 1" in caplog.text
    assert "'candidate_anchor_beeline_km': None" in caplog.text


def test_log_location_chain_diagnostics_reports_invalid_unique_chains(caplog):
    destination_steps = pl.DataFrame(
        {
            "demand_group_id": [1, 1, 2],
            "activity_seq_id": [10, 10, 20],
            "time_seq_id": [100, 100, 200],
            "dest_seq_id": [1000, 1000, 2000],
            "seq_step_index": [0, 1, 0],
            "from": [1, 2, 3],
            "to": [2, 1, 4],
        }
    )
    trip_chains = pl.DataFrame(
        {
            "demand_group_id": [1, 2],
            "activity_seq_id": [10, 20],
            "time_seq_id": [100, 200],
            "dest_seq_id": [1000, 2000],
            "locations": [[1, 2, 1], [3]],
        }
    )
    unique_destination_chains = pl.DataFrame(
        {
            "dest_seq_id": [1000, 2000],
            "locations": [[1, 2, 1], [3]],
        }
    )

    with caplog.at_level(logging.DEBUG):
        log_location_chain_diagnostics(
            iteration=5,
            destination_steps=destination_steps,
            trip_chains=trip_chains,
            unique_destination_chains=unique_destination_chains,
        )

    assert "Mode search chain lengths at iteration 5" in caplog.text
    assert "Mode search invalid location chains at iteration 5" in caplog.text
    assert "1 unique dest_seq_id values and 1 grouped chains" in caplog.text
    assert "'dest_seq_id': 2000" in caplog.text


def test_log_location_chain_diagnostics_skips_warning_for_valid_chains(caplog):
    trip_chains = pl.DataFrame(
        {
            "demand_group_id": [1],
            "activity_seq_id": [10],
            "time_seq_id": [100],
            "dest_seq_id": [1000],
            "locations": [[1, 2]],
        }
    )
    unique_destination_chains = pl.DataFrame(
        {
            "dest_seq_id": [1000],
            "locations": [[1, 2]],
        }
    )

    with caplog.at_level(logging.DEBUG):
        log_location_chain_diagnostics(
            iteration=5,
            destination_steps=pl.DataFrame(),
            trip_chains=trip_chains,
            unique_destination_chains=unique_destination_chains,
        )

    assert "Mode search chain lengths at iteration 5" in caplog.text
    assert "Mode search invalid location chains" not in caplog.text
