import logging
from typing import Any

import polars as pl

from mobility.runtime.logging_levels import TRACE_LEVEL, is_trace_enabled

from .demand_subgroups import DEMAND_UNIT_COLS


def log_destination_spatialization_step(
    *,
    seq_step_index: int,
    sequence_step: pl.DataFrame,
    non_anchor_count: int,
    anchor_count: int,
) -> None:
    """Log the size of one destination spatialization step."""
    if not logging.root.isEnabledFor(logging.DEBUG):
        return

    unique_draws = sequence_step.select(
        pl.struct(DEMAND_UNIT_COLS + ["activity_seq_id", "time_seq_id", "dest_draw_id"]).n_unique()
    ).item()
    logging.debug(
        "Destination spatialization step %s input rows=%s non_anchor=%s anchor=%s unique_draws=%s",
        seq_step_index,
        sequence_step.height,
        non_anchor_count,
        anchor_count,
        unique_draws,
    )


def log_missing_anchor_destination_samples(
    *,
    sequence_key_cols: list[str],
    expected_anchor_steps: pl.DataFrame,
    sampled_anchor_steps: pl.DataFrame,
) -> None:
    """Warn when an anchor activity has no reachable destination candidate."""
    if sampled_anchor_steps.height >= expected_anchor_steps.height:
        return

    missing_anchors = (
        expected_anchor_steps
        .join(
            sampled_anchor_steps.select(sequence_key_cols + ["seq_step_index"]),
            on=sequence_key_cols + ["seq_step_index"],
            how="anti",
        )
        .select(sequence_key_cols + ["activity", "seq_step_index", "from"])
    )
    logging.warning(
        "Dropping %s anchor destination steps because no reachable anchor candidate could be sampled. "
        "Sample steps: %s",
        missing_anchors.height,
        missing_anchors.head(20).to_dicts(),
    )


def log_incomplete_destination_draws(
    *,
    iteration: int,
    activity_sequences_with_counts: pl.DataFrame,
    sequence_key_cols: list[str],
) -> None:
    """Warn when a destination draw lost one or more steps."""
    incomplete_draws = (
        activity_sequences_with_counts
        .filter(pl.col("step_count_after_spatialization") != pl.col("step_count"))
        .select(sequence_key_cols + ["step_count", "step_count_after_spatialization"])
        .unique()
        .sort(sequence_key_cols)
    )
    if incomplete_draws.height == 0:
        return

    logging.warning(
        "Dropping %s incomplete destination draws at iteration %s because one or more steps disappeared during spatialization. "
        "Sample draws: %s",
        incomplete_draws.height,
        iteration,
        incomplete_draws.head(20).to_dicts(),
    )


def log_destination_sequence_diagnostics(
    *,
    iteration: int,
    source_activity_sequences: pl.DataFrame,
    destination_sequences: pl.DataFrame,
) -> None:
    """Log destination-sequence diagnostics only when trace logging is enabled."""
    if not is_trace_enabled():
        return

    sequence_lengths = _destination_sequence_lengths(destination_sequences)
    logging.log(
        TRACE_LEVEL,
        "Destination sequence step counts at iteration %s | grouped=%s",
        iteration,
        sequence_lengths.group_by("step_count").len().sort("step_count").to_dicts(),
    )

    one_step_sequences = sequence_lengths.filter(pl.col("step_count") == 1)
    if one_step_sequences.height == 0:
        return

    one_step_keys = one_step_sequences.select(DEMAND_UNIT_COLS + ["activity_seq_id", "time_seq_id", "dest_seq_id"])
    logging.warning(
        "Destination sequences contain %s one-step sequences at iteration %s. "
        "Sample grouped sequences: %s | Sample raw rows: %s",
        one_step_sequences.height,
        iteration,
        one_step_sequences.head(10).to_dicts(),
        _one_step_sequence_rows(
            destination_sequences=destination_sequences,
            one_step_keys=one_step_keys,
        ).head(20).to_dicts(),
    )
    logging.warning(
        "Activity-sequence source rows behind one-step destination sequences at iteration %s: %s",
        iteration,
        _source_rows_for_sequence_keys(
            source_activity_sequences=source_activity_sequences,
            one_step_keys=one_step_keys,
        ).head(20).to_dicts(),
    )


def _destination_sequence_lengths(destination_sequences: pl.DataFrame) -> pl.DataFrame:
    """Build one row per destination sequence with ordered locations."""
    return (
        destination_sequences
        .group_by(DEMAND_UNIT_COLS + ["activity_seq_id", "time_seq_id", "dest_seq_id"])
        .agg(
            step_count=pl.len(),
            from_locations=pl.col("from").sort_by("seq_step_index"),
            to_locations=pl.col("to").sort_by("seq_step_index"),
        )
        .sort(["step_count", "dest_seq_id"] + DEMAND_UNIT_COLS + ["activity_seq_id", "time_seq_id"])
    )


def _one_step_sequence_rows(
    *,
    destination_sequences: pl.DataFrame,
    one_step_keys: pl.DataFrame,
) -> pl.DataFrame:
    """Return the raw step rows behind one-step destination sequences."""
    return (
        destination_sequences
        .join(
            one_step_keys,
            on=DEMAND_UNIT_COLS + ["activity_seq_id", "time_seq_id", "dest_seq_id"],
            how="inner",
        )
        .sort(["dest_seq_id"] + DEMAND_UNIT_COLS + ["activity_seq_id", "time_seq_id", "seq_step_index"])
    )


def _source_rows_for_sequence_keys(
    *,
    source_activity_sequences: pl.DataFrame,
    one_step_keys: pl.DataFrame,
) -> pl.DataFrame:
    """Return the source activity rows behind the problematic destination sequences."""
    return (
        source_activity_sequences
        .join(
            one_step_keys.select(DEMAND_UNIT_COLS + ["activity_seq_id", "time_seq_id"]).unique(),
            on=DEMAND_UNIT_COLS + ["activity_seq_id", "time_seq_id"],
            how="inner",
        )
        .sort(DEMAND_UNIT_COLS + ["activity_seq_id", "time_seq_id", "seq_step_index"])
    )


def log_step_dropout_diagnostics(
    *,
    seq_step_index: int,
    sequence_step: pl.DataFrame,
    costs: pl.DataFrame,
    non_anchor_candidates: pl.LazyFrame,
    candidates_with_origin_costs: pl.LazyFrame,
    candidates_with_costs: pl.LazyFrame,
    sampled_non_anchor_steps: pl.LazyFrame,
    sequence_key_cols: list[str],
    transport_zones: Any | None,
) -> None:
    """Log spatialization dropouts only when trace logging is enabled."""
    if not is_trace_enabled():
        return

    # Start from the non-anchor rows that must survive this step.
    non_anchor_input = (
        sequence_step
        .filter(pl.col("is_anchor").not_())
        .select(sequence_key_cols + ["activity", "seq_step_index", "from", "anchor_to"])
        .unique()
    )
    if non_anchor_input.height == 0:
        return

    candidate_keys = _collect_unique_keys(non_anchor_candidates, sequence_key_cols)
    origin_cost_keys = _collect_unique_keys(candidates_with_origin_costs, sequence_key_cols)
    costed_keys = _collect_unique_keys(candidates_with_costs, sequence_key_cols)
    sampled_keys = _collect_unique_keys(sampled_non_anchor_steps, sequence_key_cols)

    # Compute the first stage where each sequence disappears.
    missing_by_stage = {
        "probability": non_anchor_input.join(candidate_keys, on=sequence_key_cols, how="anti"),
        "origin_cost": (
            non_anchor_input
            .join(candidate_keys, on=sequence_key_cols, how="inner")
            .join(origin_cost_keys, on=sequence_key_cols, how="anti")
        ),
        "anchor_cost": (
            non_anchor_input
            .join(origin_cost_keys, on=sequence_key_cols, how="inner")
            .join(costed_keys, on=sequence_key_cols, how="anti")
        ),
        "sampling": (
            non_anchor_input
            .join(costed_keys, on=sequence_key_cols, how="inner")
            .join(sampled_keys, on=sequence_key_cols, how="anti")
        ),
    }

    if all(dropouts.height == 0 for dropouts in missing_by_stage.values()):
        return

    candidate_samples = {
        stage: _candidate_samples_for_dropouts(
            dropouts=dropouts,
            non_anchor_candidates=non_anchor_candidates,
            sequence_key_cols=sequence_key_cols,
        )
        for stage, dropouts in missing_by_stage.items()
    }
    logging.warning(
        "Destination spatialization dropouts at step %s | input_non_anchor=%s | after_probability_missing=%s | after_origin_cost_missing=%s | "
        "after_anchor_cost_missing=%s | after_sampling_missing=%s | probability_samples=%s | origin_cost_samples=%s | "
        "anchor_cost_samples=%s | sampling_samples=%s | probability_candidates=%s | origin_cost_candidates=%s | "
        "anchor_cost_candidates=%s | sampling_candidates=%s | origin_cost_traces=%s | anchor_cost_traces=%s",
        seq_step_index,
        non_anchor_input.height,
        missing_by_stage["probability"].height,
        missing_by_stage["origin_cost"].height,
        missing_by_stage["anchor_cost"].height,
        missing_by_stage["sampling"].height,
        missing_by_stage["probability"].head(10).to_dicts(),
        missing_by_stage["origin_cost"].head(10).to_dicts(),
        missing_by_stage["anchor_cost"].head(10).to_dicts(),
        missing_by_stage["sampling"].head(10).to_dicts(),
        candidate_samples["probability"],
        candidate_samples["origin_cost"],
        candidate_samples["anchor_cost"],
        candidate_samples["sampling"],
        _cost_trace_for_dropouts(
            dropouts=missing_by_stage["origin_cost"],
            non_anchor_candidates=non_anchor_candidates,
            sequence_key_cols=sequence_key_cols,
            costs=costs,
            transport_zones=transport_zones,
        ),
        _cost_trace_for_dropouts(
            dropouts=missing_by_stage["anchor_cost"],
            non_anchor_candidates=non_anchor_candidates,
            sequence_key_cols=sequence_key_cols,
            costs=costs,
            transport_zones=transport_zones,
        ),
    )


def _collect_unique_keys(frame: pl.LazyFrame, sequence_key_cols: list[str]) -> pl.DataFrame:
    """Collect one unique key row per surviving sequence."""
    return (
        frame
        .select(sequence_key_cols)
        .unique()
        .collect(engine="streaming")
    )


def _candidate_samples_for_dropouts(
    *,
    dropouts: pl.DataFrame,
    non_anchor_candidates: pl.LazyFrame,
    sequence_key_cols: list[str],
) -> list[dict[str, Any]]:
    """Return a small sample of destination candidates for dropped sequences."""
    if dropouts.height == 0:
        return []
    return (
        non_anchor_candidates
        .join(dropouts.lazy().select(sequence_key_cols), on=sequence_key_cols, how="inner")
        .select(sequence_key_cols + ["activity", "seq_step_index", "from", "to", "anchor_to", "p_ij"])
        .sort(sequence_key_cols + ["to"])
        .limit(20)
        .collect(engine="streaming")
        .to_dicts()
    )


def _cost_trace_for_dropouts(
    *,
    dropouts: pl.DataFrame,
    non_anchor_candidates: pl.LazyFrame,
    sequence_key_cols: list[str],
    costs: pl.DataFrame,
    transport_zones: Any | None,
) -> list[dict[str, Any]]:
    """Trace OD cost availability for a few dropped sequences."""
    if dropouts.height == 0:
        return []

    candidate_rows = (
        non_anchor_candidates
        .join(dropouts.lazy().select(sequence_key_cols), on=sequence_key_cols, how="inner")
        .select(sequence_key_cols + ["from", "to", "anchor_to"])
        .sort(sequence_key_cols + ["to"])
        .limit(10)
        .collect(engine="streaming")
    )
    if candidate_rows.height == 0:
        return []

    zones = _transport_zone_lookup(transport_zones)
    traces: list[dict[str, Any]] = []
    for row in candidate_rows.to_dicts():
        # Include a beeline distance hint to spot obvious disconnected OD pairs.
        beeline_km = None
        if zones is not None and row["to"] in zones.index and row["anchor_to"] in zones.index:
            from_zone = zones.loc[row["to"]]
            anchor_zone = zones.loc[row["anchor_to"]]
            if from_zone is not None and anchor_zone is not None:
                dx = float(from_zone["x"]) - float(anchor_zone["x"])
                dy = float(from_zone["y"]) - float(anchor_zone["y"])
                beeline_km = (dx ** 2 + dy ** 2) ** 0.5 / 1000.0
        traces.append(
            {
                **row,
                "candidate_anchor_beeline_km": beeline_km,
                "origin_cost_rows": costs.filter(
                    (pl.col("from") == row["from"]) & (pl.col("to") == row["to"])
                ).head(3).to_dicts(),
                "anchor_cost_rows": costs.filter(
                    (pl.col("from") == row["to"]) & (pl.col("to") == row["anchor_to"])
                ).head(3).to_dicts(),
                "reverse_anchor_cost_rows": costs.filter(
                    (pl.col("from") == row["anchor_to"]) & (pl.col("to") == row["to"])
                ).head(3).to_dicts(),
            }
        )
    return traces


def _transport_zone_lookup(transport_zones: Any | None):
    """Build a simple lookup table for beeline checks when available."""
    if transport_zones is None:
        return None
    return transport_zones.get().drop(columns="geometry").set_index("transport_zone_id")
