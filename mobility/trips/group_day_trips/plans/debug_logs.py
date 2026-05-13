import logging
from typing import Any

import polars as pl


def log_destination_sequence_diagnostics(
    *,
    iteration: int,
    source_activity_sequences: pl.DataFrame,
    destination_sequences: pl.DataFrame,
) -> None:
    """Log destination-chain diagnostics only when debug logging is enabled."""
    if not logging.root.isEnabledFor(logging.DEBUG):
        return

    chain_lengths = _destination_chain_lengths(destination_sequences)
    logging.debug(
        "Destination sequence step counts at iteration %s | grouped=%s",
        iteration,
        _distribution_by_count(chain_lengths, "step_count"),
    )

    one_step_chains = chain_lengths.filter(pl.col("step_count") == 1)
    if one_step_chains.height == 0:
        return

    one_step_keys = one_step_chains.select(
        ["demand_group_id", "activity_seq_id", "time_seq_id", "dest_seq_id"]
    )
    logging.warning(
        "Destination sequences contain %s one-step chains at iteration %s. "
        "Sample grouped chains: %s | Sample raw rows: %s",
        one_step_chains.height,
        iteration,
        one_step_chains.head(10).to_dicts(),
        _one_step_chain_rows(
            destination_sequences=destination_sequences,
            one_step_keys=one_step_keys,
        ).head(20).to_dicts(),
    )
    logging.warning(
        "Activity-sequence source rows behind one-step destination chains at iteration %s: %s",
        iteration,
        _source_rows_for_chain_keys(
            source_activity_sequences=source_activity_sequences,
            one_step_keys=one_step_keys,
        ).head(20).to_dicts(),
    )


def _destination_chain_lengths(destination_sequences: pl.DataFrame) -> pl.DataFrame:
    """Build one row per destination chain with ordered locations."""
    return (
        destination_sequences
        .group_by(["demand_group_id", "activity_seq_id", "time_seq_id", "dest_seq_id"])
        .agg(
            step_count=pl.len(),
            from_locations=pl.col("from").sort_by("seq_step_index"),
            to_locations=pl.col("to").sort_by("seq_step_index"),
        )
        .sort(["step_count", "dest_seq_id", "demand_group_id", "activity_seq_id", "time_seq_id"])
    )


def _distribution_by_count(frame: pl.DataFrame, count_col: str) -> list[dict[str, Any]]:
    """Return a compact count distribution for debug logging."""
    return (
        frame
        .group_by(count_col)
        .len()
        .sort(count_col)
        .to_dicts()
    )


def _one_step_chain_rows(
    *,
    destination_sequences: pl.DataFrame,
    one_step_keys: pl.DataFrame,
) -> pl.DataFrame:
    """Return the raw step rows behind one-step destination chains."""
    return (
        destination_sequences
        .join(
            one_step_keys,
            on=["demand_group_id", "activity_seq_id", "time_seq_id", "dest_seq_id"],
            how="inner",
        )
        .sort(["dest_seq_id", "demand_group_id", "activity_seq_id", "time_seq_id", "seq_step_index"])
    )


def _source_rows_for_chain_keys(
    *,
    source_activity_sequences: pl.DataFrame,
    one_step_keys: pl.DataFrame,
) -> pl.DataFrame:
    """Return the source activity rows behind the problematic destination chains."""
    return (
        source_activity_sequences
        .join(
            one_step_keys.select(["demand_group_id", "activity_seq_id", "time_seq_id"]).unique(),
            on=["demand_group_id", "activity_seq_id", "time_seq_id"],
            how="inner",
        )
        .sort(["demand_group_id", "activity_seq_id", "time_seq_id", "seq_step_index"])
    )


def log_step_dropout_diagnostics(
    *,
    seq_step_index: int,
    chains_step: pl.DataFrame,
    costs: pl.DataFrame,
    non_anchor_candidates: pl.LazyFrame,
    candidates_with_origin_costs: pl.LazyFrame,
    candidates_with_costs: pl.LazyFrame,
    sampled_non_anchor_steps: pl.LazyFrame,
    chain_key_cols: list[str],
    transport_zones: Any | None,
) -> None:
    """Log spatialization dropouts only when debug logging is enabled."""
    if not logging.root.isEnabledFor(logging.DEBUG):
        return

    # Start from the non-anchor rows that must survive this step.
    non_anchor_input = (
        chains_step
        .filter(pl.col("is_anchor").not_())
        .select(chain_key_cols + ["activity", "seq_step_index", "from", "anchor_to"])
        .unique()
    )
    if non_anchor_input.height == 0:
        return

    candidate_keys = _collect_unique_keys(non_anchor_candidates, chain_key_cols)
    origin_cost_keys = _collect_unique_keys(candidates_with_origin_costs, chain_key_cols)
    costed_keys = _collect_unique_keys(candidates_with_costs, chain_key_cols)
    sampled_keys = _collect_unique_keys(sampled_non_anchor_steps, chain_key_cols)

    # Compute the first stage where each chain disappears.
    missing_after_probability = non_anchor_input.join(candidate_keys, on=chain_key_cols, how="anti")
    missing_after_origin_costs = (
        non_anchor_input
        .join(candidate_keys, on=chain_key_cols, how="inner")
        .join(origin_cost_keys, on=chain_key_cols, how="anti")
    )
    missing_after_anchor_costs = (
        non_anchor_input
        .join(origin_cost_keys, on=chain_key_cols, how="inner")
        .join(costed_keys, on=chain_key_cols, how="anti")
    )
    missing_after_sampling = (
        non_anchor_input
        .join(costed_keys, on=chain_key_cols, how="inner")
        .join(sampled_keys, on=chain_key_cols, how="anti")
    )

    if (
        missing_after_probability.height == 0
        and missing_after_origin_costs.height == 0
        and missing_after_anchor_costs.height == 0
        and missing_after_sampling.height == 0
    ):
        return

    logging.warning(
        "Destination spatialization dropouts at step %s | input_non_anchor=%s | after_probability_missing=%s | after_origin_cost_missing=%s | "
        "after_anchor_cost_missing=%s | after_sampling_missing=%s | probability_samples=%s | origin_cost_samples=%s | "
        "anchor_cost_samples=%s | sampling_samples=%s | probability_candidates=%s | origin_cost_candidates=%s | "
        "anchor_cost_candidates=%s | sampling_candidates=%s | origin_cost_traces=%s | anchor_cost_traces=%s",
        seq_step_index,
        non_anchor_input.height,
        missing_after_probability.height,
        missing_after_origin_costs.height,
        missing_after_anchor_costs.height,
        missing_after_sampling.height,
        missing_after_probability.head(10).to_dicts(),
        missing_after_origin_costs.head(10).to_dicts(),
        missing_after_anchor_costs.head(10).to_dicts(),
        missing_after_sampling.head(10).to_dicts(),
        _candidate_samples_for_dropouts(
            dropouts=missing_after_probability,
            non_anchor_candidates=non_anchor_candidates,
            chain_key_cols=chain_key_cols,
        ),
        _candidate_samples_for_dropouts(
            dropouts=missing_after_origin_costs,
            non_anchor_candidates=non_anchor_candidates,
            chain_key_cols=chain_key_cols,
        ),
        _candidate_samples_for_dropouts(
            dropouts=missing_after_anchor_costs,
            non_anchor_candidates=non_anchor_candidates,
            chain_key_cols=chain_key_cols,
        ),
        _candidate_samples_for_dropouts(
            dropouts=missing_after_sampling,
            non_anchor_candidates=non_anchor_candidates,
            chain_key_cols=chain_key_cols,
        ),
        _cost_trace_for_dropouts(
            dropouts=missing_after_origin_costs,
            non_anchor_candidates=non_anchor_candidates,
            chain_key_cols=chain_key_cols,
            costs=costs,
            transport_zones=transport_zones,
        ),
        _cost_trace_for_dropouts(
            dropouts=missing_after_anchor_costs,
            non_anchor_candidates=non_anchor_candidates,
            chain_key_cols=chain_key_cols,
            costs=costs,
            transport_zones=transport_zones,
        ),
    )


def _collect_unique_keys(frame: pl.LazyFrame, chain_key_cols: list[str]) -> pl.DataFrame:
    """Collect one unique key row per surviving chain."""
    return (
        frame
        .select(chain_key_cols)
        .unique()
        .collect(engine="streaming")
    )


def _candidate_samples_for_dropouts(
    *,
    dropouts: pl.DataFrame,
    non_anchor_candidates: pl.LazyFrame,
    chain_key_cols: list[str],
) -> list[dict[str, Any]]:
    """Return a small sample of destination candidates for dropped chains."""
    if dropouts.height == 0:
        return []
    return (
        non_anchor_candidates
        .join(dropouts.lazy().select(chain_key_cols), on=chain_key_cols, how="inner")
        .select(chain_key_cols + ["activity", "seq_step_index", "from", "to", "anchor_to", "p_ij"])
        .sort(chain_key_cols + ["to"])
        .limit(20)
        .collect(engine="streaming")
        .to_dicts()
    )


def _cost_trace_for_dropouts(
    *,
    dropouts: pl.DataFrame,
    non_anchor_candidates: pl.LazyFrame,
    chain_key_cols: list[str],
    costs: pl.DataFrame,
    transport_zones: Any | None,
) -> list[dict[str, Any]]:
    """Trace OD cost availability for a few dropped chains."""
    if dropouts.height == 0:
        return []

    candidate_rows = (
        non_anchor_candidates
        .join(dropouts.lazy().select(chain_key_cols), on=chain_key_cols, how="inner")
        .select(chain_key_cols + ["from", "to", "anchor_to"])
        .sort(chain_key_cols + ["to"])
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
