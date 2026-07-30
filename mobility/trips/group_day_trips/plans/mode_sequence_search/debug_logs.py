import logging
from typing import Any

import polars as pl

from mobility.runtime.logging_levels import TRACE_LEVEL, is_trace_enabled

from ..demand_subgroups import DEMAND_UNIT_COLS


def log_location_chain_diagnostics(
    *,
    iteration: int,
    destination_steps: pl.DataFrame,
    trip_chains: pl.DataFrame,
    unique_destination_chains: pl.DataFrame,
) -> None:
    """Log mode-search chain diagnostics only when trace logging is enabled."""
    if not is_trace_enabled():
        return

    trip_chain_lengths = _trip_chain_lengths(trip_chains)
    unique_chain_lengths = _unique_chain_lengths(unique_destination_chains)
    logging.log(
        TRACE_LEVEL,
        "Mode search chain lengths at iteration %s | grouped=%s | unique_dest_seq=%s",
        iteration,
        _chain_length_distribution(trip_chain_lengths),
        _chain_length_distribution(unique_chain_lengths),
    )

    invalid_unique_chains = unique_chain_lengths.filter(pl.col("location_count") < 2)
    if invalid_unique_chains.height == 0:
        return

    grouped_invalid_chains = _grouped_invalid_chains(
        trip_chain_lengths=trip_chain_lengths,
        invalid_unique_chains=invalid_unique_chains,
    )

    # A one-location chain means the destination-step sequence was truncated
    # before mode search prepared the grouped `from` locations for Rust.
    logging.warning(
        "Mode search invalid location chains at iteration %s: %s unique dest_seq_id values and %s grouped chains have fewer than two locations. "
        "This usually means a one-leg destination chain was reduced to one `from` location before Rust search. "
        "Sample grouped chains: %s | Sample destination steps: %s",
        iteration,
        invalid_unique_chains.height,
        grouped_invalid_chains.height,
        grouped_invalid_chains.head(10).to_dicts(),
        _invalid_destination_steps(
            destination_steps=destination_steps,
            grouped_invalid_chains=grouped_invalid_chains,
        ).head(20).to_dicts(),
    )


def _trip_chain_lengths(trip_chains: pl.DataFrame) -> pl.DataFrame:
    """Attach a grouped location count to each trip chain."""
    return trip_chains.with_columns(
        location_count=pl.col("locations").list.len(),
    )


def _unique_chain_lengths(unique_destination_chains: pl.DataFrame) -> pl.DataFrame:
    """Attach a grouped location count to each unique destination chain."""
    return unique_destination_chains.with_columns(
        location_count=pl.col("locations").list.len(),
    )


def _chain_length_distribution(frame: pl.DataFrame) -> list[dict[str, Any]]:
    """Return a compact grouped distribution of chain lengths."""
    return (
        frame
        .group_by("location_count")
        .len()
        .sort("location_count")
        .to_dicts()
    )


def _grouped_invalid_chains(
    *,
    trip_chain_lengths: pl.DataFrame,
    invalid_unique_chains: pl.DataFrame,
) -> pl.DataFrame:
    """Return the grouped chains that reuse invalid unique destination sequences."""
    return (
        trip_chain_lengths
        .join(invalid_unique_chains.select("dest_seq_id"), on="dest_seq_id", how="inner")
        .sort(["dest_seq_id"] + DEMAND_UNIT_COLS + ["activity_seq_id", "time_seq_id"])
    )


def _invalid_destination_steps(
    *,
    destination_steps: pl.DataFrame,
    grouped_invalid_chains: pl.DataFrame,
) -> pl.DataFrame:
    """Return the raw destination rows behind invalid location chains."""
    invalid_chain_keys = grouped_invalid_chains.select(DEMAND_UNIT_COLS + ["activity_seq_id", "time_seq_id", "dest_seq_id"])
    return (
        destination_steps
        .join(
            invalid_chain_keys,
            on=DEMAND_UNIT_COLS + ["activity_seq_id", "time_seq_id", "dest_seq_id"],
            how="inner",
        )
        .sort(["dest_seq_id"] + DEMAND_UNIT_COLS + ["activity_seq_id", "time_seq_id", "seq_step_index"])
    )
