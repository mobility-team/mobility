from typing import Any
from mobility_mode_sequence_search import search_mode_sequences

import polars as pl


def run_rust_mode_sequence_search(
    *,
    unique_destination_chains: pl.DataFrame,
    leg_mode_costs: pl.DataFrame,
    modes_by_name: dict[str, Any],
    mode_id_by_name: dict[str, int],
    k_mode_sequences: int,
) -> pl.DataFrame:
    """Run the in-process Rust mode-sequence search backend."""
    
    rust_mode_metadata = build_rust_mode_metadata(
        modes_by_name=modes_by_name,
        mode_id_by_name=mode_id_by_name,
    )
    rust_cost_rows = (
        leg_mode_costs
        .rename({"from": "origin", "to": "destination"})
        .select(["origin", "destination", "mode_id", "cost"])
    )
    return search_mode_sequences(
        location_chain_steps=unique_destination_chains,
        leg_mode_costs=rust_cost_rows,
        mode_metadata=rust_mode_metadata,
        k_sequences=k_mode_sequences,
    )


def build_rust_mode_metadata(
    *,
    modes_by_name: dict[str, Any],
    mode_id_by_name: dict[str, int],
) -> pl.DataFrame:
    """Build the package-facing Rust mode metadata input table."""
    rows = []
    for name, props in modes_by_name.items():
        rows.append(
            {
                "mode_id": mode_id_by_name[name],
                "needs_vehicle": props["vehicle"] is not None,
                "vehicle_id": props["vehicle"],
                "multimodal": props["multimodal"],
                "is_return_mode": props["is_return_mode"],
                "return_mode_id": None if props["return_mode"] is None else mode_id_by_name[props["return_mode"]],
            }
        )
    return pl.DataFrame(rows).sort("mode_id")
