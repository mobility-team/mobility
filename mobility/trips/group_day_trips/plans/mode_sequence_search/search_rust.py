from typing import Any

import polars as pl


def run_rust_mode_sequence_search(
    *,
    unique_destination_chains: pl.DataFrame,
    leg_mode_costs: pl.DataFrame,
    needs_vehicle_by_id: dict[int, bool],
    return_mode_id_by_id: dict[int, int | None],
    is_return_mode_by_id: dict[int, bool],
    modes_by_name: dict[str, Any],
    mode_name_by_id: dict[int, str],
    k_mode_sequences: int,
) -> pl.DataFrame:
    """Run the in-process Rust mode-sequence search backend."""
    from mobility_mode_sequence_search import search_mode_sequences

    rust_mode_metadata = build_rust_mode_metadata(
        needs_vehicle_by_id=needs_vehicle_by_id,
        return_mode_id_by_id=return_mode_id_by_id,
        is_return_mode_by_id=is_return_mode_by_id,
        modes_by_name=modes_by_name,
        mode_name_by_id=mode_name_by_id,
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
    needs_vehicle_by_id: dict[int, bool],
    return_mode_id_by_id: dict[int, int | None],
    is_return_mode_by_id: dict[int, bool],
    modes_by_name: dict[str, Any],
    mode_name_by_id: dict[int, str],
) -> pl.DataFrame:
    """Build the package-facing Rust mode metadata input table."""
    rows = []
    for mode_id, name in sorted(mode_name_by_id.items()):
        props = modes_by_name[name]
        rows.append(
            {
                "mode_id": mode_id,
                "needs_vehicle": needs_vehicle_by_id[mode_id],
                "vehicle_id": props["vehicle"],
                "multimodal": props["multimodal"],
                "is_return_mode": is_return_mode_by_id[mode_id],
                "return_mode_id": return_mode_id_by_id[mode_id],
            }
        )
    return pl.DataFrame(rows).sort("mode_id")
