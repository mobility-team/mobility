from typing import Any

import polars as pl

from mobility.transport.modes.core.mode_values import get_mode_values
from mobility.transport.modes.choice.compute_subtour_mode_probabilities import modes_list_to_dict
from mobility.trips.group_day_trips.plans.demand_subgroups import DEMAND_UNIT_COLS

from .models import ModeSearchInputs


def build_location_chains(destination_steps: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Build grouped trip chains and one unique location chain per destination sequence."""
    trip_chains = (
        destination_steps
        .group_by(DEMAND_UNIT_COLS + ["activity_seq_id", "time_seq_id", "dest_seq_id"])
        .agg(locations=pl.col("from").sort_by("seq_step_index"))
        .sort(DEMAND_UNIT_COLS + ["activity_seq_id", "time_seq_id", "dest_seq_id"])
    )
    _validate_location_chains(trip_chains)
    unique_destination_chains = (
        trip_chains
        .group_by(["dest_seq_id"])
        .agg(pl.col("locations").first())
        .sort("dest_seq_id")
    )
    return trip_chains, unique_destination_chains


def _validate_location_chains(trip_chains: pl.DataFrame) -> None:
    """Fail loudly when destination chains are structurally invalid."""
    with_location_key = trip_chains.with_columns(
        location_count=pl.col("locations").list.len(),
        location_key=pl.col("locations").cast(pl.List(pl.String)).list.join("-"),
    )
    one_location_chains = with_location_key.filter(pl.col("location_count") < 2)
    if one_location_chains.height > 0:
        raise ValueError(
            "Mode sequence search received destination chains with fewer than two locations. "
            "This means destination sequence construction produced incomplete plans. "
            f"Sample chains: {one_location_chains.head(20).to_dicts()}"
        )

    conflicting_destination_sequences = (
        with_location_key
        .group_by("dest_seq_id")
        .agg(n_location_chains=pl.col("location_key").n_unique())
        .filter(pl.col("n_location_chains") > 1)
    )
    if conflicting_destination_sequences.height > 0:
        sample_ids = conflicting_destination_sequences["dest_seq_id"].head(20).to_list()
        sample_chains = (
            with_location_key
            .filter(pl.col("dest_seq_id").is_in(sample_ids))
            .sort(["dest_seq_id"] + DEMAND_UNIT_COLS + ["activity_seq_id", "time_seq_id"])
            .head(50)
            .to_dicts()
        )
        raise ValueError(
            "Mode sequence search received one dest_seq_id with multiple location chains. "
            "This means destination sequence ids are no longer unique enough for mode search. "
            f"Sample chains: {sample_chains}"
        )


def build_search_inputs(transport_costs: Any) -> ModeSearchInputs:
    """Build normalized inputs shared by the Rust and Python search backends."""
    modes_by_name = modes_list_to_dict(transport_costs.modes)
    mode_enum_values = get_mode_values(transport_costs.modes, "stay_home")
    mode_id_by_name = {name: index for index, name in enumerate(modes_by_name)}
    mode_name_by_id = {index: name for index, name in enumerate(modes_by_name)}
    is_return_mode_by_id = {
        mode_id_by_name[name]: props["is_return_mode"]
        for name, props in modes_by_name.items()
    }
    return_mode_id_by_id = {
        mode_id_by_name[name]: (
            None if props["return_mode"] is None else mode_id_by_name[props["return_mode"]]
        )
        for name, props in modes_by_name.items()
    }
    needs_vehicle_by_id = {
        mode_id_by_name[name]: props["vehicle"] is not None
        for name, props in modes_by_name.items()
    }
    leg_mode_costs = (
        transport_costs.get_costs_by_od_and_mode(
            ["cost"],
            detail_distances=False,
        )
        .with_columns(
            mode_id=pl.col("mode").replace_strict(mode_id_by_name, return_dtype=pl.UInt16()),
            cost=pl.col("cost").mul(1e6).cast(pl.Float64),
        )
        .sort(["from", "to", "mode_id"])
    )
    return ModeSearchInputs(
        modes_by_name=modes_by_name,
        mode_enum_values=mode_enum_values,
        mode_id_by_name=mode_id_by_name,
        mode_name_by_id=mode_name_by_id,
        is_return_mode_by_id=is_return_mode_by_id,
        return_mode_id_by_id=return_mode_id_by_id,
        needs_vehicle_by_id=needs_vehicle_by_id,
        leg_mode_costs=leg_mode_costs,
    )
