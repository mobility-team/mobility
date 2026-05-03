from typing import Any

import polars as pl

from mobility.transport.modes.core.mode_values import get_mode_values
from mobility.transport.modes.choice.compute_subtour_mode_probabilities import modes_list_to_dict

from .models import ModeSearchInputs


def build_location_chains(destination_steps: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Build grouped trip chains and one unique location chain per destination sequence."""
    trip_chains = (
        destination_steps
        .group_by(["demand_group_id", "activity_seq_id", "time_seq_id", "dest_seq_id"])
        .agg(locations=pl.col("from").sort_by("seq_step_index"))
        .sort(["demand_group_id", "activity_seq_id", "time_seq_id", "dest_seq_id"])
    )
    unique_destination_chains = (
        trip_chains
        .group_by(["dest_seq_id"])
        .agg(pl.col("locations").first())
        .sort("dest_seq_id")
    )
    return trip_chains, unique_destination_chains


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
