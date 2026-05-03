import pathlib
from typing import Any

import polars as pl

from mobility.runtime.assets.file_asset import FileAsset
from mobility.trips.group_day_trips.core.memory_logging import log_memory_checkpoint

from ..sequence_index import add_index
from .assemble import (
    assemble_mode_sequence_rows,
    build_mode_sequence_keys,
    finalize_mode_sequence_rows,
)
from .prepare import build_location_chains, build_search_inputs
from .search_python import run_python_mode_sequence_search
from .search_rust import run_rust_mode_sequence_search


class ModeSequences(FileAsset):
    """Persist candidate mode sequences for one PopulationGroupDayTrips iteration.

    This asset takes already-spatialized destination chains and enriches them with
    feasible travel mode sequences. For each unique destination sequence, it runs
    the configured mode-sequence search backend, then joins the resulting long-form
    mode choices back onto every matching demand-group/activity/time chain.

    The persisted output is a row-per-step table keyed by:
    - `demand_group_id`
    - `activity_seq_id`
    - `time_seq_id`
    - `dest_seq_id`
    - `mode_seq_id`
    - `seq_step_index`

    Each `mode_seq_id` is a stable sequence index derived from the ordered
    step-level mode choices. This allows downstream plan-update code to treat a
    complete mode sequence as a reusable candidate alternative, independent of the
    backend-specific transient `mode_seq_index` produced during search.

    Internally, the asset is organized in three phases:
    1. Prepare grouped trip chains and normalized search inputs from transport
       costs and destination steps.
    2. Run either the Rust or Python mode-sequence search backend on the unique
       destination chains.
    3. Assemble the long-form search rows into persisted output with stable
       `mode_seq_id` values and the expected storage schema.
    """

    def __init__(
        self,
        *,
        run_key: str,
        is_weekday: bool,
        iteration: int,
        base_folder: pathlib.Path,
        destination_sequences: FileAsset,
        transport_costs: Any,
        working_folder: pathlib.Path,
        sequence_index_folder: pathlib.Path,
        parameters: Any,
    ) -> None:
        self.destination_sequences = destination_sequences
        self.transport_costs = transport_costs
        self.working_folder = working_folder
        self.sequence_index_folder = sequence_index_folder
        self.parameters = parameters
        inputs = {
            "version": 1,
            "run_key": run_key,
            "is_weekday": is_weekday,
            "iteration": iteration,
            "destination_sequences": destination_sequences,
        }
        cache_path = pathlib.Path(base_folder) / f"mode_sequences_{iteration}.parquet"
        super().__init__(inputs, cache_path)

    def get_cached_asset(self) -> pl.DataFrame:
        """Return cached mode sequences for one iteration."""
        return pl.read_parquet(self.cache_path)

    def create_and_get_asset(self) -> pl.DataFrame:
        """Compute and persist mode sequences for one iteration."""
        working_folder = self.working_folder
        destination_steps = self.destination_sequences.get_cached_asset()

        log_memory_checkpoint(
            f"mode_sequences:iteration:{self.iteration}:destination_chains",
            destination_chains=destination_steps,
        )

        use_rust_search = self.parameters.use_rust_mode_sequence_search

        trip_chains, unique_destination_chains = build_location_chains(destination_steps)

        log_memory_checkpoint(
            f"mode_sequences:iteration:{self.iteration}:spatialized_chains",
            spatialized_chains=trip_chains,
        )
        log_memory_checkpoint(
            f"mode_sequences:iteration:{self.iteration}:unique_location_chains",
            unique_location_chains=unique_destination_chains,
        )

        search_inputs = build_search_inputs(self.transport_costs)

        if use_rust_search:
            search_rows = run_rust_mode_sequence_search(
                unique_destination_chains=unique_destination_chains,
                leg_mode_costs=search_inputs.leg_mode_costs,
                modes_by_name=search_inputs.modes_by_name,
                mode_id_by_name=search_inputs.mode_id_by_name,
                k_mode_sequences=self.parameters.k_mode_sequences,
            )
        else:
            search_rows = run_python_mode_sequence_search(
                iteration=self.iteration,
                parameters=self.parameters,
                working_folder=working_folder,
                unique_destination_chains=unique_destination_chains,
                leg_mode_costs=search_inputs.leg_mode_costs,
                modes_by_name=search_inputs.modes_by_name,
            )

        search_rows = assemble_mode_sequence_rows(
            trip_chains=trip_chains,
            search_rows=search_rows,
            mode_name_by_id=search_inputs.mode_name_by_id,
        )

        log_memory_checkpoint(
            f"mode_sequences:iteration:{self.iteration}:all_results",
            all_results=search_rows,
        )

        sequence_keys = build_mode_sequence_keys(search_rows)
        sequence_keys = add_index(
            sequence_keys,
            col="mode_index",
            index_col_name="mode_seq_id",
            index_folder=self.sequence_index_folder,
        )

        final_rows = finalize_mode_sequence_rows(
            iteration=self.iteration,
            search_rows=search_rows,
            sequence_keys=sequence_keys,
            mode_enum_values=search_inputs.mode_enum_values,
        )

        log_memory_checkpoint(
            f"mode_sequences:iteration:{self.iteration}:final_results",
            mode_sequences=final_rows,
        )

        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        final_rows.write_parquet(self.cache_path)
        return self.get_cached_asset()
