import pathlib
from typing import Any

import polars as pl

from mobility.runtime.assets.cache_schema import read_cached_parquet
from mobility.runtime.assets.file_asset import FileAsset
from mobility.trips.group_day_trips.core.memory_logging import log_memory_checkpoint
from mobility.trips.group_day_trips.core.progress import get_group_day_trips_progress
from mobility.trips.group_day_trips.plans.plan_ids import PLAN_STEP_KEY_SCHEMA

from ..stable_key_index import StableKeyIndex
from .assemble import (
    assemble_mode_sequence_rows,
    build_mode_sequence_keys,
    finalize_mode_sequence_rows,
)
from .debug_logs import log_location_chain_diagnostics
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

    REQUIRED_SCHEMA = {
        **PLAN_STEP_KEY_SCHEMA,
    }

    def __init__(
        self,
        *,
        is_weekday: bool,
        iteration: int,
        base_folder: pathlib.Path,
        previous_mode_sequences: FileAsset | None = None,
        destination_sequences: FileAsset,
        transport_costs: Any,
        population_segments: list[Any] | None = None,
        working_folder: pathlib.Path,
        parameters: Any,
    ) -> None:
        self.previous_mode_sequences = previous_mode_sequences
        self.destination_sequences = destination_sequences
        self.transport_costs = transport_costs
        self.population_segments = population_segments or []
        self.working_folder = working_folder
        self.parameters = parameters
        inputs = {
            "version": 6,
            "is_weekday": is_weekday,
            "iteration": iteration,
            "previous_mode_sequences": previous_mode_sequences,
            "destination_sequences": destination_sequences,
            "transport_costs": transport_costs,
            "population_segments": self.population_segments,
            "mode_sequence_parameters": (
                parameters.mode_sequences if parameters is not None else None
            ),
        }
        cache_path = {
            "sequences": pathlib.Path(base_folder) / f"mode_sequences_{iteration}.parquet",
            "index": pathlib.Path(base_folder) / f"mode_sequence_index_{iteration}.parquet",
        }
        super().__init__(inputs, cache_path)

    def get_cached_asset(self) -> pl.DataFrame:
        """Return cached mode sequences for one iteration."""
        return read_cached_parquet(
            self.cache_path["sequences"],
            table_name="mode_sequences",
            required_schema=self.REQUIRED_SCHEMA,
        )

    def get_index(self) -> pl.DataFrame:
        """Return the mode-sequence index cached with this iteration."""
        return pl.read_parquet(self.cache_path["index"])

    def create_and_get_asset(self) -> pl.DataFrame:
        """Compute and persist mode sequences for one iteration."""
        get_group_day_trips_progress().iteration_step(self.iteration, "mode sequences")
        working_folder = self.working_folder
        destination_steps = self.destination_sequences.get_cached_asset()
        demand_groups = getattr(self.destination_sequences, "demand_groups", None)

        log_memory_checkpoint(
            f"mode_sequences:iteration:{self.iteration}:destination_chains",
            destination_chains=destination_steps,
        )

        use_rust_search = self.parameters.mode_sequences.use_rust_mode_sequence_search

        if demand_groups is None:
            profile_assignments = destination_steps.select(
                ["demand_group_id", "demand_subgroup_id"]
            ).unique().with_columns(
                utility_profile_id=pl.lit(0, dtype=pl.UInt32)
            )
            utility_profiles = None
        else:
            profile_assignments, utility_profiles = (
                self.transport_costs.get_utility_profiles(
                    demand_groups,
                    self.population_segments,
                )
            )
        destination_steps = destination_steps.join(
            profile_assignments,
            on=["demand_group_id", "demand_subgroup_id"],
        )
        trip_chains, unique_destination_chains = build_location_chains(destination_steps)

        log_memory_checkpoint(
            f"mode_sequences:iteration:{self.iteration}:spatialized_chains",
            spatialized_chains=trip_chains,
        )
        log_memory_checkpoint(
            f"mode_sequences:iteration:{self.iteration}:unique_location_chains",
            unique_location_chains=unique_destination_chains,
        )
        log_location_chain_diagnostics(
            iteration=self.iteration,
            destination_steps=destination_steps,
            trip_chains=trip_chains,
            unique_destination_chains=unique_destination_chains,
        )

        if utility_profiles is None:
            profile_costs = self.transport_costs.get_costs_by_od_and_mode(
                ["cost"],
                detail_distances=False,
            ).with_columns(utility_profile_id=pl.lit(0, dtype=pl.UInt32))
        else:
            profile_costs = self.transport_costs.get_profile_costs_by_od_and_mode(
                utility_profiles,
                ["cost"],
            )
        search_inputs = build_search_inputs(
            self.transport_costs,
            leg_mode_costs=profile_costs,
        )

        if use_rust_search:
            search_rows = run_rust_mode_sequence_search(
                unique_destination_chains=unique_destination_chains,
                leg_mode_costs=search_inputs.leg_mode_costs,
                needs_vehicle_by_id=search_inputs.needs_vehicle_by_id,
                return_mode_id_by_id=search_inputs.return_mode_id_by_id,
                is_return_mode_by_id=search_inputs.is_return_mode_by_id,
                modes_by_name=search_inputs.modes_by_name,
                mode_name_by_id=search_inputs.mode_name_by_id,
                k_mode_sequences=self.parameters.mode_sequences.k_mode_sequences,
            )
        else:
            # The legacy Python backend still receives one profile at a time.
            # Production Rust search batches all profiles above.
            profile_results = []
            for profile_id in unique_destination_chains[
                "utility_profile_id"
            ].unique().sort():
                profile_chains = unique_destination_chains.filter(
                    pl.col("utility_profile_id") == profile_id
                ).drop("utility_profile_id")
                profile_leg_costs = search_inputs.leg_mode_costs.filter(
                    pl.col("utility_profile_id") == profile_id
                ).drop("utility_profile_id")
                profile_rows = run_python_mode_sequence_search(
                    iteration=self.iteration,
                    parameters=self.parameters,
                    working_folder=working_folder,
                    unique_destination_chains=profile_chains,
                    leg_mode_costs=profile_leg_costs,
                    modes_by_name=search_inputs.modes_by_name,
                    is_return_mode_by_id=search_inputs.is_return_mode_by_id,
                )
                profile_results.append(
                    profile_rows.with_columns(
                        utility_profile_id=pl.lit(profile_id, dtype=pl.UInt32)
                    )
                )
            search_rows = pl.concat(profile_results)

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
        sequence_keys, _ = StableKeyIndex(
            key_cols=["mode_sequence_key"],
            index_col="mode_seq_id",
            first_new_id=1,
        ).extend_and_cache(
            sequence_keys,
            previous_asset=self.previous_mode_sequences,
            index_path=self.cache_path["index"],
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

        self.cache_path["sequences"].parent.mkdir(parents=True, exist_ok=True)
        final_rows.write_parquet(self.cache_path["sequences"])
        return self.get_cached_asset()
