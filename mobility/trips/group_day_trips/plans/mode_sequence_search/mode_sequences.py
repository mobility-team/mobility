from typing import Any

import pathlib
import shutil

import polars as pl

from mobility.runtime.assets.asset import Asset
from mobility.runtime.assets.in_memory_asset import InMemoryAsset
from mobility.runtime.assets.resolver import (
    asset_resolution_context,
    get_current_asset_resolver,
)
from mobility.trips.group_day_trips.core.memory_logging import log_memory_checkpoint
from mobility.trips.group_day_trips.core.progress import get_group_day_trips_progress
from mobility.trips.group_day_trips.plans.demand_subgroups import DEMAND_UNIT_COLS

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


class ModeSequences(InMemoryAsset):
    """Generate mode candidates by home zone and utility profile.

    Destination chains are shared by home zone. Search runs for each utility
    profile present in that zone; complete plans are attached to demand units later.

    Each `mode_seq_id` is a stable sequence index derived from the ordered
    step-level mode choices. This allows downstream plan-update code to treat a
    complete mode sequence as a reusable candidate alternative, independent of the
    backend-specific transient `mode_seq_index` produced during search.

    Internally, the asset is organized in three phases:
    1. Prepare grouped trip chains and normalized search inputs from transport
       costs and destination steps.
    2. Run either the Rust or Python mode-sequence search backend on the unique
       destination chains.
    3. Assemble the long-form search rows with stable `mode_seq_id` values and
       the expected schema.
    """

    REQUIRED_SCHEMA = {
        "home_zone_id": pl.Int32,
        "utility_profile_id": pl.UInt32,
        "activity_seq_id": pl.UInt32,
        "time_seq_id": pl.UInt32,
        "dest_seq_id": pl.UInt32,
        "mode_seq_id": pl.UInt32,
        "seq_step_index": pl.UInt8,
    }

    def __init__(
        self,
        *,
        is_weekday: bool,
        iteration: int,
        previous_state: Asset | None = None,
        destination_sequences: Asset,
        transport_costs: Any,
        population_segments: list[Any] | None = None,
        parameters: Any,
    ) -> None:
        self.previous_state = previous_state
        self.previous_sequence_index: pl.DataFrame | None = None
        self.destination_sequences = destination_sequences
        self.transport_costs = transport_costs
        self.population_segments = population_segments or []
        self.parameters = parameters
        self.sequence_index: pl.DataFrame | None = None
        inputs = {
            "version": 8,
            "is_weekday": is_weekday,
            "iteration": iteration,
            "previous_state": previous_state,
            "destination_sequences": destination_sequences,
            "transport_costs": transport_costs,
            "population_segments": self.population_segments,
            "mode_sequence_parameters": (
                parameters.mode_sequences if parameters is not None else None
            ),
        }
        super().__init__(inputs)

    def get_index(self) -> pl.DataFrame:
        """Return the mode-sequence index built with this iteration."""
        self.get()
        if self.sequence_index is None:
            raise RuntimeError("Mode sequences have no sequence index.")
        return self.sequence_index

    def get(self, *args, **kwargs) -> pl.DataFrame:
        """Return mode sequences and spill their stable index together."""
        resolver = get_current_asset_resolver()
        if resolver is None:
            with asset_resolution_context() as resolver:
                return self._get_with_resolver(resolver, *args, **kwargs)
        return self._get_with_resolver(resolver, *args, **kwargs)

    def _get_with_resolver(self, resolver, *args, **kwargs) -> pl.DataFrame:
        return resolver.get_in_memory_asset(
            self,
            lambda: self.create_and_get_asset(*args, **kwargs),
            spill_to_disk=self._spill_sequences,
            load_from_disk=self._load_spilled_sequences,
        )

    def _spill_sequences(self, sequences: pl.DataFrame, path: pathlib.Path) -> None:
        """Spill sampled rows and their stable index as one transient asset."""
        if path.exists():
            shutil.rmtree(path)
        path.mkdir(parents=True)
        sequences.write_parquet(path / "sequences.parquet")
        if self.sequence_index is not None:
            self.sequence_index.write_parquet(path / "sequence_index.parquet")

    def _load_spilled_sequences(self, path: pathlib.Path) -> pl.DataFrame:
        """Restore sampled rows and the matching stable index."""
        index_path = path / "sequence_index.parquet"
        self.sequence_index = (
            pl.read_parquet(index_path)
            if index_path.exists()
            else None
        )
        return pl.read_parquet(path / "sequences.parquet")

    def create_and_get_asset(self) -> pl.DataFrame:
        """Compute mode sequences for one iteration."""
        get_group_day_trips_progress().iteration_step(self.iteration, "mode sequences")
        destination_steps = self.destination_sequences.get()
        self.previous_sequence_index = (
            self.previous_state.get().mode_sequence_index
            if self.previous_state is not None
            else None
        )
        demand_groups = getattr(self.destination_sequences, "demand_groups", None)
        if demand_groups is None and self.destination_sequences.previous_state is not None:
            demand_groups = self.destination_sequences.previous_state.get().demand_groups

        log_memory_checkpoint(
            f"mode_sequences:iteration:{self.iteration}:destination_chains",
            destination_chains=destination_steps,
        )

        if destination_steps.height == 0:
            empty_keys = pl.DataFrame(schema={"mode_sequence_key": pl.String})
            _, self.sequence_index = StableKeyIndex(
                key_cols=["mode_sequence_key"],
                index_col="mode_seq_id",
                first_new_id=1,
            ).extend(
                empty_keys,
                previous_index=self.previous_sequence_index,
            )
            return pl.DataFrame(
                schema={
                    **self.REQUIRED_SCHEMA,
                    "mode": pl.String,
                    "iteration": pl.UInt16,
                }
            )

        use_rust_search = self.parameters.mode_sequences.use_rust_mode_sequence_search

        if demand_groups is None:
            zone_profiles = destination_steps.select("home_zone_id").unique().with_columns(
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
            zone_profiles = (
                profile_assignments
                .join(
                    demand_groups.select(DEMAND_UNIT_COLS + ["home_zone_id"]),
                    on=DEMAND_UNIT_COLS,
                )
                .select(["home_zone_id", "utility_profile_id"])
                .unique()
            )
        destination_steps = destination_steps.join(
            zone_profiles,
            on="home_zone_id",
            how="inner",
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
        sequence_keys, self.sequence_index = StableKeyIndex(
            key_cols=["mode_sequence_key"],
            index_col="mode_seq_id",
            first_new_id=1,
        ).extend(
            sequence_keys,
            previous_index=(
                self.previous_sequence_index
            ),
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

        return final_rows
