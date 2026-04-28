import json
import logging
import os
import pathlib
import pickle
import shutil
import subprocess
from collections import defaultdict
from importlib import resources
from typing import Any

import polars as pl
import psutil
from rich.live import Live
from rich.spinner import Spinner

from .sequence_index import add_index
from mobility.runtime.assets.file_asset import FileAsset
from mobility.transport.modes.core.mode_values import get_mode_values
from mobility.transport.modes.choice.compute_subtour_mode_probabilities import (
    compute_subtour_mode_probabilities_serial,
    modes_list_to_dict,
)


class ModeSequences(FileAsset):
    """Persist mode sequences produced for one PopulationGroupDayTrips iteration."""

    @staticmethod
    def _log_memory_checkpoint(label: str, **objects: Any) -> None:
        """Log process memory plus cheap summaries of already-available objects."""
        memory_info = psutil.Process().memory_info()
        parts = [
            f"rss={memory_info.rss / (1024 ** 3):.2f}GB",
            f"vms={memory_info.vms / (1024 ** 3):.2f}GB",
        ]
        private = getattr(memory_info, "private", None)
        if private is not None:
            parts.append(f"private={private / (1024 ** 3):.2f}GB")

        for name, obj in objects.items():
            if obj is None:
                parts.append(f"{name}=none")
            elif isinstance(obj, pl.DataFrame):
                parts.append(
                    f"{name}=rows={obj.height}, cols={obj.width}, est={obj.estimated_size('mb'):.2f}MB"
                )
            elif isinstance(obj, pl.LazyFrame):
                parts.append(f"{name}=lazy cols={len(obj.collect_schema().names())}")
            elif isinstance(obj, dict):
                parts.append(f"{name}=entries={len(obj)}")
            elif isinstance(obj, (list, tuple, set)):
                parts.append(f"{name}=len={len(obj)}")
            else:
                parts.append(f"{name}={type(obj).__name__}")

        logging.debug("Memory checkpoint %s | %s", label, " | ".join(parts))

    def __init__(
        self,
        *,
        run_key: str,
        is_weekday: bool,
        iteration: int,
        base_folder: pathlib.Path,
        destination_sequences: FileAsset | None = None,
        transport_costs: Any = None,
        working_folder: pathlib.Path | None = None,
        sequence_index_folder: pathlib.Path | None = None,
        parameters: Any = None,
    ) -> None:
        self.destination_sequences = destination_sequences
        self.transport_costs = transport_costs
        self.working_folder = working_folder
        self.sequence_index_folder = sequence_index_folder
        self.parameters = parameters
        self.iteration = iteration
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
        if self.destination_sequences is None:
            raise ValueError("Cannot build mode sequences without destination sequences.")
        if self.transport_costs is None:
            raise ValueError("Cannot build mode sequences without transport costs.")
        if self.working_folder is None:
            raise ValueError("Cannot build mode sequences without a working folder.")
        if self.sequence_index_folder is None:
            raise ValueError("Cannot build mode sequences without a sequence index folder.")
        if self.parameters is None:
            raise ValueError("Cannot build mode sequences without parameters.")

        parent_folder_path = self.working_folder
        destination_chains = self.destination_sequences.get_cached_asset()
        self._log_memory_checkpoint(
            f"mode_sequences:iteration:{self.iteration}:destination_chains",
            destination_chains=destination_chains,
        )

        tmp_path = parent_folder_path / "tmp_results"
        shutil.rmtree(tmp_path, ignore_errors=True)
        os.makedirs(tmp_path)

        spatialized_chains = (
            destination_chains
            .group_by(["demand_group_id", "activity_seq_id", "time_seq_id", "dest_seq_id"])
            .agg(locations=pl.col("from").sort_by("seq_step_index"))
            .sort(["demand_group_id", "activity_seq_id", "time_seq_id", "dest_seq_id"])
        )
        self._log_memory_checkpoint(
            f"mode_sequences:iteration:{self.iteration}:spatialized_chains",
            spatialized_chains=spatialized_chains,
        )
        unique_location_chains = (
            spatialized_chains
            .group_by(["dest_seq_id"])
            .agg(pl.col("locations").first())
            .sort("dest_seq_id")
        )
        self._log_memory_checkpoint(
            f"mode_sequences:iteration:{self.iteration}:unique_location_chains",
            unique_location_chains=unique_location_chains,
        )

        modes = modes_list_to_dict(self.transport_costs.modes)
        mode_values = get_mode_values(self.transport_costs.modes, "stay_home")
        mode_id = {name: index for index, name in enumerate(modes)}
        id_to_mode = {index: name for index, name in enumerate(modes)}

        costs = (
            self.transport_costs.get_costs_by_od_and_mode(
                ["cost"],
                detail_distances=False,
            )
            .with_columns(
                mode_id=pl.col("mode").replace_strict(mode_id, return_dtype=pl.UInt8()),
                cost=pl.col("cost").mul(1e6).cast(pl.Int64),
            )
            .sort(["from", "to", "mode_id"])
        )
        costs = {
            (row["from"], row["to"], row["mode_id"]): row["cost"]
            for row in costs.to_dicts()
        }
        self._log_memory_checkpoint(
            f"mode_sequences:iteration:{self.iteration}:costs_dict",
            costs=costs,
        )

        is_return_mode = {mode_id[key]: value["is_return_mode"] for key, value in modes.items()}
        leg_modes = defaultdict(list)
        for from_zone, to_zone, mode in costs.keys():
            if not is_return_mode[mode]:
                leg_modes[(from_zone, to_zone)].append(mode)
        self._log_memory_checkpoint(
            f"mode_sequences:iteration:{self.iteration}:leg_modes",
            leg_modes=leg_modes,
        )

        if self.parameters.mode_sequence_search_parallel is False:
            logging.info("Finding probable mode sequences for the spatialized trip chains...")
            compute_subtour_mode_probabilities_serial(
                self.parameters.k_mode_sequences,
                unique_location_chains,
                costs,
                leg_modes,
                modes,
                tmp_path,
            )
        else:
            self._run_parallel_search(
                parent_folder_path=parent_folder_path,
                unique_location_chains=unique_location_chains,
                costs=costs,
                leg_modes=leg_modes,
                modes=modes,
                tmp_path=tmp_path,
            )

        all_results = (
            spatialized_chains.select(["demand_group_id", "activity_seq_id", "time_seq_id", "dest_seq_id"])
            .join(pl.read_parquet(tmp_path), on="dest_seq_id")
            .with_columns(mode=pl.col("mode_index").replace_strict(id_to_mode))
        )
        self._log_memory_checkpoint(
            f"mode_sequences:iteration:{self.iteration}:all_results",
            all_results=all_results,
        )
        mode_sequences = (
            all_results
            .group_by(["demand_group_id", "activity_seq_id", "time_seq_id", "dest_seq_id", "mode_seq_index"])
            .agg(mode_index=pl.col("mode_index").sort_by("seq_step_index").cast(pl.Utf8()))
            .with_columns(mode_index=pl.col("mode_index").list.join("-"))
            .sort(["demand_group_id", "activity_seq_id", "time_seq_id", "dest_seq_id", "mode_seq_index", "mode_index"])
        )
        mode_sequences = add_index(
            mode_sequences,
            col="mode_index",
            index_col_name="mode_seq_id",
            index_folder=self.sequence_index_folder,
        )
        all_results = (
            all_results
            .join(
                mode_sequences.select(
                    ["demand_group_id", "activity_seq_id", "time_seq_id", "dest_seq_id", "mode_seq_index", "mode_seq_id"]
                ),
                on=["demand_group_id", "activity_seq_id", "time_seq_id", "dest_seq_id", "mode_seq_index"],
            )
            .drop("mode_seq_index")
            .select(["demand_group_id", "activity_seq_id", "time_seq_id", "dest_seq_id", "mode_seq_id", "seq_step_index", "mode"])
            .with_columns(
                seq_step_index=pl.col("seq_step_index").cast(pl.UInt8),
                mode=pl.col("mode").cast(pl.Enum(mode_values)),
                iteration=pl.lit(self.iteration, dtype=pl.UInt16()),
            )
        )
        self._log_memory_checkpoint(
            f"mode_sequences:iteration:{self.iteration}:final_results",
            mode_sequences=all_results,
        )

        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        all_results.write_parquet(self.cache_path)
        return self.get_cached_asset()


    def _run_parallel_search(
        self,
        *,
        parent_folder_path: pathlib.Path,
        unique_location_chains: pl.DataFrame,
        costs: dict[tuple[Any, Any, Any], Any],
        leg_modes: dict[tuple[Any, Any], list[Any]],
        modes: dict[str, Any],
        tmp_path: pathlib.Path,
    ) -> None:
        """Run the mode-sequence search in a subprocess."""
        costs_path = parent_folder_path / "tmp-costs.pkl"
        leg_modes_path = parent_folder_path / "tmp-leg-modes.pkl"
        modes_path = parent_folder_path / "modes-props.json"
        location_chains_path = parent_folder_path / "tmp-location-chains.parquet"

        with open(modes_path, "w", encoding="utf-8") as file:
            file.write(json.dumps(modes))
        with open(costs_path, "wb") as file:
            pickle.dump(costs, file, protocol=pickle.HIGHEST_PROTOCOL)
        with open(leg_modes_path, "wb") as file:
            pickle.dump(leg_modes, file, protocol=pickle.HIGHEST_PROTOCOL)
        unique_location_chains.write_parquet(location_chains_path)

        with Live(
            Spinner("dots", text="Finding probable mode sequences for the spatialized trip chains..."),
            refresh_per_second=10,
        ):
            process = subprocess.Popen(
                [
                    "python",
                    "-u",
                    str(
                        resources.files("mobility.transport.modes.choice")
                        / "compute_subtour_mode_probabilities.py"
                    ),
                    "--k_sequences",
                    str(self.parameters.k_mode_sequences),
                    "--location_chains_path",
                    str(location_chains_path),
                    "--costs_path",
                    str(costs_path),
                    "--leg_modes_path",
                    str(leg_modes_path),
                    "--modes_path",
                    str(modes_path),
                    "--tmp_path",
                    str(tmp_path),
                ]
            )
            process.wait()
