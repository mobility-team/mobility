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
from rich.live import Live
from rich.spinner import Spinner

from mobility.trips.group_day_trips.core.memory_logging import log_memory_checkpoint
from mobility.transport.modes.choice.compute_subtour_mode_probabilities import (
    compute_subtour_mode_probabilities_serial,
)


def run_python_mode_sequence_search(
    *,
    iteration: int,
    parameters: Any,
    working_folder: pathlib.Path,
    unique_destination_chains: pl.DataFrame,
    leg_mode_costs: pl.DataFrame,
    modes_by_name: dict[str, Any],
    is_return_mode_by_id: dict[int, bool],
) -> pl.DataFrame:
    """Run the Python mode-sequence search backend."""
    tmp_folder = working_folder / "tmp_results"
    shutil.rmtree(tmp_folder, ignore_errors=True)
    os.makedirs(tmp_folder)

    cost_by_origin_destination_mode = {
        (row["from"], row["to"], row["mode_id"]): row["cost"]
        for row in leg_mode_costs.to_dicts()
    }

    log_memory_checkpoint(
        f"mode_sequences:iteration:{iteration}:costs_dict",
        costs=cost_by_origin_destination_mode,
    )

    mode_ids_by_leg = defaultdict(list)
    for origin, destination, mode_id in cost_by_origin_destination_mode.keys():
        if not is_return_mode_by_id[mode_id]:
            mode_ids_by_leg[(origin, destination)].append(mode_id)

    log_memory_checkpoint(
        f"mode_sequences:iteration:{iteration}:leg_modes",
        leg_modes=mode_ids_by_leg,
    )

    if parameters.mode_sequence_search_parallel is False:
        logging.info("Finding probable mode sequences for the spatialized trip chains...")
        compute_subtour_mode_probabilities_serial(
            parameters.k_mode_sequences,
            unique_destination_chains,
            cost_by_origin_destination_mode,
            mode_ids_by_leg,
            modes_by_name,
            tmp_folder,
        )
    else:
        run_python_mode_sequence_search_subprocess(
            parameters=parameters,
            working_folder=working_folder,
            unique_destination_chains=unique_destination_chains,
            cost_by_origin_destination_mode=cost_by_origin_destination_mode,
            mode_ids_by_leg=mode_ids_by_leg,
            modes_by_name=modes_by_name,
            tmp_folder=tmp_folder,
        )

    return pl.read_parquet(tmp_folder)


def run_python_mode_sequence_search_subprocess(
    *,
    parameters: Any,
    working_folder: pathlib.Path,
    unique_destination_chains: pl.DataFrame,
    cost_by_origin_destination_mode: dict[tuple[Any, Any, Any], Any],
    mode_ids_by_leg: dict[tuple[Any, Any], list[Any]],
    modes_by_name: dict[str, Any],
    tmp_folder: pathlib.Path,
) -> None:
    """Run the mode-sequence search in a subprocess."""
    costs_path = working_folder / "tmp-costs.pkl"
    leg_modes_path = working_folder / "tmp-leg-modes.pkl"
    modes_path = working_folder / "modes-props.json"
    location_chains_path = working_folder / "tmp-location-chains.parquet"

    with open(modes_path, "w", encoding="utf-8") as file:
        file.write(json.dumps(modes_by_name))
    with open(costs_path, "wb") as file:
        pickle.dump(cost_by_origin_destination_mode, file, protocol=pickle.HIGHEST_PROTOCOL)
    with open(leg_modes_path, "wb") as file:
        pickle.dump(mode_ids_by_leg, file, protocol=pickle.HIGHEST_PROTOCOL)
    unique_destination_chains.write_parquet(location_chains_path)

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
                str(parameters.k_mode_sequences),
                "--location_chains_path",
                str(location_chains_path),
                "--costs_path",
                str(costs_path),
                "--leg_modes_path",
                str(leg_modes_path),
                "--modes_path",
                str(modes_path),
                "--tmp_path",
                str(tmp_folder),
            ]
        )
        process.wait()
