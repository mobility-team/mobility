import os
import subprocess
import pickle
import json
import shutil
import logging

import polars as pl

from importlib import resources
from collections import defaultdict
from rich.spinner import Spinner
from rich.live import Live

from mobility.transport_modes.compute_subtour_mode_probabilities import compute_subtour_mode_probabilities_serial, modes_list_to_dict
from mobility.choice_models.add_index import add_index

class TopKModeSequenceSearch:
    """Finds top-k mode sequences for spatialized trip chains.
    
    Prepares per-iteration inputs (costs, allowed leg modes, location chains),
    invokes an external probability computation, and aggregates chunked results
    into per-chain mode sequences with a compact index.
    """
    
    def run(self, iteration, costs_aggregator, tmp_folders, parameters):
        """Compute top-k mode sequences for all spatialized chains of an iteration.
        
        Builds temporary artifacts (mode props, OD costs, allowed leg modes,
        unique location chains), runs the external scorer, then assembles and
        indexes the resulting sequences.
        
        Args:
            iteration (int): Iteration number (>=1).
            costs_aggregator (TravelCostsAggregator): Provides per-mode OD costs.
            tmp_folders (dict[str, pathlib.Path]): Workspace; must include
                "spatialized-chains", "modes", and a parent folder for temp files.
            parameters (PopulationTripsParameters): Provides k for top-k and other
                tuning values.
        
        Returns:
            pl.DataFrame: Per-step results with columns
                ["demand_group_id","motive_seq_id","dest_seq_id","mode_seq_id",
                 "seq_step_index","mode","iteration"].
        
        Notes:
            - Spawns a subprocess running `compute_subtour_mode_probabilities.py`.
            - Uses on-disk intermediates (pickle/parquet/json) under the parent of
              "spatialized-chains".
            - Assigns stable small integers to `mode_seq_id` via `add_index`.
        """
        
        parent_folder_path = tmp_folders["spatialized-chains"].parent
        
        chains_path = tmp_folders["spatialized-chains"] / f"spatialized_chains_{iteration}.parquet"
        
        tmp_path = parent_folder_path / "tmp_results"
        shutil.rmtree(tmp_path, ignore_errors=True)
        os.makedirs(tmp_path)
        
        # Prepare a list of location chains
        spat_chains = ( 
            pl.scan_parquet(chains_path)
            .group_by(["demand_group_id", "motive_seq_id", "dest_seq_id"])
            .agg(
                locations=pl.col("from").sort_by("seq_step_index")
            )
            .collect()
            .sort(["demand_group_id", "motive_seq_id", "dest_seq_id"])
        )
        
        unique_location_chains = ( 
            spat_chains
            .group_by(["dest_seq_id"])
            .agg(
                pl.col("locations").first()
            )
            .sort("dest_seq_id")
        )
        
        modes = modes_list_to_dict(costs_aggregator.modes)
        
        mode_id = {n: i for i, n in enumerate(modes)}
        id_to_mode = {i: n for i, n in enumerate(modes)}
    
        costs = ( 
            costs_aggregator.get_costs_by_od_and_mode(
                ["cost"],
                congestion=True,
                detail_distances=False
            )
            # Cast costs to ints to avoid float comparison instabilities later
            .with_columns(
                mode_id=pl.col("mode").replace_strict(mode_id, return_dtype=pl.UInt8()),
                cost=pl.col("cost").mul(1e6).cast(pl.Int64)
            )
            .sort(["from", "to", "mode_id"])
        )

        costs = {(row["from"], row["to"], row["mode_id"]): row["cost"] for row in costs.to_dicts()}
            
        is_return_mode = {mode_id[k]: v["is_return_mode"] for  k, v in modes.items()}
        
        leg_modes = defaultdict(list)
        for (from_, to_, mode) in costs.keys():
            if not is_return_mode[mode]:
                leg_modes[(from_, to_)].append(mode)
        
        
        if parameters.mode_sequence_search_parallel is False:
            
            logging.info("Finding probable mode sequences for the spatialized trip chains...")
            
            compute_subtour_mode_probabilities_serial(
                parameters.k_mode_sequences,
                unique_location_chains,
                costs,
                leg_modes,
                modes,
                tmp_path
            )
            
        else:
            
            costs_path = parent_folder_path / "tmp-costs.pkl"
            leg_modes_path = parent_folder_path / "tmp-leg-modes.pkl"
            modes_path = parent_folder_path / "modes-props.json"
            location_chains_path = parent_folder_path / "tmp-location-chains.parquet"
            
            with open(modes_path, "w") as f:
                f.write(json.dumps(modes))

            with open(costs_path, "wb") as f:
                pickle.dump(costs, f, protocol=pickle.HIGHEST_PROTOCOL)  
            
            with open(leg_modes_path, "wb") as f:
                pickle.dump(leg_modes, f, protocol=pickle.HIGHEST_PROTOCOL)
                
            unique_location_chains.write_parquet(location_chains_path)
            
            # Launch the mode sequence probability calculation
            with Live(Spinner("dots", text="Finding probable mode sequences for the spatialized trip chains..."), refresh_per_second=10):
            
                process = subprocess.Popen(
                    [
                        "python",
                        "-u",
                        str(resources.files('mobility') / "transport_modes" / "compute_subtour_mode_probabilities.py"),
                        "--k_sequences", str(parameters.k_mode_sequences),
                        "--location_chains_path", str(location_chains_path),
                        "--costs_path", str(costs_path),
                        "--leg_modes_path", str(leg_modes_path),
                        "--modes_path", str(modes_path),
                        "--tmp_path", str(tmp_path)
                    ]
                )
                
                process.wait()
    
            
        # Agregate all mode sequences chunks
        all_results = (
            spat_chains.select(["demand_group_id", "motive_seq_id", "dest_seq_id"])
            .join(pl.read_parquet(tmp_path), on="dest_seq_id")
            .with_columns(
                mode=pl.col("mode_index").replace_strict(id_to_mode)
            )
        )
        
        mode_sequences = (
            all_results
            .group_by(["demand_group_id", "motive_seq_id", "dest_seq_id", "mode_seq_index"])
            .agg(
                mode_index=pl.col("mode_index").sort_by("seq_step_index").cast(pl.Utf8())
            )
            .with_columns(
                mode_index=pl.col("mode_index").list.join("-")
            )
            .sort(["demand_group_id", "motive_seq_id", "dest_seq_id", "mode_seq_index", "mode_index"])
        )
        
        mode_sequences = add_index(
            mode_sequences,
            col="mode_index",
            index_col_name="mode_seq_id",
            tmp_folders=tmp_folders
        )
        
        all_results = (
            all_results
            .join(
                mode_sequences.select(["demand_group_id", "motive_seq_id", "dest_seq_id", "mode_seq_index", "mode_seq_id"]),
                on=["demand_group_id", "motive_seq_id", "dest_seq_id", "mode_seq_index"]
            )
            .drop("mode_seq_index")
            .select(["demand_group_id", "motive_seq_id", "dest_seq_id", "mode_seq_id", "seq_step_index", "mode"])
            .with_columns(iteration=pl.lit(iteration, dtype=pl.UInt32()))
        )
        
        return all_results
            