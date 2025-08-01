import logging
import argparse
import json
import os

import polars as pl

from concurrent.futures import ProcessPoolExecutor
from rich.progress import Progress, BarColumn, TextColumn, TimeRemainingColumn

from mobility.transport_modes.compute_subtour_mode_probs_parallel_utilities import process_batch, worker_init, chunked

def compute_subtour_mode_probabilities(chains_path, costs_path, modes_path, tmp_path, output_path):
    
    logging.info("Computing subtour mode probabilities...")
    
    logging.info("Formatting data for processing...")
    
    # Prepare a list of location chains
    chains_groups = ( 
        pl.scan_parquet(chains_path)
        .select(["home_zone_id", "csp", "motive_subseq", "subseq_step_index", "from"])
        .sort("subseq_step_index")
        .group_by(["home_zone_id", "csp", "motive_subseq"])
        .agg(
            locations=pl.col("from"),
            locations_str=pl.col("from").str.join("-")
        )
        .collect(engine="streaming")
    )
    
    unique_location_chains = ( 
        chains_groups
        .group_by("locations_str")
        .agg(
            locations=pl.col("locations").first()
        )
        .with_row_index()
    )

    location_chains = [
        (l[0], l[1] + [l[1][0]])
        for l in zip(unique_location_chains["index"].to_list(), unique_location_chains["locations"].to_list())
    ]
    

    with open(modes_path, "r") as f:
        modes = json.load(f)
    
    id_to_mode = {i: n for i, n in enumerate(modes)}

    # Run the mode sequence search in parallel
    logging.info("Running the mode sequence search in parallel...")
    
    batch_size = 3000
    batches = list(chunked(location_chains, batch_size))[0:20]
    total = len(batches)
    
    with Progress(
        "[progress.percentage]{task.percentage:>3.0f}%",
        BarColumn(),
        TimeRemainingColumn(),
        TextColumn("{task.completed}/{task.total} batches"),
    ) as progress:
        
        task = progress.add_task("[green]Processing...", total=total)
        
        n_workers = max(1, os.cpu_count() - 2)
        
        ppe = ProcessPoolExecutor(
            max_workers=n_workers,
            initializer=worker_init,
            initargs=(
                costs_path,
                modes_path,
                tmp_path
            )
        )
        
        with ppe as executor:
            for batch_results in executor.map(process_batch, batches):
                progress.update(task, advance=1)

        
        logging.info("Aggregating results...")
   
        all_results = (
            pl.read_parquet(tmp_path)
            .with_columns(
                mode=pl.col("mode_index").replace_strict(id_to_mode)
            )
            .join(unique_location_chains.select(["index", "locations_str"]), on=["index"])
            .join(chains_groups.select(["locations_str", "home_zone_id", "csp", "motive_subseq"]), on=["locations_str"])
            .join(pl.read_parquet(chains_path), on=["home_zone_id", "csp", "motive_subseq", "subseq_step_index"])
        )
        
        all_results.write_parquet(output_path)
        
        
    return None



def modes_list_to_dict(modes_list):
    
    modes = {
        mode.name: {
            "vehicle": mode.vehicle,
            "multimodal": mode.multimodal,
            "is_return_mode": False,
            "return_mode": mode.return_mode
        }
        for mode in modes_list
    }
    
    for mode in modes_list:
        if not mode.return_mode is None:
            modes[mode.return_mode] = {
                "vehicle": mode.vehicle,
                "multimodal": mode.multimodal,
                "is_return_mode": True,
                "return_mode": None
            }
            
    modes = json.dumps(modes)
            
    return modes
        


if __name__ == "__main__":
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--chains_path")
    parser.add_argument("--costs_path")
    parser.add_argument("--modes_path")
    parser.add_argument("--tmp_path")
    parser.add_argument("--output_path")
    args = parser.parse_args()

    compute_subtour_mode_probabilities(
        args.chains_path,
        args.costs_path,
        args.modes_path,
        args.tmp_path,
        args.output_path
    )
    