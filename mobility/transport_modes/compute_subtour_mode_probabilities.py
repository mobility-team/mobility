import argparse
import os
import polars as pl

from concurrent.futures import ProcessPoolExecutor
from mobility.transport_modes.compute_subtour_mode_probs_parallel_utilities import process_batch, worker_init, chunked

def compute_subtour_mode_probabilities(
        k_sequences,
        location_chains_path,
        costs_path,
        leg_modes_path,
        modes_path,
        tmp_path,
        output_path
 ):
    
    unique_location_chains = pl.read_parquet(location_chains_path)
        
    location_chains = [
        (l[0], l[1] + [l[1][0]])
        for l in zip(
            unique_location_chains["locations_index"].to_list(),
            unique_location_chains["locations"].to_list()
        )
    ]
    
    batch_size = 50000
    batches = list(chunked(location_chains, batch_size))
    n_workers = max(1, int(os.cpu_count()/2))
    
    # To debug without parallel processing that masks errors
    # worker_init(
    #     k_sequences,
    #     costs_path,
    #     leg_modes_path,
    #     modes_path,
    #     tmp_path
    # )
    # process_batch(batches[0][0:100], debug=True)
    
    ppe = ProcessPoolExecutor(
        max_workers=n_workers,
        initializer=worker_init,
        initargs=(
            k_sequences,
            costs_path,
            leg_modes_path,
            modes_path,
            tmp_path
        )
    )
    
    with ppe as executor:
        for batch_results in executor.map(process_batch, batches):
            pass
        
        
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
            
    return modes
        


if __name__ == "__main__":
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--k_sequences")
    parser.add_argument("--location_chains_path")
    parser.add_argument("--costs_path")
    parser.add_argument("--leg_modes_path")
    parser.add_argument("--modes_path")
    parser.add_argument("--tmp_path")
    parser.add_argument("--output_path")
    args = parser.parse_args()

    compute_subtour_mode_probabilities(
        args.k_sequences,
        args.location_chains_path,
        args.costs_path,
        args.leg_modes_path,
        args.modes_path,
        args.tmp_path,
        args.output_path
    )
    