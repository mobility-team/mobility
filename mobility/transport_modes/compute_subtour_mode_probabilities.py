import argparse
import os
import polars as pl

from concurrent.futures import ProcessPoolExecutor
from mobility.transport_modes.compute_subtour_mode_probs_parallel_utilities import process_batch_parallel, process_batch_serial, worker_init, chunked

def compute_subtour_mode_probabilities_parallel(
        k_sequences,
        location_chains_path,
        costs_path,
        leg_modes_path,
        modes_path,
        tmp_path
 ):
    
    unique_location_chains = pl.read_parquet(location_chains_path)
        
    location_chains = [
        (l[0], l[1] + [l[1][0]])
        for l in zip(
            unique_location_chains["dest_seq_id"].to_list(),
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
    # process_batch(batches[0], debug=True)
    # process_batch([(0, [585, 554, 472, 455, 585])], debug=True)
    
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
        for batch_results in executor.map(process_batch_parallel, batches):
            pass
        
        
    return None


def compute_subtour_mode_probabilities_serial(
        k_sequences,
        unique_location_chains,
        costs,
        leg_modes,
        modes,
        tmp_path
 ):
    
    mode_id = {n:i for i, n in enumerate(modes)}

    needs_vehicle = {mode_id[k]: not v["vehicle"] is None for  k, v in modes.items()}
    multimodal = {mode_id[k]: v["multimodal"] for  k, v in modes.items()}
    return_mode = {mode_id[k]: mode_id[v["return_mode"]] for k, v in modes.items() if not v["return_mode"] is None}
    is_return_mode = {mode_id[k]: v["is_return_mode"] for  k, v in modes.items()}

    vehicles = set([v["vehicle"] for v in modes.values() if not v["vehicle"] is None])
    vehicles = {v: i for i, v in enumerate(vehicles)}
    vehicle_for_mode = {mode_id[k]: vehicles[v["vehicle"]] for k, v in modes.items() if not v["vehicle"] is None}
    n_vehicles = len(vehicles)
        
    location_chains = [
        (l[0], l[1] + [l[1][0]])
        for l in zip(
            unique_location_chains["dest_seq_id"].to_list(),
            unique_location_chains["locations"].to_list()
        )
    ]
    
    batch_size = 50000
    batches = list(chunked(location_chains, batch_size))

    for batch in batches:
        process_batch_serial(
            batch,
            n_vehicles,
            leg_modes,
            costs,
            needs_vehicle,
            vehicle_for_mode,
            multimodal,
            is_return_mode,
            return_mode,
            k_sequences,
            tmp_path
        )
    
        
    return None



def modes_list_to_dict(modes_list):
    
    modes = {
        mode.inputs["parameters"].name: {
            "vehicle": mode.inputs["parameters"].vehicle,
            "multimodal": mode.inputs["parameters"].multimodal,
            "is_return_mode": False,
            "return_mode": mode.inputs["parameters"].return_mode
        }
        for mode in modes_list
    }
    
    for mode in modes_list:
        if mode.inputs["parameters"].return_mode is not None:
            modes[mode.inputs["parameters"].return_mode] = {
                "vehicle": mode.inputs["parameters"].vehicle,
                "multimodal": mode.inputs["parameters"].multimodal,
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
    args = parser.parse_args()

    compute_subtour_mode_probabilities_parallel(
        args.k_sequences,
        args.location_chains_path,
        args.costs_path,
        args.leg_modes_path,
        args.modes_path,
        args.tmp_path
    )
    
