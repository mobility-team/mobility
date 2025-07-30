import heapq
import json
import pathlib
import shortuuid
import logging

import polars as pl
import numpy as np

from collections import defaultdict

def chunked(seq, batch_size):
    for i in range(0, len(seq), batch_size):
        yield seq[i:i+batch_size]
        
        
def worker_init(costs_path, modes_path, tmp_path_):
    
    logging.info("Initializing worker...")
    
    global costs, leg_modes, n_vehicles, leg_modes, needs_vehicle, vehicle_for_mode
    global multimodal, is_return_mode, return_mode
    global tmp_path
    
    tmp_path = pathlib.Path(tmp_path_)
    
    # Prepare mode properties
    with open(modes_path, "r") as f:
        modes = json.load(f)
    
    mode_id = {n:i for i, n in enumerate(modes)}

    needs_vehicle = {mode_id[k]: not v["vehicle"] is None for  k, v in modes.items()}
    multimodal = {mode_id[k]: v["multimodal"] for  k, v in modes.items()}
    return_mode = {mode_id[k]: mode_id[v["return_mode"]] for k, v in modes.items() if not v["return_mode"] is None}
    is_return_mode = {mode_id[k]: v["is_return_mode"] for  k, v in modes.items()}

    vehicles = set([v["vehicle"] for v in modes.values() if not v["vehicle"] is None])
    vehicles = {v: i for i, v in enumerate(vehicles)}
    vehicle_for_mode = {mode_id[k]: vehicles[v["vehicle"]] for k, v in modes.items() if not v["vehicle"] is None}
    n_vehicles = len(vehicles)
    
    costs = ( 
        pl.read_parquet(costs_path)
        .with_columns(
            mode_id=pl.col("mode").replace_strict(mode_id, return_dtype=pl.UInt8())
        )
    )

    costs = {(row["from"], row["to"], row["mode_id"]): row["cost"] for row in costs.to_dicts()}
    
    leg_modes = defaultdict(list)
    for (from_, to_, mode) in costs.keys():
        if not is_return_mode[mode]:
            leg_modes[(from_, to_)].append(mode)
            



def process_batch(batch_of_locations, k=10):
    
    ( 
        pl.concat(
            [
                run_top_k_search(
                    loc[0],
                    loc[1],
                    k=k
                ) for loc in batch_of_locations
            ]
        )
        .write_parquet(
            tmp_path / (shortuuid.uuid() + ".parquet")
        )
    )

    return None


def run_top_k_search(
        index,
        locations,
        k=10
    ):
    
    if len(locations) == 2:
        
        available_mode_ids = leg_modes[(locations[0], locations[1])]
        results = [(costs[(locations[0], locations[1], m_id)], [m_id]) for m_id in available_mode_ids]
        
    else:
    
        subtours = get_possible_subtours_from_locations(locations)
            
        n_legs = len(locations) - 1
        vehicle_locations = [locations[0]] * n_vehicles
        mode_sequence = []
        return_mode_constraints = {}
        state = (0, vehicle_locations, mode_sequence, return_mode_constraints)
        heap = [(0.0, state)]
        results = []
        
        # Create a map between start and end destinations of subtours, when they 
        # have a length > 2 and that their first and last leg are symetrical 
        # (ie a->b and b->a)
        subtour_first_leg_eq_last_leg = {s[0]: (locations[s[0]] == locations[s[-1]] and locations[s[1]] == locations[s[-2]], s[-1]) for s in subtours if len(s) > 2}
        subtour_first_leg_eq_last_leg = {k: v[1] for k, v in subtour_first_leg_eq_last_leg.items() if v[0] is True}
        
        while heap and len(results) < k:
            
            cost, (leg_idx, vehicle_locations, mode_sequence, return_mode_constraints) = heapq.heappop(heap)
            
            # If we reached the end of the tour and all vehicle are at home,
            # push the result and go to the next value on the heap
            if leg_idx == n_legs:
                if all(vl == locations[0] for vl in vehicle_locations):
                    results.append((cost, mode_sequence))
                continue
            
            current_location = locations[leg_idx]
            next_location = locations[leg_idx+1]
            
            enforced_mode = return_mode_constraints.get(leg_idx, None)
            available_mode_ids = leg_modes[(current_location, next_location)] if enforced_mode is None else [enforced_mode]
                
            for m_id in available_mode_ids:
                    
                mode_cost = costs[(current_location, next_location, m_id)]
                
                next_vehicle_locations = list(vehicle_locations)
                next_return_mode_constraints = dict(return_mode_constraints)
                
                if needs_vehicle[m_id]:
                    
                    v_id = vehicle_for_mode[m_id]
        
                    # Check if the vehicle needed is available for the trip,
                    # if not go to the next value on the heap
                    if vehicle_locations[v_id] != current_location and enforced_mode is None:
                        continue
                    
                    # Move the vehicle to next location
                    next_vehicle_locations[v_id] = next_location
                    
                    # Special case for multimodal modes
                    if multimodal[m_id] and not is_return_mode[m_id]:
                        
                        # Check if the leg is the start of a subtour that is compatible
                        # with a multimodal mode with vehicle (ie the vehicle has to 
                        # be retrieved at the end of the subtour)
                        if leg_idx not in subtour_first_leg_eq_last_leg:
                            continue
                        
                        # Force the return mode on the last leg of the subtour
                        subtour_last_leg_index = subtour_first_leg_eq_last_leg[leg_idx]-1
                        next_return_mode_constraints[subtour_last_leg_index] = return_mode[m_id]
                            
                
                # Push the new state to the heap
                state = (leg_idx+1, next_vehicle_locations, mode_sequence + [m_id], next_return_mode_constraints)
                heapq.heappush(heap, (cost+mode_cost, state))
    
    c = np.array([r[0] for r in results])
    p = np.exp(-c)
    p /= p.sum()
    i_max = np.argmax(p.cumsum() > 0.98)
      
    rows = []
    for i, (total_cost, mode_seq) in enumerate(results):
        if i < i_max+1:
            for leg_idx, m_id in enumerate(mode_seq):
                rows.append([i, leg_idx+1, m_id, p[i]])
    
    results = ( 
        pl.DataFrame(
            rows,
            schema=["mode_seq_index", "subseq_step_index", "mode_index", "p_mode_seq"],
            orient="row"
        )
        .with_columns(
            index=pl.lit(index)
        )
    )
    
    return results
    

def get_possible_subtours_from_locations(locations):
    
    last_seen = {}
    subtours = []
    n_locations = len(locations)
    for end_idx, place in enumerate(locations):
        if place in last_seen:
            start_idx = last_seen[place]
            if start_idx != 0 or end_idx != n_locations-1:
                subtours.append(np.arange(start_idx, end_idx+1))
        last_seen[place] = end_idx
        
    return subtours
