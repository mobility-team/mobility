import heapq
import json
import pathlib
import shortuuid
import logging
import pickle

import polars as pl
import numpy as np



def chunked(seq, batch_size):
    for i in range(0, len(seq), batch_size):
        yield seq[i:i+batch_size]
        
        
def worker_init(k_sequences_, costs_path, leg_modes_path, modes_path, tmp_path_):
    
    logging.info("Initializing worker...")
    
    global k_sequences
    global costs, leg_modes, n_vehicles, leg_modes, needs_vehicle, vehicle_for_mode
    global multimodal, is_return_mode, return_mode
    global tmp_path
    
    k_sequences = int(k_sequences_)
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
    
    with open(costs_path, "rb") as f:
        costs = pickle.load(f)
        
    with open(leg_modes_path, "rb") as f:
        leg_modes = pickle.load(f)
            


def split_at_home(locations):
    parts = []
    home = locations[0]
    current = [locations[0]]
    for loc in locations[1:]:
        current.append(loc)
        if loc == home and len(current) > 1:
            parts.append(current)
            current = [home]
    if len(current) > 1:
        parts.append(current)
    return parts


def merge_two_mode_sequences(L1, L2, k):
    if not L1 or not L2:
        return []
    out, seen, heap = [], set(), []
    heapq.heappush(heap, (L1[0][0] + L2[0][0], 0, 0))
    seen.add((0, 0))
    while heap and len(out) < k:
        c, i, j = heapq.heappop(heap)
        out.append((c, L1[i][1] + L2[j][1]))  # concat sequences
        if i + 1 < len(L1) and (i + 1, j) not in seen:
            heapq.heappush(heap, (L1[i+1][0] + L2[j][0], i + 1, j)); seen.add((i + 1, j))
        if j + 1 < len(L2) and (i, j + 1) not in seen:
            heapq.heappush(heap, (L1[i][0] + L2[j+1][0], i, j + 1)); seen.add((i, j + 1))
    return out

def merge_mode_sequences_list(lists_of_lists, k):
    cur = sorted(lists_of_lists[0], key=lambda x: x[0])
    for L in lists_of_lists[1:]:
        cur = merge_two_mode_sequences(cur, sorted(L, key=lambda x: x[0]), k)
    return cur[:k]

def process_batch_parallel(batch_of_locations, debug=False):
    
    try:
        
        mode_sequences = [
            run_top_k_search(
                loc[0],
                loc[1],
                n_vehicles,
                leg_modes,
                costs,
                needs_vehicle,
                vehicle_for_mode,
                multimodal,
                is_return_mode,
                return_mode,
                k=k_sequences,
                debug=debug
            ) for loc in batch_of_locations
        ]
        
        mode_sequences = [ms for ms in mode_sequences if ms is not None]
        
        ( 
            pl.concat(mode_sequences)
            .write_parquet(
                tmp_path / (shortuuid.uuid() + ".parquet")
            )
        )
        
    except Exception:
        logging.exception("Error when running run_top_k_search.")
        raise


def process_batch_serial(
        batch_of_locations,
        n_vehicles,
        leg_modes,
        costs,
        needs_vehicle,
        vehicle_for_mode,
        multimodal,
        is_return_mode,
        return_mode,
        k_sequences,
        tmp_path,
        debug=False
    ):
    
    try:
        
        mode_sequences = [
            run_top_k_search(
                loc[0],
                loc[1],
                n_vehicles,
                leg_modes,
                costs,
                needs_vehicle,
                vehicle_for_mode,
                multimodal,
                is_return_mode,
                return_mode,
                k=k_sequences,
                debug=debug
            ) for loc in batch_of_locations
        ]
        
        mode_sequences = [ms for ms in mode_sequences if ms is not None]
        
        ( 
            pl.concat(mode_sequences)
            .write_parquet(
                tmp_path / (shortuuid.uuid() + ".parquet")
            )
        )
        
    except Exception:
        logging.exception("Error when running run_top_k_search.")
        raise


def run_top_k_search(
        dest_seq_id,
        locations_full,
        n_vehicles,
        leg_modes,
        costs,
        needs_vehicle,
        vehicle_for_mode,
        multimodal,
        is_return_mode,
        return_mode,
        k=10,
        debug=False
    ):
    
    if debug:
        print("---")
        print(locations_full)
    
    # Split the location sequences at each home location to avoid very long sequences
    # (hypothesis : vehicles are back home at the end of each of these sub sequences)
    locations_parts = split_at_home(locations_full)
    all_results = []
    
    for locations in locations_parts:
    
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
                        if vehicle_locations[v_id] != current_location:# and enforced_mode is None:
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
                    
        all_results.append(results)       
            
    results = merge_mode_sequences_list(all_results, k=k)
    
    if len(results) == 0:
        
        results = None
    
    else:
        
        # Transform costs into utilities
        # (remove the max so that exponentials don't overflow in the next step)
        utilities = -np.array([r[0] for r in results])
        utilities -= np.max(utilities)
        
        # Compute probabilities
        prob = np.exp(utilities)
        prob = prob/prob.sum()
        
        # Keep only the first 98 % of the cumulative distribution
        i_max = np.argmax(prob.cumsum() > 0.98)
          
        rows = []
        for i, (total_cost, mode_seq) in enumerate(results):
            if i < i_max+1:
                for leg_idx, m_id in enumerate(mode_seq):
                    rows.append([i, locations_full[leg_idx+1], leg_idx+1, m_id])
        
        results = ( 
            pl.DataFrame(
                rows,
                schema=["mode_seq_index", "location", "seq_step_index", "mode_index"],
                orient="row"
            )
            .with_columns(
                dest_seq_id=pl.lit(dest_seq_id, dtype=pl.UInt64())
            )
        )
    
        if debug:
            print(results)
            
    
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
