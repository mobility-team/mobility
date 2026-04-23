import numpy as np
from numba import njit
import itertools
import random
import pandas as pd
from itertools import combinations
import time

def prepare_mode_properties_arrays(modes):
    
    mode_id = {n:i for i, n in enumerate(modes)}
    needs_vehicle = np.array([m["vehicle"] is not None for m in modes.values()], dtype=np.bool_)
    multimodal = np.array([m["multimodal"] for m in modes.values()], dtype=np.bool_)

    veh_id_map = {}
    vehicle_for_mode = np.full(len(modes), -1, np.int32)
    for n, i in mode_id.items():
        v = modes[n]["vehicle"]
        if v is not None:
            vehicle_for_mode[i] = veh_id_map.setdefault(v, len(veh_id_map))

    return_mode = np.full(len(modes), -1, np.int32)
    for n, i in mode_id.items():
        r = modes[n]["return_mode"]
        if r is not None:
            return_mode[i] = mode_id[r]
    
    is_return_mode = np.repeat(False, len(modes))
    is_return_mode[return_mode[return_mode > 0]] = True
    
    modes_wo_veh_ids = np.where(~needs_vehicle)[0]
    modes_wo_veh_wo_ret_ids = np.where(~needs_vehicle & ~is_return_mode)[0]
            
    return mode_id, multimodal, needs_vehicle, vehicle_for_mode, modes_wo_veh_ids, modes_wo_veh_wo_ret_ids, return_mode
    

@njit(cache=True)
def is_mode_available_numba(
        loc_ids,
        base_modes,        # original modes seq (int32)
        subtour_idx,       # int32 array of leg indices (exclude last leg since we set modes on legs)
        subtour_mode_id,   # candidate mode
        vehicle_for_mode,
        multimodal,
        return_mode,
        walk_id,
        n_vehicles,
        multimodal_needs_vehicle
    ):
    
    if n_vehicles == 0:
        return True

    # copy base_modes -> scratch_modes
    n_legs = base_modes.size
    scratch_modes = base_modes.copy()

    n_legs_subtour = subtour_idx.size - 1

    # Build subtour pattern
    if multimodal_needs_vehicle[subtour_mode_id]:
        # needs vehicle AND multimodal
        if n_legs_subtour > 2:
            # first leg: mode
            scratch_modes[subtour_idx[0]] = subtour_mode_id
            # middle legs: walk
            for k in range(1, n_legs_subtour - 1):
                scratch_modes[subtour_idx[k]] = walk_id
            # last leg: return mode
            scratch_modes[subtour_idx[-2]] = return_mode[subtour_mode_id]
        elif n_legs_subtour == 2:
            # two legs: mode then return
            scratch_modes[subtour_idx[0]] = subtour_mode_id
            scratch_modes[subtour_idx[1]] = return_mode[subtour_mode_id]
        else:
            return False
    else:
        # just set all legs to the same mode
        scratch_modes[subtour_idx[:-1]] = subtour_mode_id

    # set all vehicles at start location
    vehicle_locations = np.empty(n_vehicles, dtype=np.int64)
    for v in range(n_vehicles):
        vehicle_locations[v] = loc_ids[0]
    
    # simulate trip
    # loc_ids has len = n_legs + 1 (cities per leg)
    for i in range(n_legs):
        m = scratch_modes[i]
        veh = vehicle_for_mode[m]
        
        if veh != -1:
            # check availability at start
            if vehicle_locations[veh] != loc_ids[i]:
                return False
            # leave vehicle at destination if not multimodal
            if not multimodal[m]:
                vehicle_locations[veh] = loc_ids[i + 1]

    return True



def timed(func):
    def wrapper(*args, **kwargs):
        t0 = time.perf_counter()
        result = func(*args, **kwargs)
        t1 = time.perf_counter()
        # print(f"{func.__name__} took {t1-t0:.4f} seconds")
        return result
    return wrapper

@timed
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
   
@timed
def get_subtours_combinations(subtours):
    
    n_subtours = len(subtours)
    
    subtour_combinations = []
    
    if n_subtours == 1:
        
        subtour_combinations = [(0,)]
    
    else:
    
        subtour_index = list(range(n_subtours))
        
        n_perm = 2**n_subtours-1
        
        if n_perm < 20:
            
            subtour_combinations = list(itertools.permutations(subtour_index, 3))
                
        else:
            
            n_samples = int(20.0*(n_perm/20.0)**0.5)
            subtour_combinations = [np.random.choice(subtour_index, size=3, replace=False) for k in range(n_samples)]
    
    return subtour_combinations

@timed
def get_costs(locations, modes, return_modes):
    
    modes = [m["name"] for m in modes]

    base_costs = list(combinations(set(locations), 2))
    base_costs = pd.DataFrame(base_costs, columns=["from", "to"])
    
    all_costs = []
    
    for m in modes:
        
        costs = base_costs.copy()
        costs["mode"] = m
        costs["cost"] = np.random.uniform(size=base_costs.shape[0])
        all_costs.append(costs)
        
        ret_costs = costs.copy()
        ret_costs.rename({"from": "to", "to": "from"}, axis=1, inplace=True)
        all_costs.append(ret_costs)
        
        if m in return_modes.keys():
            rev_costs = costs.copy()
            rev_costs["mode"] = return_modes[m]["name"]
            all_costs.append(rev_costs)
            
            rev_ret_costs = ret_costs.copy()
            rev_ret_costs["mode"] = return_modes[m]["name"]
            all_costs.append(rev_ret_costs)
        
    all_costs = pd.concat(all_costs)
    
    return all_costs

@timed
def get_mode_probabilities(costs):
    
    probs = costs.copy()
    
    probs["exp_u"] = np.exp(-probs["cost"])
    
    probs = pd.merge(probs, probs.groupby(["from", "to"], as_index=False)["exp_u"].sum(), on=["from", "to"])
    probs["p"] = probs["exp_u_x"]/probs["exp_u_y"]
    
    probs = probs.reset_index()
    probs = probs[["from", "to", "mode", "p"]]
    
    probs = { (row['from'], row['to'], row['mode']) : row['p'] for _, row in probs.iterrows() }
    
    return probs
 
 

@timed
def sample_modes(
    legs,
    available_modes,
    needs_vehicle,
    probs
):
    
    n_legs = len(legs)
    m_id = random.choice(available_modes)
    
    # If the sampled mode needs a vehicle, set all legs to this mode
    # Else sample modes from the modes that do not need a vehicle
    
    if needs_vehicle[m_id]:
        modes = [m_id]*n_legs
        
    else:
        
        p = np.zeros((n_legs, len(available_modes)))
        for i, (from_, to_) in enumerate(legs):
            p_cum = 0.0
            for j, m in enumerate(available_modes):
                d = probs.get((from_, to_, m), 1e-9)
                p[i, j] = d
                p_cum += d
            p[i, :] /= p_cum
                
        e = -np.log(np.random.uniform(0, 1, size=p.shape))
        index = np.argmin(e/p, axis=1)
        modes = available_modes[index]

    return modes


@timed
def get_modes_seq_after_subtours_modifications(
    locations,
    legs,
    init_modes_seq,
    subtours,
    subtour_combinations,
    mode_ids_wo_return,
    vehicle_for_mode,
    multimodal,
    return_mode,
    walk_id,
    n_vehicles,
    multimodal_needs_vehicle,
    needs_vehicle,
    probs
):
    
    modified_mod_seqs = []
    
    for sc in subtour_combinations:
        
        # logging.debug("modifiying subtour combinations :")
        
        m_seq = init_modes_seq.copy()
        
        for subtour_index in sc:
            
            subtour = subtours[subtour_index]
            
            # logging.debug("modifiying modes for subtour :")
            # logging.debug(subtour)
            
            available_modes = []
            
            for m_id in mode_ids_wo_return:
                
                mode_av = is_mode_available_numba(
                    locations,
                    m_seq,
                    subtour,
                    m_id,
                    vehicle_for_mode,
                    multimodal,
                    return_mode,
                    walk_id,
                    n_vehicles,
                    multimodal_needs_vehicle
                )
                
                if mode_av:
                    available_modes.append(m_id)
            
            available_modes = np.array(available_modes)
            
            if len(available_modes) > 0:    
                
                subtour_legs = legs[subtour[0]:subtour[-1]]
                subtour_modes = sample_modes(subtour_legs, available_modes, needs_vehicle, probs)
                m_seq[subtour[0]:subtour[-1]] = subtour_modes
            
                modified_mod_seqs.append(m_seq)
            
    return modified_mod_seqs

@timed
def modes_sequence_to_probs(locations, modes_sequences, mode_costs):
    
    legs = pd.DataFrame([(locations[i], locations[i+1]) for i in range(len(locations)-1)], columns=["from", "to"])
    legs["leg_index"] = np.arange(0, legs.shape[0])

    modes_sequences = pd.merge(modes_sequences, legs, on="leg_index")
    modes_sequences = pd.merge(modes_sequences, mode_costs, on=["from", "to", "mode"], how="left")

    modes_seq_prob = modes_sequences.groupby("seq_index", as_index=False)["cost"].sum()
    modes_seq_prob["exp_u"] = np.exp(-1.0*modes_seq_prob["cost"])
    modes_seq_prob["p"] = modes_seq_prob["exp_u"]/modes_seq_prob["exp_u"].sum()

    modes_sequences = pd.merge(modes_sequences, modes_seq_prob[["seq_index", "p"]], on="seq_index")

    mode_probs = modes_sequences.groupby(["from", "to", "mode"])["p"].sum()
    mode_probs = mode_probs/mode_probs.groupby(["from", "to"]).sum()
    
    return mode_probs.reset_index()
     
@timed
def get_mode_sequences(
    locations,
    start_modes,
    multimodal_needs_vehicle,
    modes_wo_veh_ids,
    return_mode,
    mode_ids_wo_return,
    vehicle_for_mode,
    multimodal,
    walk_id,
    n_vehicles,
    needs_vehicle,
    probs
):
    
    subtours = get_possible_subtours_from_locations(locations)
    subtours_combs = get_subtours_combinations(subtours)

    legs = [(locations[i], locations[i+1]) for i in range(len(locations)-1)]
    n_legs = len(legs)
    
    start_modes_ids = start_modes[(locations[0], locations[1])]
    
    modes_seq = []
    
    if n_legs == 1:
        
        modes_seq = np.array(start_modes_ids).reshape((len(start_modes_ids), 1))
        
    else:
    
        for m_id in start_modes_ids:
            
            # Create an initial mode sequence based on the sampled mode
            # If multimodal and needing a vehicle, use this mode as first and last 
            # leg and fill the remaining legs with non vehicle modes
            # Else use this mode for all legs
            init_modes_seq = []
            
            if multimodal_needs_vehicle[m_id]:
                
                ms = []
                
                for i in range(10):
                
                    middle_legs_modes = sample_modes(
                        legs[1:(len(legs)-1)],
                        modes_wo_veh_ids,
                        needs_vehicle,
                        probs
                    )
                    
                    ms.append(
                        np.r_[m_id, middle_legs_modes, return_mode[m_id]]
                    )
                    
                ms = np.unique(np.vstack(ms), axis=0)
                ms = [row for row in ms]
                
                init_modes_seq.extend(ms)
                    
            else:
                
                init_modes_seq.append(np.repeat(m_id, n_legs))
            
            modes_seq.extend(init_modes_seq)
            
            # logging.debug("sample init mode seq : ")
            # logging.debug(init_modes_names_seq)
            
            if len(subtours) > 0:
                
                for m_seq in init_modes_seq:
            
                    mod_m_seqs = get_modes_seq_after_subtours_modifications(
                        locations,
                        legs,
                        m_seq,
                        subtours,
                        subtours_combs,
                        mode_ids_wo_return,
                        vehicle_for_mode,
                        multimodal,
                        return_mode,
                        walk_id,
                        n_vehicles,
                        multimodal_needs_vehicle,
                        needs_vehicle,
                        probs
                    )
                    
                    modes_seq.extend(mod_m_seqs)
            
        modes_seq = np.unique(np.vstack(modes_seq), axis=0)
    
    return modes_seq
