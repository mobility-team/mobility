import itertools
import random
import pandas as pd
import numpy as np
from itertools import combinations



def get_possible_subtours_from_locations(locations):
    
    last_seen = {}
    subtours = []
    for end_idx, place in enumerate(locations):
        if place in last_seen:
            start_idx = last_seen[place]
            subtours.append(list(range(start_idx, end_idx+1)))
        last_seen[place] = end_idx
        
    return subtours
   

def get_subtours_combinations(subtours):
    
    n_subtours = len(subtours)
    subtour_index = list(range(n_subtours))
    
    n_perm = 2**n_subtours-1
    
    if n_perm < 20:
        
        subtour_combinations = list(itertools.permutations(subtour_index, 3))
            
    else:
        
        n_samples = int(20.0*(n_perm/20.0)**0.5)
        subtour_combinations = [np.random.choice(subtour_index, size=3, replace=False) for k in range(n_samples)]
        
    return subtour_combinations


def get_costs(locations, modes):
    
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
    
    all_costs = pd.concat(all_costs)
    
    return all_costs

def get_mode_probabilities(costs):
    
    probs = costs.copy()
    
    probs["exp_u"] = np.exp(-probs["cost"])
    
    probs = pd.merge(probs, probs.groupby(["from", "to"], as_index=False)["exp_u"].sum(), on=["from", "to"])
    probs["p"] = probs["exp_u_x"]/probs["exp_u_y"]
    
    probs = probs.reset_index()
    probs = probs[["from", "to", "mode", "p"]]
    
    probs = { (row['from'], row['to'], row['mode']) : row['p'] for _, row in probs.iterrows() }
    
    return probs
     

def is_mode_available(locations, modes_seq, subtour, subtour_mode):

    new_modes_seq = modes_seq.copy()
    
    # Change the modes of the subtour to the mode that we want to test
    for j in subtour[:-1]:
        new_modes_seq[j] = subtour_mode
    
    # Initialize vehicle locations
    vehicle_locations = {m["name"]: locations[0] for m in modes if m["needs_vehicle"] is True}
    
    # Follow the trip sequences to check for consistency (vehicle availability for each trip)
    for i in range(len(new_modes_seq)):
        
        mode = new_modes_seq[i]["name"]
        
        if mode in vehicle_locations.keys():
            
            # Check if the vehicle is available for the trip
            if vehicle_locations[mode] != locations[i]:
                return False
            
            # Move the vehicle to the destination of the trip
            vehicle_locations[mode] = locations[i+1]
            
            
    return True
            


def sample_modes(legs, modes, mode_probs):
    
    n_legs = len(legs)
    modes_wo_vehicle = [mode for mode in modes if mode["needs_vehicle"] is False]
    
    mode = random.choice(modes)
    
    if mode["needs_vehicle"] is True:
        modes = [mode]*n_legs
        
    else:
        
        p = np.zeros((n_legs, len(modes_wo_vehicle)))
        for i, (from_, to_) in enumerate(legs):
            for j, mode in enumerate(modes_wo_vehicle):
                p[i, j] = mode_probs.get((from_, to_, mode["name"]), 0.0)
                
        e = -np.log(np.random.uniform(0, 1, size=p.shape))
        index = np.argmax(e/p, axis=1)
        modes = [modes_wo_vehicle[i] for i in index]

    return modes



def get_modes_seq_after_subtours_modifications(legs, init_modes_seq, subtours, subtour_combinations, modes, mode_probs):
    
    all_new_modes_seq = []
    
    for sc in subtour_combinations:
        
        modes_seq = init_modes_seq.copy()
        
        for subtour_index in sc:
            
            subtour = subtours[subtour_index]
            
            available_modes = []
            
            for mode in modes:
                if is_mode_available(locations, modes_seq, subtour, mode):
                    available_modes.append(mode)
            
            if len(available_modes) > 0:    
                
                subtour_legs = legs[subtour[0]:subtour[-1]]
                subtour_modes = sample_modes(subtour_legs, available_modes, mode_probs)
                
                for j in range(len(subtour)-1):
                    modes_seq[subtour[j]] = subtour_modes[j]
            
                all_new_modes_seq.append(list(modes_seq))
            
    return all_new_modes_seq
        

def get_modes_sequences(locations, modes, mode_probs):
    
    subtours = get_possible_subtours_from_locations(locations)
    subtours_combs = get_subtours_combinations(subtours)

    legs = [(locations[i], locations[i+1]) for i in range(len(locations)-1)]
    n_legs = len(legs)
    
    modes_seq = []
    
    for i in range(100):
        
        mode = random.choice(modes)
        
        init_modes_seq = [mode]*n_legs
        mod_modes_seq = get_modes_seq_after_subtours_modifications(
            legs,
            init_modes_seq,
            subtours,
            subtours_combs,
            modes,
            mode_probs
        )
        
        modes_seq.append(init_modes_seq)
        modes_seq.extend(mod_modes_seq)
        
    modes_seq = [[m["name"] for m in s] for s in modes_seq]
    modes_seq = pd.DataFrame(modes_seq).drop_duplicates()
    modes_seq["seq_index"] = np.arange(0, modes_seq.shape[0])
    
    modes_seq = modes_seq.melt("seq_index")
    modes_seq.columns = ["seq_index", "leg_index", "mode"]
        
    return modes_seq


locations = ["home_start", "a", "b", "a", "b", "a", "home_end"]

modes = [
    {"name": "car", "needs_vehicle": True},
    {"name": "bicycle", "needs_vehicle": True},
    {"name": "walk", "needs_vehicle": False},
    {"name": "walk/pt/walk", "needs_vehicle": False}
]

mode_costs = get_costs(locations, modes)
mode_probs = get_mode_probabilities(mode_costs)

modes_sequences = get_modes_sequences(locations, modes, mode_probs)

legs = pd.DataFrame([(locations[i], locations[i+1]) for i in range(len(locations)-1)], columns=["from", "to"])
legs["leg_index"] = np.arange(0, legs.shape[0])

modes_sequences = pd.merge(modes_sequences, legs, on="leg_index")
modes_sequences = pd.merge(modes_sequences, mode_costs, on=["from", "to", "mode"])

modes_seq_prob = modes_sequences.groupby("seq_index", as_index=False)["cost"].sum()
modes_seq_prob["exp_u"] = np.exp(-5*modes_seq_prob["cost"])
modes_seq_prob["p"] = modes_seq_prob["exp_u"]/modes_seq_prob["exp_u"].sum()

modes_sequences = pd.merge(modes_sequences, modes_seq_prob[["seq_index", "p"]], on="seq_index")

print(modes_sequences.groupby(["from", "to", "mode"])["p"].count())







