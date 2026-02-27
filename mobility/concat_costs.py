import pandas as pd
import numpy as np

def concat_travel_costs(modes, year):
    
    mode_names = [mode.inputs["parameters"].name for mode in modes]
    
    def get_travel_costs(mode):
        if mode.inputs["parameters"].congestion:
            return mode.inputs["travel_costs"].get(congestion=mode.inputs["parameters"].congestion)
        else:
            return mode.inputs["travel_costs"].get()
    
    costs = {m.inputs["parameters"].name: get_travel_costs(m) for m in modes}
    costs = [tc.assign(mode=m) for m, tc in costs.items()]
    costs = pd.concat(costs)
    
    # Compute GHG emissions
    ef = {
        "car_ice": 0.2176,
        "car_electric": 0.1034,
        "public_transport": 0.030,
        "bicycle": 0.0,
        "walk": 0.0
    }
    
    if year == 2024:
        ef["car_average"] = 0.02*ef["car_electric"] + 0.98*ef["car_ice"]
    else:
        ef["car_average"] = 0.9*ef["car_electric"] + 0.1*ef["car_ice"]
        
    ef["carpool"] = ef["car_average"]/2
    
    costs["ghg_emissions"] = 0.0
    
    if "car" in mode_names:
        costs["ghg_emissions"] = np.where(
            costs["mode"] == "car",
            costs["distance"]*ef["car_average"],
            costs["ghg_emissions"]
        )
    
    if "carpool" in mode_names:
        costs["ghg_emissions"] = np.where(
            costs["mode"] == "carpool",
            costs["car_distance"]*ef["car_average"] + costs["carpooling_distance"]*ef["carpool"],
            costs["ghg_emissions"]
        )
    
    if "walk/public_transport/walk" in mode_names:
        costs["ghg_emissions"] = np.where(
            costs["mode"] == "walk/public_transport/walk",
            costs["mid_distance"]*ef["public_transport"],
            costs["ghg_emissions"]
        )
    
    if "car/public_transport/walk" in mode_names:
        costs["ghg_emissions"] = np.where(
            costs["mode"] == "car/public_transport/walk",
            costs["start_distance"]*ef["car_average"] + costs["mid_distance"]*ef["public_transport"],
            costs["ghg_emissions"]
        )
    
    if "bicycle/public_transport/walk" in mode_names:
        costs["ghg_emissions"] = np.where(
            costs["mode"] == "bicycle/public_transport/walk",
            costs["mid_distance"]*ef["public_transport"],
            costs["ghg_emissions"]
        )
        
    # Sum travel times and distances for modes that have multiple legs
    if "carpool" in mode_names:
        
        costs["time"] = np.where(
            costs["mode"] == "carpool",
            costs["car_time"] + costs["carpooling_time"],
            costs["time"]
        )
        
        costs["distance"] = np.where(
            costs["mode"] == "carpool",
            costs["car_distance"] + costs["carpooling_distance"],
            costs["distance"]
        )
    
    if "car/public_transport/walk" in mode_names or "bicycle/public_transport/walk" in mode_names  or "walk/public_transport/walk" in mode_names :
        costs["time"] = np.where(
            costs["mode"].str.contains("public_transport"),
            costs["start_real_time"] + costs["mid_real_time"] + costs["last_real_time"],
            costs["time"]
        )
        
        costs["distance"] = np.where(
            costs["mode"].str.contains("public_transport"),
            costs["start_distance"] + costs["mid_distance"] + costs["last_distance"],
            costs["distance"]
        )
    
    costs = costs[["from", "to", "mode", "distance", "time", "ghg_emissions"]]
    
    return costs


def concat_generalized_cost(modes):
    
    def get_gen_costs(mode):
        if mode.inputs["parameters"].congestion:
            return mode.inputs["generalized_cost"].get(congestion=mode.inputs["parameters"].congestion)
        else:
            return mode.inputs["generalized_cost"].get()
    
    costs = {m.inputs["parameters"].name: get_gen_costs(m) for m in modes}
    costs = [gc.assign(mode=m) for m, gc in costs.items()]
    costs = pd.concat(costs)
    
    return costs
