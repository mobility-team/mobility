import pandas as pd

from mobility.in_memory_asset import InMemoryAsset

class PublicTransportGeneralizedCost(InMemoryAsset):
    
    def __init__(
            self,
            travel_costs,
            first_leg_mode_name,
            last_leg_mode_name,
            start_parameters,
            mid_parameters,
            last_parameters
        ):
        
        inputs = {
            "travel_costs": travel_costs,
            "start_parameters": start_parameters,
            "mid_parameters": mid_parameters,
            "last_parameters": last_parameters,
            "first_leg_mode_name": first_leg_mode_name,
            "last_leg_mode_name": last_leg_mode_name
        }
        
        super().__init__(inputs)
        
        
    def get(
            self,
            metrics=["cost"],
            congestion: bool = True,
            detail_distances: bool = False
        ) -> pd.DataFrame:

        first_leg_mode_name = self.inputs["first_leg_mode_name"]
        last_leg_mode_name = self.inputs["last_leg_mode_name"]
        
        metrics = list(metrics)
        costs = self.inputs["travel_costs"].get()
        
        study_area = self.inputs["travel_costs"].inputs["transport_zones"].study_area.get()
        transport_zones = self.inputs["travel_costs"].inputs["transport_zones"].get()
        
        transport_zones = pd.merge(transport_zones, study_area[["local_admin_unit_id", "country"]], on="local_admin_unit_id")
        
        costs = pd.merge(
            costs,
            transport_zones[["transport_zone_id", "local_admin_unit_id", "country"]].rename({"transport_zone_id": "from"}, axis=1).set_index("from"),
            on="from"
        )
        
        costs["distance"] = costs["start_distance"] + costs["mid_distance"] + costs["last_distance"]
        
        start_parameters = self.inputs["start_parameters"]
        mid_parameters = self.inputs["mid_parameters"]
        last_parameters = self.inputs["last_parameters"]
        gen_cost = start_parameters.cost_of_distance * costs["start_distance"]
        gen_cost += start_parameters.cost_of_time.compute(costs["start_distance"], costs["country"]) * costs["start_real_time"]
        
        gen_cost += mid_parameters.cost_constant
        gen_cost += mid_parameters.cost_of_distance * costs["mid_distance"]
        gen_cost += mid_parameters.cost_of_time.compute(costs["mid_distance"], costs["country"]) * costs["mid_perceived_time"]
        
        gen_cost += last_parameters.cost_of_distance * costs["last_distance"]
        gen_cost += last_parameters.cost_of_time.compute(costs["last_distance"], costs["country"]) * costs["last_real_time"]
        
        costs["cost"] = gen_cost
        
        costs["time"] = costs["start_real_time"] + costs["mid_real_time"] + costs["last_real_time"]
        
        if detail_distances is True:
            
            first_mode_col = first_leg_mode_name + "_distance"
            last_mode_col = last_leg_mode_name + "_distance"
            
            if first_mode_col == last_mode_col:
                
                costs["start_distance"] += costs["last_distance"] 
                cols = {
                    "start_distance": first_mode_col,
                    "mid_distance": "public_transport_distance",
                }
                
            else:
                
                cols = {
                    "start_distance": first_mode_col,
                    "mid_distance": "public_transport_distance",
                    "last_distance": last_mode_col
                }
                
            costs.rename(cols, axis=1, inplace=True)
            metrics.extend(list(cols.values()))
        
        metrics = ["from", "to"] + metrics
        costs = costs[metrics]

        costs["mode"] = first_leg_mode_name + "/public_transport/" + last_leg_mode_name

        # If the access/egress modes are asymetrical, we need to add the return trip
        # ie if we computed a car/PT/walk travel cost between two transport zones,
        # we have to add a walk/PT/car cost, so that both trips are possible in the model.
        # We make the hypothesis that costs are symetrical.
        if first_leg_mode_name != last_leg_mode_name:
            ret_costs = costs.copy()
            ret_costs["mode"] = last_leg_mode_name + "/public_transport/" + first_leg_mode_name
            ret_costs["ret_from"] = ret_costs["to"]
            ret_costs["ret_to"] = ret_costs["from"]
            ret_costs.drop(["from", "to"], axis=1, inplace=True)
            ret_costs.rename({"ret_from": "from", "ret_to": "to"}, axis=1, inplace=True)
            costs = pd.concat([costs, ret_costs])
        
        return costs
            
