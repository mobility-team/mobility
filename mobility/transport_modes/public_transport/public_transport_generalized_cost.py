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
            "last_leg_mode_name": first_leg_mode_name
        }
        
        super().__init__(inputs)
        
        
    def get(
            self,
            metrics=["cost"],
            congestion: bool = True,
            detail_distances: bool = False
        ) -> pd.DataFrame:
        
        metrics = list(metrics)
        costs = self.travel_costs.get()
        
        study_area = self.travel_costs.transport_zones.study_area.get()
        transport_zones = self.travel_costs.transport_zones.get()
        
        transport_zones = pd.merge(transport_zones, study_area[["local_admin_unit_id", "country"]], on="local_admin_unit_id")
        
        costs = pd.merge(
            costs,
            transport_zones[["transport_zone_id", "local_admin_unit_id", "country"]].rename({"transport_zone_id": "from"}, axis=1).set_index("from"),
            on="from"
        )
        
        costs["distance"] = costs["start_distance"] + costs["mid_distance"] + costs["last_distance"]
        
        gen_cost = self.start_parameters.cost_of_distance*costs["start_distance"]
        gen_cost += self.start_parameters.cost_of_time.compute(costs["start_distance"], costs["country"])*costs["start_real_time"]
        
        gen_cost += self.mid_parameters.cost_constant
        gen_cost += self.mid_parameters.cost_of_distance*costs["mid_distance"]
        gen_cost += self.mid_parameters.cost_of_time.compute(costs["mid_distance"], costs["country"])*costs["mid_perceived_time"]
        
        gen_cost += self.last_parameters.cost_of_distance*costs["last_distance"]
        gen_cost += self.last_parameters.cost_of_time.compute(costs["last_distance"], costs["country"])*costs["last_real_time"]
        
        costs["cost"] = gen_cost
        
        if detail_distances is True:
            
            first_mode_col = self.inputs["first_leg_mode_name"] + "_distance"
            last_mode_col = self.inputs["last_leg_mode_name"] + "_distance"
            
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
        
        return costs
            