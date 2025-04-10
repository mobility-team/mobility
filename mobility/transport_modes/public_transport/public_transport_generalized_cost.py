import pandas as pd

from mobility.in_memory_asset import InMemoryAsset

class PublicTransportGeneralizedCost(InMemoryAsset):
    
    def __init__(self, travel_costs, start_parameters, mid_parameters, last_parameters):
        
        inputs = {
            "travel_costs": travel_costs,
            "start_parameters": start_parameters,
            "mid_parameters": mid_parameters,
            "last_parameters": last_parameters
        }
        
        super().__init__(inputs)
        
        
    def get(self, metrics=["cost"], congestion: bool = True) -> pd.DataFrame:
        
        costs = self.travel_costs.get()
        
        study_area = self.travel_costs.transport_zones.study_area.get()
        transport_zones = self.travel_costs.transport_zones.get()
        
        transport_zones = pd.merge(transport_zones, study_area[["local_admin_unit_id", "country"]], on="local_admin_unit_id")
        
        costs = pd.merge(
            costs,
            transport_zones[["transport_zone_id", "local_admin_unit_id", "country"]].rename({"transport_zone_id": "from"}, axis=1).set_index("from"),
            on="from"
        )
        
        # gen_cost = self.start_parameters.cost_constant
        gen_cost = self.start_parameters.cost_of_distance*costs["start_distance"]
        gen_cost += self.start_parameters.cost_of_time.compute(costs["start_distance"], costs["country"])*costs["start_real_time"]
        
        gen_cost += self.mid_parameters.cost_constant
        gen_cost += self.mid_parameters.cost_of_distance*costs["mid_distance"]
        gen_cost += self.mid_parameters.cost_of_time.compute(costs["mid_distance"], costs["country"])*costs["mid_perceived_time"]
        
        # gen_cost += self.last_parameters.cost_constant
        gen_cost += self.last_parameters.cost_of_distance*costs["last_distance"]
        gen_cost += self.last_parameters.cost_of_time.compute(costs["last_distance"], costs["country"])*costs["last_real_time"]
        
        costs["cost"] = gen_cost
        
        metrics = ["from", "to"] + metrics
        costs = costs[metrics]
        
        return costs
            