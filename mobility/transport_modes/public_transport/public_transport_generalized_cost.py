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
        
        
    def get(self) -> pd.DataFrame:
        
        costs = self.travel_costs.get()
        
        gen_cost = self.start_parameters.cost_constant
        gen_cost += self.start_parameters.cost_of_distance*costs["start_distance"]
        gen_cost += self.start_parameters.cost_of_time.compute(costs["start_distance"])*costs["start_time"]
        
        gen_cost += self.mid_parameters.cost_constant
        gen_cost += self.mid_parameters.cost_of_distance*costs["mid_distance"]
        gen_cost += self.mid_parameters.cost_of_time.compute(costs["mid_distance"])*costs["mid_time"]
        
        gen_cost += self.last_parameters.cost_constant
        gen_cost += self.last_parameters.cost_of_distance*costs["last_distance"]
        gen_cost += self.last_parameters.cost_of_time.compute(costs["last_distance"])*costs["last_time"]
        
        costs["cost"] = gen_cost
        
        costs = costs[["from", "to", "cost"]]
        
        return costs
            