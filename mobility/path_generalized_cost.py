import pandas as pd
from mobility.in_memory_asset import InMemoryAsset

class PathGeneralizedCost(InMemoryAsset):
    
    def __init__(self, travel_costs, parameters):
        inputs = {
            "travel_costs": travel_costs,
            "parameters": parameters
        }
        super().__init__(inputs)
        
        
    def get(self) -> pd.DataFrame:
        
        costs = self.travel_costs.get()
        
        gen_cost = self.parameters.cost_constant
        gen_cost += self.parameters.cost_of_distance*costs["distance"]
        gen_cost += self.parameters.cost_of_time.compute(costs["distance"])*costs["time"]
        
        costs["cost"] = gen_cost
        
        costs = costs[["from", "to", "cost"]]
        
        return costs
            