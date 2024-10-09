import pandas as pd

def concat_travel_costs(modes):
    
    costs = {m.name: m.travel_costs.get() for m in modes}
    costs = [tc.assign(mode=m) for m, tc in costs.items()]
    costs = pd.concat(costs)
    
    return costs


def concat_generalized_cost(modes):
    
    costs = {m.name: m.generalized_cost.get() for m in modes}
    costs = [gc.assign(mode=m) for m, gc in costs.items()]
    costs = pd.concat(costs)
    
    return costs