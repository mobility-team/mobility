import pandas as pd
from mobility.in_memory_asset import InMemoryAsset

class PathGeneralizedCost(InMemoryAsset):
    
    def __init__(self, travel_costs, parameters, mode_name):
        inputs = {
            "travel_costs": travel_costs,
            "parameters": parameters,
            "mode_name": mode_name
        }
        super().__init__(inputs)
        
        
    def get(self, metrics=["cost"], congestion: bool = False, detail_distances: bool = False) -> pd.DataFrame:
        
        metrics = list(metrics)
        costs = self.travel_costs.get(congestion)
        
        # study_area = self.travel_costs.transport_zones.study_area.get()
        transport_zones_df = self.travel_costs.transport_zones.get().drop(columns="geometry")
        
        
        # transport_zones = pd.merge(transport_zones, study_area[["local_admin_unit_id", "country"]], on="local_admin_unit_id")
        transport_zones_df["country"] = transport_zones_df["local_admin_unit_id"].astype(str).str[:2]
        transport_zones_df["country"] = transport_zones_df["country"].astype(str)
        
        costs = pd.merge(
            costs,
            transport_zones_df[["transport_zone_id", "country"]].rename({"transport_zone_id": "from"}, axis=1).set_index("from"),
            on="from"
        )
        
        costs = pd.merge(
            costs,
            transport_zones_df[["transport_zone_id", "country"]].rename({"transport_zone_id": "to"}, axis=1).set_index("to"),
            on="to",
            suffixes=["_from", "_to"]
        )
        
        gen_cost = self.parameters.cost_constant
        gen_cost += self.parameters.cost_of_distance*costs["distance"]
        gen_cost += self.parameters.cost_of_time.compute(costs["distance"], costs["country_from"])*costs["time"]
        
        costs["cost"] = gen_cost
        
        if detail_distances is True:
            col = self.inputs["mode_name"] + "_distance"
            costs[col] = costs["distance"]
            metrics.append(col)
        
        metrics = ["from", "to"] + metrics
        costs = costs[metrics]

        costs["mode"] = self.inputs["mode_name"]
        
        return costs
