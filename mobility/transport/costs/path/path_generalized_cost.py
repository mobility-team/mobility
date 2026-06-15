from __future__ import annotations

import pandas as pd

from mobility.runtime.assets.in_memory_asset import InMemoryAsset
from mobility.transport.costs.od_flows_asset import VehicleODFlowsAsset

class PathGeneralizedCost(InMemoryAsset):
    
    def __init__(self, travel_costs, parameters, mode_name):
        inputs = {
            "travel_costs": travel_costs,
            "parameters": parameters,
            "mode_name": mode_name
        }
        super().__init__(inputs)
        
        
    def get(
        self,
        metrics=["cost"],
        congestion: bool = False,
        detail_distances: bool = False,
        road_flow_asset: VehicleODFlowsAsset | None = None,
    ) -> pd.DataFrame:
        
        metrics = list(metrics)
        costs = self.inputs["travel_costs"].get(
            congestion=congestion,
            road_flow_asset=road_flow_asset,
        )
        
        transport_zones_df = self.inputs["travel_costs"].inputs["transport_zones"].get().drop(columns="geometry")
        if "country" not in transport_zones_df.columns:
            raise ValueError("Transport zones must contain a `country` column.")
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
        
        params = self.inputs["parameters"]
        gen_cost = params.cost_constant
        gen_cost += params.cost_of_distance * costs["distance"]
        gen_cost += params.cost_of_time.compute(costs["distance"], costs["country_from"]) * costs["time"]
        
        costs["cost"] = gen_cost
        
        if detail_distances is True:
            col = self.inputs["mode_name"] + "_distance"
            costs[col] = costs["distance"]
            metrics.append(col)
        
        metrics = ["from", "to"] + metrics
        costs = costs[metrics]

        costs["mode"] = self.inputs["mode_name"]
        
        return costs
