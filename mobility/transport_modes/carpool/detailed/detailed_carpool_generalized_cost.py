import pandas as pd
import numpy as np
from typing import Annotated
from pydantic import BaseModel, ConfigDict, Field

from mobility.in_memory_asset import InMemoryAsset
from mobility.cost_of_time_parameters import CostOfTimeParameters

class DetailedCarpoolGeneralizedCost(InMemoryAsset):
    
    def __init__(self, travel_costs, parameters):
        inputs = {
            "travel_costs": travel_costs,
            "parameters": parameters
        }
        super().__init__(inputs)
        
        
    def get(self, metrics=["cost"], congestion: bool = False, detail_distances: bool = False) -> pd.DataFrame:
        
        metrics = list(metrics)
        costs = self.inputs["travel_costs"].get(congestion)
        
        study_area = self.inputs["travel_costs"].inputs["car_travel_costs"].inputs["transport_zones"].study_area.get()
        transport_zones = self.inputs["travel_costs"].inputs["car_travel_costs"].inputs["transport_zones"].get()
        
        transport_zones = pd.merge(transport_zones, study_area[["local_admin_unit_id", "country"]], on="local_admin_unit_id")
        
        costs = pd.merge(
            costs,
            transport_zones[["transport_zone_id", "local_admin_unit_id", "country"]].rename({"transport_zone_id": "from"}, axis=1).set_index("from"),
            on="from"
        )
        
        costs = pd.merge(
            costs,
            transport_zones[["transport_zone_id", "local_admin_unit_id", "country"]].rename({"transport_zone_id": "to"}, axis=1).set_index("to"),
            on="to",
            suffixes=["_from", "_to"]
        )
        
        costs["distance"] = costs["car_distance"] + costs["carpooling_distance"]
        costs["time"] = costs["car_time"] + costs["carpooling_time"]

        params = self.inputs["parameters"]
        gen_cost = params.car_cost_constant
        gen_cost += params.car_cost_of_distance * costs["car_distance"]
        gen_cost += params.car_cost_of_time.compute(costs["car_distance"], costs["country_from"]) * costs["car_time"]
        
        gen_cost += params.carpooling_cost_constant
        gen_cost += params.carpooling_cost_of_distance * costs["carpooling_distance"]
        gen_cost += params.carpooling_cost_of_time.compute(costs["carpooling_distance"], costs["country_from"]) * costs["carpooling_time"]
        
        # ct = np.where(costs["country_from"] == "fr", ct*self.parameters.cost_of_time_country_coeff_fr, ct)
        # ct = np.where(costs["country_to"] == "ch", ct*self.parameters.cost_of_time_country_coeff_ch, ct)
        
        # Compute revenues        
        revenues_distance = np.where(
            costs["local_admin_unit_id_from"].isin(params.revenue_distance_local_admin_units_ids),
            params.revenue_distance_r0 + params.revenue_distance_r1 * costs["carpooling_distance"],
            0.0
        )
        revenues_distance = np.minimum(revenues_distance, params.revenue_distance_max)
        
        revenues_passenger = np.where(
            costs["local_admin_unit_id_from"].isin(params.revenue_passengers_local_admin_units_ids),
            params.revenue_passengers_r1 * params.number_persons,
            0.0
        )
        
        # Add all cost and revenues components
        gen_cost -= revenues_distance + revenues_passenger
        
        costs["cost"] = gen_cost
        
        if detail_distances is True:
            metrics.extend(["car_distance", "carpooling_distance"])
        
        metrics = ["from", "to"] + metrics
        costs = costs[metrics]

        costs["mode"] = "carpool"

        # Add the return cost (symetrical by hypothesis)
        ret_costs = costs.copy()
        ret_costs["mode"] = "carpool_return"
        ret_costs["ret_from"] = ret_costs["to"]
        ret_costs["ret_to"] = ret_costs["from"]
        ret_costs.drop(["from", "to"], axis=1, inplace=True)
        ret_costs.rename({"ret_from": "from", "ret_to": "to"}, axis=1, inplace=True)
        costs = pd.concat([costs, ret_costs])
        
        return costs


class DetailedCarpoolGeneralizedCostParameters(BaseModel):
    model_config = ConfigDict(extra="forbid")

    number_persons: Annotated[int, Field(default=2, ge=1)]

    car_cost_of_time: Annotated[CostOfTimeParameters, Field(default_factory=CostOfTimeParameters)]
    carpooling_cost_of_time: Annotated[CostOfTimeParameters, Field(default_factory=CostOfTimeParameters)]

    cost_of_time_od_coeffs: Annotated[list[dict[str, list[str] | float]], Field(
        default_factory=lambda: [{
            "local_admin_unit_id_from": [],
            "local_admin_unit_id_to": [],
            "coeff": 1.0
        }]
    )]

    car_cost_of_distance: Annotated[float, Field(default=0.1, ge=0.0)]
    carpooling_cost_of_distance: Annotated[float, Field(default=0.05, ge=0.0)]

    car_cost_constant: Annotated[float, Field(default=0.0)]
    carpooling_cost_constant: Annotated[float, Field(default=0.0)]

    revenue_distance_local_admin_units_ids: Annotated[list[str], Field(default_factory=list)]
    revenue_distance_r0: Annotated[float, Field(default=1.5, ge=0.0)]
    revenue_distance_r1: Annotated[float, Field(default=0.1, ge=0.0)]
    revenue_distance_max: Annotated[float, Field(default=3.0, ge=0.0)]

    revenue_passengers_local_admin_units_ids: Annotated[list[str], Field(default_factory=list)]
    revenue_passengers_r1: Annotated[float, Field(default=1.5, ge=0.0)]
