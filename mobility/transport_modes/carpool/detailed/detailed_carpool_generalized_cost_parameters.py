from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field

from mobility.cost_of_time_parameters import CostOfTimeParameters


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
    
    # Cost of distance parameters
    car_cost_of_distance: Annotated[float, Field(default=0.1, ge=0.0)]
    carpooling_cost_of_distance: Annotated[float, Field(default=0.05, ge=0.0)]
    
    # Constant
    car_cost_constant: Annotated[float, Field(default=0.0)]
    carpooling_cost_constant: Annotated[float, Field(default=0.0)]
    
    # Revenues
    # Based on travelled distance
    revenue_distance_local_admin_units_ids: Annotated[list[str], Field(default_factory=list)]
    revenue_distance_r0: Annotated[float, Field(default=1.5, ge=0.0)]
    revenue_distance_r1: Annotated[float, Field(default=0.1, ge=0.0)]
    revenue_distance_max: Annotated[float, Field(default=3.0, ge=0.0)]
    
    # Based on the number of passengers
    revenue_passengers_local_admin_units_ids: Annotated[list[str], Field(default_factory=list)]
    revenue_passengers_r1: Annotated[float, Field(default=1.5, ge=0.0)]
