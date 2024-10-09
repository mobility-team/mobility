from dataclasses import dataclass, field
from typing import List, Union, Dict

from mobility.cost_of_time_parameters import CostOfTimeParameters

@dataclass
class DetailedCarpoolGeneralizedCostParameters:
    
    number_persons: int = 2
    
    car_cost_of_time = CostOfTimeParameters()
    carpooling_cost_of_time = CostOfTimeParameters()
    
    cost_of_time_od_coeffs: List[
        Dict[
            str,
            Union[
                List[str],
                float
            ]
        ]
    ] = field(
        default_factory=lambda: [{
            "local_admin_unit_id_from": [],
            "local_admin_unit_id_to": [],
            "coeff": 1.0
        }]
    )
    
    # Cost of distance parameters
    car_cost_of_distance: float = 0.1
    carpooling_cost_of_distance: float = 0.05
    
    # Constant
    car_cost_constant: float = 10.0
    carpooling_cost_constant: float = 0.0
    
    # Revenues
    # Based on travelled distance
    revenue_distance_local_admin_units_ids: List[str] = field(default_factory=lambda: [])
    revenue_distance_r0: float = 1.5
    revenue_distance_r1: float = 0.1
    revenue_distance_max: float = 3.0
    
    # Based on the number of passengers
    revenue_passengers_local_admin_units_ids: List[str] = field(default_factory=lambda: [])
    revenue_passengers_r1: float = 1.5