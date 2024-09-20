from dataclasses import dataclass, field
from mobility.parameters import ModeParameters
from typing import List, Union, Dict, Tuple

@dataclass
class DetailedCarpoolParameters(ModeParameters):
    """
    Attributes:
        number_persons (int): number of persons in the vehicule.
        absolute_delay_per_passenger (int): absolute delay per supplementary passenger, in minutes. Default: 5
        relative_delay_per_passenger (float) : relative delay per supplementary passenger in proportion of the total travel time. Default: 0.05
        absolute_extra_distance_per_passenger (float): absolute extra distance per supplementary passenger, in km. Default: 1
        relative_extra_distance_per_passenger (flaot) : relative extra distance per supplementary passenger in proportion of the total distance. Default: 0.05
    """
    
    number_persons: int = 2
    
    # Ridesharing parking locations (CRS : WGS 84 - 4326)
    parking_locations: List[Tuple[float, float]] = field(default_factory=list)
    
    # Delays compared to the single occupant car mode
    absolute_delay_per_passenger: int = 5
    relative_delay_per_passenger: float = 0.05
    absolute_extra_distance_per_passenger: float = 1
    relative_extra_distance_per_passenger: float = 0.05
    
    # Generalized cost parameters
    # Cost of time parameters
    cost_of_time_c0_short: float = 0.0
    cost_of_time_c0: float = 0.0
    cost_of_time_c1: float = 0.0
    cost_of_time_country_coeff_fr: float = 1.0
    cost_of_time_country_coeff_ch: float = 1.0
    
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
    cost_of_distance: float = 0.1
    
    # Constant
    cost_constant: float = 10.0
    
    # Revenues
    # Based on travelled distance
    revenue_distance_local_admin_units_ids: List[str] = field(default_factory=lambda: [])
    revenue_distance_r0: float = 1.5
    revenue_distance_r1: float = 0.1
    revenue_distance_max: float = 3.0
    
    # Based on the number of passengers
    revenue_passengers_local_admin_units_ids: List[str] = field(default_factory=lambda: [])
    revenue_passengers_r1: float = 1.5
    
    def __post_init__(self):
        self.name = "detailed_carpool" + str(self.number_persons)
    
    
    
    
    