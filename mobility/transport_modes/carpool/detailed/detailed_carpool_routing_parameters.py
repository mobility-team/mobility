from dataclasses import dataclass, field
from typing import List, Tuple

@dataclass
class DetailedCarpoolRoutingParameters:
    """
    Attributes:
        number_persons (int): number of persons in the vehicule.
        absolute_delay_per_passenger (int): absolute delay per supplementary passenger, in minutes. Default: 5
        relative_delay_per_passenger (float) : relative delay per supplementary passenger in proportion of the total travel time. Default: 0.05
        absolute_extra_distance_per_passenger (float): absolute extra distance per supplementary passenger, in km. Default: 1
        relative_extra_distance_per_passenger (flaot) : relative extra distance per supplementary passenger in proportion of the total distance. Default: 0.05
    """
    
    # Ridesharing parking locations (CRS : WGS 84 - 4326)
    parking_locations: List[Tuple[float, float]] = field(default_factory=list)
    
    # Delays compared to the single occupant car mode
    absolute_delay_per_passenger: int = 5
    relative_delay_per_passenger: float = 0.05
    absolute_extra_distance_per_passenger: float = 1
    relative_extra_distance_per_passenger: float = 0.05
    
    
    
    
    