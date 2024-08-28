from dataclasses import dataclass
from mobility.parameters import Parameters

@dataclass
class CarpoolParameters(Parameters):
    """
    Attributes:
        number_persons (int): number of persons in the vehicule.
        absolute_delay_per_passenger (int): absolute delay per supplementary passenger, in minutes. Default: 5
        relative_delay_per_passenger (float) : relative delay per supplementary passenger in proportion of the total travel time. Default: 0.05
        absolute_extra_distance_per_passenger (float): absolute extra distance per supplementary passenger, in km. Default: 1
        relative_extra_distance_per_passenger (flaot) : relative extra distance per supplementary passenger in proportion of the total distance. Default: 0.05
    """
    
    number_persons: int
    absolute_delay_per_passenger: int = 5
    relative_delay_per_passenger: float = 0.05
    absolute_extra_distance_per_passenger: float = 1
    relative_extra_distance_per_passenger: float = 0.05