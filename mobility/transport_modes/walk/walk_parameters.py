from dataclasses import dataclass
from mobility.parameters import ModeParameters

@dataclass
class WalkParameters(ModeParameters):

    # Generalized cost parameters
    # Cost of time parameters
    cost_of_time_c0_short: float = 0.0
    cost_of_time_c0: float = 0.0
    cost_of_time_c1: float = 0.0
    cost_of_time_country_coeff_fr: float = 1.0
    cost_of_time_country_coeff_ch: float = 1.0
    
    # Cost of distance parameters
    cost_of_distance: float = 0.1
    
    # Constant
    cost_constant: float = 10.0
    
    def __post_init__(self):
        self.name = "walk"
    

    
    