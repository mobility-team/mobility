from dataclasses import dataclass
from mobility.parameters import ModeParameters

@dataclass
class CarParameters(ModeParameters):
    """A dataclass for car parameters.
    
    Attributes
    ----------
        cost_of_time_c0_short (float): cost of time for short distances. Default: 0.0
        cost_of_time_c0 (float): cost of time for medium distances. Default: 0.0
        cost_of_time_c1 (float): cost of time for longer distances. Default: 0.0
        cost_of_time_country_coeff_fr (float): coefficient for the cost of time in France. Default: 1.0
        cost_of_time_country_coeff_ch (float): coefficient for the cost of time in Switzerland. Default: 1.0
        cost_of_distance (float): cost per km for cars. Default: 1.0
        cost_constant (float): constant of cost for every car journey. Default: 10.0
    
    """
    
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
        self.name = "car"
    

    
    
    