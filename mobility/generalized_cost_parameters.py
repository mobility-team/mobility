from dataclasses import dataclass, field
from mobility.cost_of_time_parameters import CostOfTimeParameters

@dataclass
class GeneralizedCostParameters:
    
    cost_constant: float = 0.0
    cost_of_time: CostOfTimeParameters = field(default_factory=lambda: CostOfTimeParameters())
    cost_of_distance: float = 0.0
