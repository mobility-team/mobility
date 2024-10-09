from dataclasses import dataclass, field
from typing import List

import numpy as np
from numpy.typing import NDArray

@dataclass
class CostOfTimeParameters():
    
    intercept: float = 18.6
    breaks: List[float] = field(default_factory=lambda: [0.0, 20.0, 80.0, 10000000.0])
    slopes: List[float] = field(default_factory=lambda: [0.0, 0.215, 0.017])
    max_value: float = 37.0
    
    country_coeff_fr: float = 1.0
    country_coeff_ch: float = 1.0
    
    
    def __post_init__(self):
        if len(self.slopes) != len(self.breaks) - 1:
            raise ValueError("The number of breaks and slopes do not match, there should be N-1 slopes for N breaks.")
    
    
    def compute(self, distance: NDArray[np.float64]) -> NDArray[np.float64]:
        
        cost = self.intercept
        
        if len(self.slopes) > 0:
            
            base = self.intercept
            
            for i, slope in enumerate(self.slopes):
                
                left_break = self.breaks[i]
                right_break = self.breaks[i+1]
                    
                cost = np.where(
                    distance > left_break,
                    base + slope*(distance - left_break),
                    cost
                )
                
                base = base + slope*(right_break - left_break)
        
        cost = np.where(cost > self.max_value, self.max_value, cost)
        
        return cost
                   