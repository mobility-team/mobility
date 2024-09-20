from typing import List, Optional

from mobility.transport_zones import TransportZones
from mobility.transport_modes.transport_mode import TransportMode
from mobility.transport_modes.car import CarMode

from mobility.transport_modes.multimodal.multimodal_travel_costs import MultiModalTravelCosts
from mobility.parameters import ModeParameters

class MultiModalMode(TransportMode):
    
    def __init__(
        self,
        transport_zones: TransportZones,
        modes: Optional[List[TransportMode]] = None
    ):
        
        if modes is None:
            modes = [CarMode(transport_zones)]
            
        travel_costs = MultiModalTravelCosts(modes)
        super().__init__(travel_costs, ModeParameters("multimodal"))
        
    