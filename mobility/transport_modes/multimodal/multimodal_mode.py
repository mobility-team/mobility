from typing import List, Optional

from mobility.transport_zones import TransportZones
from mobility.transport_modes.transport_mode import TransportMode
from mobility.transport_modes.car import CarMode
from mobility.transport_modes.walk import WalkMode
from mobility.transport_modes.bicycle import BicycleMode
from mobility.transport_modes.carpool import CarpoolMode, CarpoolParameters
from mobility.transport_modes.public_transport import PublicTransportMode

from mobility.transport_modes.multimodal.multimodal_travel_costs import MultiModalTravelCosts
from mobility.parameters import ModeParameters

class MultiModalMode(TransportMode):
    
    def __init__(
        self,
        transport_zones: TransportZones,
        modes: Optional[List[TransportMode]] = None
    ):
        
        if modes is None:
            car_mode = CarMode(transport_zones)
            modes = [
                WalkMode(transport_zones),
                BicycleMode(transport_zones),
                PublicTransportMode(transport_zones),
                car_mode,
                CarpoolMode(car_mode, CarpoolParameters(number_persons=2)),
                CarpoolMode(car_mode, CarpoolParameters(number_persons=3)),
                CarpoolMode(car_mode, CarpoolParameters(number_persons=4))
            ]
        
        travel_costs = MultiModalTravelCosts(transport_zones, modes)
        super().__init__(travel_costs, ModeParameters("multimodal"))
        
    