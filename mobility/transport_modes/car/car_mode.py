from mobility.transport_zones import TransportZones
from mobility.path_travel_costs import PathTravelCosts
from mobility.transport_modes.transport_mode import TransportMode

class CarMode(TransportMode):
    
    def __init__(
        self,
        transport_zones: TransportZones
    ):
        travel_costs = PathTravelCosts(transport_zones, "car")
        super().__init__("car", travel_costs)
    