from mobility.transport_zones import TransportZones
from mobility.transport_modes.transport_mode import TransportMode
from mobility.transport_modes.public_transport.public_transport_parameters import PublicTransportParameters
from mobility.transport_modes.public_transport.public_transport_travel_costs import PublicTransportTravelCosts
from mobility.transport_modes.walk import WalkMode

class PublicTransportMode(TransportMode):
    
    def __init__(
        self,
        transport_zones: TransportZones,
        walk_mode: WalkMode = None,
        parameters: PublicTransportParameters = PublicTransportParameters()
    ):
        
        if walk_mode is None:
            walk_mode = WalkMode(transport_zones)
        
        travel_costs = PublicTransportTravelCosts(
            transport_zones,
            walk_mode.travel_costs,
            parameters
        )
        
        super().__init__(travel_costs, parameters)