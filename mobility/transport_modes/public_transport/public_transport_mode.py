from mobility.transport_zones import TransportZones
from mobility.transport_modes.transport_mode import TransportMode
from mobility.transport_modes.public_transport.public_transport_parameters import PublicTransportParameters
from mobility.transport_modes.public_transport.public_transport_travel_costs import PublicTransportTravelCosts

from dataclasses import asdict

class PublicTransportMode(TransportMode):
    
    def __init__(
        self,
        transport_zones: TransportZones,
        parameters: PublicTransportParameters = PublicTransportParameters()
    ):
        
        travel_costs = PublicTransportTravelCosts(
            transport_zones,
            parameters
        )
        
        super().__init__(travel_costs, parameters)