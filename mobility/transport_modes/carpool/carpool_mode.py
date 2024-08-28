from mobility.transport_modes.transport_mode import TransportMode
from mobility.transport_modes.car import CarMode
from mobility.transport_modes.carpool.carpool_parameters import CarpoolParameters
from mobility.transport_modes.carpool.carpool_travel_costs import CarpoolTravelCosts

class CarpoolMode(TransportMode):
    
    def __init__(
        self,
        car_mode: CarMode,
        parameters: CarpoolParameters = CarpoolParameters(2)
    ):
        travel_costs = CarpoolTravelCosts(car_mode.travel_costs, parameters)
        super().__init__(travel_costs, parameters)
        
        
