from mobility.transport_modes.transport_mode import TransportMode
from mobility.transport_modes.car import CarMode
from mobility.transport_modes.carpool.carpool_parameters import CarpoolParameters
from mobility.transport_modes.carpool.carpool_travel_costs import CarpoolTravelCosts

class CarpoolMode(TransportMode):
    
    def __init__(
        self,
        car_mode: CarMode,
        parameters: CarpoolParameters
    ):
        name = "carpool" + str(parameters.number_persons)
        travel_costs = CarpoolTravelCosts(car_mode.travel_costs, name, parameters)
        super().__init__(name, travel_costs, parameters)
        
        
