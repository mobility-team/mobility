from mobility.transport_zones import TransportZones
from mobility.path_travel_costs import PathTravelCosts
from mobility.transport_modes.transport_mode import TransportMode
from mobility.transport_modes.car.car_parameters import CarParameters

class CarMode(TransportMode):
    
    def __init__(
        self,
        transport_zones: TransportZones,
        parameters: CarParameters = CarParameters()
    ):
        travel_costs = PathTravelCosts(transport_zones, parameters)
        super().__init__(travel_costs, parameters)
    