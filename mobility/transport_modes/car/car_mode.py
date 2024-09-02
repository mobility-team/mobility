from mobility.transport_zones import TransportZones
from mobility.path_travel_costs import PathTravelCosts
from mobility.transport_modes.transport_mode import TransportMode
from mobility.transport_modes.car.car_parameters import CarParameters

class CarMode(TransportMode):
    """
    A class for car transportation. Creates travel costs for the mode, using the provided parameters or default ones.
    
    Attributes:
        transport_zones (TransportZones): transport zones to consider
        parameters(CarParameters): composants of cost. Default: standard parameters.
    """
    
    def __init__(
        self,
        transport_zones: TransportZones,
        parameters: CarParameters = CarParameters()
    ):
        travel_costs = PathTravelCosts(transport_zones, parameters)
        super().__init__(travel_costs, parameters)
    