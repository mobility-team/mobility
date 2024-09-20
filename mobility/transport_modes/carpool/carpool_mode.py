from mobility.transport_modes.transport_mode import TransportMode
from mobility.transport_modes.car import CarMode

from mobility.transport_modes.carpool.simple import SimpleCarpoolParameters, SimpleCarpoolTravelCosts
from mobility.transport_modes.carpool.detailed import DetailedCarpoolParameters, DetailedCarpoolTravelCosts

class CarpoolMode(TransportMode):
    
    def __init__(
        self,
        car_mode: CarMode,
        detailed: bool = False,
        simple_parameters: SimpleCarpoolParameters = None,
        detailed_parameters: DetailedCarpoolParameters = None
    ):
        if detailed:
            
            parameters = detailed_parameters or DetailedCarpoolParameters()
            travel_costs = DetailedCarpoolTravelCosts(car_mode.travel_costs, detailed_parameters)
            
        else:
            
            parameters = simple_parameters or SimpleCarpoolParameters()
            travel_costs = SimpleCarpoolTravelCosts(car_mode.travel_costs, simple_parameters)
            
        super().__init__(travel_costs, parameters)
        
        
