from mobility.transport_zones import TransportZones
from mobility.path_travel_costs import PathTravelCosts
from mobility.transport_modes.transport_mode import TransportMode
from mobility.path_routing_parameters import PathRoutingParameters
from mobility.generalized_cost_parameters import GeneralizedCostParameters
from mobility.cost_of_time_parameters import CostOfTimeParameters
from mobility.path_generalized_cost import PathGeneralizedCost

class CarMode(TransportMode):
    
    def __init__(
        self,
        transport_zones: TransportZones,
        routing_parameters: PathRoutingParameters = None,
        generalized_cost_parameters: GeneralizedCostParameters = None,
        congestion: bool = False
    ):
        
        if routing_parameters is None:
            routing_parameters = PathRoutingParameters(
                filter_max_time=1.0,
                filter_max_speed=60.0
            )
        
        if generalized_cost_parameters is None:
            generalized_cost_parameters = GeneralizedCostParameters(
                cost_constant=0.0,
                cost_of_distance=0.1,
                cost_of_time=CostOfTimeParameters()
            )
            
        
        travel_costs = PathTravelCosts("car", transport_zones, routing_parameters)
        generalized_cost = PathGeneralizedCost(travel_costs, generalized_cost_parameters)
        
        super().__init__("car", travel_costs, generalized_cost, congestion)
        