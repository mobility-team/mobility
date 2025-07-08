from mobility.transport_zones import TransportZones
from mobility.transport_costs.path_travel_costs import PathTravelCosts
from mobility.transport_modes.transport_mode import TransportMode
from mobility.path_routing_parameters import PathRoutingParameters
from mobility.generalized_cost_parameters import GeneralizedCostParameters
from mobility.cost_of_time_parameters import CostOfTimeParameters
from mobility.transport_costs.path_generalized_cost import PathGeneralizedCost
from mobility.transport_modes.osm_capacity_parameters import OSMCapacityParameters
from mobility.transport_graphs.speed_modifier import SpeedModifier

from typing import List

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
        routing_parameters: PathRoutingParameters = None,
        osm_capacity_parameters: OSMCapacityParameters = None,
        generalized_cost_parameters: GeneralizedCostParameters = None,
        congestion: bool = False,
        congestion_flows_scaling_factor: float = 0.1,
        speed_modifiers: List[SpeedModifier] = []
    ):
        
        mode_name = "car"
        
        if routing_parameters is None:
            routing_parameters = PathRoutingParameters(
                filter_max_time=1.0,
                filter_max_speed=60.0
            )
            
        if osm_capacity_parameters is None:
            osm_capacity_parameters = OSMCapacityParameters(mode_name)
        
        if generalized_cost_parameters is None:
            generalized_cost_parameters = GeneralizedCostParameters(
                cost_constant=0.0,
                cost_of_distance=0.1,
                cost_of_time=CostOfTimeParameters()
            )
            
        
        travel_costs = PathTravelCosts(
            mode_name, 
            transport_zones, 
            routing_parameters, 
            osm_capacity_parameters,
            congestion,
            congestion_flows_scaling_factor,
            speed_modifiers
        )
        
        generalized_cost = PathGeneralizedCost(
            travel_costs,
            generalized_cost_parameters,
            mode_name=mode_name
        )
        
        super().__init__(mode_name, travel_costs, generalized_cost, congestion)
        