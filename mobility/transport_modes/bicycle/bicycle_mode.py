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

class BicycleMode(TransportMode):
    
    def __init__(
        self,
        transport_zones: TransportZones,
        routing_parameters: PathRoutingParameters = None,
        osm_capacity_parameters: OSMCapacityParameters = None,
        generalized_cost_parameters: GeneralizedCostParameters = None,
        speed_modifiers: List[SpeedModifier] = [],
        survey_ids: List[str] = ["2.20"]
    ):
        
        mode_name = "bicycle"
        
        if routing_parameters is None:
            routing_parameters = PathRoutingParameters(
                filter_max_time=1.0,
                filter_max_speed=20.0
            )
            
        if osm_capacity_parameters is None:
            osm_capacity_parameters = OSMCapacityParameters(mode_name)
        
        if generalized_cost_parameters is None:
            generalized_cost_parameters = GeneralizedCostParameters(
                cost_constant=0.0,
                cost_of_distance=0.0,
                cost_of_time=CostOfTimeParameters()
            )
        
        travel_costs = PathTravelCosts(mode_name, transport_zones, routing_parameters, osm_capacity_parameters, speed_modifiers=speed_modifiers)
        generalized_cost = PathGeneralizedCost(travel_costs, generalized_cost_parameters, mode_name)
        
        super().__init__(
            mode_name,
            travel_costs,
            generalized_cost,
            vehicle="car",
            survey_ids=survey_ids
        )
        