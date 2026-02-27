from typing import List
from typing import Literal

from mobility.transport_zones import TransportZones
from mobility.transport_costs.path_travel_costs import PathTravelCosts
from mobility.transport_modes.transport_mode import TransportMode, TransportModeParameters
from mobility.path_routing_parameters import PathRoutingParameters
from mobility.generalized_cost_parameters import GeneralizedCostParameters
from mobility.cost_of_time_parameters import CostOfTimeParameters
from mobility.transport_costs.path_generalized_cost import PathGeneralizedCost
from mobility.transport_modes.osm_capacity_parameters import OSMCapacityParameters
from pydantic import Field

class WalkMode(TransportMode):
    
    def __init__(
        self,
        transport_zones: TransportZones,
        routing_parameters: PathRoutingParameters = None,
        osm_capacity_parameters: OSMCapacityParameters = None,
        generalized_cost_parameters: GeneralizedCostParameters = None,
        survey_ids: List[str] | None = None,
        ghg_intensity: float | None = None,
        parameters: "WalkModeParameters | None" = None,
    ):
        
        mode_name = "walk"
        
        if routing_parameters is None:
            routing_parameters = PathRoutingParameters(
                filter_max_time=1.0,
                filter_max_speed=5.0
            )
            
        if osm_capacity_parameters is None:
            osm_capacity_parameters = OSMCapacityParameters(mode_name)
        
        if generalized_cost_parameters is None:
            generalized_cost_parameters = GeneralizedCostParameters(
                cost_constant=0.0,
                cost_of_distance=0.0,
                cost_of_time=CostOfTimeParameters()
            )
        
        travel_costs = PathTravelCosts(
            mode_name,
            transport_zones,
            routing_parameters,
            osm_capacity_parameters
        )
        
        generalized_cost = PathGeneralizedCost(
            travel_costs,
            generalized_cost_parameters,
            mode_name
        )
        
        super().__init__(
            mode_name,
            travel_costs,
            generalized_cost, 
            ghg_intensity=ghg_intensity,
            survey_ids=survey_ids,
            parameters=parameters,
            parameters_cls=WalkModeParameters,
        )


class WalkModeParameters(TransportModeParameters):
    """Parameters for walk mode."""

    name: Literal["walk"] = "walk"
    ghg_intensity: float = 0.0
    congestion: bool = False
    vehicle: None = None
    multimodal: bool = False
    return_mode: None = None
    survey_ids: list[str] = Field(default_factory=lambda: ["1.10", "1.11", "1.13"])

