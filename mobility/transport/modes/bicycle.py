from mobility.spatial.transport_zones import TransportZones
from mobility.transport.costs.path.path_travel_costs import PathTravelCosts
from mobility.transport.modes.core.transport_mode import TransportMode, TransportModeParameters
from mobility.transport.costs.parameters.path_routing_parameters import PathRoutingParameters
from mobility.transport.costs.parameters.generalized_cost_parameters import GeneralizedCostParameters
from mobility.transport.costs.parameters.cost_of_time_parameters import CostOfTimeParameters
from mobility.transport.costs.path.path_generalized_cost import PathGeneralizedCost
from mobility.transport.modes.core.osm_capacity_parameters import OSMCapacityParameters
from mobility.transport.graphs.modified.modifiers.speed_modifier import SpeedModifier
from typing import List, Literal
from pydantic import Field

class Bicycle(TransportMode):
    
    def __init__(
        self,
        transport_zones: TransportZones,
        routing_parameters: PathRoutingParameters = None,
        osm_capacity_parameters: OSMCapacityParameters = None,
        generalized_cost_parameters: GeneralizedCostParameters = None,
        speed_modifiers: List[SpeedModifier] = [],
        survey_ids: List[str] | None = None,
        ghg_intensity: float | None = None,
        parameters: "BicycleParameters | None" = None,
    ):
        
        mode_name = "bicycle"
        
        if routing_parameters is None:
            routing_parameters = PathRoutingParameters(
                max_beeline_distance=20.0
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
            mode_name=mode_name,
            transport_zones=transport_zones,
            routing_parameters=routing_parameters,
            osm_capacity_parameters=osm_capacity_parameters,
            speed_modifiers=speed_modifiers,
        )
        generalized_cost = PathGeneralizedCost(travel_costs, generalized_cost_parameters, mode_name)
        
        super().__init__(
            mode_name,
            travel_costs,
            generalized_cost,
            ghg_intensity=ghg_intensity,
            vehicle="bicycle",
            survey_ids=survey_ids,
            parameters=parameters,
            parameters_cls=BicycleParameters,
        )


class BicycleParameters(TransportModeParameters):
    """Parameters for bicycle mode."""

    name: Literal["bicycle"] = "bicycle"
    ghg_intensity: float = 0.00017
    congestion: bool = False
    vehicle: Literal["bicycle"] = "bicycle"
    multimodal: bool = False
    return_mode: None = None
    survey_ids: list[str] = Field(default_factory=lambda: ["2.20"])


BicycleMode = Bicycle
