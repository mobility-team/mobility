from typing import List, Literal

from mobility.transport_modes.transport_mode import TransportMode
from mobility.transport_modes.transport_mode import TransportModeParameters
from mobility.transport_modes.car import CarMode
from mobility.transport_modes.carpool.detailed import DetailedCarpoolRoutingParameters, DetailedCarpoolGeneralizedCostParameters, DetailedCarpoolTravelCosts, DetailedCarpoolGeneralizedCost
from mobility.transport_modes.modal_transfer import IntermodalTransfer
from pydantic import Field

class CarpoolMode(TransportMode):
    
    def __init__(
        self,
        car_mode: CarMode,
        routing_parameters: DetailedCarpoolRoutingParameters = None,
        generalized_cost_parameters: DetailedCarpoolGeneralizedCostParameters = None,
        intermodal_transfer: IntermodalTransfer = None,
        survey_ids: List[str] | None = None,
        ghg_intensity: float | None = None,
        parameters: "CarpoolModeParameters | None" = None,
    ):
            
        routing_parameters = routing_parameters or DetailedCarpoolRoutingParameters()
        travel_costs = DetailedCarpoolTravelCosts(
            car_mode.inputs["travel_costs"],
            routing_parameters,
            intermodal_transfer,
        )
        
        congestion = car_mode.inputs["parameters"].congestion
        
        generalized_cost_parameters = generalized_cost_parameters or DetailedCarpoolGeneralizedCostParameters()
        generalized_cost = DetailedCarpoolGeneralizedCost(travel_costs, generalized_cost_parameters)
            
        super().__init__(
            "carpool",
            travel_costs,
            generalized_cost,
            congestion=congestion,
            ghg_intensity=ghg_intensity,
            vehicle=car_mode.inputs["parameters"].vehicle,
            multimodal=True,
            return_mode="carpool_return",
            survey_ids=survey_ids,
            parameters=parameters,
            parameters_cls=CarpoolModeParameters,
        )


class CarpoolModeParameters(TransportModeParameters):
    """Parameters for carpool mode."""

    name: Literal["carpool"] = "carpool"
    ghg_intensity: float = 0.109
    multimodal: bool = True
    return_mode: Literal["carpool_return"] = "carpool_return"
    survey_ids: list[str] = Field(default_factory=list)
