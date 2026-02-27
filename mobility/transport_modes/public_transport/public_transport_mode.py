import logging

from typing import List

from mobility.transport_zones import TransportZones
from mobility.transport_modes.transport_mode import TransportMode
from mobility.transport_modes.public_transport.public_transport_routing_parameters import PublicTransportRoutingParameters
from mobility.transport_modes.public_transport.public_transport_travel_costs import PublicTransportTravelCosts
from mobility.transport_modes.public_transport.public_transport_generalized_cost import PublicTransportGeneralizedCost
from mobility.transport_modes.modal_transfer import (
    IntermodalTransfer,
    default_intermodal_transfer_for_mode,
)
from mobility.generalized_cost_parameters import GeneralizedCostParameters
from mobility.cost_of_time_parameters import CostOfTimeParameters
from mobility.transport_modes.transport_mode_parameters import (
    PublicTransportModeParameters,
)

class PublicTransportMode(TransportMode):
    """
    A class for public transport mode, including different modes for first and last legs.
    Modes such as this one should be defined in the code before being called by models
    
    Args:
        transport_zones (gpd.GeoDataFrame): GeoDataFrame containing transport zone geometries.
        first_leg_mode: mode for the first leg mode, such as walk, car or bicycle
        last_leg_mode: mode for the last leg
        first_modal_transfer: transfer parameters between the first mode and public transport
        last_modal_transfer: transfer parameters between public transport and the last mode
        routing_parameters: PublicTransportRoutingParameters
        generalized_cost_parameters: GeneralizedCostParameters

    """
    
    def __init__(
        self,
        transport_zones: TransportZones,
        first_leg_mode: TransportMode,
        last_leg_mode: TransportMode,
        first_intermodal_transfer: IntermodalTransfer = None,
        last_intermodal_transfer: IntermodalTransfer = None,
        routing_parameters: PublicTransportRoutingParameters | None = None,
        generalized_cost_parameters: GeneralizedCostParameters = None,
        survey_ids: List[str] | None = None,
        ghg_intensity: float | None = None,
        parameters: PublicTransportModeParameters | None = None,
    ):

        if first_leg_mode is None or last_leg_mode is None:
            raise ValueError(
                "PublicTransportMode requires both `first_leg_mode` and `last_leg_mode`."
            )

        if first_intermodal_transfer is None:
            first_intermodal_transfer = default_intermodal_transfer_for_mode(
                mode_name=first_leg_mode.inputs["parameters"].name,
                vehicle=first_leg_mode.inputs["parameters"].vehicle,
            )

        if last_intermodal_transfer is None:
            last_intermodal_transfer = default_intermodal_transfer_for_mode(
                mode_name=last_leg_mode.inputs["parameters"].name,
                vehicle=last_leg_mode.inputs["parameters"].vehicle,
            )

        if routing_parameters is None:
            routing_parameters = PublicTransportRoutingParameters()
        
        travel_costs = PublicTransportTravelCosts(
            transport_zones,
            routing_parameters,
            first_leg_mode,
            last_leg_mode,
            first_intermodal_transfer,
            last_intermodal_transfer
        )
        
        congestion = (
            first_leg_mode.inputs["parameters"].congestion
            or last_leg_mode.inputs["parameters"].congestion
        )
        
        if generalized_cost_parameters is None:
            generalized_cost_parameters = GeneralizedCostParameters(
                cost_constant=0.0,
                cost_of_distance=0.1,
                cost_of_time=CostOfTimeParameters()
            )
        
        generalized_cost = PublicTransportGeneralizedCost(
            travel_costs,
            first_leg_mode_name=first_leg_mode.inputs["parameters"].name,
            last_leg_mode_name=last_leg_mode.inputs["parameters"].name,
            start_parameters=first_leg_mode.inputs["generalized_cost"].inputs["parameters"],
            mid_parameters=generalized_cost_parameters,
            last_parameters=last_leg_mode.inputs["generalized_cost"].inputs["parameters"],
        )
        
        name = (
            first_leg_mode.inputs["parameters"].name
            + "/public_transport/"
            + last_leg_mode.inputs["parameters"].name
        )
        vehicle = first_leg_mode.inputs["parameters"].vehicle
        
        if last_leg_mode.inputs["parameters"].name != first_leg_mode.inputs["parameters"].name:
            return_mode_name = (
                last_leg_mode.inputs["parameters"].name
                + "/public_transport/"
                + first_leg_mode.inputs["parameters"].name
            )
        else:
            return_mode_name = None
        
        super().__init__(
            name,
            travel_costs,
            generalized_cost,
            congestion=congestion,
            ghg_intensity=ghg_intensity,
            vehicle=vehicle,
            multimodal=True,
            return_mode=return_mode_name,
            survey_ids=survey_ids,
            parameters=parameters,
            parameters_cls=PublicTransportModeParameters,
        )    
        
    def audit_gtfs(self):
        logging.info("Auditing GTFS for this mode")
        travel_costs = self.inputs["travel_costs"].audit_gtfs()
        return travel_costs
