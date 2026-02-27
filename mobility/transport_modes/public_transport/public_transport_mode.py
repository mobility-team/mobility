import logging

from typing import List

from mobility.transport_zones import TransportZones
from mobility.transport_modes.transport_mode import TransportMode, TransportModeParameters
from mobility.transport_modes.mode_registry import ModeRegistry
from mobility.transport_modes.public_transport.public_transport_graph import PublicTransportRoutingParameters
from mobility.transport_modes.public_transport.public_transport_travel_costs import PublicTransportTravelCosts
from mobility.transport_modes.public_transport.public_transport_generalized_cost import PublicTransportGeneralizedCost
from mobility.transport_modes.modal_transfer import (
    IntermodalTransfer,
    default_intermodal_transfer_for_mode,
)
from mobility.generalized_cost_parameters import GeneralizedCostParameters
from mobility.cost_of_time_parameters import CostOfTimeParameters
from pydantic import Field


DEFAULT_PUBLIC_TRANSPORT_SURVEY_IDS = [
    "4.42",
    "4.43",
    "5.50",
    "5.51",
    "5.52",
    "5.53",
    "5.54",
    "5.55",
    "5.56",
    "5.57",
    "5.58",
    "5.59",
    "6.60",
    "6.61",
    "6.62",
    "6.63",
    "6.69",
]


class PublicTransportMode(TransportMode):
    """Public transport mode with configurable access and egress leg modes."""

    def __init__(
        self,
        transport_zones: TransportZones,
        first_leg_mode: TransportMode | None = None,
        last_leg_mode: TransportMode | None = None,
        mode_registry: ModeRegistry | None = None,
        default_first_leg_mode_name: str = "walk",
        default_last_leg_mode_name: str = "walk",
        first_intermodal_transfer: IntermodalTransfer | None = None,
        last_intermodal_transfer: IntermodalTransfer | None = None,
        routing_parameters: PublicTransportRoutingParameters | None = None,
        generalized_cost_parameters: GeneralizedCostParameters | None = None,
        survey_ids: List[str] | None = None,
        ghg_intensity: float | None = None,
        parameters: "PublicTransportModeParameters | None" = None,
    ):
        first_leg_mode = self._resolve_leg_mode(
            leg_mode=first_leg_mode,
            mode_registry=mode_registry,
            default_mode_name=default_first_leg_mode_name,
            leg_label="first_leg_mode",
        )
        last_leg_mode = self._resolve_leg_mode(
            leg_mode=last_leg_mode,
            mode_registry=mode_registry,
            default_mode_name=default_last_leg_mode_name,
            leg_label="last_leg_mode",
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
            last_intermodal_transfer,
        )

        congestion = (
            first_leg_mode.inputs["parameters"].congestion
            or last_leg_mode.inputs["parameters"].congestion
        )

        if generalized_cost_parameters is None:
            generalized_cost_parameters = GeneralizedCostParameters(
                cost_constant=0.0,
                cost_of_distance=0.1,
                cost_of_time=CostOfTimeParameters(),
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

    @staticmethod
    def _resolve_leg_mode(
        leg_mode: TransportMode | None,
        mode_registry: ModeRegistry | None,
        default_mode_name: str,
        leg_label: str,
    ) -> TransportMode:
        if leg_mode is not None:
            return leg_mode

        if mode_registry is None:
            raise ValueError(
                "PublicTransportMode requires explicit `first_leg_mode` and "
                "`last_leg_mode`, or a `mode_registry` to resolve defaults "
                "(example: ModeRegistry([walk_mode, ...]))."
            )

        resolved_mode = mode_registry.get(default_mode_name)
        if resolved_mode is None:
            raise ValueError(
                f"Could not resolve {leg_label} with mode id '{default_mode_name}'."
            )
        return resolved_mode


class PublicTransportModeParameters(TransportModeParameters):
    """Parameters for public transport mode."""

    ghg_intensity: float = 0.05
    multimodal: bool = True
    survey_ids: list[str] = Field(default_factory=lambda: list(DEFAULT_PUBLIC_TRANSPORT_SURVEY_IDS))
