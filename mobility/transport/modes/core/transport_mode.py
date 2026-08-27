from typing import Annotated, List

from mobility.runtime.assets.in_memory_asset import InMemoryAsset
from mobility.runtime.parameter_values import SensitivityCase
from mobility.transport.costs.generalized_cost import GeneralizedCost
from mobility.transport.costs.travel_costs_asset import TravelCostsBase
from pydantic import BaseModel, ConfigDict, Field

class TransportMode(InMemoryAsset):
    """
    A base class for all transport modes (car, bicycle, walk...).
    
    Attributes:
        name (str): the name of the mode.
        travel_costs (PathTravelCosts | PublicTransportTravelCosts | DetailedCarpoolTravelCosts): 
            a travel costs object instance that computes and stores travel costs
            (time, distance) between transport zones.
        generalized_cost :
        congestion (bool):
            boolean flag to enable congestion, if the travel costs class can 
            handle it (only PathTravelCosts).
    """
    
    def __init__(
        self,
        name: str,
        travel_costs,
        generalized_cost,
        ghg_intensity: float,
        congestion: bool = False,
        vehicle: str = None,
        multimodal: bool = False,
        return_mode: str = None,
        survey_ids: List[str] = None,
        parameters: "TransportModeParameters | None" = None,
        parameters_cls: type["TransportModeParameters"] | None = None,
    ):
        """Initialize a transport mode and store its parameter metadata.

        Args:
            name: Mode name.
            travel_costs: Travel costs object associated with this mode.
            generalized_cost: Generalized cost object associated with this mode.
            ghg_intensity: Greenhouse gas intensity in kgCO2e/pass.km.
            congestion: Whether congestion is enabled for this mode.
            vehicle: Vehicle type for this mode.
            multimodal: Whether this mode is multimodal.
            return_mode: Return mode name for asymmetric multimodal chains.
            survey_ids: Survey mode identifiers mapped to this mode.
            parameters: Optional pre-built pydantic parameter model.
        """
        if parameters_cls is None:
            parameters_cls = TransportModeParameters

        parameters = self.prepare_parameters(
            parameters=parameters,
            parameters_cls=parameters_cls,
            explicit_args={
                "name": name,
                "ghg_intensity": ghg_intensity,
                "congestion": congestion,
                "vehicle": vehicle,
                "multimodal": multimodal,
                "return_mode": return_mode,
                "survey_ids": survey_ids,
            },
            owner_name="TransportMode",
        )

        inputs = {
            "version": "1",
            "parameters": parameters,
            "travel_costs": travel_costs,
            "generalized_cost": generalized_cost,
        }

        super().__init__(inputs)

    def build_congestion_flows(self, od_flows_by_mode):
        """Build congestion-relevant flows for this mode.

        Args:
            od_flows_by_mode (pl.DataFrame): Aggregated person flows by
                ``["from", "to", "mode"]``.

        Returns:
            pl.DataFrame | None: Congestion-relevant flows for this mode, or
            ``None`` when this mode does not directly refresh congestion.
        """
        return None

    def for_iteration(
        self,
        iteration: int,
        scenario: str | None = None,
        sensitivity_case: SensitivityCase | None = None,
    ) -> "TransportMode":
        """Return this mode with its own parameter values resolved.

        This default implementation covers simple in-memory modes such as walk,
        bicycle, and car generalized costs. More complex modes can override it
        when they need to rebuild child modes or other derived assets.
        """
        generalized_cost = self.inputs.get("generalized_cost")
        if not isinstance(generalized_cost, GeneralizedCost):
            return self

        travel_costs = self.inputs.get("travel_costs")
        if isinstance(travel_costs, TravelCostsBase):
            resolved_travel_costs = travel_costs.for_iteration(
                iteration,
                scenario=scenario,
                sensitivity_case=sensitivity_case,
            )
        else:
            resolved_travel_costs = travel_costs

        resolved_generalized_cost = generalized_cost.for_iteration(
            iteration,
            travel_costs=resolved_travel_costs,
            scenario=scenario,
            sensitivity_case=sensitivity_case,
        )
        if (
            resolved_generalized_cost is generalized_cost
            and resolved_travel_costs is travel_costs
        ):
            return self

        return TransportModeVariant(
            source_mode=self,
            travel_costs=resolved_travel_costs,
            generalized_cost=resolved_generalized_cost,
        )


class TransportModeVariant(TransportMode):
    """Concrete mode assets selected for one scenario and iteration."""

    def __init__(
        self,
        *,
        source_mode: TransportMode,
        travel_costs,
        generalized_cost: GeneralizedCost,
    ) -> None:
        """Create a mode variant without copying state from the source mode."""
        self.source_mode = source_mode
        parameters = source_mode.inputs["parameters"]
        super().__init__(
            name=parameters.name,
            travel_costs=travel_costs,
            generalized_cost=generalized_cost,
            ghg_intensity=parameters.ghg_intensity,
            congestion=parameters.congestion,
            vehicle=parameters.vehicle,
            multimodal=parameters.multimodal,
            return_mode=parameters.return_mode,
            survey_ids=parameters.survey_ids,
            parameters=parameters,
            parameters_cls=parameters.__class__,
        )

    def build_congestion_flows(self, od_flows_by_mode):
        """Use the source mode's road-flow conversion for this variant."""
        return self.source_mode.build_congestion_flows(od_flows_by_mode)


class TransportModeParameters(BaseModel):
    """Common parameters for transport mode definitions."""

    model_config = ConfigDict(extra="forbid")

    name: Annotated[
        str,
        Field(
            title="Mode name",
            description="Unique name used to identify the transport mode.",
        ),
    ]

    ghg_intensity: Annotated[
        float,
        Field(
            ge=0.0,
            title="GHG intensity",
            description="Greenhouse gas intensity in kgCO2e per passenger.km.",
            json_schema_extra={"unit": "kgCO2e/pass.km"},
        ),
    ]

    congestion: Annotated[
        bool,
        Field(
            default=False,
            title="Congestion enabled",
            description=(
                "Whether congestion feedback should be enabled for this mode "
                "when supported by travel cost computations."
            ),
        ),
    ]

    vehicle: Annotated[
        str | None,
        Field(
            default=None,
            title="Vehicle type",
            description="Vehicle family used by this mode, when relevant.",
        ),
    ]

    multimodal: Annotated[
        bool,
        Field(
            default=False,
            title="Multimodal",
            description="Whether this mode combines multiple sub-modes.",
        ),
    ]

    return_mode: Annotated[
        str | None,
        Field(
            default=None,
            title="Return mode",
            description="Optional return mode name for asymmetric multimodal chains.",
        ),
    ]

    survey_ids: Annotated[
        list[str] | None,
        Field(
            default=None,
            title="Survey IDs",
            description="Survey mode identifiers mapped to this transport mode.",
        ),
    ]
