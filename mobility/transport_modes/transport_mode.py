from typing import Annotated, List

from mobility.in_memory_asset import InMemoryAsset
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

    def clone(self):
        """Create a shallow clone of the mode with cloned travel costs."""
        params = self.inputs["parameters"]
        return TransportMode(
            name=params.name,
            travel_costs=self.inputs["travel_costs"].clone(),
            generalized_cost=self.inputs["generalized_cost"],
            ghg_intensity=params.ghg_intensity,
            congestion=params.congestion,
            vehicle=params.vehicle,
            multimodal=params.multimodal,
            return_mode=params.return_mode,
            survey_ids=params.survey_ids,
            parameters=params.model_copy(),
            parameters_cls=params.__class__,
        )


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
