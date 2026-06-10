from typing import Annotated, List

from mobility.runtime.assets.in_memory_asset import InMemoryAsset
from mobility.runtime.parameter_values import SensitivityCase, resolve_parameter_values
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
        if not isinstance(generalized_cost, InMemoryAsset):
            return self

        # Keep routing assets untouched here. They often own derived table
        # assets, so modes that vary routing should rebuild themselves.
        resolved_gc_inputs = resolve_parameter_values(
            generalized_cost.inputs,
            scenario=scenario,
            iteration=iteration,
            sensitivity_case=sensitivity_case,
        )
        if resolved_gc_inputs == generalized_cost.inputs:
            return self

        resolved_generalized_cost = self._copy_in_memory_asset(
            generalized_cost,
            resolved_gc_inputs,
        )
        resolved_inputs = dict(self.inputs)
        resolved_inputs["generalized_cost"] = resolved_generalized_cost
        return self._copy_in_memory_asset(self, resolved_inputs)

    @staticmethod
    def _copy_in_memory_asset(asset: InMemoryAsset, inputs: dict):
        """Copy an in-memory asset with new inputs and a matching input hash."""
        clone = asset.__class__.__new__(asset.__class__)
        clone.__dict__ = dict(asset.__dict__)
        clone.inputs = inputs
        clone.inputs_hash = clone.compute_inputs_hash()
        for name, value in inputs.items():
            setattr(clone, name, value)
        return clone

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
