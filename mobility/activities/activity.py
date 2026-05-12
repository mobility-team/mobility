from __future__ import annotations

import pandas as pd
import polars as pl

from typing import Annotated, List, Dict

from mobility.runtime.assets.in_memory_asset import InMemoryAsset
from pydantic import BaseModel, ConfigDict, Field
from mobility.runtime.parameter_profiles import (
    ScalarParameterProfile,
    resolve_model_for_iteration,
)
from mobility.runtime.validation_types import NonNegativeFloat, UnitIntervalFloat


class Activity(InMemoryAsset):

    def __init__(
            self,
            name: str,
            value_of_time: float = None,
            saturation_fun_ref_level: float = None,
            saturation_fun_beta: float = None,
            value_of_time_v2: float = None,
            survey_ids: List[str] = None,
            radiation_lambda: float = None,
            has_opportunities: bool = True,
            is_anchor: bool = False,
            opportunities: pd.DataFrame = None,
            utilities: pd.DataFrame = None,
            country_value_coefficients: Dict = None,
            sink_saturation_coeff: float = None,
            extra_inputs: dict | None = None,
            parameters: "ActivityParameters" | None = None,
        ):

        parameters = self.prepare_parameters(
            parameters=parameters,
            parameters_cls=ActivityParameters,
            explicit_args={
                "value_of_time": value_of_time,
                "saturation_fun_ref_level": saturation_fun_ref_level,
                "saturation_fun_beta": saturation_fun_beta,
                "value_of_time_v2": value_of_time_v2,
                "survey_ids": survey_ids,
                "radiation_lambda": radiation_lambda,
                "country_value_coefficients": country_value_coefficients,
                "sink_saturation_coeff": sink_saturation_coeff,
            },
            required_fields=["value_of_time", "saturation_fun_ref_level", "saturation_fun_beta"],
            owner_name=f"Activity({name})",
        )

        self.name = name
        self.has_opportunities = has_opportunities
        self.is_anchor = is_anchor
        self.opportunities = opportunities
        self.utilities = utilities

        inputs = {
            "parameters": parameters
        }
        if extra_inputs is not None:
            inputs.update(extra_inputs)

        super().__init__(inputs)


    def get_parameters_for_iteration(self, iteration: int) -> "ActivityParameters":
        """Returns the activity parameters in effect at one simulation iteration.

        Args:
            iteration: Simulation iteration used to evaluate iteration-varying parameter
                profiles.

        Returns:
            ActivityParameters: Parameter model with all iteration-varying fields
                resolved to scalar values for ``iteration``.
        """
        return resolve_model_for_iteration(self.inputs["parameters"], iteration)

    def enforce_opportunities_schema(self, opportunities):
        
        opportunities = (
            
            opportunities
            .with_columns(
                to=pl.col("to").cast(pl.Int32()),
                n_opp=pl.col("n_opp").cast(pl.Float64())
            )
            
        )
        
        return opportunities
    



class ActivityParameters(BaseModel):
    """Common parameters for activities."""

    model_config = ConfigDict(extra="forbid")

    value_of_time: Annotated[
        NonNegativeFloat | ScalarParameterProfile,
        Field(
            default=10.0,
            title="Value of time",
            description="Utility weight for time spent traveling or on activities.",
        ),
    ]

    saturation_fun_ref_level: Annotated[
        NonNegativeFloat,
        Field(
            default=1.5,
            title="Saturation reference level",
            description="Reference level used by the sink saturation utility function.",
        ),
    ]

    saturation_fun_beta: Annotated[
        NonNegativeFloat,
        Field(
            default=4.0,
            title="Saturation beta",
            description="Shape parameter of the sink saturation utility function.",
        ),
    ]

    value_of_time_v2: Annotated[
        NonNegativeFloat | ScalarParameterProfile | None,
        Field(
            default=None,
            title="Alternative value of time",
            description="Optional alternative value of time for second utility formulation.",
        ),
    ]

    survey_ids: Annotated[
        list[str] | None,
        Field(
            default=None,
            title="Survey activity IDs",
            description="List of survey-specific IDs mapped to this activity.",
        ),
    ]

    radiation_lambda: Annotated[
        UnitIntervalFloat | None,
        Field(
            default=None,
            title="Radiation model lambda",
            description="Radiation-model parameter controlling destination choice dispersion.",
        ),
    ]

    country_value_coefficients: Annotated[
        dict[str, float] | None,
        Field(
            default=None,
            title="Country value coefficients",
            description=(
                "Optional destination-country coefficients applied to the "
                "positive activity-value term for this activity. Coefficients "
                "default to `1.0` when a country is not listed."
            ),
        ),
    ]

    sink_saturation_coeff: Annotated[
        NonNegativeFloat,
        Field(
            default=1.0,
            title="Sink saturation coefficient",
            description="Coefficient scaling sink saturation in activity utility.",
        ),
    ]

    arrival_time_rigidity: Annotated[
        UnitIntervalFloat | ScalarParameterProfile | None,
        Field(
            default=None,
            title="Arrival time rigidity",
            description=(
                "Share of a travel-time change absorbed on the departure side "
                "for trips ending at this activity. `0` keeps departure fixed, "
                "`1` keeps arrival fixed, intermediate values split the shift."
            ),
        ),
    ]


def resolve_activity_parameters(
    activities: list[Activity],
    iteration: int,
) -> dict[str, ActivityParameters]:
    """Resolve all activity parameter models for one simulation iteration."""

    return {
        activity.name: activity.get_parameters_for_iteration(iteration)
        for activity in activities
    }


def resolve_activity_arrival_time_rigidity(
    activities: list[Activity],
    iteration: int,
) -> dict[str, float]:
    """Resolve per-activity arrival-time rigidity with anchor-based defaults."""

    rigidity_by_activity: dict[str, float] = {}
    for activity in activities:
        parameters = activity.get_parameters_for_iteration(iteration)
        rigidity = parameters.arrival_time_rigidity
        if rigidity is None:
            rigidity = 1.0 if activity.is_anchor else 0.0
        rigidity_by_activity[activity.name] = float(rigidity)

    return rigidity_by_activity
