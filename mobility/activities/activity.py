from __future__ import annotations

import pandas as pd
import polars as pl

from typing import Annotated, List, Dict

from mobility.runtime.assets.in_memory_asset import InMemoryAsset
from pydantic import BaseModel, ConfigDict, Field
from mobility.runtime.parameter_profiles import (
    ScalarParameterProfile,
    SimulationStep,
    resolve_model_for_step,
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
            country_utilities: Dict = None,
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
                "country_utilities": country_utilities,
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


    def get_parameters_at_step(self, step: SimulationStep) -> "ActivityParameters":
        """Returns the activity parameters in effect at a simulation step.

        Args:
            step: Simulation step used to evaluate step-varying parameter
                profiles.

        Returns:
            ActivityParameters: Parameter model with all step-varying fields
                resolved to scalar values for ``step``.
        """
        return resolve_model_for_step(self.inputs["parameters"], step)


    def get_utilities(self, transport_zones, parameters: "ActivityParameters" | None = None):

        parameters = parameters or self.inputs["parameters"]

        if self.utilities is not None:

            utilities = self.utilities

        elif parameters.country_utilities is not None:
            
            transport_zones = transport_zones.get().drop("geometry", axis=1)
            transport_zones["country"] = transport_zones["local_admin_unit_id"].str[0:2]
            transport_zones["utility"] = transport_zones["country"].map(parameters.country_utilities)

            utilities = pl.from_pandas( 
                transport_zones[["transport_zone_id", "utility"]]
                .rename({"transport_zone_id": "to"}, axis=1)
            )

        else:

            utilities = None

        return utilities


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

    country_utilities: Annotated[
        dict[str, float] | None,
        Field(
            default=None,
            title="Country utility offsets",
            description="Optional country-level utility offsets used for this activity.",
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


def resolve_activity_parameters(
    activities: list[Activity],
    step: SimulationStep,
) -> dict[str, ActivityParameters]:
    """Resolve all activity parameter models for one simulation step."""

    return {
        activity.name: activity.get_parameters_at_step(step)
        for activity in activities
    }
