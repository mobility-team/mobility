from pydantic import BaseModel, ConfigDict, Field
from typing import Annotated


class MotiveParameters(BaseModel):
    """Common parameters for motives."""

    model_config = ConfigDict(extra="forbid")

    value_of_time: Annotated[
        float,
        Field(
            default=10.0,
            ge=0.0,
            title="Value of time",
            description="Utility weight for time spent traveling or on activities.",
        ),
    ]

    saturation_fun_ref_level: Annotated[
        float,
        Field(
            default=1.5,
            ge=0.0,
            title="Saturation reference level",
            description="Reference level used by the sink saturation utility function.",
        ),
    ]

    saturation_fun_beta: Annotated[
        float,
        Field(
            default=4.0,
            ge=0.0,
            title="Saturation beta",
            description="Shape parameter of the sink saturation utility function.",
        ),
    ]

    value_of_time_v2: Annotated[
        float | None,
        Field(
            default=None,
            ge=0.0,
            title="Alternative value of time",
            description="Optional alternative value of time for second utility formulation.",
        ),
    ]

    survey_ids: Annotated[
        list[str] | None,
        Field(
            default=None,
            title="Survey motive IDs",
            description="List of survey-specific IDs mapped to this motive.",
        ),
    ]

    radiation_lambda: Annotated[
        float | None,
        Field(
            default=None,
            ge=0.0,
            le=1.0,
            title="Radiation model lambda",
            description="Radiation-model parameter controlling destination choice dispersion.",
        ),
    ]

    country_utilities: Annotated[
        dict[str, float] | None,
        Field(
            default=None,
            title="Country utility offsets",
            description="Optional country-level utility offsets used for this motive.",
        ),
    ]

    sink_saturation_coeff: Annotated[
        float,
        Field(
            default=1.0,
            ge=0.0,
            title="Sink saturation coefficient",
            description="Coefficient scaling sink saturation in activity utility.",
        ),
    ]

