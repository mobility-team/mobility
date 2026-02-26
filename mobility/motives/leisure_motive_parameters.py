from pydantic import Field
from typing import Annotated

from mobility.motives.motive_parameters import MotiveParameters


class LeisureMotiveParameters(MotiveParameters):
    """Parameters specific to the leisure motive."""

    value_of_time: Annotated[float, Field(default=10.0, ge=0.0)]
    saturation_fun_ref_level: Annotated[float, Field(default=1.5, ge=0.0)]
    saturation_fun_beta: Annotated[float, Field(default=4.0, ge=0.0)]
    survey_ids: Annotated[
        list[str],
        Field(default_factory=lambda: ["7.71", "7.72", "7.73", "7.74", "7.75", "7.76", "7.77", "7.78"]),
    ]
    radiation_lambda: Annotated[float, Field(default=0.99986, ge=0.0, le=1.0)]

