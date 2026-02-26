from pydantic import Field
from typing import Annotated

from mobility.motives.motive_parameters import MotiveParameters


class ShoppingMotiveParameters(MotiveParameters):
    """Parameters specific to the shopping motive."""

    value_of_time: Annotated[float, Field(default=10.0, ge=0.0)]
    saturation_fun_ref_level: Annotated[float, Field(default=1.5, ge=0.0)]
    saturation_fun_beta: Annotated[float, Field(default=4.0, ge=0.0)]
    survey_ids: Annotated[list[str], Field(default_factory=lambda: ["2.20", "2.21"])]
    radiation_lambda: Annotated[float, Field(default=0.99986, ge=0.0, le=1.0)]

