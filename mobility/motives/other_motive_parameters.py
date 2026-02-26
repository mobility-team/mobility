from pydantic import Field
from typing import Annotated

from mobility.motives.motive_parameters import MotiveParameters


class OtherMotiveParameters(MotiveParameters):
    """Parameters specific to the other motive."""

    value_of_time: Annotated[
        float,
        Field(default=10.0, ge=0.0),
    ]

    saturation_fun_ref_level: Annotated[
        float,
        Field(default=1.5, ge=0.0),
    ]

    saturation_fun_beta: Annotated[
        float,
        Field(default=4.0, ge=0.0),
    ]

    radiation_lambda: Annotated[
        float,
        Field(default=0.99986, ge=0.0, le=1.0),
    ]

