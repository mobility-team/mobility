from pydantic import Field
from typing import Annotated

from mobility.motives.motive_parameters import MotiveParameters


class HomeMotiveParameters(MotiveParameters):
    """Parameters specific to the home motive."""

    value_of_time: Annotated[
        float,
        Field(default=10.0, ge=0.0),
    ]

    value_of_time_stay_home: Annotated[
        float,
        Field(default=0.0, ge=0.0),
    ]

    saturation_fun_ref_level: Annotated[
        float,
        Field(default=100.0, ge=0.0),
    ]

    saturation_fun_beta: Annotated[
        float,
        Field(default=4.0, ge=0.0),
    ]

    survey_ids: Annotated[
        list[str],
        Field(default_factory=lambda: ["1.1"]),
    ]

