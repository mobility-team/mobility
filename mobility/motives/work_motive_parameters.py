from pydantic import Field
from typing import Annotated

from mobility.motives.motive_parameters import MotiveParameters


class WorkMotiveParameters(MotiveParameters):
    """Parameters specific to the work motive."""

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

    survey_ids: Annotated[
        list[str],
        Field(default_factory=lambda: ["9.91"]),
    ]

    radiation_lambda: Annotated[
        float,
        Field(default=0.99986, ge=0.0, le=1.0),
    ]

    country_utilities: Annotated[
        dict[str, float],
        Field(default_factory=lambda: {"fr": 0.0, "ch": 5.0}),
    ]

