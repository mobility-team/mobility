from __future__ import annotations

from typing import Annotated, List

from pydantic import Field
from mobility.runtime.parameter_profiles import ScalarParameterProfile

from mobility.activities.activity import Activity, ActivityParameters


class HomeActivity(Activity):

    def __init__(
        self,
        value_of_time: float = None,
        value_of_time_stay_home: float = None,
        saturation_fun_ref_level: float = None,
        saturation_fun_beta: float = None,
        survey_ids: List[str] = None,
        parameters: "HomeParameters" | None = None
    ):

        parameters = self.prepare_parameters(
            parameters=parameters,
            parameters_cls=HomeParameters,
            explicit_args={
                "value_of_time": value_of_time,
                "value_of_time_stay_home": value_of_time_stay_home,
                "saturation_fun_ref_level": saturation_fun_ref_level,
                "saturation_fun_beta": saturation_fun_beta,
                "survey_ids": survey_ids,
                "value_of_time_v2": value_of_time_stay_home,
            },
            owner_name="HomeActivity",
        )

        super().__init__(
            name="home",
            has_opportunities=False,
            is_anchor=True,
            parameters=parameters
        )

    def get_opportunities(self, transport_zones):
        return None


class HomeParameters(ActivityParameters):
    """Parameters specific to the home activity."""

    value_of_time: Annotated[
        float | ScalarParameterProfile,
        Field(default=10.0),
    ]

    value_of_time_stay_home: Annotated[
        float | ScalarParameterProfile,
        Field(default=0.0),
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
