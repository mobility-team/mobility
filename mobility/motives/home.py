from typing import List

from mobility.motives.motive import Motive
from mobility.motives.home_motive_parameters import HomeMotiveParameters

class HomeMotive(Motive):

    def __init__(
        self,
        value_of_time: float = None,
        value_of_time_stay_home: float = None,
        saturation_fun_ref_level: float = None,
        saturation_fun_beta: float = None,
        survey_ids: List[str] = None,
        parameters: HomeMotiveParameters | None = None
    ):

        parameters = self.prepare_parameters(
            parameters=parameters,
            parameters_cls=HomeMotiveParameters,
            explicit_args={
                "value_of_time": value_of_time,
                "value_of_time_stay_home": value_of_time_stay_home,
                "saturation_fun_ref_level": saturation_fun_ref_level,
                "saturation_fun_beta": saturation_fun_beta,
                "survey_ids": survey_ids,
                "value_of_time_v2": value_of_time_stay_home,
            },
            owner_name="HomeMotive",
        )

        super().__init__(
            name="home",
            has_opportunities=False,
            is_anchor=True,
            parameters=parameters
        )

    def get_opportunities(self, transport_zones):
        return None
