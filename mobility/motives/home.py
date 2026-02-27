from typing import List

from mobility.motives.motive import Motive

class HomeMotive(Motive):

    def __init__(
        self,
        value_of_time: float = 10.0,
        value_of_time_stay_home: float = 0.0,
        saturation_fun_ref_level: float = 100.0,
        saturation_fun_beta: float = 4.0,
        survey_ids: List[str] = ["1.1"]
    ):
        
        self.value_of_time_stay_home = value_of_time_stay_home

        super().__init__(
            name="home",
            survey_ids=survey_ids,
            has_opportunities=False,
            is_anchor=True,
            value_of_time=value_of_time,
            value_of_time_v2=value_of_time_stay_home,
            saturation_fun_ref_level=saturation_fun_ref_level,
            saturation_fun_beta=saturation_fun_beta
        )

    def get_opportunities(self, transport_zones):
        return None
