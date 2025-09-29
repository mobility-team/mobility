from typing import List

from mobility.motives.motive import Motive

class HomeMotive(Motive):

    def __init__(
        self,
        survey_ids: List[str] = ["1.1"]
    ):

        super().__init__(
            name="home",
            survey_ids=survey_ids,
            has_opportunities=False,
            is_anchor=True
        )

    def get_opportunities(self, transport_zones):
        return None
