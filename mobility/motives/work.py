import pandas as pd
import polars as pl

from typing import List, Dict

from mobility.motives.motive import Motive
from mobility.parsers import JobsActivePopulationDistribution

class WorkMotive(Motive):

    def __init__(
        self,
        value_of_time: float = 10.0,
        saturation_fun_ref_level: float = 1.5,
        saturation_fun_beta: float = 4.0,
        survey_ids: List[str] = ["9.91"],
        radiation_lambda: float = 0.9999999,
        opportunities: pd.DataFrame = None,
        utilities: pd.DataFrame = None,
        country_utilities: Dict = None
    ):

        if country_utilities is None:
            country_utilities = {
                "fr": 0.0,
                "ch": 5.0
            }

        super().__init__(
            name="work",
            value_of_time=value_of_time,
            survey_ids=survey_ids,
            radiation_lambda=radiation_lambda,
            is_anchor=True,
            opportunities=opportunities,
            utilities=utilities,
            country_utilities=country_utilities,
            saturation_fun_ref_level=saturation_fun_ref_level,
            saturation_fun_beta=saturation_fun_beta
        )

    def get_opportunities(self, transport_zones):

        if self.opportunities is not None:

            opportunities = self.opportunities

        else:

            transport_zones = transport_zones.get().drop("geometry", axis=1)
            transport_zones["country"] = transport_zones["local_admin_unit_id"].str[0:2]
            
            tz_lau_ids = transport_zones["local_admin_unit_id"].unique().tolist()

            opportunities = JobsActivePopulationDistribution().get()[0]
            opportunities = opportunities.loc[tz_lau_ids, "n_jobs_total"].reset_index()

            opportunities = pd.merge(
                transport_zones[["transport_zone_id", "local_admin_unit_id", "country", "weight"]],
                opportunities[["local_admin_unit_id", "n_jobs_total"]],
                on="local_admin_unit_id"
            )
            
            opportunities["n_opp"] = opportunities["weight"]*opportunities["n_jobs_total"]

            opportunities = ( 
                opportunities[["transport_zone_id", "n_opp"]]
                .rename({"transport_zone_id": "to"}, axis=1)
            )

        opportunities = pl.from_pandas(opportunities)
        opportunities = self.enforce_opportunities_schema(opportunities)

        return opportunities
