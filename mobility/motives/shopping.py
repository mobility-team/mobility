import pandas as pd
import polars as pl

from typing import List

from mobility.motives.motive import Motive
from mobility.parsers.shops_turnover_distribution import ShopsTurnoverDistribution

class ShoppingMotive(Motive):

    def __init__(
        self,
        survey_ids: List[str] = ["2.20", "2.21"],
        radiation_lambda: float = 0.99986,
        opportunities: pd.DataFrame = None
    ):

        super().__init__(
            name="shopping",
            survey_ids=survey_ids,
            radiation_lambda=radiation_lambda,
            opportunities=opportunities
        )

    
    def get_opportunities(self, transport_zones):

        if self.opportunities is not None:

            opportunities = self.opportunities

        else:

            transport_zones = transport_zones.get().drop("geometry", axis=1)
            transport_zones["country"] = transport_zones["local_admin_unit_id"].str[0:2]

            opportunities = ShopsTurnoverDistribution().get()
            opportunities = opportunities.groupby("local_admin_unit_id", as_index=False)[["turnover"]].sum()

            opportunities = pd.merge(
                transport_zones[["transport_zone_id", "local_admin_unit_id", "country", "weight"]],
                opportunities[["local_admin_unit_id", "turnover"]],
                on="local_admin_unit_id"
            )
            
            opportunities["n_opp"] = opportunities["weight"]*opportunities["turnover"]

            opportunities = ( 
                opportunities[["transport_zone_id", "n_opp"]]
                .rename({"transport_zone_id": "to"}, axis=1)
            )
            
        opportunities = pl.from_pandas(opportunities)
        opportunities = self.enforce_opportunities_schema(opportunities)

        return opportunities
    

