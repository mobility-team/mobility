import pandas as pd
import polars as pl

from typing import List

from mobility.motives.motive import Motive
    

class LeisureMotive(Motive):

    def __init__(
        self,
        value_of_time: float = 10.0,
        saturation_fun_ref_level: float = 1.5,
        saturation_fun_beta: float = 4.0,
        survey_ids: List[str] = ["7.71", "7.72", "7.73", "7.74", "7.75", "7.76", "7.77", "7.78"],
        radiation_lambda: float = 0.99986,
        opportunities: pd.DataFrame = None
    ):
        
        if opportunities is None:
            raise ValueError("No built in leisure opportunities data for now, please provide an opportunities dataframe when creating instantiating the LeisureMotive class (or don't use it at all and let the OtherMotive model handle this motive).")

        super().__init__(
            name="leisure",
            value_of_time=value_of_time,
            survey_ids=survey_ids,
            radiation_lambda=radiation_lambda,
            opportunities=opportunities,
            saturation_fun_ref_level=saturation_fun_ref_level,
            saturation_fun_beta=saturation_fun_beta
        )

    
    def get_opportunities(self, transport_zones):

        transport_zones = transport_zones.get().drop("geometry", axis=1)
        transport_zones["country"] = transport_zones["local_admin_unit_id"].str[0:2]
        
        tz_lau_ids = transport_zones["local_admin_unit_id"].unique().tolist()

        opportunities = self.opportunities.loc[tz_lau_ids, "n_opp"].reset_index()

        opportunities = pd.merge(
            transport_zones[["transport_zone_id", "local_admin_unit_id", "country", "weight"]],
            opportunities[["local_admin_unit_id", "n_opp"]],
            on="local_admin_unit_id"
        )
        
        opportunities["n_opp"] = opportunities["weight"]*opportunities["n_opp"]

        opportunities = ( 
            opportunities[["transport_zone_id", "n_opp"]]
            .rename({"transport_zone_id": "to"}, axis=1)
        )

        opportunities = pl.from_pandas(opportunities)
        opportunities = self.enforce_opportunities_schema(opportunities)
    
        return opportunities
            
