import pandas as pd
import polars as pl

from mobility.motives.motive import Motive
from mobility.population import Population

class OtherMotive(Motive):

    def __init__(
        self,
        value_of_time: float = 10.0,
        saturation_fun_ref_level: float = 1.5,
        saturation_fun_beta: float = 4.0,
        radiation_lambda: float = 0.9999999,
        opportunities: pd.DataFrame = None,
        population: Population = None
    ):
        
        if population is None and opportunities is None:
            raise ValueError("Please provide an opportunities proxy dataframe, or a Population instance if you want to use residents as proxy for the 'other' motive.")
        
        self.population = population

        super().__init__(
            name="other",
            value_of_time=value_of_time,
            radiation_lambda=radiation_lambda,
            opportunities=opportunities,
            saturation_fun_ref_level=saturation_fun_ref_level,
            saturation_fun_beta=saturation_fun_beta
        )

    
    def get_opportunities(self, transport_zones):

        if self.opportunities is not None:

            opportunities = self.opportunities

        elif self.population is not None:

            opportunities = (
                pl.scan_parquet(self.population.get()["population_groups"])
                .group_by(["transport_zone_id"])
                .agg(
                    n_opp=pl.col("weight").sum()
                )
                .collect(engine="streaming")
                .rename({"transport_zone_id": "to"})
                .to_pandas()
            )

        opportunities = pl.from_pandas(opportunities)
        opportunities = self.enforce_opportunities_schema(opportunities)

        return opportunities
            
    
