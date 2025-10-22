import pandas as pd
import polars as pl

from typing import List

from mobility.motives.motive import Motive
from mobility.population import Population

class OtherMotive(Motive):

    def __init__(
        self,
        radiation_lambda: float = 0.99986,
        opportunities: pd.DataFrame = None,
        population: Population = None
    ):
        
        if population is None and opportunities is None:
            raise ValueError("Please provide an opportunities proxy dataframe, or a Population instance if you want to use residents as proxy for the 'other' motive.")
        
        self.population = population

        super().__init__(
            name="other",
            radiation_lambda=radiation_lambda,
            opportunities=opportunities
        )

    
    def get_opportunities(self, transport_zones):

        if self.opportunities is not None:

            opportunities = self.opportunities

        elif self.population is not None:

            transport_zones = transport_zones.get().drop("geometry", axis=1)

            tz_ids = transport_zones["transport_zone_id"].unique().tolist()

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

        return pl.from_pandas(opportunities)
    
