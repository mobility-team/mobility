import pandas as pd
import polars as pl

from mobility.motives.motive import Motive
from mobility.motives.other_motive_parameters import OtherMotiveParameters
from mobility.population import Population

class OtherMotive(Motive):

    def __init__(
        self,
        value_of_time: float = None,
        saturation_fun_ref_level: float = None,
        saturation_fun_beta: float = None,
        radiation_lambda: float = None,
        opportunities: pd.DataFrame = None,
        population: Population = None,
        parameters: OtherMotiveParameters | None = None
    ):
        
        if population is None and opportunities is None:
            raise ValueError("Please provide an opportunities proxy dataframe, or a Population instance if you want to use residents as proxy for the 'other' motive.")
        
        parameters = self.prepare_parameters(
            parameters=parameters,
            parameters_cls=OtherMotiveParameters,
            explicit_args={
                "value_of_time": value_of_time,
                "saturation_fun_ref_level": saturation_fun_ref_level,
                "saturation_fun_beta": saturation_fun_beta,
                "radiation_lambda": radiation_lambda,
            },
            owner_name="OtherMotive",
        )

        super().__init__(
            name="other",
            opportunities=opportunities,
            extra_inputs={"population": population},
            parameters=parameters
        )

    
    def get_opportunities(self, transport_zones):

        if self.opportunities is not None:

            opportunities = self.opportunities

        elif self.inputs["population"] is not None:

            opportunities = (
                pl.scan_parquet(self.inputs["population"].get()["population_groups"])
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
            
    
