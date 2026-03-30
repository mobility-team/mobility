from __future__ import annotations

import pandas as pd
import polars as pl
from typing import Annotated

from pydantic import Field

from mobility.activities.activity import Activity, ActivityParameters
from mobility.population import Population
from mobility.runtime.parameter_profiles import ParameterProfile
from mobility.validation_types import NonNegativeFloat, UnitIntervalFloat


class Other(Activity):

    def __init__(
        self,
        value_of_time: float = None,
        saturation_fun_ref_level: float = None,
        saturation_fun_beta: float = None,
        radiation_lambda: float = None,
        opportunities: pd.DataFrame = None,
        population: Population = None,
        parameters: "OtherParameters" | None = None
    ):
        
        if population is None and opportunities is None:
            raise ValueError(
                "Please provide an opportunities proxy dataframe, or a Population "
                "instance if you want to use residents as proxy for the 'other' activity."
            )
        
        parameters = self.prepare_parameters(
            parameters=parameters,
            parameters_cls=OtherParameters,
            explicit_args={
                "value_of_time": value_of_time,
                "saturation_fun_ref_level": saturation_fun_ref_level,
                "saturation_fun_beta": saturation_fun_beta,
                "radiation_lambda": radiation_lambda,
            },
            owner_name="Other",
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
            
    

class OtherParameters(ActivityParameters):
    """Parameters specific to the other activity."""

    value_of_time: Annotated[
        float | ParameterProfile,
        Field(default=10.0),
    ]

    saturation_fun_ref_level: Annotated[
        float,
        Field(default=1.5, ge=0.0),
    ]

    saturation_fun_beta: Annotated[
        float,
        Field(default=4.0, ge=0.0),
    ]

    radiation_lambda: Annotated[
        UnitIntervalFloat,
        Field(default=0.99986),
    ]
