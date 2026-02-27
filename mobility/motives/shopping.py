from __future__ import annotations

import pandas as pd
import polars as pl

from typing import Annotated, List

from pydantic import Field
from mobility.motives.motive import Motive, MotiveParameters
from mobility.parsers.shops_turnover_distribution import ShopsTurnoverDistribution


class ShoppingMotive(Motive):

    def __init__(
        self,
        value_of_time: float = None,
        saturation_fun_ref_level: float = None,
        saturation_fun_beta: float = None,
        survey_ids: List[str] = None,
        radiation_lambda: float = None,
        opportunities: pd.DataFrame = None,
        parameters: "ShoppingMotiveParameters" | None = None
    ):

        parameters = self.prepare_parameters(
            parameters=parameters,
            parameters_cls=ShoppingMotiveParameters,
            explicit_args={
                "value_of_time": value_of_time,
                "saturation_fun_ref_level": saturation_fun_ref_level,
                "saturation_fun_beta": saturation_fun_beta,
                "survey_ids": survey_ids,
                "radiation_lambda": radiation_lambda,
            },
            owner_name="ShoppingMotive",
        )

        super().__init__(
            name="shopping",
            opportunities=opportunities,
            parameters=parameters
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
    

class ShoppingMotiveParameters(MotiveParameters):
    """Parameters specific to the shopping motive."""

    value_of_time: Annotated[float, Field(default=10.0, ge=0.0)]
    saturation_fun_ref_level: Annotated[float, Field(default=1.5, ge=0.0)]
    saturation_fun_beta: Annotated[float, Field(default=4.0, ge=0.0)]
    survey_ids: Annotated[list[str], Field(default_factory=lambda: ["2.20", "2.21"])]
    radiation_lambda: Annotated[float, Field(default=0.99986, ge=0.0, le=1.0)]