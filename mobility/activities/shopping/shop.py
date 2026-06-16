from __future__ import annotations

import pandas as pd
import polars as pl

from typing import Annotated, List

from pydantic import Field

from mobility.activities.activity import Activity, ActivityParameters
from mobility.activities.shopping.shopping_opportunities import ShoppingOpportunities
from mobility.runtime.parameter_values import ParameterValue, SensitivityValue
from mobility.runtime.validation_types import UnitIntervalFloat


class ShopActivity(Activity):

    def __init__(
        self,
        value_of_time: float = None,
        saturation_fun_ref_level: float = None,
        saturation_fun_beta: float = None,
        destination_soft_capacity_factor: float = None,
        destination_shadow_price_sensitivity_coefficient: float = None,
        destination_shadow_price_min_coefficient: float = None,
        destination_sampling_overload_gamma: float = None,
        destination_sampling_min_attraction_factor: float = None,
        survey_ids: List[str] = None,
        radiation_lambda: float = None,
        opportunities: pd.DataFrame = None,
        country_value_coefficients: dict[str, float] | None = None,
        parameters: "ShopParameters" | None = None
    ):

        parameters = self.prepare_parameters(
            parameters=parameters,
            parameters_cls=ShopParameters,
            explicit_args={
                "value_of_time": value_of_time,
                "saturation_fun_ref_level": saturation_fun_ref_level,
                "saturation_fun_beta": saturation_fun_beta,
                "destination_soft_capacity_factor": destination_soft_capacity_factor,
                "destination_shadow_price_sensitivity_coefficient": (
                    destination_shadow_price_sensitivity_coefficient
                ),
                "destination_shadow_price_min_coefficient": (
                    destination_shadow_price_min_coefficient
                ),
                "destination_sampling_overload_gamma": destination_sampling_overload_gamma,
                "destination_sampling_min_attraction_factor": (
                    destination_sampling_min_attraction_factor
                ),
                "survey_ids": survey_ids,
                "radiation_lambda": radiation_lambda,
                "country_value_coefficients": country_value_coefficients,
            },
            owner_name="ShopActivity",
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

            transport_zones_asset = transport_zones
            transport_zones = transport_zones.get().drop("geometry", axis=1)

            tz_lau_ids = transport_zones["local_admin_unit_id"].unique().tolist()
            opportunities = ShoppingOpportunities(
                countries=transport_zones_asset.countries,
                local_admin_unit_ids=tz_lau_ids,
            ).get()
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
    

class ShopParameters(ActivityParameters):
    """Parameters specific to the shopping activity."""

    value_of_time: Annotated[float | ParameterValue | SensitivityValue, Field(default=10.0)]
    saturation_fun_ref_level: Annotated[float, Field(default=1.5, ge=0.0)]
    saturation_fun_beta: Annotated[float, Field(default=4.0, ge=0.0)]
    survey_ids: Annotated[list[str], Field(default_factory=lambda: ["2.20", "2.21"])]
    radiation_lambda: Annotated[UnitIntervalFloat, Field(default=0.99986)]
    country_value_coefficients: Annotated[
        dict[str, float],
        Field(default_factory=lambda: {"fr": 1.0, "ch": 0.9}),
    ]
