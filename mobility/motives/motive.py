import pandas as pd
import polars as pl

from typing import List, Dict

from mobility.in_memory_asset import InMemoryAsset
from mobility.motives.motive_parameters import MotiveParameters

class Motive(InMemoryAsset):

    def __init__(
            self,
            name: str,
            value_of_time: float = None,
            saturation_fun_ref_level: float = None,
            saturation_fun_beta: float = None,
            value_of_time_v2: float = None,
            survey_ids: List[str] = None,
            radiation_lambda: float = None,
            has_opportunities: bool = True,
            is_anchor: bool = False,
            opportunities: pd.DataFrame = None,
            utilities: pd.DataFrame = None,
            country_utilities: Dict = None,
            sink_saturation_coeff: float = None,
            extra_inputs: dict | None = None,
            parameters: MotiveParameters | None = None,
        ):

        parameters = self.prepare_parameters(
            parameters=parameters,
            parameters_cls=MotiveParameters,
            explicit_args={
                "value_of_time": value_of_time,
                "saturation_fun_ref_level": saturation_fun_ref_level,
                "saturation_fun_beta": saturation_fun_beta,
                "value_of_time_v2": value_of_time_v2,
                "survey_ids": survey_ids,
                "radiation_lambda": radiation_lambda,
                "country_utilities": country_utilities,
                "sink_saturation_coeff": sink_saturation_coeff,
            },
            required_fields=["value_of_time", "saturation_fun_ref_level", "saturation_fun_beta"],
            owner_name=f"Motive({name})",
        )

        self.name = name
        self.has_opportunities = has_opportunities
        self.is_anchor = is_anchor
        self.opportunities = opportunities
        self.utilities = utilities

        inputs = {
            "parameters": parameters
        }
        if extra_inputs is not None:
            inputs.update(extra_inputs)

        super().__init__(inputs)


    def get_utilities(self, transport_zones):

        if self.utilities is not None:

            utilities = self.utilities

        elif self.inputs["parameters"].country_utilities is not None:
            
            transport_zones = transport_zones.get().drop("geometry", axis=1)
            transport_zones["country"] = transport_zones["local_admin_unit_id"].str[0:2]
            transport_zones["utility"] = transport_zones["country"].map(self.inputs["parameters"].country_utilities)

            utilities = pl.from_pandas( 
                transport_zones[["transport_zone_id", "utility"]]
                .rename({"transport_zone_id": "to"}, axis=1)
            )

        else:

            utilities = None

        return utilities


    def enforce_opportunities_schema(self, opportunities):
        
        opportunities = (
            
            opportunities
            .with_columns(
                to=pl.col("to").cast(pl.Int32()),
                n_opp=pl.col("n_opp").cast(pl.Float64())
            )
            
        )
        
        return opportunities
    


