import pandas as pd
import polars as pl

from typing import List, Dict

from mobility.in_memory_asset import InMemoryAsset

class Motive(InMemoryAsset):

    def __init__(
            self,
            name: str,
            survey_ids: List[str] = None,
            radiation_lambda: float = None,
            has_opportunities: bool = True,
            is_anchor: bool = False,
            opportunities: pd.DataFrame = None,
            utilities: pd.DataFrame = None,
            country_utilities: Dict = None,
            sink_saturation_coeff: float = 1.0
        ):

        self.name = name
        self.survey_ids = survey_ids
        self.radiation_lambda = radiation_lambda
        self.has_opportunities = has_opportunities
        self.is_anchor = is_anchor
        self.opportunities = opportunities
        self.utilities = utilities
        self.country_utilities = country_utilities
        self.sink_saturation_coeff = sink_saturation_coeff

        inputs = {
            "survey_ids": survey_ids,
            "radiation_lambda": radiation_lambda,
            "country_utilities": country_utilities,
            "sink_saturation_coeff": sink_saturation_coeff
        }

        super().__init__(inputs)


    def get_utilities(self, transport_zones):

        if self.utilities is not None:

            utilities = self.utilities

        elif self.country_utilities is not None:
            
            transport_zones = transport_zones.get().drop("geometry", axis=1)
            transport_zones["country"] = transport_zones["local_admin_unit_id"].str[0:2]
            transport_zones["utility"] = transport_zones["country"].map(self.country_utilities)

            utilities = pl.from_pandas( 
                transport_zones[["transport_zone_id", "utility"]]
                .rename({"transport_zone_id": "to"}, axis=1)
            )

        else:

            utilities = None

        return utilities

    


