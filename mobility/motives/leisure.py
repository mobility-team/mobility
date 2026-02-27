from __future__ import annotations

import pandas as pd
import polars as pl
import geopandas as gpd
from typing import List
import numpy as np
import os

from mobility.motives.motive import Motive
from mobility.parsers.leisure_facilities_distribution import LeisureFacilitiesDistribution
 
from typing import Annotated, List

from pydantic import Field
from mobility.motives.motive import Motive, MotiveParameters


class LeisureMotive(Motive):

    def __init__(
        self,
        value_of_time: float = None,
        saturation_fun_ref_level: float = None,
        saturation_fun_beta: float = None,
        survey_ids: List[str] = None,
        radiation_lambda: float = None,
        opportunities: pd.DataFrame = None,
        parameters: "LeisureMotiveParameters" | None = None
    ):
        
        if opportunities is None:
            raise ValueError("No built in leisure opportunities data for now, please provide an opportunities dataframe when creating instantiating the LeisureMotive class (or don't use it at all and let the OtherMotive model handle this motive).")

        parameters = self.prepare_parameters(
            parameters=parameters,
            parameters_cls=LeisureMotiveParameters,
            explicit_args={
                "value_of_time": value_of_time,
                "saturation_fun_ref_level": saturation_fun_ref_level,
                "saturation_fun_beta": saturation_fun_beta,
                "survey_ids": survey_ids,
                "radiation_lambda": radiation_lambda,
            },
            owner_name="LeisureMotive",
        )

        super().__init__(
            name="leisure",
            opportunities=opportunities,
            parameters=parameters
        )

    
    def get_opportunities(self, transport_zones):

        if self.opportunities is not None:

            opportunities = self.opportunities

        else:

            transport_zones = transport_zones.get()
            
            opportunities = LeisureFacilitiesDistribution().get()
            
            opportunities = gpd.sjoin(
                opportunities,
                transport_zones,
                how="left",  
                predicate="within"   
            ).drop(columns=["index_right"])
            opportunities = opportunities.dropna(subset=["transport_zone_id"])
            
            opportunities["country"] = opportunities["local_admin_unit_id"].str[0:2]
            
            opportunities = (
                opportunities.groupby(["transport_zone_id", "local_admin_unit_id", "country", "weight"], dropna=False)["freq_score"]
                  .sum()
                  .reset_index()
            )
                        
            opportunities["n_opp"] = opportunities["weight"]*opportunities["freq_score"]

            opportunities = ( 
                opportunities[["transport_zone_id", "n_opp"]]
                .rename({"transport_zone_id": "to"}, axis=1)
            )
            opportunities["to"] = opportunities["to"].astype("Int32")
            
            if os.environ.get("MOBILITY_DEBUG") == "1":
                self.plot_opportunities_map(
                    transport_zones, 
                    opportunities, 
                    use_log = False
                    )
            
        opportunities = pl.from_pandas(opportunities)
        opportunities = self.enforce_opportunities_schema(opportunities)

        return opportunities
    
        
    def plot_opportunities_map(
            self,
            transport_zones: gpd.GeoDataFrame,
            opportunities: pd.DataFrame,
            zone_id_col: str = "transport_zone_id",
            opp_zone_col: str = "to",
            value_col: str = "n_opp",
            use_log: bool = False            
        ):
        
            if not isinstance(transport_zones, gpd.GeoDataFrame):
                tz = gpd.GeoDataFrame(transport_zones, geometry="geometry", crs="EPSG:4326")
            else:
                tz = transport_zones
        
            if pl is not None and isinstance(opportunities, pl.DataFrame):
                opp = opportunities.to_pandas()
            else:
                opp = opportunities.copy()
        
            m = tz.merge(
                opp.rename(columns={opp_zone_col: zone_id_col}),
                on=zone_id_col,
                how="left"
            )
        
            m[value_col] = m[value_col].fillna(0)
            m = m[m["geometry"].notna()]
            m = m[~m.geometry.is_empty]
        
            if not m.geometry.is_valid.all():
                m["geometry"] = m.buffer(0)
                m = m[m["geometry"].notna()]
                m = m[~m.geometry.is_empty]
        
            if use_log:
                log_col = f"log_{value_col}"
                m[log_col] = np.log1p(m[value_col])
                col_to_plot = log_col
            else:
                col_to_plot = value_col
        
            ax = m.plot(
                column=col_to_plot,
                legend=True,
                cmap="plasma",
                linewidth=0.1,
                edgecolor="white",
                aspect=1
            )
            ax.set_axis_off()
        
            return ax
        
            

class LeisureMotiveParameters(MotiveParameters):
    """Parameters specific to the leisure motive."""

    value_of_time: Annotated[float, Field(default=10.0, ge=0.0)]
    saturation_fun_ref_level: Annotated[float, Field(default=1.5, ge=0.0)]
    saturation_fun_beta: Annotated[float, Field(default=4.0, ge=0.0)]
    survey_ids: Annotated[
        list[str],
        Field(default_factory=lambda: ["7.71", "7.72", "7.73", "7.74", "7.75", "7.76", "7.77", "7.78"]),
    ]
    radiation_lambda: Annotated[float, Field(default=0.99986, ge=0.0, le=1.0)]
