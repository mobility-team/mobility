import pandas as pd
import polars as pl
import geopandas as gpd
from typing import List
import numpy as np
import os
import polars as pl

from mobility.motives.motive import Motive
from mobility.parsers.schools_capacity_distribution import SchoolsCapacityDistribution

class StudiesMotive(Motive):

    def __init__(
        self,
        survey_ids: List[str] = ["9.92"], 
        radiation_lambda: float = 0.99986,
        opportunities: pd.DataFrame = None
    ):

        super().__init__(
            name="shopping",
            survey_ids=survey_ids,
            radiation_lambda=radiation_lambda,
            opportunities=opportunities
        )

    
    def get_opportunities(self, transport_zones):

        if self.opportunities is not None:

            opportunities = self.opportunities

        else:

            transport_zones = transport_zones.get()
            
            opportunities = SchoolsCapacityDistribution().get()
            
            opportunities.drop(columns="local_admin_unit_id", inplace= True)

            
            opportunities = gpd.sjoin(
                opportunities,
                transport_zones,
                how="left",  
                predicate="within"   
            ).drop(columns=["index_right"])
            opportunities = opportunities.dropna(subset=["transport_zone_id"])
            
            opportunities["country"] = opportunities["local_admin_unit_id"].str[0:2]
            

            
            opportunities = (
                opportunities.groupby(["transport_zone_id", "local_admin_unit_id", "country", "weight"], dropna=False)["n_students"]
                  .sum()
                  .reset_index()
            )
                        
            opportunities["n_opp"] = opportunities["weight"]*opportunities["n_students"]

            opportunities = ( 
                opportunities[["transport_zone_id", "n_opp"]]
                .rename({"transport_zone_id": "to"}, axis=1)
            )
            
            if os.environ.get("MOBILITY_DEBUG") == "1":
                self.plot_opportunities_map(
                    transport_zones, 
                    opportunities, 
                    use_log = True
                    )

        return pl.from_pandas(opportunities)
    
        
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
        
            # 5. Si demandé, transformer les valeurs en log
            if use_log:
                log_col = f"log_{value_col}"
                m[log_col] = np.log1p(m[value_col])
                col_to_plot = log_col
            else:
                col_to_plot = value_col
        
            # 6. Tracé
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
        
