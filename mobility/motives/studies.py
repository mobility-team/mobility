import pandas as pd
import polars as pl
import geopandas as gpd
from typing import List

from mobility.motives.motive import Motive
from mobility.parsers.schools_capacity_distribution import SchoolsCapacityDistribution

class StudiesMotive(Motive):

    def __init__(
        self,
        survey_ids: List[str] = ["2.20", "2.21"], # à modifier
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
            
            import geopandas as gpd
            import matplotlib.pyplot as plt

            
            fig, ax = plt.subplots(figsize=(8, 8))
            transport_zones.plot(ax=ax, facecolor="none", edgecolor="lightblue", linewidth=0.3)
            opportunities.plot(
                column="n_students",      # colonne pour la couleur
                cmap="viridis",           # palette (ex. 'viridis', 'plasma', 'OrRd', 'YlGnBu')
                legend=True,              # affiche la barre de couleur
                markersize=5,             # taille des points si géométries ponctuelles
                ax=ax
            )            
            ax.set_title("Boundary Suisse (union des communes)")
            ax.set_axis_off()
            plt.show()

            
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

        return pl.from_pandas(opportunities)
    

