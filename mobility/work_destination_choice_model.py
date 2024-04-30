import pandas as pd
import geopandas as gpd

from mobility.destination_choice_model import DestinationChoiceModel
from mobility.get_insee_data import get_insee_data

class WorkDestinationChoiceModel(DestinationChoiceModel):
    
    def __init__(
            self, transport_zones: gpd.GeoDataFrame, travel_costs: pd.DataFrame,
            cost_of_time: float = 20.0, radiation_model_alpha: float = 0.0, radiation_model_beta: float = 1.0
        ):
        
        self.insee_data = get_insee_data()
        
        super().__init__("work", transport_zones, travel_costs, cost_of_time, radiation_model_alpha, radiation_model_beta)
    
    
    def prepare_sources(self, transport_zones: gpd.GeoDataFrame) -> pd.DataFrame:
        
        active_population = self.insee_data["active_population"]
        active_population = active_population.loc[transport_zones["admin_id"]].sum(axis=1).reset_index()
        active_population = pd.merge(active_population, transport_zones[["admin_id", "transport_zone_id"]], left_on="CODGEO", right_on="admin_id")

        active_population = active_population[["transport_zone_id", 0]]
        active_population.columns = ["transport_zone_id", "source_volume"]
        active_population.set_index("transport_zone_id", inplace=True)
        
        return active_population
    
    
    def prepare_sinks(self, transport_zones: gpd.GeoDataFrame) -> pd.DataFrame:
        
        jobs = self.insee_data["jobs"]
        jobs = jobs.loc[transport_zones["admin_id"]].sum(axis=1).reset_index()
        jobs = pd.merge(jobs, transport_zones[["admin_id", "transport_zone_id"]], left_on="CODGEO", right_on="admin_id")
        
        jobs = jobs[["transport_zone_id", 0]]
        jobs.columns = ["transport_zone_id", "sink_volume"]
        jobs.set_index("transport_zone_id", inplace=True)
        
        return jobs
    

