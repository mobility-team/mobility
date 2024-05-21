import logging
import pandas as pd
import geopandas as gpd

from mobility.destination_choice_model import DestinationChoiceModel
from mobility.parsers.jobs_active_population_distribution import JobsActivePopulationDistribution
from mobility.parsers.jobs_active_population_flows import JobsActivePopulationFlows

class WorkDestinationChoiceModel(DestinationChoiceModel):
    
    def __init__(
            self, transport_zones: gpd.GeoDataFrame, travel_costs: pd.DataFrame,
            cost_of_time: float = None,
            radiation_model_alpha: float = None, radiation_model_beta: float = None,
            fit_radiation_model: bool = True, ssi_min_flow_volume: float = 200.0
        ):
        
        self.jobs_active_population = JobsActivePopulationDistribution()
        self.reference_flows = JobsActivePopulationFlows()
        
        if fit_radiation_model is False:
            if radiation_model_alpha is None and radiation_model_beta is None:
                logging.log("The radiation model automatic fit was disabled by setting fit_radiation_model to False. Using cost_of_time=20.0, alpha=0.5, beta=0.5 as default parameters (you can set them manually with the radiation_model_alpha and radiation_model_beta parameters).")
                cost_of_time = 20.0
                radiation_model_alpha = 0.5
                radiation_model_beta = 0.5
            elif cost_of_time is None:
                raise ValueError("The radiation model automatic fit was disabled but you did not provide pass a value for the cost_of_time parameter.")
            elif radiation_model_alpha is None:
                raise ValueError("The radiation model automatic fit was disabled but you did not provide pass a value for the radiation_model_alpha parameter.")
            elif radiation_model_beta is None:
                raise ValueError("The radiation model automatic fit was disabled but you did not provide pass a value for the radiation_model_beta parameter.")
        
        super().__init__("work", transport_zones, travel_costs, cost_of_time, radiation_model_alpha, radiation_model_beta, fit_radiation_model, ssi_min_flow_volume)
        
        
    def prepare_sources_and_sinks(self, transport_zones: gpd.GeoDataFrame):
        
        active_population, jobs = self.jobs_active_population.get()
        sources = self.prepare_sources(transport_zones, active_population)
        sinks = self.prepare_sinks(transport_zones, jobs)
        
        return sources, sinks
    
    
    def prepare_sources(self, transport_zones: gpd.GeoDataFrame, active_population: pd.DataFrame)-> pd.DataFrame:

        active_population = active_population.loc[transport_zones["local_admin_unit_id"], "active_pop"].reset_index()
        active_population = pd.merge(active_population, transport_zones[["local_admin_unit_id", "transport_zone_id"]], on="local_admin_unit_id")

        active_population = active_population[["transport_zone_id", "active_pop"]]
        active_population.columns = ["transport_zone_id", "source_volume"]
        active_population.set_index("transport_zone_id", inplace=True)
        
        return active_population
    
    
    def prepare_sinks(self, transport_zones: gpd.GeoDataFrame, jobs: pd.DataFrame) -> pd.DataFrame:
        
        jobs = jobs.loc[transport_zones["local_admin_unit_id"], "n_jobs_total"].reset_index()
        jobs = pd.merge(jobs, transport_zones[["local_admin_unit_id", "transport_zone_id"]], on="local_admin_unit_id")
        
        jobs = jobs[["transport_zone_id", "n_jobs_total"]]
        jobs.columns = ["transport_zone_id", "sink_volume"]
        jobs.set_index("transport_zone_id", inplace=True)
        
        return jobs
    
    
    def prepare_reference_flows(self, transport_zones: gpd.GeoDataFrame):
        
        admin_ids = transport_zones["local_admin_unit_id"].values
        
        ref_flows = self.reference_flows.get()
        ref_flows = ref_flows[(ref_flows["local_admin_unit_id_from"].isin(admin_ids)) & (ref_flows["local_admin_unit_id_to"].isin(admin_ids))]
        ref_flows.rename({"flow_volume": "ref_flow_volume"}, axis=1, inplace=True)
        
        return ref_flows
    

