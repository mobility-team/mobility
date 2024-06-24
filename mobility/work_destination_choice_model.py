import logging
import pandas as pd
import geopandas as gpd
import numpy as np

from mobility.destination_choice_model import DestinationChoiceModel
from mobility.parsers.jobs_active_population_distribution import JobsActivePopulationDistribution
from mobility.parsers.jobs_active_population_flows import JobsActivePopulationFlows

class WorkDestinationChoiceModel(DestinationChoiceModel):
    
    def __init__(
            self,
            transport_zones: gpd.GeoDataFrame,
            travel_costs: pd.DataFrame,
            active_population: pd.DataFrame = None,
            jobs: pd.DataFrame = None,
            reference_flows: pd.DataFrame = None,
            cost_of_time: float = 25.0,
            cost_of_distance: float = 0.2,
            additional_utility: dict = {"fr-fr": 120.0, "fr-ch": 240.0, "ch-fr": 120.0, "ch-ch": 240.0},
            radiation_model_type: str = "universal",
            radiation_model_alpha: float = 0.2,
            radiation_model_beta: float = 0.8,
            radiation_model_lambda: float = 0.9999,
            fit_radiation_model: bool = False,
            ssi_min_flow_volume: float = 200.0
        ):
        
        if active_population is None:
            self.active_population = None
            self.jobs = None
            self.jobs_active_population = JobsActivePopulationDistribution()
            self.reference_flows = JobsActivePopulationFlows()
        else:
            self.active_population = active_population
            self.jobs = jobs
            self.reference_flows = reference_flows
        
        
        if fit_radiation_model is False:
            if radiation_model_alpha is None and radiation_model_beta is None:
                logging.log("The radiation model automatic fit was disabled by setting fit_radiation_model to False. Using cost_of_time=20.0, cost_of_distance=0.1, alpha=0.5, beta=0.5 as default parameters (you can set them manually with the radiation_model_alpha and radiation_model_beta parameters).")
                cost_of_time = 20.0
                cost_of_distance = 0.1
                radiation_model_alpha = 0.5
                radiation_model_beta = 0.5
            elif cost_of_time is None:
                raise ValueError("The radiation model automatic fit was disabled but you did not provide a value for the cost_of_time parameter.")
            elif cost_of_distance is None:
                raise ValueError("The radiation model automatic fit was disabled but you did not provide a value for the cost_of_distance parameter.")
            elif radiation_model_alpha is None:
                raise ValueError("The radiation model automatic fit was disabled but you did not provide a value for the radiation_model_alpha parameter.")
            elif radiation_model_beta is None:
                raise ValueError("The radiation model automatic fit was disabled but you did not provide a value for the radiation_model_beta parameter.")
        
        super().__init__(
            "work",
            transport_zones,
            travel_costs,
            cost_of_time,
            cost_of_distance,
            additional_utility,
            radiation_model_type,
            radiation_model_alpha,
            radiation_model_beta,
            radiation_model_lambda,
            fit_radiation_model,
            ssi_min_flow_volume
        )
        
        
    def prepare_sources_and_sinks(self, transport_zones: gpd.GeoDataFrame):
        
        if self.active_population is None:
        
            jobs, active_population = self.jobs_active_population.get()
            reference_flows = self.reference_flows.get()
            
        else:
            
            active_population = self.active_population
            jobs = self.jobs
            reference_flows = self.reference_flows
        
        sources = self.prepare_sources(transport_zones, active_population, reference_flows)
        sinks = self.prepare_sinks(transport_zones, jobs, reference_flows)
        
        return sources, sinks
    
    
    def prepare_sources(self, transport_zones: gpd.GeoDataFrame, active_population: pd.DataFrame, reference_flows: pd.DataFrame)-> pd.DataFrame:

        active_population = active_population.loc[transport_zones["local_admin_unit_id"], "active_pop"].reset_index()
        active_population = pd.merge(active_population, transport_zones[["local_admin_unit_id", "transport_zone_id"]], on="local_admin_unit_id")
        
        # Remove the part of the active population that works outside of the transport zones
        act_pop_ext = reference_flows.loc[
            (reference_flows["local_admin_unit_id_from"].isin(transport_zones["local_admin_unit_id"])) &
            (~reference_flows["local_admin_unit_id_to"].isin(transport_zones["local_admin_unit_id"]))
        ]
        
        act_pop_ext = act_pop_ext.groupby("local_admin_unit_id_from", as_index=False)["ref_flow_volume"].sum()
        
        active_population = pd.merge(
            active_population,
            act_pop_ext,
            left_on="local_admin_unit_id",
            right_on="local_admin_unit_id_from",
            how = "left"
        )
        
        active_population["ref_flow_volume"] = active_population["ref_flow_volume"].fillna(0.0)
        active_population["active_pop"] -= active_population["ref_flow_volume"]
        
        # There are errors in the reference data that lead to negative values,
        # so we need to set these to zero
        active_population["active_pop"] = np.where(active_population["active_pop"] < 0.0, 0.0, active_population["active_pop"])

        active_population = active_population[active_population["active_pop"] > 0.0]

        active_population = active_population[["transport_zone_id", "active_pop"]]
        active_population.columns = ["from", "source_volume"]
        active_population.set_index("from", inplace=True)
        
        logging.info("Total active population count  : " + str(round(active_population["source_volume"].sum())))
        
        return active_population
    
    
    def prepare_sinks(self, transport_zones: gpd.GeoDataFrame, jobs: pd.DataFrame, reference_flows: pd.DataFrame) -> pd.DataFrame:
        
        jobs = jobs.loc[transport_zones["local_admin_unit_id"], "n_jobs_total"].reset_index()
        jobs = pd.merge(jobs, transport_zones[["local_admin_unit_id", "transport_zone_id"]], on="local_admin_unit_id")
        
        # Remove the part of the jobs that are occupied by people living outside of the transport zones
        jobs_ext = reference_flows.loc[
            (~reference_flows["local_admin_unit_id_from"].isin(transport_zones["local_admin_unit_id"])) &
            (reference_flows["local_admin_unit_id_to"].isin(transport_zones["local_admin_unit_id"]))
        ]
        
        jobs_ext = jobs_ext.groupby("local_admin_unit_id_to", as_index=False)["ref_flow_volume"].sum()
        
        jobs = pd.merge(
            jobs,
            jobs_ext,
            left_on="local_admin_unit_id",
            right_on="local_admin_unit_id_to",
            how = "left"
        )
        
        jobs["ref_flow_volume"] = jobs["ref_flow_volume"].fillna(0.0)
        jobs["n_jobs_total"] -= jobs["ref_flow_volume"]
        
        # There are errors in the reference data that lead to negative values,
        # so we need to set these to zero
        jobs["n_jobs_total"] = np.where(jobs["n_jobs_total"] < 0.0, 0.0, jobs["n_jobs_total"])
    
        jobs = jobs[jobs["n_jobs_total"] > 0.0]
        
        jobs = jobs[["transport_zone_id", "n_jobs_total"]]
        jobs.columns = ["to", "sink_volume"]
        jobs.set_index("to", inplace=True)
        
        logging.info("Total job count : " + str(round(jobs["sink_volume"].sum())))
        
        return jobs
    
    
    def prepare_reference_flows(self, transport_zones: gpd.GeoDataFrame):
        
        admin_ids = transport_zones["local_admin_unit_id"].values
        
        if self.active_population is None:
            ref_flows = self.reference_flows.get()
        else:
            ref_flows = self.reference_flows
            
        ref_flows = ref_flows[(ref_flows["local_admin_unit_id_from"].isin(admin_ids)) & (ref_flows["local_admin_unit_id_to"].isin(admin_ids))].copy()
        ref_flows.rename({"flow_volume": "ref_flow_volume"}, axis=1, inplace=True)
        
        return ref_flows
    

