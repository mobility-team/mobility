import logging
import pandas as pd
import geopandas as gpd
import numpy as np
import pathlib
import os

from importlib import resources

from mobility.choice_models.destination_choice_model import DestinationChoiceModel
from mobility.parsers.jobs_active_population_distribution import JobsActivePopulationDistribution
from mobility.parsers.jobs_active_population_flows import JobsActivePopulationFlows
from mobility.r_utils.r_script import RScript

from mobility.transport_modes import TransportMode

from dataclasses import dataclass, field
from typing import Dict, Union, List

@dataclass
class WorkDestinationChoiceModelParameters:
    
    model: Dict[str, Union[str, float]] = field(
        default_factory=lambda: {
            "type": "radiation",
            "lambda": 0.99986
        }
    )
    
    utility: Dict[str, float] = field(
        default_factory=lambda: {
            "fr": 120.0,
            "ch": 120.0
        }
    )
    
    

class WorkDestinationChoiceModel(DestinationChoiceModel):
    
    def __init__(
            self,
            transport_zones: gpd.GeoDataFrame,
            modes: List[TransportMode],
            parameters: WorkDestinationChoiceModelParameters = WorkDestinationChoiceModelParameters(),
            active_population: pd.DataFrame = None,
            jobs: pd.DataFrame = None,
            reference_flows: pd.DataFrame = None,
            ssi_min_flow_volume: float = 200.0
        ):
        """
        

        Parameters
        ----------
        transport_zones : gpd.GeoDataFrame
            Transport zones generated by TransportZones class.
        travel_costs : pd.DataFrame
            Travel costs generated by TravelCosts class.
        utility_parameters : dict
            Dictionary that contains:
            - "mode_coefficients": for each mode (car, bicycle, walk, public_transport), a dictionary with two values:
                - "constant": cost for using that mode on one journey no matter the distance,
                - "cost_of_distance": cost for each additional km
            - "crossborder_constant"
        active_population : pd.DataFrame, optional
            DataFrame containing the active population data on the territory. 
            If not provided, population, jobs and reference flows will be retrieved thanks to parsers.
        jobs : pd.DataFrame, optional
            DataFrame containing the job data on the territory. Should be provided if active_population is also provided
        reference_flows : pd.DataFrame, optional
            Reference home-work flows to compare the model flows with. Should be provided if active_population is also provided
        model_parameters : dict, optional
            Choice between two models:
                - "type" = "radiation_selection", based on Simini et al. 2014. DOI 10.1371/journal.pone.0060069
                  Its parameter "lambda" should range between 0 and 1.
                - "type" = "radiation_universal", based on Liu & Yan 2020. DOI 10.1038/s41598-020-61613-y
                  The two parameters "alpha" and "beta" should also range between 0 and 1, with alpha+beta<=1
                The default is {"type": "radiation_selection", "lambda": 0.9999}.
        ssi_min_flow_volume : float, optional
            Minimum reference volume to consider for similarity index. The default is 200.0 per INSEE recommendation.


        """
        
        
        if active_population is None:
            self.active_population = None
            self.jobs = None
            self.jobs_active_population = JobsActivePopulationDistribution()
            self.reference_flows = JobsActivePopulationFlows()
        else:
            self.active_population = active_population
            self.jobs = jobs
            self.reference_flows = reference_flows
        
        
        if "type" not in parameters.model.keys():
            raise ValueError("The model_parameters should be a dict that specifies the type of radiation model : radiation_universal or radiation_selection")
        
        if parameters.model["type"] == "radiation_selection":
            if "lambda" not in parameters.model.keys():
                raise ValueError("Lambda parameter missing in model_parameters. It should be a dict with keys fr and ch.")
            
        if parameters.model["type"] == "radiation_universal":
            if "alpha" not in parameters.model.keys():
                raise ValueError("Alpha parameter missing in model_parameters.")
            if "beta" not in parameters.model.keys():
                raise ValueError("Beta parameter missing in model_parameters.")
        
        super().__init__(
            "work",
            transport_zones,
            modes,
            parameters,
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
    
    
    def prepare_sources(
            self,
            transport_zones: gpd.GeoDataFrame,
            active_population: pd.DataFrame,
            reference_flows: pd.DataFrame
        ) -> pd.DataFrame:
        
        tz_lau_ids = transport_zones["local_admin_unit_id"].unique()

        active_population = active_population.loc[tz_lau_ids, "active_pop"].reset_index()
        
        # Remove the part of the active population that works outside of the transport zones
        act_pop_ext = reference_flows.loc[
            (reference_flows["local_admin_unit_id_from"].isin(tz_lau_ids)) &
            (~reference_flows["local_admin_unit_id_to"].isin(tz_lau_ids))
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
        
        # There are errors in the reference data that can lead to negative values
        active_population = active_population[active_population["active_pop"] > 0.0]
        
        # Disaggregate the active population at transport zone level
        active_population = pd.merge(
            transport_zones[["transport_zone_id", "local_admin_unit_id", "weight"]],
            active_population[["local_admin_unit_id", "active_pop"]],
            on="local_admin_unit_id"
        )
        
        active_population["active_pop"] *= active_population["weight"]
    
        active_population = active_population[["transport_zone_id", "active_pop"]]
        active_population.columns = ["from", "source_volume"]
        active_population.set_index("from", inplace=True)
        
        logging.info("Total active population count  : " + str(round(active_population["source_volume"].sum())))
        
        return active_population
    
    
    def prepare_sinks(
            self,
            transport_zones: gpd.GeoDataFrame, 
            jobs: pd.DataFrame,
            reference_flows: pd.DataFrame
        ) -> pd.DataFrame:
        
        tz_lau_ids = transport_zones["local_admin_unit_id"].unique()
        
        jobs = jobs.loc[tz_lau_ids, "n_jobs_total"].reset_index()
        
        # Remove the part of the jobs that are occupied by people living outside of the transport zones
        jobs_ext = reference_flows.loc[
            (~reference_flows["local_admin_unit_id_from"].isin(tz_lau_ids)) &
            (reference_flows["local_admin_unit_id_to"].isin(tz_lau_ids))
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
        
        # There are errors in the reference data that lead to negative values
        jobs = jobs[jobs["n_jobs_total"] > 0.0]
        
        # Disaggregate the jobs counts at transport zone level
        jobs = pd.merge(
            transport_zones[["transport_zone_id", "local_admin_unit_id", "weight"]],
            jobs[["local_admin_unit_id", "n_jobs_total"]],
            on="local_admin_unit_id"
        )
        
        jobs["n_jobs_total"] *= jobs["weight"]
    
        
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
   
    
    def compute_utility_by_od_and_mode(
            self,
            transport_zones: gpd.GeoDataFrame,
            travel_costs: pd.DataFrame
        ):
        
        params = self.inputs["parameters"]
        
        travel_costs = pd.merge(
            travel_costs,
            transport_zones[["transport_zone_id", "country"]].rename({"transport_zone_id": "to"}, axis=1).set_index("to"),
            left_index=True,
            right_index=True
        )

        travel_costs["utility"] = travel_costs["country"].map(params.utility)
        travel_costs["net_utility"] = travel_costs["utility"] - 2*travel_costs["cost"]
        
        return travel_costs
    
    
    
    def plot_flows(self):
        
        output_path = pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"]) / "flows.svg"
        
        logging.info(f"Plotting flows (svg path: {output_path})")
        
        script = RScript(resources.files('mobility.r_utils').joinpath('plot_flows.R'))
        script.run(
            args=[
                str(self.inputs["transport_zones"].cache_path),
                str(self.cache_path["od_flows"]),
                output_path
            ]
        )
        
        return None

    
