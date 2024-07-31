import logging
import pandas as pd
import geopandas as gpd
import numpy as np
import pathlib
import os

from importlib import resources

from mobility.destination_choice_model import DestinationChoiceModel
from mobility.parsers.jobs_active_population_distribution import JobsActivePopulationDistribution
from mobility.parsers.jobs_active_population_flows import JobsActivePopulationFlows
from mobility.r_script import RScript

class WorkDestinationChoiceModel(DestinationChoiceModel):
    
    def __init__(
            self,
            transport_zones: gpd.GeoDataFrame,
            travel_costs: pd.DataFrame,
            utility_parameters: dict,
            active_population: pd.DataFrame = None,
            jobs: pd.DataFrame = None,
            reference_flows: pd.DataFrame = None,
            model_parameters: dict = {"type": "selection", "lambda": 0.9999},
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
        
        
        if "type" not in model_parameters.keys():
            raise ValueError("The model_parameters should be a dict that specifies the type of radiation model : radiation_universal or radiation_selection")
        
        if model_parameters["type"] == "radiation_selection":
            if "lambda" not in model_parameters.keys():
                raise ValueError("Lambda parameter missing in model_parameters.")
            
        if model_parameters["type"] == "radiation_universal":
            if "alpha" not in model_parameters.keys():
                raise ValueError("Alpha parameter missing in model_parameters.")
            if "beta" not in model_parameters.keys():
                raise ValueError("Beta parameter missing in model_parameters.")
        
        super().__init__(
            "work",
            transport_zones,
            travel_costs,
            model_parameters,
            utility_parameters,
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
   
    
    def compute_utility_by_od_and_mode(
            self,
            transport_zones: gpd.GeoDataFrame,
            travel_costs: pd.DataFrame,
            utility_parameters: dict
        ):
        
        travel_costs = pd.merge(
            travel_costs,
            transport_zones[["transport_zone_id", "local_admin_unit_id"]].rename({"transport_zone_id": "from"}, axis=1).set_index("from"),
            left_index=True,
            right_index=True
        )
        
        travel_costs = pd.merge(
            travel_costs,
            transport_zones[["transport_zone_id", "local_admin_unit_id"]].rename({"transport_zone_id": "to"}, axis=1).set_index("to"),
            left_index=True,
            right_index=True
        )
        
        travel_costs["crossborder_flow"] = travel_costs["local_admin_unit_id_x"].str[0:2] + "-" + travel_costs["local_admin_unit_id_y"].str[0:2]
        
        # Utility function : U = -ct*time*2 + -cd*distance*2 - constant + u_crossborder
        
        # Extract all OD modes (grouping all public_transport into one category for now)
        modes = travel_costs.index.get_level_values("mode")
        modes = modes.where(modes.isin(["car", "bicycle", "walk"]), "public_transport")
        
        # Cost of time (ct) : distance dependant, from https://www.ecologie.gouv.fr/sites/default/files/documents/V.2.pdf
        ct = 18.6
        ct = np.where(travel_costs["distance"] > 20, 14.4 + 0.215*travel_costs["distance"], ct)
        ct = np.where(travel_costs["distance"] > 80, 30.2 + 0.017*travel_costs["distance"], ct)
        ct = np.where(travel_costs["distance"] > 400, 37.0, ct)
        ct *= 1.17 # Inflation coeff
        
        # Cost of distance : mode dependent      
        cd = {m: c["cost_of_distance"] for m, c in utility_parameters["mode_coefficients"].items()}
        cd = modes.map(cd)
        
        # Constant : mode dependent
        constants = {m: c["constant"] for m, c in utility_parameters["mode_coefficients"].items()}
        constants = modes.map(constants)
        
        # Crossborder utility
        u_crossborder = travel_costs["crossborder_flow"].map(utility_parameters["crossborder_constant"])
        
        # Compute the total cost, utility and net utility
        travel_costs["cost"] = ct*travel_costs["time"]*2
        travel_costs["cost"] += cd*travel_costs["distance"]*2
        travel_costs["cost"] += constants
    
        travel_costs["utility"] = u_crossborder
        travel_costs["net_utility"] = travel_costs["utility"] - travel_costs["cost"]
        
        return travel_costs
    
    
    
    def plot_flows(self):
        
        output_path = pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"]) / "flows.svg"
        
        logging.info(f"Plotting flows (svg path: {output_path})")
        
        script = RScript(resources.files('mobility.R').joinpath('plot_flows.R'))
        script.run(
            args=[
                str(self.inputs["transport_zones"].cache_path),
                str(self.cache_path["od_flows"]),
                output_path
            ]
        )
        
        return None

    

