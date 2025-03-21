import logging
import pandas as pd
import geopandas as gpd
import numpy as np
import pathlib
import os
import polars as pl


from importlib import resources

from mobility.choice_models.destination_choice_model import DestinationChoiceModel
from mobility.choice_models.work_utilities import WorkUtilities
from mobility.parsers.shops_turnover_distribution import ShopsTurnoverDistribution
from mobility.r_utils.r_script import RScript

from mobility.radiation_model import radiation_model
from mobility.radiation_model_selection import apply_radiation_model

from mobility.transport_modes.transport_mode import TransportMode
from mobility.transport_zones import TransportZones

from dataclasses import dataclass, field
from typing import Dict, Union, List


@dataclass
class ShoppingDestinationChoiceModelParameters:
    
    model: Dict[str, Union[str, float]] = field(
        default_factory=lambda: {
            "type": "radiation",
            "lambda": 0.99986,
            "end_of_contract_rate": 0.00, # à supprimer ?
            "job_change_utility_constant": -5.0, # à supprimer ?
            "max_iterations": 10,
            "tolerance": 0.01,
            "cost_update": False,
            "n_iter_cost_update": 3
        }
    )
    
    utility: Dict[str, float] = field(
        default_factory=lambda: {
            "fr": 120.0,
            "ch": 120.0
        }
    )
    
    

class ShoppingDestinationChoiceModel(DestinationChoiceModel):
    
    def __init__(
            self,
            transport_zones: TransportZones,
            modes: List[TransportMode],
            parameters: ShoppingDestinationChoiceModelParameters = ShoppingDestinationChoiceModelParameters(),
            shopping_sources_and_sinks: pd.DataFrame = None,
            jobs: pd.DataFrame = None,
            reference_flows: pd.DataFrame = None,
            ssi_min_flow_volume: float = 200.0
        ):
        """

        """
        
        
        if shopping_sources_and_sinks is None:
            self.shopping_sources_and_sinks = None
            self.shopping_facilities = None
            #self.shopping_sources_and_sinks = ShoppingFacilitiesDistribution()
        else:
            self.shopping_sources_and_sinks = None
            self.shopping_sources_and_sinks = shopping_sources_and_sinks
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
            "shopping",
            transport_zones,
            modes,
            self.shopping_sources_and_sinks,
            parameters,
            ssi_min_flow_volume
        )
        
        
    def prepare_sources_and_sinks(self, transport_zones: gpd.GeoDataFrame):
        
        if self.shopping_sources_and_sinks is None:
        
            jobs, active_population = self.jobs_active_population.get()
            reference_flows = self.reference_flows.get()
            
        else:
            
            active_population = self.active_population
            jobs = self.jobs
            reference_flows = self.reference_flows
        
        sources = self.prepare_sources(transport_zones, active_population, reference_flows)
        sinks = self.prepare_sinks(transport_zones)
        
        return sources, sinks
    
    def prepare_utilities(self, transport_zones, sinks):
        utilities = WorkUtilities(transport_zones, sinks, self.inputs["parameters"].utility)
        return utilities

    
    def prepare_sources(
            self,
            transport_zones: gpd.GeoDataFrame,
            active_population: pd.DataFrame,
            reference_flows: pd.DataFrame
        ) -> pd.DataFrame:
        """
        Même code que work_destination_choice_model pour l'instant : on se base sur les domiciles
        """
        
        tz_lau_ids = set(transport_zones["local_admin_unit_id"].unique())
        
        # Check if all admin units ids are in the source dataset and print warning if some are not
        missing_ids = tz_lau_ids.difference(set(active_population.index.get_level_values(0)))
        
        if len(missing_ids) > 0:
            logging.info("No active population data available for the following admin units : " + ", ".join(missing_ids) + "")
            tz_lau_ids = list(tz_lau_ids.difference(missing_ids))
        else:
            tz_lau_ids = list(tz_lau_ids)

        # Filter the active population dataframe
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
            transport_zones_df
        ) -> pd.DataFrame:
        """
        à adapter en fonction des données sources


        """
        
        #Get all shops turnover in FR and CH, they are in EPSG:4326
        shops_turnover = ShopsTurnoverDistribution()
        all_shops = shops_turnover.get()
        all_shops = gpd.GeoDataFrame(all_shops, geometry=gpd.points_from_xy(all_shops["lon"],all_shops["lat"], crs="EPSG:4326"))
        #Convert them in EPSG:3035 (used by TransportZones)
        all_shops = all_shops.to_crs(epsg=3035)
        
        #Find which stops are in the transport zone
        all_shops = transport_zones_df.sjoin(all_shops, how="left")
        all_shops = all_shops.groupby("transport_zone_id").sum("turnover")
        all_shops = all_shops["turnover"]
        print(all_shops)
        all_shops = all_shops.to_frame().rename(columns={"turnover": "sink_volume"}, index={"transport_zone_id": "to"})
        
        print(all_shops)
        
        return all_shops
    
    
    def compute_flows(
            self,
            transport_zones,
            sources,
            sinks,
            costs,
            utilities
        ):
        
            if self.parameters.model["type"] == "radiation_selection":
                # NOT TESTED
                flows, source_rest_volume, sink_rest_volume = radiation_model(sources, sinks, costs, self.parameters.model["alpha"], self.parameters.model["beta"])
                return flows
        
            if self.parameters.model["type"] == "radiation_selection":
                # NOT TESTED
                selection_lambda = self.parameters.model["lambda"]
                flows = apply_radiation_model(sources, sinks, costs, utilities, selection_lambda)
                return flows
    
    
    def prepare_reference_flows(self, transport_zones: gpd.GeoDataFrame):
        
        # Pas de flux de référence connus
        pass
        """admin_ids = transport_zones["local_admin_unit_id"].values
        
        if self.active_population is None:
            ref_flows = self.reference_flows.get()
        else:
            ref_flows = self.reference_flows
            
        ref_flows = ref_flows[(ref_flows["local_admin_unit_id_from"].isin(admin_ids)) & (ref_flows["local_admin_unit_id_to"].isin(admin_ids))].copy()
        ref_flows.rename({"flow_volume": "ref_flow_volume"}, axis=1, inplace=True)
        
        return ref_flows"""
   
    
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
        
        output_path = pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"]) / "shopping-flows.svg"
        
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
