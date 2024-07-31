import os
import pathlib
import logging
import pandas as pd
import geopandas as gpd
import numpy as np
import seaborn as sns

from scipy.optimize import minimize

from abc import abstractmethod

from mobility.asset import Asset
from mobility import radiation_model, radiation_model_selection, TravelCosts

class DestinationChoiceModel(Asset):
    
    def __init__(
            self,
            motive: str,
            transport_zones: gpd.GeoDataFrame, 
            travel_costs: TravelCosts,
            model_parameters: dict,
            utility_parameters: dict,
            ssi_min_flow_volume: float
        ):
        
        inputs = {
            "motive": motive,
            "transport_zones": transport_zones,
            "travel_costs": travel_costs,
            "model_parameters": model_parameters,
            "utility_parameters": utility_parameters,
            "ssi_min_flow_volume": ssi_min_flow_volume
        }
        
        data_folder = pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"])
        od_flows_filename = motive + "_od_flows.parquet"
        dest_cm_filename = motive + "_destination_choice_model.parquet"
        utility_by_od_and_mode_filename = motive + "_utility_by_od_and_mode.parquet"
        
        cache_path = {
            "od_flows": data_folder / od_flows_filename,
            "destination_choice_model": data_folder / dest_cm_filename,
            "utility_by_od_and_mode": data_folder / utility_by_od_and_mode_filename
        }
        
        super().__init__(inputs, cache_path)
        
    
    def get_cached_asset(self) -> pd.DataFrame:
        
        logging.info("Destination choice model already prepared. Reusing the file : " + str(self.cache_path))
        asset = pd.read_parquet(self.cache_path["destination_choice_model"])

        return asset
    
    
    def create_and_get_asset(self) -> pd.DataFrame:
        
        logging.info("Creating destination choice model...")
        
        transport_zones = self.inputs["transport_zones"].get()
        travel_costs = self.inputs["travel_costs"].get()
        
        sources, sinks = self.prepare_sources_and_sinks(transport_zones)
        ref_flows = self.prepare_reference_flows(transport_zones)
        
        model_parameters = self.inputs["model_parameters"]
        utility_parameters = self.inputs["utility_parameters"]
        
        travel_costs = travel_costs.set_index(["from", "to", "mode"])
        travel_costs = travel_costs[travel_costs["time"] < 2.0]
        
        utility_by_od_and_mode = self.compute_utility_by_od_and_mode(
            transport_zones,
            travel_costs,
            utility_parameters
        )
        
        utility_by_od = self.compute_utility_by_od(utility_by_od_and_mode)
            
        flows = self.compute_flows(
            transport_zones,
            sources,
            sinks,
            utility_by_od,
            model_parameters
        )
        
        flows = self.add_reference_flows(transport_zones, flows, ref_flows)
        
        
        choice_model = flows[["from", "to", "flow_volume"]].set_index(["from", "to"])["flow_volume"]
        choice_model = choice_model/choice_model.groupby("from").sum()
        choice_model.name = "prob"
        choice_model = choice_model.reset_index()
        
        flows.to_parquet(self.cache_path["od_flows"])
        choice_model.to_parquet(self.cache_path["destination_choice_model"])
        utility_by_od_and_mode.to_parquet(self.cache_path["utility_by_od_and_mode"])
        
        return choice_model
    
    
    @abstractmethod
    def prepare_reference_flows(self):
        pass
        
    
    @abstractmethod
    def prepare_sources_and_sinks(self):
        pass
    
    
    def compute_utility_by_od(
            self,
            utilities: pd.DataFrame,
        ):
        
        utilities = utilities.copy()
        
        # Shift the utility to avoid exp overflows 
        # max_net_utility = utilities["net_utility"].max()
        # utilities["net_utility"] -= max_net_utility
        
        utilities["prob"] = np.exp(utilities["net_utility"])
        utilities["prob"] = utilities["prob"]/utilities.groupby(["from", "to"])["prob"].transform("sum")
        
        utilities["cost"] = utilities["prob"]*utilities["cost"]
        utilities["net_utility"] = utilities["prob"]*utilities["net_utility"]
        
        utilities = utilities.groupby(["from", "to"]).agg({
            "cost": "sum",
            "utility": "first",
            "net_utility": "sum"
        })
        
        # utilities["net_utility"] += max_net_utility
        
        return utilities
        

    def compute_flows(
            self,
            transport_zones,
            sources: pd.DataFrame,
            sinks: pd.DataFrame,
            utility_by_od: pd.DataFrame,
            model_parameters: dict
        ):
        
        if model_parameters["type"] == "radiation_universal":
            flows, _, _ = radiation_model.iter_radiation_model(
                sources=sources,
                sinks=sinks,
                costs=utility_by_od,
                alpha=model_parameters["alpha"],
                beta=model_parameters["beta"]
            )
        else:
            flows, _, _ = radiation_model_selection.iter_radiation_model_selection(
                sources=sources,
                sinks=sinks,
                costs=utility_by_od,
                selection_lambda=model_parameters["lambda"]
            )
        
        flows = flows.to_frame().reset_index()
        
        flows = pd.merge(flows, transport_zones[["transport_zone_id", "local_admin_unit_id"]], left_on="from", right_on="transport_zone_id")
        flows = pd.merge(flows, transport_zones[["transport_zone_id", "local_admin_unit_id"]], left_on="to", right_on="transport_zone_id", suffixes=["_from", "_to"])
        
        flows = flows[["from", "to", "local_admin_unit_id_from", "local_admin_unit_id_to", "flow_volume"]]
        
        return flows
    
    
    def add_reference_flows(self, transport_zones, flows, ref_flows):
        
        od_pairs = pd.concat([
            ref_flows[["local_admin_unit_id_from", "local_admin_unit_id_to"]],
            flows[["local_admin_unit_id_from", "local_admin_unit_id_to"]]
        ]).drop_duplicates()
        
        od_pairs = pd.merge(
            od_pairs,
            transport_zones[["local_admin_unit_id", "transport_zone_id"]],
            left_on="local_admin_unit_id_from",
            right_on="local_admin_unit_id"
        )
        
        od_pairs = pd.merge(
            od_pairs,
            transport_zones[["local_admin_unit_id", "transport_zone_id"]],
            left_on="local_admin_unit_id_to",
            right_on="local_admin_unit_id"
        )
        
        od_pairs = od_pairs[["local_admin_unit_id_from", "local_admin_unit_id_to", "transport_zone_id_x", "transport_zone_id_y"]]
        od_pairs.columns = ["local_admin_unit_id_from", "local_admin_unit_id_to", "from", "to"]
        
        comparison = pd.merge(
            od_pairs,
            flows[["from", "to", "flow_volume"]],
            on=["from", "to"],
            how="left"
        )
        
        comparison = pd.merge(
            comparison,
            ref_flows,
            on=["local_admin_unit_id_from", "local_admin_unit_id_to"],
            how="left"
        )
        
        comparison.fillna(0.0, inplace=True)
        
        return comparison
    
    
    def compute_ssi(self, comparison, min_flow_volume):
        
        comparison = comparison[comparison["ref_flow_volume"] > min_flow_volume]
        
        num = 2*np.minimum(comparison["ref_flow_volume"], comparison["flow_volume"])
        den = comparison["ref_flow_volume"] + comparison["flow_volume"]
        ssi = np.sum(num/den)/num.shape[0]
        
        return ssi
    
    
    def compute_total_OD_distance_error(self, comparison, travel_costs, min_flow_volume):
        
        comparison = comparison[comparison["ref_flow_volume"] > min_flow_volume]
        
        travel_costs = travel_costs[travel_costs["mode"] == "car"]
        
        comparison = pd.merge(comparison, travel_costs, on=["from", "to"])
        
        comparison["mod_distance"] = comparison["flow_volume"]*comparison["distance"]
        comparison["ref_distance"] = comparison["ref_flow_volume"]*comparison["distance"]
        
        error = comparison["mod_distance"].sum()/comparison["ref_distance"].sum() - 1.0
        
        return error
    
    
    def plot_model_fit(self):
        
        flows = pd.read_parquet(self.cache_path["od_flows"])
        flows["log_ref_flow_volume"] = np.log(flows["ref_flow_volume"])
        flows["log_flow_volume"] = np.log(flows["flow_volume"])
        
        sns.set_theme()
        sns.scatterplot(data=flows, x="log_ref_flow_volume", y="log_flow_volume", size=5, linewidth=0, alpha=0.5)
        
        
