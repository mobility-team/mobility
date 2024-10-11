import os
import pathlib
import logging
import pandas as pd
import numpy as np

from mobility.file_asset import FileAsset

from mobility.choice_models.destination_choice_model import DestinationChoiceModel
from mobility.parsers import JobsActivePopulationFlows

class TransportModeChoiceModel(FileAsset):
    
    def __init__(self, destination_choice_model: DestinationChoiceModel):
        
        inputs = {
            "destination_choice_model": destination_choice_model
        }

        file_name = "modal_choice_model.parquet"
        cache_path = pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"]) / file_name

        super().__init__(inputs, cache_path)
        
        
    def get_cached_asset(self) -> pd.DataFrame:

        logging.info("Modal choice model already prepared. Reusing the file : " + str(self.cache_path))
        prob = pd.read_parquet(self.cache_path)

        return prob
    
    def create_and_get_asset(self) -> pd.DataFrame:
        
        logging.info("Computing mode probabilities by OD...")
        
        od_utility = pd.read_parquet(self.inputs["destination_choice_model"].cache_path["utility_by_od_and_mode"])
        prob = self.compute_mode_probability_by_od_and_mode(od_utility)
        prob.to_parquet(self.cache_path)

        return prob
    
    
    def compute_mode_probability_by_od_and_mode(self, costs):
          
        costs["prob"] = np.exp(costs["net_utility"])
        costs["prob"] = costs["prob"]/costs.groupby(["from", "to"])["prob"].sum()
        
        # Remove very small probabilities
        costs = costs[costs["prob"] > 0.01].copy()
        costs["prob"] = costs["prob"]/costs.groupby(["from", "to"])["prob"].sum()
        
        costs = costs.reset_index()
        costs = costs[["from", "to", "mode", "prob"]].copy()
        
        return costs
    
    
    def get_comparison_by_origin(self, flows):
        
        flows = flows.groupby(["local_admin_unit_id_from", "mode"], as_index=False)["flow_volume"].sum()
        
        lau_ids = flows["local_admin_unit_id_from"].unique()
        
        ref_flows = JobsActivePopulationFlows().get()
        ref_flows = ref_flows[ref_flows["local_admin_unit_id_from"].isin(lau_ids) & ref_flows["local_admin_unit_id_to"].isin(lau_ids)]
        ref_flows = ref_flows.groupby(["local_admin_unit_id_from", "mode"], as_index=False)["ref_flow_volume"].sum()

        od_pairs = pd.concat([
            ref_flows[["local_admin_unit_id_from", "mode"]],
            flows[["local_admin_unit_id_from", "mode"]]
        ]).drop_duplicates()
        
        
        # Remove all flows originating from switzerland as there is no reference data
        od_pairs = od_pairs[od_pairs["local_admin_unit_id_from"].str[0:2] != "ch"]
        
        comparison = pd.merge(
            od_pairs,
            flows,
            on=["local_admin_unit_id_from", "mode"],
            how="left"
        )
        
        comparison = pd.merge(
            comparison,
            ref_flows,
            on=["local_admin_unit_id_from", "mode"],
            how="left"
        )
    
        
        comparison.fillna(0.0, inplace=True)
        
        return comparison
        