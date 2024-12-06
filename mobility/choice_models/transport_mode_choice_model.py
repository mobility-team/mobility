import os
import pathlib
import logging
import pandas as pd
import numpy as np
import polars as pl

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
                
        costs = []
        for mode in self.inputs["destination_choice_model"].costs.modes:
            
            if mode.congestion:
                gc = mode.generalized_cost.get(congestion=True)
            else:
                gc = mode.generalized_cost.get()
                
            costs.append(
                pl.DataFrame(gc)
                .with_columns(pl.lit(mode.name).alias("mode"))
            )
            
        costs = pl.concat(costs)
        
        prob = self.compute_mode_probability_by_od(costs)
        prob.to_parquet(self.cache_path)

        return prob
    
    
    def compute_mode_probability_by_od(self, costs):
        
        prob = (
            costs
            .with_columns((pl.col("cost").neg().exp()).alias("prob"))
            .with_columns((pl.col("prob")/pl.col("prob").sum().over(["from", "to"])).alias("prob"))
            .filter(pl.col("prob") > 0.01)
            .with_columns((pl.col("prob")/pl.col("prob").sum().over(["from", "to"])).alias("prob"))
            .select(["from", "to", "mode", "prob"])
        )
        
        prob = prob.to_pandas()
        
        return prob
    
    
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
        