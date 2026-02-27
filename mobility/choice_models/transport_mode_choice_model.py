import os
import pathlib
import logging
import pandas as pd
import polars as pl

from mobility.file_asset import FileAsset
from mobility.parsers import JobsActivePopulationFlows
from mobility.choice_models.destination_choice_model import DestinationChoiceModel

class TransportModeChoiceModel(FileAsset):
    
    def __init__(self, destination_choice_model: DestinationChoiceModel, logit_factor=1):
        
        inputs = {
            "destination_choice_model": destination_choice_model,
            "logit_factor": logit_factor
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
            
            if mode.inputs["parameters"].congestion:
                print(f"Getting congested costs for mode {mode.inputs['parameters'].name}")
                gc = mode.inputs["generalized_cost"].get(congestion=True)
            else:
                print(f"Getting costs for mode {mode.inputs['parameters'].name}")
                gc = mode.inputs["generalized_cost"].get()
                
            costs.append(
                pl.DataFrame(gc)
                .with_columns(pl.lit(mode.inputs["parameters"].name).alias("mode"))
            )
            
        costs = pl.concat(costs)
        
        prob = (
            costs
            .with_columns(pl.col("cost").truediv(self.logit_factor).neg().exp().alias("exp_u"))
            .with_columns((pl.col("exp_u")/pl.col("exp_u").sum().over(["from", "to"])).alias("prob"))
            .select(["from", "to", "mode", "prob"])
        )
        
        prob.write_parquet(self.cache_path)
        
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
        
