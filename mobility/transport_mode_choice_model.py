import os
import pathlib
import logging
import pandas as pd
import numpy as np

from mobility.asset import Asset

from mobility.destination_choice_model import DestinationChoiceModel

class TransportModeChoiceModel(Asset):
    
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
          
        prob = costs.copy()
        prob["prob"] = np.exp(prob["net_utility"])
        prob["prob"] = prob["prob"]/prob.groupby(["from", "to"])["prob"].sum()
        prob = prob.reset_index()
        
        return prob
        