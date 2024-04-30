import os
import pathlib
import logging
import pandas as pd
import numpy as np

from mobility.asset import Asset

from mobility.multimodal_travel_costs import MultimodalTravelCosts

class TransportModeChoiceModel(Asset):
    
    def __init__(self, travel_costs: MultimodalTravelCosts, cost_of_time: float = 20.0):
        
        inputs = {
            "travel_costs": travel_costs,
            "cost_of_time": cost_of_time
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
        
        costs = self.inputs["travel_costs"].get()
        cost_of_time = self.inputs["cost_of_time"]
        
        prob = self.compute_mode_probability_by_od(costs, cost_of_time)
        prob.to_parquet(self.cache_path)

        return prob
    
    
    def compute_mode_probability_by_od(self, costs, cost_of_time):
          
        prob = costs.copy()
        
        prob.set_index(["from", "to", "mode"], inplace=True)
        
        # Basic utility function : U = ct*time
        # Cost of time (ct) : 20 €/h by default
        prob["utility"] = -self.inputs["cost_of_time"]*prob["time"]
        
        prob["prob"] = np.exp(prob["utility"])
        prob["prob"] = prob["prob"]/prob.groupby(["from", "to"])["prob"].sum()
        
        prob = prob[["utility", "prob"]].reset_index()
        
        return prob
        