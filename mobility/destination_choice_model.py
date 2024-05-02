import os
import pathlib
import logging
import pandas as pd
import geopandas as gpd
import numpy as np

from abc import abstractmethod

from mobility.asset import Asset
from mobility import radiation_model, TravelCosts

class DestinationChoiceModel(Asset):
    
    def __init__(
            self, motive: str, transport_zones: gpd.GeoDataFrame, 
            travel_costs: TravelCosts, cost_of_time: float = 20.0,
            radiation_model_alpha: float = 0.0, radiation_model_beta: float = 1.0
        ):
        
        inputs = {
            "motive": motive,
            "transport_zones": transport_zones,
            "travel_costs": travel_costs,
            "cost_of_time": cost_of_time,
            "radiation_model_alpha": radiation_model_alpha,
            "radiation_model_beta": radiation_model_beta
        }
        
        filename = motive + "_destination_choice_model.parquet"
        cache_path = pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"]) / filename
        
        super().__init__(inputs, cache_path)
        
    
    def get_cached_asset(self) -> pd.DataFrame:

        logging.info("Destination choice model already prepared. Reusing the file : " + str(self.cache_path))
        choice_model = pd.read_parquet(self.cache_path)

        return choice_model
    
    
    def create_and_get_asset(self) -> pd.DataFrame:
        
        logging.info("Creating destination choice model...")
        
        transport_zones = self.inputs["transport_zones"].get()
        travel_costs = self.inputs["travel_costs"].get()
        
        sources, sinks = self.prepare_sources_and_sinks(transport_zones)
        
        average_cost_by_od = self.compute_average_cost_by_od(travel_costs)
        
        flows, _, _ = radiation_model.iter_radiation_model(
            sources=sources,
            sinks=sinks,
            costs=average_cost_by_od,
            alpha=self.inputs["radiation_model_alpha"],
            beta=self.inputs["radiation_model_beta"]
        )
        
        choice_model = flows/flows.groupby("from").sum()
        choice_model.name = "prob"
        choice_model = choice_model.reset_index()
        
        choice_model.to_parquet(self.cache_path)

        return choice_model
    
    
    @abstractmethod
    def prepare_sources_and_sinks(self):
        pass
    
        
    def compute_average_cost_by_od(self, costs):
          
        costs = costs.copy()
        
        costs.set_index(["from", "to", "mode"], inplace=True)
        
        # Basic utility function : U = ct*time
        # Cost of time (ct) : 20 €/h by default
        costs["utility"] = -self.inputs["cost_of_time"]*costs["time"]
        
        costs["prob"] = np.exp(costs["utility"])
        costs["prob"] = costs["prob"]/costs.groupby(["from", "to"])["prob"].sum()
        
        costs = costs[["utility", "prob"]].reset_index()
        
        costs["cost"] = -costs["prob"]*costs["utility"]
        
        costs = costs.groupby(["from", "to"])["cost"].sum()
        costs = costs.reset_index()
        costs.columns = ["from", "to", "cost"]
        
        return costs
        
        
