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
from mobility import radiation_model, TravelCosts

class DestinationChoiceModel(Asset):
    
    def __init__(
            self, motive: str, transport_zones: gpd.GeoDataFrame, 
            travel_costs: TravelCosts, cost_of_time: float,
            radiation_model_alpha: float, radiation_model_beta: float,
            fit_radiation_model: bool, ssi_min_flow_volume: float
        ):
        
        inputs = {
            "motive": motive,
            "transport_zones": transport_zones,
            "travel_costs": travel_costs,
            "cost_of_time": cost_of_time,
            "radiation_model_alpha": radiation_model_alpha,
            "radiation_model_beta": radiation_model_beta,
            "fit_radiation_model": fit_radiation_model,
            "ssi_min_flow_volume": ssi_min_flow_volume
        }
        
        data_folder = pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"])
        od_flows_filename = motive + "_od_flows.parquet"
        dest_cm_filename = motive + "_destination_choice_model.parquet"
        
        cache_path = {
            "od_flows": data_folder / od_flows_filename,
            "destination_choice_model": data_folder / dest_cm_filename
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
        
        if self.inputs["fit_radiation_model"] is True:
            
            logging.info("Fitting the radiation model to reference OD flows...")
                
            cost_of_time, alpha, beta = self.find_optimal_parameters(transport_zones, sources, sinks, travel_costs, ref_flows)
            
        else:
            
            cost_of_time = self.inputs["cost_of_time"]
            alpha = self.inputs["radiation_model_alpha"]
            beta = self.inputs["radiation_model_beta"]
            
        flows = self.compute_flows(transport_zones, sources, sinks, travel_costs, cost_of_time, alpha, beta)
        flows = self.add_reference_flows(flows, ref_flows)
        flows.to_parquet(self.cache_path["od_flows"])
        
        choice_model = flows[["from", "to", "flow_volume"]].set_index(["from", "to"])["flow_volume"]
        choice_model = choice_model/choice_model.groupby("from").sum()
        choice_model.name = "prob"
        choice_model = choice_model.reset_index()
        
        choice_model.to_parquet(self.cache_path["destination_choice_model"])
        
        return choice_model
    
    
    @abstractmethod
    def prepare_reference_flows(self):
        pass
        
    
    @abstractmethod
    def prepare_sources_and_sinks(self):
        pass
        
    
    def find_optimal_parameters(
            self, transport_zones: gpd.GeoDataFrame, sources: pd.DataFrame, sinks: pd.DataFrame,
            travel_costs: pd.DataFrame, ref_flows: pd.DataFrame
        ):
        
        logging.info("Optimizing cost_of_time, alpha and beta parameters...")
        
        x0 = [0.5, 0.5, 0.5]
        res = minimize(
            self.neg_ssi,
            x0,
            args=(transport_zones, sources, sinks, travel_costs, ref_flows),
            method="Nelder-Mead",
            bounds=((0.0, 1.0), (0.0, 1.0), (0.0, 1.0)),
            options={"maxiter": 20, "fatol": 1e-3}
        )
        
        cost_of_time, alpha, beta = res.x
        cost_of_time *= 40
        
        ssi = round((1.0 - res.fun)*1000)/10
        
        logging.info(f"Optimal parameters : cost_of_time={cost_of_time}, alpha={alpha}, beta={beta}.")
        logging.info(f"Final SSI value : {ssi}.")
        
        return cost_of_time, alpha, beta
    
    def neg_ssi(self, x, transport_zones, sources, sinks, travel_costs, ref_flows):
        
        cost_of_time, alpha, beta = x
        cost_of_time *= 40
        
        flows = self.compute_flows(
            transport_zones=transport_zones,
            sources=sources,
            sinks=sinks,
            travel_costs=travel_costs,
            cost_of_time=cost_of_time,
            alpha=alpha,
            beta=beta
        )
    
        flows = self.add_reference_flows(flows, ref_flows)
        nssi = 1.0 - self.compute_ssi(flows, self.inputs["ssi_min_flow_volume"])
        
        # Log optimization progress
        cost_of_time = round(cost_of_time*1000)/1000
        alpha = round(alpha*1000)/1000
        beta = round(beta*1000)/1000
        ssi = round((1.0 - nssi)*10000)/100
        
        logging.info(f"cost_of_time={cost_of_time} - alpha={alpha} - beta={beta} - SSI={ssi}")

        return nssi
    
    def compute_flows(
            self, transport_zones, sources: pd.DataFrame, sinks: pd.DataFrame,
            travel_costs: pd.DataFrame, cost_of_time: float, alpha: float, beta: float
        ):
        
        average_cost_by_od = self.compute_average_cost_by_od(travel_costs, cost_of_time)
        
        flows, _, _ = radiation_model.iter_radiation_model(
            sources=sources,
            sinks=sinks,
            costs=average_cost_by_od,
            alpha=alpha,
            beta=beta
        )
        
        flows = flows.to_frame().reset_index()
        flows = pd.merge(flows, transport_zones[["transport_zone_id", "admin_id"]], left_on="from", right_on="transport_zone_id")
        flows = pd.merge(flows, transport_zones[["transport_zone_id", "admin_id"]], left_on="to", right_on="transport_zone_id", suffixes=["_from", "_to"])
        
        flows = flows[["from", "to", "admin_id_from", "admin_id_to", "flow_volume"]]
        
        return flows
    
    
    def add_reference_flows(self, flows, ref_flows):
        
        comparison = pd.merge(
            ref_flows,
            flows,
            on=["admin_id_from", "admin_id_to"],
            how = "outer"
        )
        
        comparison["ref_flow_volume"] = comparison["ref_flow_volume"].fillna(0.0)
        comparison["flow_volume"] = comparison["flow_volume"].fillna(0.0)
        
        return comparison
    
    
    def compute_ssi(self, comparison, ssi_min_flow_volume):
        
        comparison = comparison[comparison["ref_flow_volume"] > ssi_min_flow_volume]
        
        num = 2*np.minimum(comparison["ref_flow_volume"], comparison["flow_volume"])
        den = comparison["ref_flow_volume"] + comparison["flow_volume"]
        ssi = np.sum(num/den)/num.shape[0]
        
        return ssi
    
        
    def compute_average_cost_by_od(self, costs: pd.DataFrame, cost_of_time: float):
          
        costs = costs.copy()
        
        costs.set_index(["from", "to", "mode"], inplace=True)
        
        # Basic utility function : U = ct*time
        # Cost of time (ct) : 20 €/h by default
        costs["utility"] = -cost_of_time*costs["time"]
        
        costs["prob"] = np.exp(costs["utility"])
        costs["prob"] = costs["prob"]/costs.groupby(["from", "to"])["prob"].sum()
        
        costs = costs[["utility", "prob"]].reset_index()
        
        costs["cost"] = -costs["prob"]*costs["utility"]
        
        costs = costs.groupby(["from", "to"])["cost"].sum()
        costs = costs.reset_index()
        costs.columns = ["from", "to", "cost"]
        
        return costs
    
    
    def plot_model_fit(self):
        
        flows = pd.read_parquet(self.cache_path["od_flows"])
        flows["log_ref_flow_volume"] = np.log(flows["ref_flow_volume"])
        flows["log_flow_volume"] = np.log(flows["flow_volume"])
        
        sns.set_theme()
        sns.scatterplot(data=flows, x="log_ref_flow_volume", y="log_flow_volume", size=5, linewidth=0, alpha=0.5)
        
        
