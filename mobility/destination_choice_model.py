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
            cost_of_time: float,
            cost_of_distance: float,
            additional_utility: dict,
            radiation_model_type: str,
            radiation_model_alpha: float,
            radiation_model_beta: float,
            radiation_model_lambda: float,
            fit_radiation_model: bool, ssi_min_flow_volume: float
        ):
        
        inputs = {
            "motive": motive,
            "transport_zones": transport_zones,
            "travel_costs": travel_costs,
            "cost_of_time": cost_of_time,
            "cost_of_distance": cost_of_distance,
            "additional_utility": additional_utility,
            "radiation_model_type": radiation_model_type,
            "radiation_model_alpha": radiation_model_alpha,
            "radiation_model_beta": radiation_model_beta,
            "radiation_model_lambda": radiation_model_lambda,
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
        
        radiation_model_type = self.inputs["radiation_model_type"]
        additional_utility = self.inputs["additional_utility"]
        
        if self.inputs["fit_radiation_model"] is True:
            
            logging.info("Fitting the radiation model to reference OD flows...")
                
            cost_of_time, cost_of_distance, alpha, beta, selection_lambda = self.find_optimal_parameters(transport_zones, sources, sinks, travel_costs, ref_flows, radiation_model_type)
            
        else:
            
            cost_of_time = self.inputs["cost_of_time"]
            cost_of_distance = self.inputs["cost_of_distance"]
            alpha = self.inputs["radiation_model_alpha"]
            beta = self.inputs["radiation_model_beta"]
            selection_lambda = self.inputs["radiation_model_lambda"]
            
        flows = self.compute_flows(
            transport_zones,
            sources,
            sinks,
            travel_costs,
            cost_of_time,
            cost_of_distance,
            additional_utility,
            radiation_model_type,
            alpha,
            beta,
            selection_lambda
        )
        
        flows = self.add_reference_flows(transport_zones, flows, ref_flows)
        
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
            travel_costs: pd.DataFrame, ref_flows: pd.DataFrame, radiation_model_type: str
        ):
        
        if radiation_model_type == "universal":
            
            logging.info("Optimizing cost_of_time, alpha and beta parameters...")
            
            x0 = [0.5, 0.1, 0.2, 0.8]
            res = minimize(
                self.neg_ssi,
                x0,
                args=(transport_zones, sources, sinks, travel_costs, ref_flows, radiation_model_type),
                method="Nelder-Mead",
                bounds=((0.0, 1.0), (0.0, 0.1), (0.0, 1.0), (0.0, 1.0)),
                options={"maxiter": 20, "fatol": 1e-3}
            )
            
            cost_of_time, cost_of_distance, alpha, beta = res.x
            cost_of_time *= 40
            
        elif radiation_model_type == "selection":
            
            logging.info("Optimizing cost_of_time and lambda parameters...")
            
            x0 = [0.5, 0.1, 0.9999]
            res = minimize(
                self.neg_ssi,
                x0,
                args=(transport_zones, sources, sinks, travel_costs, ref_flows, radiation_model_type),
                method="Nelder-Mead",
                bounds=((0.0, 1.0), (0.0, 0.1), (0.0, 1.0)),
                options={"maxiter": 20, "fatol": 1e-3}
            )
            
            cost_of_time, cost_of_distance, alpha, beta = res.x
            cost_of_time *= 40
            
        else:
            
            raise ValueError("Radiation model type " + radiation_model_type + " is not available (should be 'universal' or 'selection').")
            
        
        ssi = round((1.0 - res.fun)*1000)/10
        
        logging.info(f"Optimal parameters : cost_of_time={cost_of_time}, cost_of_distance={cost_of_distance}, alpha={alpha}, beta={beta}.")
        logging.info(f"Final SSI value : {ssi}.")
        
        return cost_of_time, alpha, beta
    
    def neg_ssi(self, x, transport_zones, sources, sinks, travel_costs, ref_flows, radiation_model_type):
        
        if radiation_model_type == "universal":
            cost_of_time, cost_of_distance, alpha, beta = x
            selection_lambda = None
        else:
            cost_of_time, cost_of_distance, selection_lambda = x
            alpha = beta = None
        
        cost_of_time *= 40
        
        flows = self.compute_flows(
            transport_zones=transport_zones,
            sources=sources,
            sinks=sinks,
            travel_costs=travel_costs,
            cost_of_time=cost_of_time,
            cost_of_distance=cost_of_distance,
            additional_utility=self.inputs["additional_utility"],
            radiation_model_type=radiation_model_type,
            alpha=alpha,
            beta=beta,
            selection_lambda=selection_lambda
        )
    
        flows = self.add_reference_flows(transport_zones, flows, ref_flows)
        nssi = 1.0 - self.compute_ssi(flows, self.inputs["ssi_min_flow_volume"])
        
        # Log optimization progress
        cost_of_time = round(cost_of_time*1000)/1000
        cost_of_distance = round(cost_of_distance*1000)/1000
        alpha = round(alpha*1000)/1000
        beta = round(beta*1000)/1000
        ssi = round((1.0 - nssi)*10000)/100
        
        logging.info(f"cost_of_time={cost_of_time} - cost_of_distance={cost_of_distance} - alpha={alpha} - beta={beta} - SSI={ssi}")

        return nssi
    
    def compute_flows(
            self,
            transport_zones,
            sources: pd.DataFrame,
            sinks: pd.DataFrame,
            travel_costs: pd.DataFrame,
            cost_of_time: float,
            cost_of_distance: float,
            additional_utility: dict,
            radiation_model_type: str,
            alpha: float,
            beta: float,
            selection_lambda: float
        ):
        
        travel_costs = travel_costs.set_index(["from", "to", "mode"])
        travel_costs = travel_costs[travel_costs["time"] < 2.0]
        
        utility_by_od = self.compute_utility_by_od(
            transport_zones,
            travel_costs,
            cost_of_time,
            cost_of_distance,
            additional_utility
        )
        
        if radiation_model_type == "universal":
            flows, _, _ = radiation_model.iter_radiation_model(
                sources=sources,
                sinks=sinks,
                costs=utility_by_od,
                alpha=alpha,
                beta=beta
            )
        else:
            flows, _, _ = radiation_model_selection.iter_radiation_model_selection(
                sources=sources,
                sinks=sinks,
                costs=utility_by_od,
                selection_lambda=selection_lambda
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
    
        
    def compute_utility_by_od(
            self,
            transport_zones: gpd.GeoDataFrame,
            travel_costs: pd.DataFrame,
            cost_of_time: float,
            cost_of_distance: float,
            additional_utility: dict
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
        
        
        # Utility function : U = -ct*time*2 + -cd*distance*2 + u_crossborder
        
        # Cost of time (ct) : distance dependant, from https://www.ecologie.gouv.fr/sites/default/files/documents/V.2.pdf
        # 
        ct = 18.6
        ct = np.where(travel_costs["distance"] > 20, 14.4 + 0.215*travel_costs["distance"], ct)
        ct = np.where(travel_costs["distance"] > 80, 30.2 + 0.017*travel_costs["distance"], ct)
        ct = np.where(travel_costs["distance"] > 400, 37.0, ct)
        ct *= 1.17
        
        if "ch" in [k[0:2] for k in additional_utility.keys()]:
            ct_ch = ct*2.0
            ct = np.where(travel_costs["crossborder_flow"].str.contains("ch-"), ct_ch, ct)
        
        travel_costs["cost"] = ct*travel_costs["time"]*2
        travel_costs["cost"] += cost_of_distance*travel_costs["distance"]*2
        
        travel_costs["utility"] = travel_costs["crossborder_flow"].map(additional_utility)
        
        travel_costs["net_utility"] = travel_costs["utility"] - travel_costs["cost"]
        
        # Shift the utility to avoid exp overflows 
        max_net_utility = travel_costs["net_utility"].max()
        travel_costs["net_utility"] -= max_net_utility
        
        travel_costs["prob"] = np.exp(travel_costs["net_utility"])
        travel_costs["prob"] = travel_costs["prob"]/travel_costs.groupby(["from", "to"])["prob"].transform("sum")
        
        travel_costs["cost"] = travel_costs["prob"]*travel_costs["cost"]
        travel_costs["net_utility"] = travel_costs["prob"]*travel_costs["net_utility"]
        
        travel_costs = travel_costs.groupby(["from", "to"]).agg({
            "cost": "sum",
            "utility": "first",
            "net_utility": "sum"
        })
        
        travel_costs["net_utility"] += max_net_utility
        
        return travel_costs
    
    
    
    def plot_model_fit(self):
        
        flows = pd.read_parquet(self.cache_path["od_flows"])
        flows["log_ref_flow_volume"] = np.log(flows["ref_flow_volume"])
        flows["log_flow_volume"] = np.log(flows["flow_volume"])
        
        sns.set_theme()
        sns.scatterplot(data=flows, x="log_ref_flow_volume", y="log_flow_volume", size=5, linewidth=0, alpha=0.5)
        
        
