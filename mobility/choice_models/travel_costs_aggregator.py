import polars as pl
import logging

from typing import List
from mobility.in_memory_asset import InMemoryAsset

class TravelCostsAggregator(InMemoryAsset):
    
    def __init__(self, modes):
        self.modes = modes
        inputs = {mode.name: mode.generalized_cost for mode in modes}
        super().__init__(inputs)
        
        
    def get(
            self,
            metrics=["cost", "distance"],
            congestion: bool = False,
            aggregate_by_od: bool = True,
            detail_distances: bool = False
        ):
        
        logging.info("Aggregating costs...")
        
        if aggregate_by_od is True:
            costs = self.get_costs_by_od(metrics, congestion)
        else:
            costs = self.get_costs_by_od_and_mode(metrics, congestion, detail_distances)
        
        return costs
    
    
    def get_costs_by_od(self, metrics: List, congestion: bool):
        
        costs = self.get_costs_by_od_and_mode(metrics, congestion, detail_distances=False)
        
        costs = costs.with_columns([
            (pl.col("cost").neg().exp()).alias("prob")
        ])
        
        costs = costs.with_columns([
            (pl.col("prob") / pl.col("prob").sum().over(["from", "to"])).alias("prob")
        ])
        
        costs = costs.with_columns([
            (pl.col("prob") * pl.col("cost")).alias("cost")
        ])
        
        costs = costs.group_by(["from", "to"]).agg([
            pl.col("cost").sum()
        ])
        
        return costs
        
        
    def get_costs_by_od_and_mode(self, metrics: List, congestion: bool, detail_distances: bool = False):
        
        costs = []
        
        # Put the car first so that road congestion is computed first
        modes = sorted(self.modes, key=lambda mode: mode.name != "car")
        
        for mode in modes:
            
            if mode.congestion:
                gc = pl.DataFrame(mode.generalized_cost.get(metrics, congestion, detail_distances=detail_distances))
            else:
                gc = pl.DataFrame(mode.generalized_cost.get(metrics, detail_distances=detail_distances))
                
            costs.append(
                pl.DataFrame(gc)
            )
        
        costs = pl.concat(costs, how="diagonal")
        
        # Replace null distances by zeros
        if detail_distances is True:
            dist_cols = [col for col in costs.columns if "_distance" in col]
            dist_cols = {col: pl.col(col).fill_null(0.0) for col in dist_cols}
            costs = costs.with_columns(**dist_cols)

        costs = costs.with_columns([
            pl.col("from").cast(pl.Int32),
            pl.col("to").cast(pl.Int32)
        ])
        
        return costs
    
    
    def get_prob_by_od_and_mode(self, metrics: List, congestion: bool):
        
        costs = self.get_costs_by_od_and_mode(metrics, congestion, detail_distances=False)
        
        prob = (
            
            costs
            .with_columns(exp_u=pl.col("cost").neg().exp())
            .with_columns(prob=pl.col("exp_u")/pl.col("exp_u").sum().over(["from", "to"]))
            
            # Keep only the first 99.9 % of the distribution
            .sort(["prob"], descending=True)
            .with_columns(
                prob_cum=pl.col("prob").cum_sum().over(["from", "to"]),
                p_count=pl.col("prob").cum_count().over(["from", "to"])
            )
            .with_columns(
                prob_cum=pl.col("prob_cum").shift(1, fill_value=0.0).over(["from", "to"])
            )
            
            .filter((pl.col("prob_cum") < 0.999))
            .with_columns(prob=pl.col("prob")/pl.col("prob").sum().over(["from", "to"]))
            
            .select(["from", "to", "mode", "prob"])
        )
        
        return prob
        
        
    def update(self, od_flows_by_mode):
        
        logging.info("Updating travel costs given OD flows...")
        
        # prob_by_od_and_mode = self.get_prob_by_od_and_mode(["cost"], congestion=True)
        
        # od_flows_by_mode = (
        #     od_flows
        #     .join(prob_by_od_and_mode, on=["from", "to"])
        #     .with_columns((pl.col("flow_volume")*pl.col("prob")).alias("flow_volume"))
        #     .select(["from", "to", "mode", "flow_volume"])
        # )
        
        for mode in self.modes:
            
            if mode.congestion is True:
                
                if mode.name in ["car", "carpool"]:
                    
                    flows = (
                        od_flows_by_mode
                        .filter(pl.col("mode").is_in(["car", "carpool"]))
                        .with_columns((pl.when(pl.col("mode") == "car").then(1.0).otherwise(0.5)).alias("veh_per_pers"))
                        .with_columns((pl.col("flow_volume")*pl.col("veh_per_pers")).alias("vehicle_volume"))
                        .group_by(["from", "to"])
                        .agg(pl.col("vehicle_volume").sum())
                        .select(["from", "to", "vehicle_volume"])
                    )
                    
                elif mode.name == "car/public_transport/walk":
                    
                    logging.info(
                        """
                        Intermodal mode car/public_transport/walk has no flow 
                        volume to vehicle volume for now : no vehicle will be 
                        assigned to the road network and the congestion will
                        not account for this specific transport mode.
                        """
                    )
                    
                else:
                    
                    raise ValueError("No flow volume to vehicle volume model for mode : " + mode.name)
                
                mode.travel_costs.update(flows)
            
        