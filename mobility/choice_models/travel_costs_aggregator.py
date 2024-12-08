import polars as pl
import logging

from mobility.in_memory_asset import InMemoryAsset

class TravelCostsAggregator(InMemoryAsset):
    
    def __init__(self, modes):
        self.modes = modes
        inputs = {mode.name: mode.generalized_cost for mode in modes}
        super().__init__(inputs)
        
        
    def get(self, congestion: bool = False):
        
        logging.info("Aggregating costs...")
        
        costs = []
        
        # Put the car first so that road congestion is computed first
        modes = sorted(self.modes, key=lambda mode: mode.name != "car")
        
        for mode in modes:
            if mode.congestion:
                costs.append(pl.DataFrame(mode.generalized_cost.get(congestion)))
            else:
                costs.append(pl.DataFrame(mode.generalized_cost.get()))
        
        costs = pl.concat(costs)
        
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
        
        costs = costs.with_columns([
            pl.col("from").cast(pl.Int64),
            pl.col("to").cast(pl.Int64)
        ])
        
        return costs
        
        
    def update(self, od_flows):
        
        logging.info("Updating travel costs given OD flows...")
        
        for mode in self.modes:
            if mode.congestion is True:
                mode_od_flows = od_flows
                mode.travel_costs.update(mode_od_flows)
            
        