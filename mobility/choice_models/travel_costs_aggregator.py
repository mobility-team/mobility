import os
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
        
        
    def get_costs_by_od_and_mode(
            self,
            metrics: List,
            congestion: bool,
            detail_distances: bool = False
        ):
        
        # Hack to match the current API and compute the GHG emissions from
        # detailed distances and GHG intensities in this method, but this 
        # should be done by the generalized_cost method of each mode.
        if "ghg_emissions" in metrics:
            original_detail_distances = detail_distances
            detail_distances = True
            compute_ghg_emissions = True
            metrics = [m for m in metrics if m != "ghg_emissions"]
        else:
            compute_ghg_emissions=False
        
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
        
        # Final step of the GHG emissions computation hack above
        if compute_ghg_emissions:
            
            # Build a mode -> GHG emissions polars formula dict
            # (uses the multimodal flag to handle the public transport mode,
            # this should be improved)
            pl_columns = {}
            dist_col_names = []
            
            for mode in modes:
                
                mode_name = "public_transport" if mode.multimodal else mode.name
                ghg_col_name = mode_name + "_ghg_emissions"
                dist_col_name = mode_name + "_distance"
                
                pl_columns[ghg_col_name] = ( 
                    pl.col(dist_col_name)*mode.ghg_intensity
                )
                
                dist_col_names.append(dist_col_name)
                
            
            # Compute the GHG emissions with the formulas and then sum them
            costs = (
                costs
                .with_columns(**pl_columns)
                .with_columns(
                    ghg_emissions_per_trip=pl.sum_horizontal(
                        list(pl_columns.keys())
                    )
                )
                .drop(list(pl_columns.keys()))
            )
            
            # Keep the detailed distances only if asked in the first place
            if original_detail_distances is False:
                costs = ( 
                    costs
                    .drop(dist_col_names) 
                )
                
        
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
        
        
    def update(self, od_flows_by_mode, run_key=None, iteration=None):
        
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
                
                flow_asset = None
                if run_key is not None and iteration is not None:
                    # Persist vehicle flows as a first-class asset so downstream congestion
                    # snapshots are isolated per run/iteration and safe for parallel runs.
                    from mobility.transport_costs.od_flows_asset import VehicleODFlowsAsset
                    if os.environ.get("MOBILITY_DEBUG_CONGESTION") == "1":
                        try:
                            n_rows = flows.height
                            vol_sum = float(flows["vehicle_volume"].sum()) if "vehicle_volume" in flows.columns else float("nan")
                        except Exception:
                            n_rows, vol_sum = None, None
                        logging.info(
                            "Congestion update input: run_key=%s iteration=%s mode=%s rows=%s vehicle_volume_sum=%s",
                            str(run_key),
                            str(iteration),
                            str(mode.name),
                            str(n_rows),
                            str(vol_sum),
                        )
                    flow_asset = VehicleODFlowsAsset(
                        flows.to_pandas(),
                        run_key=str(run_key),
                        iteration=int(iteration),
                        mode_name=str(mode.name)
                    )
                    flow_asset.get()
                    if os.environ.get("MOBILITY_DEBUG_CONGESTION") == "1":
                        logging.info(
                            "Flow asset ready: inputs_hash=%s path=%s",
                            flow_asset.inputs_hash,
                            str(flow_asset.cache_path),
                        )

                mode.travel_costs.update(flows, flow_asset=flow_asset)
            
        
