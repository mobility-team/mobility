from __future__ import annotations

import polars as pl
import logging

from typing import TYPE_CHECKING, List

from mobility.runtime.assets.in_memory_asset import InMemoryAsset
from mobility.transport.costs.od_flows_asset import VehicleODFlowsAsset

if TYPE_CHECKING:
    from mobility.trips.group_day_trips.transitions.congestion_state import CongestionState


class TransportCostsAggregator(InMemoryAsset):
    
    def __init__(self, modes):
        self.modes = modes
        inputs = {mode.inputs["parameters"].name: mode.inputs["generalized_cost"] for mode in modes}
        super().__init__(inputs)
        
        
    def get(
            self,
            metrics=["cost", "distance"],
            congestion: bool = False,
            congestion_state: CongestionState | None = None,
            aggregate_by_od: bool = True,
            detail_distances: bool = False
        ):
        
        logging.info("Aggregating costs...")
        
        if aggregate_by_od is True:
            costs = self.get_costs_by_od(metrics, congestion, congestion_state=congestion_state)
        else:
            costs = self.get_costs_by_od_and_mode(
                metrics,
                congestion,
                detail_distances,
                congestion_state=congestion_state,
            )
        
        return costs
    
    
    def get_costs_by_od(self, metrics: List, congestion: bool, congestion_state: CongestionState | None = None):
        
        costs = self.get_costs_by_od_and_mode(
            metrics,
            congestion,
            detail_distances=False,
            congestion_state=congestion_state,
        )
        
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
            detail_distances: bool = False,
            congestion_state: CongestionState | None = None,
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
        modes = sorted(self.modes, key=lambda mode: mode.inputs["parameters"].name != "car")
        
        for mode in modes:
            
            gc = pl.DataFrame(
                mode.inputs["generalized_cost"].get(
                    metrics,
                    congestion=congestion,
                    detail_distances=detail_distances,
                    congestion_state=congestion_state,
                )
            )
                
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
                
                mode_name = "public_transport" if mode.inputs["parameters"].multimodal else mode.inputs["parameters"].name
                ghg_col_name = mode_name + "_ghg_emissions"
                dist_col_name = mode_name + "_distance"
                
                pl_columns[ghg_col_name] = ( 
                    pl.col(dist_col_name) * mode.inputs["parameters"].ghg_intensity
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
    
    
    def get_prob_by_od_and_mode(
        self,
        metrics: List,
        congestion: bool,
        congestion_state: CongestionState | None = None,
    ):
        
        costs = self.get_costs_by_od_and_mode(
            metrics,
            congestion,
            detail_distances=False,
            congestion_state=congestion_state,
        )
        
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
        
        
    def iter_congestion_enabled_modes(self):
        """Yield congestion-enabled modes in dependency-safe order."""
        return iter(
            sorted(
                (mode for mode in self.modes if mode.inputs["parameters"].congestion),
                key=lambda mode: (
                    mode.inputs["parameters"].name != "car",
                    mode.inputs["parameters"].name != "carpool",
                    mode.inputs["parameters"].name,
                ),
            )
        )

    def merge_congestion_flows(self, *congestion_flows):
        """Merge multiple OD vehicle-flow contributions into one table.

        Args:
            *congestion_flows: Optional ``pl.DataFrame`` objects with
                ``["from", "to", "vehicle_volume"]``.

        Returns:
            pl.DataFrame | None: Merged OD vehicle flows, or ``None`` when no
            contribution is provided.
        """
        valid_flows = [flows for flows in congestion_flows if flows is not None]
        if not valid_flows:
            return None

        return (
            pl.concat(valid_flows)
            .group_by(["from", "to"])
            .agg(pl.col("vehicle_volume").sum())
            .select(["from", "to", "vehicle_volume"])
        )

    def create_vehicle_flow_snapshot(
        self,
        congestion_flows,
        *,
        run_key=None,
        is_weekday=None,
        iteration=None,
        mode_name: str,
    ):
        """Persist congestion flows as a period-scoped snapshot asset."""
        if run_key is None or is_weekday is None or iteration is None:
            return None

        flow_asset = VehicleODFlowsAsset(
            congestion_flows.to_pandas(),
            run_key=str(run_key),
            is_weekday=bool(is_weekday),
            iteration=int(iteration),
            mode_name=str(mode_name),
        )
        flow_asset.get()
        return flow_asset

    def build_congestion_state(self, od_flows_by_mode, *, run_key=None, is_weekday=None, iteration=None):
        """Build the explicit congestion state for the current iteration."""
        logging.info("Building congestion state from OD flows...")
        congestion_flows_by_mode = {
            mode.inputs["parameters"].name: mode.build_congestion_flows(od_flows_by_mode)
            for mode in self.iter_congestion_enabled_modes()
        }

        merged_road_flows = self.merge_congestion_flows(
            congestion_flows_by_mode.get("car"),
            congestion_flows_by_mode.get("carpool"),
        )

        flow_assets_by_mode = {}
        for mode in self.iter_congestion_enabled_modes():
            mode_name = mode.inputs["parameters"].name
            congestion_flows = (
                merged_road_flows
                if mode_name in {"car", "carpool"}
                else congestion_flows_by_mode.get(mode_name)
            )

            if congestion_flows is None:
                continue

            flow_asset = self.create_vehicle_flow_snapshot(
                congestion_flows,
                run_key=run_key,
                is_weekday=is_weekday,
                iteration=iteration,
                mode_name=mode_name,
            )
            if flow_asset is not None:
                flow_assets_by_mode[mode_name] = flow_asset

        if not flow_assets_by_mode or run_key is None or is_weekday is None or iteration is None:
            return None

        return CongestionState(
            run_key=str(run_key),
            is_weekday=bool(is_weekday),
            iteration=int(iteration),
            flow_assets_by_mode=flow_assets_by_mode,
        )

    def has_enabled_congestion(self) -> bool:
        """Return whether any mode has congestion feedback enabled.

        Returns:
            bool: True when at least one configured mode has congestion enabled.
        """
        return any(mode.inputs["parameters"].congestion for mode in self.modes)

    def should_recompute_congested_costs(self, iteration: int, update_interval: int) -> bool:
        """Return whether congested costs should be recomputed this iteration.

        Args:
            iteration (int): Current model iteration, using 1-based indexing.
            update_interval (int): Number of iterations between congestion
                recomputations. Zero disables congestion updates.

        Returns:
            bool: True when congestion updates are enabled and this iteration
            matches the configured recomputation schedule.
        """
        return update_interval > 0 and (iteration - 1) % update_interval == 0

    def get_costs_for_next_iteration(
        self,
        *,
        iteration: int,
        cost_update_interval: int,
        od_flows_by_mode,
        congestion_state: CongestionState | None = None,
        run_key=None,
        is_weekday=None,
    ):
        """Return the costs to use after processing the current iteration.

        When congestion is enabled and the update interval matches, this
        recomputes congested costs from the current iteration OD flows before
        returning the current congested view. Otherwise, it returns the current
        cost view unchanged.

        Args:
            iteration (int): Current model iteration, using 1-based indexing.
            cost_update_interval (int): Number of iterations between congestion
                recomputations. Zero disables congestion updates.
            od_flows_by_mode (pl.DataFrame): Aggregated OD flows with one row per
                ``["from", "to", "mode"]`` and a ``flow_volume`` column.
            run_key (str | None): Optional run identifier used to isolate
                per-run congestion snapshots.
            is_weekday (bool | None): Whether the current simulation pass is the
                weekday pass. Used to isolate weekday/weekend flow snapshots.

        Returns:
            tuple[pl.DataFrame, CongestionState | None]: The OD costs to use
                after processing the current iteration, and the explicit
                congestion state that produced them.
        """
        if self.should_recompute_congested_costs(iteration, cost_update_interval):
            congestion_state = self.build_congestion_state(
                od_flows_by_mode,
                run_key=run_key,
                is_weekday=is_weekday,
                iteration=iteration,
            )
        return (
            self.get(
                congestion=(congestion_state is not None),
                congestion_state=congestion_state,
            ),
            congestion_state,
        )
