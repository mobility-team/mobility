from __future__ import annotations

import logging

import polars as pl

from mobility.transport.costs.congestion_state import CongestionState
from mobility.transport.costs.od_flows_asset import VehicleODFlowsAsset


class CongestionStateManager:
    """Manage persisted congestion states for transport-cost iterations."""

    def __init__(self, transport_costs) -> None:
        """Initialize the manager for one transport-cost configuration.

        Args:
            transport_costs: Parent transport-cost asset whose modes define the
                congestion-enabled network and flow-building behavior.
        """
        self.transport_costs = transport_costs

    def build(self, od_flows_by_mode, *, run_key=None, is_weekday=None, iteration=None):
        """Build and persist a congestion state from current OD flows.

        Args:
            od_flows_by_mode: Per-mode OD flows aggregated from current plan steps.
            run_key: Unique identifier of the run owning the congestion state.
            is_weekday: Whether the run is the weekday variant.
            iteration: Simulation iteration that produced these flows.

        Returns:
            The persisted congestion state for the provided flows, or `None`
            when no congestion-enabled mode produced any persisted flow asset.
        """
        logging.info("Building congestion state from OD flows...")
        congestion_flows_by_mode = {
            mode.inputs["parameters"].name: mode.build_congestion_flows(od_flows_by_mode)
            for mode in self._iter_congestion_enabled_modes()
        }

        merged_road_flows = self._merge_congestion_flows(
            congestion_flows_by_mode.get("car"),
            congestion_flows_by_mode.get("carpool"),
        )

        flow_assets_by_mode = {}
        for mode in self._iter_congestion_enabled_modes():
            mode_name = mode.inputs["parameters"].name
            congestion_flows = (
                merged_road_flows
                if mode_name in {"car", "carpool"}
                else congestion_flows_by_mode.get(mode_name)
            )

            if congestion_flows is None:
                continue

            flow_asset = self._create_vehicle_flow_snapshot(
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

    def load(
        self,
        *,
        run_key,
        is_weekday,
        last_completed_iteration: int,
        cost_update_interval: int,
    ) -> CongestionState | None:
        """Load the congestion state active after the last completed iteration.

        Args:
            run_key: Unique identifier of the run owning the congestion state.
            is_weekday: Whether the run is the weekday variant.
            last_completed_iteration: Latest completed simulation iteration.
            cost_update_interval: Number of iterations between congestion updates.

        Returns:
            The latest persisted congestion state compatible with the completed
            run history, or `None` when no congestion state should exist yet.
        """
        if (
            not self.transport_costs.has_enabled_congestion()
            or cost_update_interval <= 0
            or last_completed_iteration <= 0
        ):
            return None

        latest_refresh_iteration = max(
            iteration
            for iteration in range(1, int(last_completed_iteration) + 1)
            if self.transport_costs.should_recompute_congested_costs(iteration, cost_update_interval)
        )

        flow_assets_by_mode = {}

        for mode in self._iter_congestion_enabled_modes():
            mode_name = mode.inputs["parameters"].name
            flow_asset = VehicleODFlowsAsset.from_inputs(
                run_key=str(run_key),
                is_weekday=bool(is_weekday),
                iteration=int(latest_refresh_iteration),
                mode_name=mode_name,
            )
            if flow_asset.cache_path.exists():
                flow_assets_by_mode[mode_name] = flow_asset

        if not flow_assets_by_mode:
            return None

        return CongestionState(
            run_key=str(run_key),
            is_weekday=bool(is_weekday),
            iteration=int(latest_refresh_iteration),
            flow_assets_by_mode=flow_assets_by_mode,
        )

    def _iter_congestion_enabled_modes(self):
        """Yield congestion-enabled modes in dependency-safe order."""
        return iter(
            sorted(
                (mode for mode in self.transport_costs.modes if mode.inputs["parameters"].congestion),
                key=lambda mode: (
                    mode.inputs["parameters"].name != "car",
                    mode.inputs["parameters"].name != "carpool",
                    mode.inputs["parameters"].name,
                ),
            )
        )

    def _merge_congestion_flows(self, *congestion_flows):
        """Merge multiple congestion-flow tables into one OD vehicle-flow table."""
        valid_flows = [flows for flows in congestion_flows if flows is not None]
        if not valid_flows:
            return None

        return (
            pl.concat(valid_flows)
            .group_by(["from", "to"])
            .agg(pl.col("vehicle_volume").sum())
            .select(["from", "to", "vehicle_volume"])
        )

    def _create_vehicle_flow_snapshot(
        self,
        congestion_flows,
        *,
        run_key=None,
        is_weekday=None,
        iteration=None,
        mode_name: str,
    ):
        """Persist one mode-specific OD vehicle-flow asset."""
        if run_key is None or is_weekday is None or iteration is None:
            return None

        flow_asset = VehicleODFlowsAsset(
            congestion_flows.to_pandas(),
            run_key=str(run_key),
            is_weekday=bool(is_weekday),
            iteration=int(iteration),
            mode_name=str(mode_name),
        )
        flow_asset.create_and_get_asset()
        return flow_asset
