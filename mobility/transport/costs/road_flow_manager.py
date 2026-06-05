from __future__ import annotations

import logging

from mobility.transport.costs.od_flows_asset import VehicleODFlowsAsset


class RoadFlowManager:
    """Build road vehicle flows used by congestion assignment."""

    def __init__(self, transport_costs):
        self.transport_costs = transport_costs

    def build(self, person_od_flows_by_mode) -> VehicleODFlowsAsset | None:
        """Build and cache road vehicle flows from current person OD flows."""
        logging.debug("Building road vehicle flows from person OD flows...")
        road_flow_parameters = self.road_flow_parameters()
        if not road_flow_parameters:
            return None

        road_flow_asset = VehicleODFlowsAsset(
            person_od_flows_by_mode=person_od_flows_by_mode,
            road_flow_parameters=road_flow_parameters,
        )
        road_flow_asset.get()
        return road_flow_asset

    def _iter_congestion_enabled_modes(self):
        """Yield modes that have congestion enabled."""
        return (
            mode for mode in self.transport_costs.modes
            if mode.inputs["parameters"].congestion
        )

    def road_flow_parameters(self) -> list[dict[str, float | str]]:
        """Return road-mode conversion settings for congestion assignment."""
        road_parameters = []
        for mode in self._iter_congestion_enabled_modes():
            mode_parameters = mode.inputs["parameters"]
            mode_name = mode_parameters.name
            if mode_name == "car":
                road_parameters.append(
                    {"mode_name": "car", "vehicles_per_person": 1.0}
                )
            elif mode_name == "carpool":
                road_parameters.append(
                    {
                        "mode_name": "carpool",
                        "vehicles_per_person": 1.0 / float(mode_parameters.persons_per_vehicle),
                    }
                )
        return sorted(road_parameters, key=lambda parameters: str(parameters["mode_name"]))
