from __future__ import annotations

import pathlib
import os
import logging
import json
import pandas as pd
from typing import Annotated

from importlib import resources
from pydantic import BaseModel, ConfigDict, Field

from mobility.transport.costs.travel_costs_asset import TravelCostsBase
from mobility.runtime.assets.file_asset import FileAsset
from mobility.runtime.assets.in_memory_asset import InMemoryAsset
from mobility.runtime.r_integration.r_script_runner import RScriptRunner
from mobility.transport.costs.od_flows_asset import VehicleODFlowsAsset
from mobility.transport.modes.core.modal_transfer import IntermodalTransfer
from mobility.transport.costs.path.path_travel_costs import PathTravelCosts


class DetailedCarpoolTravelCostsTable(TravelCostsBase, FileAsset):
    """Single detailed carpool travel-cost table."""

    def __init__(
        self,
        *,
        car_travel_costs: PathTravelCosts,
        parameters: "DetailedCarpoolRoutingParameters",
        modal_transfer: IntermodalTransfer,
        congestion: bool,
        road_flow_asset: VehicleODFlowsAsset | None = None,
    ) -> None:
        self.car_travel_costs = car_travel_costs
        self.parameters = parameters
        self.modal_transfer = modal_transfer
        self.congestion = bool(congestion)
        self.road_flow_asset = road_flow_asset
        inputs = {
            "version": 1,
            "car_travel_costs": car_travel_costs,
            "parameters": parameters,
            "modal_transfer": modal_transfer,
            "congestion": bool(congestion),
            "road_flow_asset": road_flow_asset,
        }
        cost_kind = "congested" if congestion else "free_flow"
        cache_path = (
            pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"])
            / f"travel_costs_{cost_kind}_carpool.parquet"
        )
        super().__init__(inputs, cache_path)

    def get_cached_asset(self) -> pd.DataFrame:
        """Return the cached detailed carpool OD costs."""
        logging.debug("Travel costs already prepared. Reusing the file : %s", str(self.cache_path))
        return pd.read_parquet(self.cache_path)

    def create_and_get_asset(self) -> pd.DataFrame:
        """Compute this detailed carpool OD cost table."""
        logging.info("Preparing carpool travel costs for occupants...")
        return self._compute_travel_costs()

    def _compute_travel_costs(self) -> pd.DataFrame:
        """Compute detailed carpool OD travel costs."""
        script = RScriptRunner(
            resources.files('mobility.transport.modes.carpool.detailed').joinpath(
                'compute_carpool_travel_costs.R'
            )
        )

        if self.congestion:
            graph = self.car_travel_costs.get_congested_graph_path(self.road_flow_asset)
        else:
            graph = self.car_travel_costs.modified_path_graph.get()

        script.run(
            args=[
                str(self.car_travel_costs.transport_zones.cache_path),
                str(self.car_travel_costs.transport_zones.study_area.cache_path["polygons"]),
                str(graph),
                str(graph),
                json.dumps(self.modal_transfer.model_dump(mode="json")),
                self.cache_path,
            ]
        )
        return pd.read_parquet(self.cache_path)


class DetailedCarpoolTravelCosts(TravelCostsBase, InMemoryAsset):

    def __init__(
            self,
            car_travel_costs: PathTravelCosts,
            parameters: "DetailedCarpoolRoutingParameters",
            modal_transfer: IntermodalTransfer,
            road_flow_asset: VehicleODFlowsAsset | None = None,
        ):

        self.car_travel_costs = car_travel_costs
        self.parameters = parameters
        self.modal_transfer = modal_transfer
        self.road_flow_asset = road_flow_asset
        self.freeflow_costs = DetailedCarpoolTravelCostsTable(
            car_travel_costs=car_travel_costs,
            parameters=parameters,
            modal_transfer=modal_transfer,
            congestion=False,
            road_flow_asset=None,
        )
        self.congested_costs = DetailedCarpoolTravelCostsTable(
            car_travel_costs=car_travel_costs,
            parameters=parameters,
            modal_transfer=modal_transfer,
            congestion=True,
            road_flow_asset=road_flow_asset,
        )
        self.default_congestion = road_flow_asset is not None
        inputs = {
            "car_travel_costs": car_travel_costs,
            "parameters": parameters,
            "modal_transfer": modal_transfer,
            "road_flow_asset": road_flow_asset,
            "default_congestion": self.default_congestion,
        }
        super().__init__(inputs)

    def get(self, congestion: bool = False, road_flow_asset: VehicleODFlowsAsset | None = None) -> pd.DataFrame:
        if congestion and road_flow_asset is not None:
            asset = self.asset_for_road_flows(road_flow_asset)
            if asset is not None:
                return asset.get()

        # Without an explicit road-flow asset, the congested and free-flow
        # carpool costs are the same logical state.
        if self.default_congestion:
            return self.congested_costs.get()
        return self.freeflow_costs.get()
    
    
    def asset_for_road_flows(
        self,
        road_flow_asset: VehicleODFlowsAsset | None,
    ):
        if road_flow_asset is None:
            return None

        return self.asset_for_flow_asset(road_flow_asset)

    def asset_for_flow_asset(self, flow_asset: VehicleODFlowsAsset):
        return DetailedCarpoolTravelCosts(
            car_travel_costs=self.inputs["car_travel_costs"],
            parameters=self.inputs["parameters"],
            modal_transfer=self.inputs["modal_transfer"],
            road_flow_asset=flow_asset,
        )

    def remove(self) -> None:
        """Remove detailed carpool travel-cost tables owned by this selector."""
        self.freeflow_costs.remove()
        self.congested_costs.remove()

    def remove_congestion_artifacts(self, road_flow_asset: VehicleODFlowsAsset) -> None:
        """Remove only the carpool costs tied to one road-flow asset."""
        variant = self.asset_for_road_flows(road_flow_asset)
        if variant is self or variant is None:
            return
        variant.congested_costs.remove()


class DetailedCarpoolRoutingParameters(BaseModel):
    """
    Attributes:
        absolute_delay_per_passenger (int): absolute delay per supplementary passenger, in minutes. Default: 5
        relative_delay_per_passenger (float): relative delay per supplementary passenger in proportion of total travel time. Default: 0.05
        absolute_extra_distance_per_passenger (float): absolute extra distance per supplementary passenger, in km. Default: 1
        relative_extra_distance_per_passenger (float): relative extra distance per supplementary passenger in proportion of total distance. Default: 0.05
    """

    model_config = ConfigDict(extra="forbid")

    parking_locations: Annotated[list[tuple[float, float]], Field(default_factory=list)]
    absolute_delay_per_passenger: Annotated[int, Field(default=5, ge=0)]
    relative_delay_per_passenger: Annotated[float, Field(default=0.05, ge=0.0)]
    absolute_extra_distance_per_passenger: Annotated[float, Field(default=1.0, ge=0.0)]
    relative_extra_distance_per_passenger: Annotated[float, Field(default=0.05, ge=0.0)]
