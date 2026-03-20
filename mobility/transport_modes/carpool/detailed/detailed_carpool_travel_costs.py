import pathlib
import os
import logging
import json
import pandas as pd
import shutil
from typing import Annotated

from importlib import resources
from pydantic import BaseModel, ConfigDict, Field

from mobility.choice_models.congestion_state import CongestionState
from mobility.file_asset import FileAsset
from mobility.r_utils.r_script import RScript
from mobility.transport_modes.modal_transfer import IntermodalTransfer
from mobility.transport_costs.path_travel_costs import PathTravelCosts
from mobility.transport_modes.carpool.detailed.detailed_carpool_travel_costs_snapshot import (
    DetailedCarpoolTravelCostsSnapshot,
)

class DetailedCarpoolTravelCosts(FileAsset):

    def __init__(
            self,
            car_travel_costs: PathTravelCosts,
            parameters: "DetailedCarpoolRoutingParameters",
            modal_transfer: IntermodalTransfer,
        ):

        inputs = {
            "car_travel_costs": car_travel_costs,
            "parameters": parameters,
            "modal_transfer": modal_transfer
        }
        
        cache_path = {
            "freeflow": pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"]) / ("travel_costs_free_flow_carpool.parquet"),
            "congested": pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"]) / ("travel_costs_congested_carpool.parquet")
        }


        super().__init__(inputs, cache_path)

    def get(self, congestion: bool = False, congestion_state: CongestionState | None = None) -> pd.DataFrame:
        requested_congestion = congestion and congestion_state is None
        self.update_ancestors_if_needed()

        if self.is_update_needed():
            asset = self.create_and_get_asset(congestion=requested_congestion)
            self.update_hash(self.inputs_hash)
            if congestion_state is None:
                return asset

        if congestion and congestion_state is not None:
            snapshot = self.get_snapshot_asset(congestion_state)
            if snapshot is not None:
                return snapshot.get()

        return self.get_cached_asset(congestion=congestion)

    def get_cached_asset(self, congestion: bool = False) -> pd.DataFrame:

        if congestion is False:
            path = self.cache_path["freeflow"]
        else:
            path = self.cache_path["congested"]

        logging.info("Travel costs already prepared. Reusing the file : " + str(path))
        costs = pd.read_parquet(path)

        return costs

    def create_and_get_asset(self, congestion: bool = False) -> pd.DataFrame:
        
        logging.info("Preparing carpool travel costs for occupants...")
        
        if congestion is False:
            output_path = self.cache_path["freeflow"]
        else:
            output_path = self.cache_path["congested"]
        
        costs = self.compute_travel_costs(
            self.inputs["car_travel_costs"],
            self.inputs["parameters"],
            self.inputs["modal_transfer"],
            congestion,
            output_path
        )
        
        if congestion is False:
            shutil.copy(self.cache_path["freeflow"], self.cache_path["congested"])

        return costs

    def compute_travel_costs(
            self,
            car_travel_costs: PathTravelCosts,
            params: "DetailedCarpoolRoutingParameters",
            modal_transfer: IntermodalTransfer,
            congestion: bool,
            output_path: pathlib.Path
        ) -> pd.DataFrame:
        
        script = RScript(resources.files('mobility.transport_modes.carpool.detailed').joinpath('compute_carpool_travel_costs.R'))
        
        if congestion is True:
            graph = car_travel_costs.get_congested_graph_path()
        else:
            graph = car_travel_costs.modified_path_graph.get()
        
        script.run(
            args=[
                str(car_travel_costs.transport_zones.cache_path),
                str(car_travel_costs.transport_zones.study_area.cache_path["polygons"]),
                str(graph),
                str(graph),
                json.dumps(modal_transfer.model_dump(mode="json")),
                output_path
            ]
        )

        costs = pd.read_parquet(output_path)
        
        return costs
    
    
    def get_snapshot_asset(
        self,
        congestion_state: CongestionState,
    ) -> DetailedCarpoolTravelCostsSnapshot | None:
        flow_asset = congestion_state.for_mode("carpool")
        if flow_asset is None:
            return None

        return DetailedCarpoolTravelCostsSnapshot(
            car_travel_costs=self.inputs["car_travel_costs"],
            parameters=self.inputs["parameters"],
            modal_transfer=self.inputs["modal_transfer"],
            road_flow_asset=flow_asset,
        )


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
