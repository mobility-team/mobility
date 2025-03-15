import pathlib
import os
import logging
import json
import pandas as pd
import shutil

from importlib import resources

from dataclasses import asdict

from mobility.file_asset import FileAsset
from mobility.r_utils.r_script import RScript
from mobility.transport_modes.carpool.detailed.detailed_carpool_routing_parameters import DetailedCarpoolRoutingParameters
from mobility.transport_modes.modal_transfer import IntermodalTransfer
from mobility.transport_costs.path_travel_costs import PathTravelCosts

class DetailedCarpoolTravelCosts(FileAsset):

    def __init__(
            self,
            car_travel_costs: PathTravelCosts,
            parameters: DetailedCarpoolRoutingParameters,
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
            self.car_travel_costs,
            self.parameters,
            self.modal_transfer,
            congestion,
            output_path
        )
        
        if congestion is False:
            shutil.copy(self.cache_path["freeflow"], self.cache_path["congested"])

        return costs

    def compute_travel_costs(
            self,
            car_travel_costs: PathTravelCosts,
            params: DetailedCarpoolRoutingParameters,
            modal_transfer: IntermodalTransfer,
            congestion: bool,
            output_path: pathlib.Path
        ) -> pd.DataFrame:
        
        script = RScript(resources.files('mobility.transport_modes.carpool.detailed').joinpath('compute_carpool_travel_costs.R'))
        
        script.run(
            args=[
                str(car_travel_costs.transport_zones.cache_path),
                str(car_travel_costs.transport_zones.study_area.cache_path["polygons"]),
                str(car_travel_costs.simplified_path_graph.get()),
                str(car_travel_costs.simplified_path_graph.get()),
                json.dumps(asdict(modal_transfer)),
                str(congestion),
                output_path
            ]
        )

        costs = pd.read_parquet(output_path)
        
        return costs
    
    
    def update(self, od_flows):
        
        self.create_and_get_asset(congestion=True)