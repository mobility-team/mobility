import pathlib
import os
import logging
import json
import pandas as pd
import numpy as np

from importlib import resources

from dataclasses import asdict

from mobility.file_asset import FileAsset
from mobility.r_utils.r_script import RScript
from mobility.transport_modes.carpool.detailed.detailed_carpool_routing_parameters import DetailedCarpoolRoutingParameters
from mobility.transport_modes.car import CarMode
from mobility.transport_modes.modal_shift import ModalShift
from mobility.path_travel_costs import PathTravelCosts

class DetailedCarpoolTravelCosts(FileAsset):

    def __init__(
            self,
            mode_name: str,
            car_travel_costs: PathTravelCosts,
            parameters: DetailedCarpoolRoutingParameters,
            modal_shift: ModalShift,
        ):

        inputs = {
            "mode_name": mode_name,
            "car_travel_costs": car_travel_costs,
            "parameters": parameters,
            "modal_shift": modal_shift
        }

        file_name = mode_name + "_travel_costs.parquet"
        cache_path = pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"]) / file_name

        super().__init__(inputs, cache_path)

    def get_cached_asset(self, congestion: bool = False) -> pd.DataFrame:

        logging.info("Travel costs already prepared. Reusing the file : " + str(self.cache_path))
        costs = pd.read_parquet(self.cache_path)

        return costs

    def create_and_get_asset(self, congestion: bool = False) -> pd.DataFrame:
        
        logging.info("Preparing carpool travel costs for occupants...")
        
        costs = self.compute_travel_costs(
            self.car_travel_costs,
            self.parameters,
            self.modal_shift,
            congestion
        )
        costs.to_parquet(self.cache_path)

        return costs

    def compute_travel_costs(
            self,
            car_travel_costs: PathTravelCosts,
            params: DetailedCarpoolRoutingParameters,
            modal_shift: ModalShift,
            congestion: bool
        ) -> pd.DataFrame:
        
        script = RScript(resources.files('mobility.transport_modes.carpool.detailed').joinpath('compute_carpool_travel_costs.R'))
        
        script.run(
            args=[
                str(car_travel_costs.transport_zones.cache_path),
                str(car_travel_costs.simplified_path_graph.get()),
                str(car_travel_costs.simplified_path_graph.get()),
                json.dumps(asdict(modal_shift)),
                str(congestion),
                str(self.cache_path)
            ]
        )

        costs = pd.read_parquet(self.cache_path)
        
        return costs
    
    
    def update(self, od_flows):
        
        self.create_and_get_asset(congestion=True)