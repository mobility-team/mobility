import json
import os
import pathlib

import pandas as pd

from importlib import resources

from mobility.runtime.assets.file_asset import FileAsset
from mobility.runtime.r_integration.r_script_runner import RScriptRunner


class DetailedCarpoolTravelCostsSnapshot(FileAsset):
    """Per-congestion-state carpool travel costs derived from a road snapshot."""

    def __init__(
        self,
        *,
        car_travel_costs,
        parameters,
        modal_transfer,
        road_flow_asset,
    ):
        inputs = {
            "car_travel_costs": car_travel_costs,
            "parameters": parameters,
            "modal_transfer": modal_transfer,
            "road_flow_asset": road_flow_asset,
            "schema_version": 1,
        }

        cache_path = pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"]) / "travel_costs_congested_carpool.parquet"
        super().__init__(inputs, cache_path)

    def get_cached_asset(self) -> pd.DataFrame:
        return pd.read_parquet(self.cache_path)

    def create_and_get_asset(self) -> pd.DataFrame:
        car_travel_costs = self.inputs["car_travel_costs"]
        graph = car_travel_costs.get_congested_graph_path(self.inputs["road_flow_asset"])

        script = RScriptRunner(
            resources.files("mobility.transport.modes.carpool.detailed").joinpath(
                "compute_carpool_travel_costs.R"
            )
        )
        script.run(
            args=[
                str(car_travel_costs.transport_zones.cache_path),
                str(car_travel_costs.transport_zones.study_area.cache_path["polygons"]),
                str(graph),
                str(graph),
                json.dumps(self.inputs["modal_transfer"].model_dump(mode="json")),
                str(self.cache_path),
            ]
        )

        return pd.read_parquet(self.cache_path)
