import os
import pathlib
import pandas as pd
import logging
from typing import Any

import polars as pl
from mobility.runtime.assets.file_asset import FileAsset


class VehicleODFlowsAsset(FileAsset):
    """Persist road vehicle OD flows for congestion assignment.

    This intentionally stores only what the congestion builder needs:
    ["from","to","vehicle_volume"].

    The cache key follows the normal asset DAG: it depends on the upstream
    person-flow asset and on the road-mode conversion settings.
    """

    def __init__(
        self,
        *,
        person_od_flows_by_mode: FileAsset,
        road_flow_parameters: list[dict[str, Any]],
    ):
        inputs = {
            "schema_version": 2,
            "person_od_flows_by_mode": person_od_flows_by_mode,
            "road_flow_parameters": [
                {
                    "mode_name": str(parameters["mode_name"]),
                    "vehicles_per_person": float(parameters["vehicles_per_person"]),
                }
                for parameters in sorted(
                    road_flow_parameters,
                    key=lambda value: str(value["mode_name"]),
                )
            ],
        }
        folder_path = pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"])
        cache_path = folder_path / "od_flows" / "vehicle_od_flows_road.parquet"

        self.person_od_flows_by_mode = person_od_flows_by_mode
        self.road_flow_parameters = inputs["road_flow_parameters"]
        super().__init__(inputs, cache_path)

    def get_cached_asset(self) -> pd.DataFrame:
        return pd.read_parquet(self.cache_path)

    def create_and_get_asset(self) -> pd.DataFrame:
        person_flows = self.person_od_flows_by_mode.get()
        if person_flows is None:
            person_flows = pl.DataFrame(
                schema={
                    "from": pl.Int32,
                    "to": pl.Int32,
                    "mode": pl.String,
                    "flow_volume": pl.Float64,
                }
            )

        self.cache_path.parent.mkdir(parents=True, exist_ok=True)

        vehicle_flows = []
        for parameters in self.road_flow_parameters:
            mode_name = parameters["mode_name"]
            vehicles_per_person = parameters["vehicles_per_person"]
            vehicle_flows.append(
                person_flows
                .filter(pl.col("mode") == mode_name)
                .with_columns(
                    (pl.col("flow_volume") * vehicles_per_person).alias("vehicle_volume")
                )
                .select(["from", "to", "vehicle_volume"])
            )

        if vehicle_flows:
            df = (
                pl.concat(vehicle_flows, how="vertical")
                .group_by(["from", "to"])
                .agg(pl.col("vehicle_volume").sum())
                .select(["from", "to", "vehicle_volume"])
                .to_pandas()
            )
        else:
            df = pd.DataFrame(columns=["from", "to", "vehicle_volume"])

        df.to_parquet(self.cache_path, index=False)
        logging.debug("Road vehicle OD flows are ready: %s", str(self.cache_path))

        return df
