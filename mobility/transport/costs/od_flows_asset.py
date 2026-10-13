import pathlib
import pandas as pd
import logging
from typing import Any

import polars as pl
from mobility.runtime.assets.asset import Asset
from mobility.runtime.assets.in_memory_asset import InMemoryAsset
from mobility.runtime.assets.resolver import get_current_asset_resolver


class VehicleODFlowsAsset(InMemoryAsset):
    """Build transient road vehicle OD flows for congestion assignment.

    This intentionally stores only what the congestion builder needs:
    ["from","to","vehicle_volume"].

    The cache key follows the normal asset DAG: it depends on the upstream
    person-flow asset and on the road-mode conversion settings.
    """

    def __init__(
        self,
        *,
        person_od_flows_by_mode: Asset,
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
        self.person_od_flows_by_mode = person_od_flows_by_mode
        self.road_flow_parameters = inputs["road_flow_parameters"]
        super().__init__(inputs)

    def create_and_get_asset(self) -> pathlib.Path:
        """Build flows and place the R input table in this run's temp folder."""
        resolver = get_current_asset_resolver()
        if resolver is None:
            raise RuntimeError("Vehicle OD flows need an active asset resolution context.")
        self.cache_path = resolver.temporary_asset_path(self, suffix=".parquet")
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
        logging.debug("Transient road vehicle OD flows are ready.")

        return self.cache_path
