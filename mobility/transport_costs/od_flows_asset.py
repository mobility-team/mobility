import os
import pathlib
import pandas as pd

from mobility.file_asset import FileAsset


class VehicleODFlowsAsset(FileAsset):
    """Persist vehicle OD flows for congestion as a first-class FileAsset.

    This intentionally stores only what the congestion builder needs:
    ["from","to","vehicle_volume"].

    The cache key is (run_key, iteration, mode_name), where run_key should be
    PopulationTrips.inputs_hash (includes the seed).
    """

    def __init__(self, vehicle_od_flows: pd.DataFrame, *, run_key: str, iteration: int, mode_name: str):
        inputs = {
            "run_key": str(run_key),
            "iteration": int(iteration),
            "mode_name": str(mode_name),
            "schema_version": 1
        }
        folder_path = pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"])
        cache_path = folder_path / "od_flows" / f"vehicle_od_flows_{mode_name}.parquet"

        self._vehicle_od_flows = vehicle_od_flows
        super().__init__(inputs, cache_path)

    def get_cached_asset(self) -> pd.DataFrame:
        return pd.read_parquet(self.cache_path)

    def create_and_get_asset(self) -> pd.DataFrame:
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)

        # Ensure the file always exists and has the expected schema, even if empty.
        df = self._vehicle_od_flows
        expected_cols = ["from", "to", "vehicle_volume"]
        df = df[expected_cols] if all(c in df.columns for c in expected_cols) else df
        df.to_parquet(self.cache_path, index=False)
        return df
