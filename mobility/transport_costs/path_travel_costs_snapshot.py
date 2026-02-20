import os
import pathlib
import logging
import pandas as pd

from importlib import resources

from mobility.file_asset import FileAsset
from mobility.r_utils.r_script import RScript
from mobility.transport_zones import TransportZones
from mobility.path_routing_parameters import PathRoutingParameters
from mobility.transport_graphs.contracted_path_graph_snapshot import ContractedPathGraphSnapshot


class PathTravelCostsSnapshot(FileAsset):
    """A per-run/iteration travel-cost snapshot based on a contracted graph snapshot."""

    def __init__(
        self,
        *,
        mode_name: str,
        transport_zones: TransportZones,
        routing_parameters: PathRoutingParameters,
        contracted_graph: ContractedPathGraphSnapshot,
    ):
        inputs = {
            "mode_name": str(mode_name),
            "transport_zones": transport_zones,
            "routing_parameters": routing_parameters,
            "contracted_graph": contracted_graph,
            "schema_version": 1,
        }

        folder_path = pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"])
        cache_path = folder_path / f"travel_costs_congested_{mode_name}.parquet"
        super().__init__(inputs, cache_path)

    def get_cached_asset(self) -> pd.DataFrame:
        logging.info("Congested travel costs snapshot already prepared. Reusing: " + str(self.cache_path))
        return pd.read_parquet(self.cache_path)

    def create_and_get_asset(self) -> pd.DataFrame:
        logging.info("Computing congested travel costs snapshot...")

        transport_zones: TransportZones = self.inputs["transport_zones"]
        contracted_graph: ContractedPathGraphSnapshot = self.inputs["contracted_graph"]
        routing_parameters: PathRoutingParameters = self.inputs["routing_parameters"]

        transport_zones.get()
        contracted_graph.get()

        script = RScript(resources.files('mobility.r_utils').joinpath('prepare_dodgr_costs.R'))
        script.run(
            args=[
                str(transport_zones.cache_path),
                str(contracted_graph.cache_path),
                str(routing_parameters.filter_max_speed),
                str(routing_parameters.filter_max_time),
                str(self.cache_path),
            ]
        )

        return pd.read_parquet(self.cache_path)

