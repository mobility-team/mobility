import os
import pathlib
import logging
import shutil
import shortuuid
import pandas as pd
import geopandas as gpd

from importlib import resources
from mobility.transport_graphs.path_graph import PathGraph
from mobility.file_asset import FileAsset
from mobility.r_utils.r_script import RScript
from mobility.transport_zones import TransportZones
from mobility.path_routing_parameters import PathRoutingParameters
from mobility.transport_modes.osm_capacity_parameters import OSMCapacityParameters
from mobility.transport_graphs.speed_modifier import SpeedModifier
from mobility.transport_graphs.congested_path_graph_snapshot import CongestedPathGraphSnapshot
from mobility.transport_graphs.contracted_path_graph_snapshot import ContractedPathGraphSnapshot
from mobility.transport_costs.path_travel_costs_snapshot import PathTravelCostsSnapshot

from typing import List

class PathTravelCosts(FileAsset):
    """
    A class for managing travel cost calculations for certain modes using OpenStreetMap (OSM) data, inheriting from the Asset class.

    This class is responsible for creating, caching, and retrieving travel costs for modes car, walk, and bicycle,
    based on specified transport zones and travel modes.

    Attributes:
        dodgr_modes (dict): Mapping of general travel modes to specific dodgr package modes.
        transport_zones (gpd.GeoDataFrame): The geographical areas for which travel costs are calculated.
        mode (str): The mode of transportation used for calculating travel costs.
        gtfs (GTFS): GTFS object containing data about public transport routes and schedules.

    Methods:
        get_cached_asset: Retrieve a cached DataFrame of travel costs.
        create_and_get_asset: Calculate and retrieve travel costs based on the current inputs.
        dodgr_graph: Create a routable graph for the specified mode of transportation.
        dodgr_costs: Calculate travel costs using the generated graph.
    """

    def __init__(
            self,
            mode_name: str,
            transport_zones: TransportZones,
            routing_parameters: PathRoutingParameters,
            osm_capacity_parameters: OSMCapacityParameters,
            congestion: bool = False,
            congestion_flows_scaling_factor: float = 1.0,
            speed_modifiers: List[SpeedModifier] = [],
        ):
        """
        Initializes a TravelCosts object with the given transport zones and travel mode.

        Args:
            transport_zones (gpd.GeoDataFrame): GeoDataFrame defining the transport zones.
            mode (str): Mode of transportation for calculating travel costs.
        """

        path_graph = PathGraph(
            mode_name,
            transport_zones,
            osm_capacity_parameters,
            congestion,
            congestion_flows_scaling_factor,
            speed_modifiers
        )
        
        inputs = {
            "transport_zones": transport_zones,
            "mode_name": mode_name,
            "simplified_path_graph": path_graph.simplified,
            "modified_path_graph": path_graph.modified,
            "congested_path_graph": path_graph.congested,
            "contracted_path_graph": path_graph.contracted,
            "routing_parameters": routing_parameters
        }

        cache_path = {
            "freeflow": pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"]) / ("travel_costs_free_flow_" + mode_name + ".parquet"),
            "congested": pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"]) / ("travel_costs_congested_" + mode_name + ".parquet")
        }

        super().__init__(inputs, cache_path)

        # When congestion updates are used, we keep a pointer to the latest
        # per-iteration snapshot so `get(congestion=True)` is isolated per run.
        self._current_congested_snapshot = None

    def get_cached_asset(self, congestion: bool = False) -> pd.DataFrame:
        """
        Retrieves the travel costs DataFrame from the cache.

        Returns:
            pd.DataFrame: The cached DataFrame of travel costs.
        """
        
        if congestion is False:
            path = self.cache_path["freeflow"]
        else:
            if self._current_congested_snapshot is not None:
                if os.environ.get("MOBILITY_DEBUG_CONGESTION") == "1":
                    logging.info(
                        "PathTravelCosts.get(congestion=True) using snapshot: snapshot_hash=%s snapshot_path=%s",
                        self._current_congested_snapshot.inputs_hash,
                        str(self._current_congested_snapshot.cache_path),
                    )
                return self._current_congested_snapshot.get()
            # If no congestion snapshot has been applied in this run, treat
            # "congested" as free-flow to avoid reusing stale shared caches.
            if os.environ.get("MOBILITY_DEBUG_CONGESTION") == "1":
                logging.info(
                    "PathTravelCosts.get(congestion=True) no snapshot -> fallback to freeflow: %s",
                    str(self.cache_path["freeflow"]),
                )
            path = self.cache_path["freeflow"]

        logging.info("Travel costs already prepared. Reusing the file : " + str(path))
        costs = pd.read_parquet(path)

        return costs

    def create_and_get_asset(self, congestion: bool = False) -> pd.DataFrame:
        """
        Creates and retrieves travel costs based on the current inputs.

        Returns:
            pd.DataFrame: A DataFrame of calculated travel costs.
        """
        
        mode = self.inputs["mode_name"]
        
        logging.info("Preparing travel costs for mode " + mode)
        
        self.inputs["transport_zones"].get()
        self.inputs["contracted_path_graph"].get()
        
        if congestion is False:
            output_path = self.cache_path["freeflow"]
        else:
            if self._current_congested_snapshot is not None:
                return self._current_congested_snapshot.get()
            # Same rationale as get_cached_asset(): without an applied snapshot,
            # compute free-flow costs.
            output_path = self.cache_path["freeflow"]
        
        costs = self.compute_costs_by_OD(
            self.inputs["transport_zones"],
            self.inputs["contracted_path_graph"],
            output_path,
        )
        
        if congestion is False:
            shutil.copy(self.cache_path["freeflow"], self.cache_path["congested"])

        return costs



    def compute_costs_by_OD(
            self,
            transport_zones: TransportZones,
            path_graph: PathGraph,
            output_path: pathlib.Path
        ) -> pd.DataFrame:
        """
        Calculates travel costs for the specified mode of transportation using the created graph.

        Args:
            transport_zones (gpd.GeoDataFrame): GeoDataFrame containing transport zone geometries.
            graph (str): Path to the routable graph file.

        Returns:
            pd.DataFrame: A DataFrame containing calculated travel costs.
        """

        logging.info("Computing travel times and distances by OD...")
        
        script = RScript(resources.files('mobility.r_utils').joinpath('prepare_dodgr_costs.R'))
        script.run(
            args=[
                str(transport_zones.cache_path),
                str(path_graph.cache_path),
                str(self.inputs["routing_parameters"].filter_max_speed),
                str(self.inputs["routing_parameters"].filter_max_time),
                str(output_path)
            ]
        )
        
        costs = pd.read_parquet(output_path)

        return costs
    
    
    def update(self, od_flows, flow_asset=None):
        """
            Update congestion state.
        """

        if flow_asset is None:
            if os.environ.get("MOBILITY_DEBUG_CONGESTION") == "1":
                logging.info(
                    "PathTravelCosts.update legacy(shared) path: mode=%s",
                    str(self.inputs["mode_name"]),
                )
            self.inputs["contracted_path_graph"].update(od_flows)
            self._current_congested_snapshot = None
            self.create_and_get_asset(congestion=True)
            return

        self._apply_flow_snapshot(flow_asset)

    def apply_flow_snapshot(self, flow_asset) -> None:
        """Repoint this mode's congested costs to the snapshot defined by `flow_asset`.

        This is primarily used when resuming a run from a checkpoint: the snapshot
        files exist on disk, but the in-memory pointer to the "current snapshot"
        is lost on restart.
        """
        self._apply_flow_snapshot(flow_asset)

    def _apply_flow_snapshot(self, flow_asset) -> None:
        congested_graph = CongestedPathGraphSnapshot(
            modified_graph=self.inputs["modified_path_graph"],
            transport_zones=self.inputs["transport_zones"],
            vehicle_flows=flow_asset,
            congestion_flows_scaling_factor=self.inputs["congested_path_graph"].congestion_flows_scaling_factor,
        )
        contracted_graph = ContractedPathGraphSnapshot(congested_graph)

        snapshot = PathTravelCostsSnapshot(
            mode_name=self.inputs["mode_name"],
            transport_zones=self.inputs["transport_zones"],
            routing_parameters=self.inputs["routing_parameters"],
            contracted_graph=contracted_graph,
        )

        self._current_congested_snapshot = snapshot
        if os.environ.get("MOBILITY_DEBUG_CONGESTION") == "1":
            logging.info(
                "PathTravelCosts snapshot selected: mode=%s flow_hash=%s snapshot_hash=%s snapshot_path=%s",
                str(self.inputs["mode_name"]),
                flow_asset.get_cached_hash(),
                snapshot.inputs_hash,
                str(snapshot.cache_path),
            )
        snapshot.get()
        
    def clone(self):
        
        ptc = PathTravelCosts(
            self.inputs["mode_name"],
            self.inputs["transport_zones"],
            self.inputs["routing_parameters"],
            self.inputs["simplified_path_graph"].inputs["osm_capacity_parameters"],
            self.inputs["contracted_path_graph"].handles_congestion,
        )

        ptc.cache_path = {
            "freeflow": pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"]) / (self.inputs_hash + "-travel_costs_free_flow_" + self.inputs["mode_name"] + ".parquet"),
            "congested": pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"]) / (self.inputs_hash + "-travel_costs_congested_" + self.inputs["mode_name"] + "_clone_" + shortuuid.uuid() + ".parquet")
        }
    
        return ptc
    
