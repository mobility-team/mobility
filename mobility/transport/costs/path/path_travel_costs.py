from __future__ import annotations

import os
import pathlib
import logging
import shutil
import pandas as pd
import geopandas as gpd

from importlib import resources
from mobility.transport.graphs.core.path_graph import PathGraph
from mobility.runtime.assets.file_asset import FileAsset
from mobility.runtime.r_integration.r_script_runner import RScriptRunner
from mobility.spatial.transport_zones import TransportZones
from mobility.transport.costs.parameters.path_routing_parameters import PathRoutingParameters
from mobility.transport.modes.core.osm_capacity_parameters import OSMCapacityParameters
from mobility.transport.graphs.modified.modifiers.speed_modifier import SpeedModifier
from mobility.transport.graphs.congested.congested_path_graph import CongestedPathGraph
from mobility.transport.graphs.contracted.contracted_path_graph import ContractedPathGraph
from mobility.transport.costs.congestion_state import CongestionState

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
            contracted_graph: ContractedPathGraph | None = None,
        ):
        """
        Initializes a TravelCosts object with the given transport zones and travel mode.

        Args:
            transport_zones (gpd.GeoDataFrame): GeoDataFrame defining the transport zones.
            mode (str): Mode of transportation for calculating travel costs.
        """

        if contracted_graph is None:
            path_graph = PathGraph(
                mode_name,
                transport_zones,
                osm_capacity_parameters,
                congestion,
                congestion_flows_scaling_factor,
                speed_modifiers
            )
            simplified_path_graph = path_graph.simplified
            modified_path_graph = path_graph.modified
            congested_path_graph = path_graph.congested
            contracted_path_graph = path_graph.contracted
        else:
            path_graph = None
            contracted_path_graph = contracted_graph
            congested_path_graph = contracted_graph.inputs["congested_graph"]
            modified_path_graph = congested_path_graph.inputs["modified_graph"]
            simplified_path_graph = None
        
        inputs = {
            "transport_zones": transport_zones,
            "mode_name": mode_name,
            "simplified_path_graph": simplified_path_graph,
            "modified_path_graph": modified_path_graph,
            "congested_path_graph": congested_path_graph,
            "contracted_path_graph": contracted_path_graph,
            "routing_parameters": routing_parameters,
            "osm_capacity_parameters": osm_capacity_parameters,
        }

        cache_path = {
            "freeflow": pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"]) / ("travel_costs_free_flow_" + mode_name + ".parquet"),
            "congested": pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"]) / ("travel_costs_congested_" + mode_name + ".parquet")
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
            asset = self.asset_for_congestion_state(congestion_state)
            if asset is not None:
                return asset.get()

        return self.get_cached_asset(congestion=congestion)

    def get_cached_asset(self, congestion: bool = False) -> pd.DataFrame:
        """
        Retrieves the travel costs DataFrame from the cache.

        Returns:
            pd.DataFrame: The cached DataFrame of travel costs.
        """
        
        if congestion is False:
            path = self.cache_path["freeflow"]
        else:
            path = self.cache_path["congested"]

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
        
        if congestion is False:
            self.inputs["contracted_path_graph"].get()
            output_path = self.cache_path["freeflow"]
            path_graph = self.inputs["contracted_path_graph"]
        else:
            self.inputs["congested_path_graph"].get()
            output_path = self.cache_path["congested"]
            path_graph = self.inputs["congested_path_graph"]
        
        costs = self.compute_costs_by_OD(
            self.inputs["transport_zones"],
            path_graph,
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
        
        script = RScriptRunner(resources.files('mobility.transport.costs.path').joinpath('prepare_dodgr_costs.R'))
        script.run(
            args=[
                str(transport_zones.cache_path),
                str(path_graph.cache_path),
                str(self.inputs["routing_parameters"].max_beeline_distance),
                str(output_path)
            ]
        )
        
        costs = pd.read_parquet(output_path)

        return costs
    
    def get_congested_graph_path(self, flow_asset=None) -> pathlib.Path:
        """Return the graph path backing the current congested cost view."""
        if flow_asset is not None:
            return self.asset_for_flow_asset(flow_asset).inputs["contracted_path_graph"].inputs["congested_graph"].get()
        return self.inputs["congested_path_graph"].get()

    def asset_for_iteration(self, run, iteration: int):
        """Return the travel-cost asset instance corresponding to one simulation iteration."""
        if iteration < 1:
            raise ValueError("Iteration should be >= 1.")
        if iteration > int(run.parameters.n_iterations):
            raise ValueError(
                f"Iteration should be <= {int(run.parameters.n_iterations)} for this run."
            )

        flow_asset = self.inputs["congested_path_graph"].get_flow_asset_for_iteration(run, iteration)
        if flow_asset is None:
            return self
        return self.asset_for_flow_asset(flow_asset)

    def get_for_iteration(self, run, iteration: int):
        """Materialize the travel costs corresponding to one simulation iteration."""
        return self.asset_for_iteration(run, iteration).get()

    def asset_for_congestion_state(self, congestion_state: CongestionState):
        flow_asset = congestion_state.for_mode(self.inputs["mode_name"])
        if flow_asset is None:
            return None
        return self.asset_for_flow_asset(flow_asset)

    def asset_for_flow_asset(self, flow_asset):
        congested_graph = CongestedPathGraph(
            modified_graph=self.inputs["modified_path_graph"],
            transport_zones=self.inputs["transport_zones"],
            handles_congestion=self.inputs["congested_path_graph"].handles_congestion,
            congestion_flows_scaling_factor=self.inputs["congested_path_graph"].congestion_flows_scaling_factor,
            vehicle_flows=flow_asset,
        )
        contracted_graph = ContractedPathGraph(congested_graph)

        variant = PathTravelCosts(
            mode_name=self.inputs["mode_name"],
            transport_zones=self.inputs["transport_zones"],
            routing_parameters=self.inputs["routing_parameters"],
            osm_capacity_parameters=self.inputs["osm_capacity_parameters"],
            contracted_graph=contracted_graph,
        )
        return variant
        
