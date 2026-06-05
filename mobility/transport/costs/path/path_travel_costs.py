from __future__ import annotations

import os
import pathlib
import logging
import pandas as pd

from importlib import resources
from mobility.transport.graphs.core.path_graph import PathGraph
from mobility.transport.costs.travel_costs_asset import TravelCostsBase
from mobility.runtime.assets.file_asset import FileAsset
from mobility.runtime.assets.in_memory_asset import InMemoryAsset
from mobility.runtime.r_integration.r_script_runner import RScriptRunner
from mobility.spatial.transport_zones import TransportZones
from mobility.transport.costs.parameters.path_routing_parameters import PathRoutingParameters
from mobility.transport.modes.core.osm_capacity_parameters import OSMCapacityParameters
from mobility.transport.graphs.modified.modifiers.speed_modifier import SpeedModifier
from mobility.transport.graphs.congested.congested_path_graph import CongestedPathGraph
from mobility.transport.graphs.contracted.contracted_path_graph import ContractedPathGraph
from mobility.transport.costs.od_flows_asset import VehicleODFlowsAsset

from typing import List


class PathTravelCostsTable(TravelCostsBase, FileAsset):
    """Single path travel-cost table for one routing graph."""

    def __init__(
        self,
        *,
        mode_name: str,
        transport_zones: TransportZones,
        routing_graph,
        routing_parameters: PathRoutingParameters,
        cost_kind: str,
    ) -> None:
        self.mode_name = mode_name
        self.transport_zones = transport_zones
        self.routing_graph = routing_graph
        self.routing_parameters = routing_parameters
        self.cost_kind = cost_kind
        inputs = {
            "version": 1,
            "mode_name": mode_name,
            "transport_zones": transport_zones,
            "routing_graph": routing_graph,
            "routing_parameters": routing_parameters,
            "cost_kind": cost_kind,
        }
        cache_path = (
            pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"])
            / f"travel_costs_{cost_kind}_{mode_name}.parquet"
        )
        super().__init__(inputs, cache_path)

    def get_cached_asset(self) -> pd.DataFrame:
        """Return the cached OD travel-cost table."""
        logging.debug("Travel costs already prepared. Reusing the file : %s", str(self.cache_path))
        return pd.read_parquet(self.cache_path)

    def create_and_get_asset(self) -> pd.DataFrame:
        """Compute this OD travel-cost table from its routing graph."""
        logging.info("Preparing travel costs for mode %s", self.mode_name)
        self.transport_zones.get()
        return self._compute_costs_by_od()

    def _compute_costs_by_od(self) -> pd.DataFrame:
        """Compute path travel times and distances by OD."""
        logging.info("Computing travel times and distances by OD...")
        script = RScriptRunner(
            resources.files('mobility.transport.costs.path').joinpath(
                'prepare_dodgr_costs.R'
            )
        )
        script.run(
            args=[
                str(self.transport_zones.cache_path),
                str(self.routing_graph.get()),
                str(self.routing_parameters.max_beeline_distance),
                str(self.cache_path),
            ]
        )
        return pd.read_parquet(self.cache_path)


class PathTravelCosts(TravelCostsBase, InMemoryAsset):
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
            target_max_vehicles_per_od_endpoint: float = 1000.0,
            congestion_assignment_max_iterations: int = 10,
            congestion_assignment_max_gap: float = 0.05,
            congestion_assignment_retained_volume_share: float = 0.95,
            speed_modifiers: List[SpeedModifier] = [],
            contracted_graph: ContractedPathGraph | None = None,
            default_congestion: bool = False,
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
                target_max_vehicles_per_od_endpoint,
                congestion_assignment_max_iterations,
                congestion_assignment_max_gap,
                congestion_assignment_retained_volume_share,
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

        self.mode_name = mode_name
        self.transport_zones = transport_zones
        self.simplified_path_graph = simplified_path_graph
        self.modified_path_graph = modified_path_graph
        self.congested_path_graph = congested_path_graph
        self.contracted_path_graph = contracted_path_graph
        self.freeflow_costs = PathTravelCostsTable(
            mode_name=mode_name,
            transport_zones=transport_zones,
            routing_graph=contracted_path_graph,
            routing_parameters=routing_parameters,
            cost_kind="free_flow",
        )
        self.congested_costs = PathTravelCostsTable(
            mode_name=mode_name,
            transport_zones=transport_zones,
            routing_graph=contracted_path_graph,
            routing_parameters=routing_parameters,
            cost_kind="congested",
        )
        self.default_congestion = bool(default_congestion)
        
        inputs = {
            "transport_zones": transport_zones,
            "mode_name": mode_name,
            "simplified_path_graph": simplified_path_graph,
            "modified_path_graph": modified_path_graph,
            "congested_path_graph": congested_path_graph,
            "contracted_path_graph": contracted_path_graph,
            "routing_parameters": routing_parameters,
            "osm_capacity_parameters": osm_capacity_parameters,
            "target_max_vehicles_per_od_endpoint": target_max_vehicles_per_od_endpoint,
            "congestion_assignment_max_iterations": congestion_assignment_max_iterations,
            "congestion_assignment_max_gap": congestion_assignment_max_gap,
            "congestion_assignment_retained_volume_share": congestion_assignment_retained_volume_share,
            "default_congestion": self.default_congestion,
        }
        super().__init__(inputs)

    def get(self, congestion: bool = False, road_flow_asset: VehicleODFlowsAsset | None = None) -> pd.DataFrame:
        if congestion and self._handles_congestion() is False:
            return self.freeflow_costs.get()

        if congestion and road_flow_asset is not None:
            asset = self.asset_for_road_flows(road_flow_asset)
            if asset is not None:
                return asset.get()

        # A congested cost table only differs from free-flow costs when it is
        # tied to an explicit road-flow asset.
        if self.default_congestion:
            return self.congested_costs.get()
        return self.freeflow_costs.get()
    
    def get_congested_graph_path(self, flow_asset=None) -> pathlib.Path:
        """Return the graph path backing the current congested cost view."""
        if flow_asset is not None:
            return self.asset_for_flow_asset(flow_asset).congested_path_graph.get()
        return self.congested_path_graph.get()

    def asset_for_road_flows(self, road_flow_asset: VehicleODFlowsAsset | None):
        if road_flow_asset is None:
            return None
        if self._handles_congestion() is False:
            return None
        return self.asset_for_flow_asset(road_flow_asset)

    def _handles_congestion(self) -> bool:
        """Return whether this path mode can use congestion-sensitive costs."""
        if hasattr(self, "congested_path_graph"):
            congested_path_graph = self.congested_path_graph
        else:
            congested_path_graph = self.inputs["congested_path_graph"]
        return bool(congested_path_graph.inputs["handles_congestion"])

    def asset_for_flow_asset(self, flow_asset):
        congested_graph = CongestedPathGraph(
            modified_graph=self.inputs["modified_path_graph"],
            transport_zones=self.inputs["transport_zones"],
            handles_congestion=self.inputs["congested_path_graph"].inputs["handles_congestion"],
            congestion_flows_scaling_factor=self.inputs["congested_path_graph"].inputs["congestion_flows_scaling_factor"],
            target_max_vehicles_per_od_endpoint=self.inputs["congested_path_graph"].inputs["target_max_vehicles_per_od_endpoint"],
            congestion_assignment_max_iterations=self.inputs["congested_path_graph"].inputs["congestion_assignment_max_iterations"],
            congestion_assignment_max_gap=self.inputs["congested_path_graph"].inputs["congestion_assignment_max_gap"],
            congestion_assignment_retained_volume_share=self.inputs["congested_path_graph"].inputs["congestion_assignment_retained_volume_share"],
            vehicle_flows=flow_asset,
        )
        contracted_graph = ContractedPathGraph(congested_graph)

        variant = PathTravelCosts(
            mode_name=self.inputs["mode_name"],
            transport_zones=self.inputs["transport_zones"],
            routing_parameters=self.inputs["routing_parameters"],
            osm_capacity_parameters=self.inputs["osm_capacity_parameters"],
            target_max_vehicles_per_od_endpoint=self.inputs["target_max_vehicles_per_od_endpoint"],
            congestion_assignment_max_iterations=self.inputs["congestion_assignment_max_iterations"],
            congestion_assignment_max_gap=self.inputs["congestion_assignment_max_gap"],
            congestion_assignment_retained_volume_share=self.inputs["congestion_assignment_retained_volume_share"],
            contracted_graph=contracted_graph,
            default_congestion=True,
        )
        return variant

    def remove(self) -> None:
        """Remove path travel-cost tables owned by this selector."""
        self.freeflow_costs.remove()
        self.congested_costs.remove()

    def remove_congestion_artifacts(self, road_flow_asset: VehicleODFlowsAsset) -> None:
        """Remove one congestion-specific travel-cost variant and owned graph caches."""
        variant = self.asset_for_road_flows(road_flow_asset)
        if variant is None or variant is self:
            return

        variant.remove()

        contracted_graph = variant.inputs.get("contracted_path_graph")
        if contracted_graph is not None:
            contracted_graph.remove()
            congested_graph = getattr(contracted_graph, "inputs", {}).get("congested_graph")
            if congested_graph is not None:
                congested_graph.remove()
