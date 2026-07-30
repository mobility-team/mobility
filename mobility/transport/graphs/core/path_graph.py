import pathlib
import geopandas as gpd

from mobility.spatial.transport_zones import TransportZones
from mobility.transport.graphs.simplified.simplified_path_graph import SimplifiedPathGraph
from mobility.transport.graphs.modified.modified_path_graph import ModifiedPathGraph
from mobility.transport.graphs.congested.congested_path_graph import CongestedPathGraph
from mobility.transport.graphs.cch.cch_path_graph import CCHPathGraph
from mobility.transport.graphs.contracted.contracted_path_graph import ContractedPathGraph
from mobility.transport.modes.core.osm_capacity_parameters import OSMCapacityParameters
from mobility.transport.graphs.modified.modifiers.speed_modifier import SpeedModifier

from typing import List

class PathGraph:
    
    def __init__(
        self,
        mode_name: str,
        transport_zones: TransportZones,
        osm_capacity_parameters: OSMCapacityParameters,
        congestion: bool = False,
        congestion_flows_scaling_factor: float = 1.0,
        target_max_vehicles_per_od_endpoint: float = 1000.0,
        congestion_assignment_max_iterations: int = 10,
        congestion_assignment_max_gap: float = 0.05,
        congestion_assignment_retained_volume_share: float = 0.95,
        speed_modifiers: List[SpeedModifier] = []
    ):
        
        self.simplified = SimplifiedPathGraph(
            mode_name,
            transport_zones,
            osm_capacity_parameters
        )
        
        self.modified = ModifiedPathGraph(
            self.simplified,
            speed_modifiers
        )

        self.contracted = ContractedPathGraph(self.modified)

        # Only congestion-sensitive modes need the CCH topology and congested
        # graph. Keeping these inactive assets out of non-congested modes avoids
        # preparing a walk CCH graph just because it appears in the cache DAG.
        self.cch = None
        self.congested = None
        if congestion:
            self.cch = CCHPathGraph(self.modified)
            self.congested = CongestedPathGraph(
                self.modified,
                self.cch,
                transport_zones,
                congestion,
                congestion_flows_scaling_factor,
                target_max_vehicles_per_od_endpoint,
                congestion_assignment_max_iterations,
                congestion_assignment_max_gap,
                congestion_assignment_retained_volume_share
            )
        
