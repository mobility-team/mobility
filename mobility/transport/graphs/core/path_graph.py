import pathlib
import geopandas as gpd

from mobility.spatial.transport_zones import TransportZones
from mobility.transport.graphs.simplified.simplified_path_graph import SimplifiedPathGraph
from mobility.transport.graphs.modified.modified_path_graph import ModifiedPathGraph
from mobility.transport.graphs.congested.congested_path_graph import CongestedPathGraph
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

        self.congested = CongestedPathGraph(
            self.modified,
            transport_zones,
            congestion,
            congestion_flows_scaling_factor
        )
        
        self.contracted = ContractedPathGraph(
            self.congested
        )
        

        
        
