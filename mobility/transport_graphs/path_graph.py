import pathlib
import geopandas as gpd

from mobility.transport_zones import TransportZones
from mobility.transport_graphs.simplified_path_graph import SimplifiedPathGraph
from mobility.transport_graphs.modified_path_graph import ModifiedPathGraph
from mobility.transport_graphs.congested_path_graph import CongestedPathGraph
from mobility.transport_graphs.contracted_path_graph import ContractedPathGraph
from mobility.transport_modes.osm_capacity_parameters import OSMCapacityParameters
from mobility.transport_graphs.speed_modifier import SpeedModifier

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
        

        
        