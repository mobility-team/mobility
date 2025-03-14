import geopandas as gpd

from mobility.simplified_path_graph import SimplifiedPathGraph
from mobility.contracted_path_graph import ContractedPathGraph
from mobility.transport_modes.osm_capacity_parameters import OSMCapacityParameters

class PathGraph:
    
    def __init__(
        self,
        mode_name: str,
        transport_zones: gpd.GeoDataFrame,
        osm_capacity_parameters: OSMCapacityParameters,
        congestion: bool,
        congestion_flows_scaling_factor: float = 1.0
    ):
        
        self.simplified = SimplifiedPathGraph(mode_name, transport_zones, osm_capacity_parameters)
        self.contracted = ContractedPathGraph(self.simplified, transport_zones, congestion, congestion_flows_scaling_factor)
        

        
        