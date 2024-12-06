import geopandas as gpd

from mobility.simplified_path_graph import SimplifiedPathGraph
from mobility.contracted_path_graph import ContractedPathGraph

class PathGraph:
    
    def __init__(
        self,
        mode_name: str,
        transport_zones: gpd.GeoDataFrame,
        congestion: bool
    ):
        
        self.simplified = SimplifiedPathGraph(mode_name, transport_zones)
        self.contracted = ContractedPathGraph(self.simplified, transport_zones, congestion)
        

        
        