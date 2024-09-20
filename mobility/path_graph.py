import geopandas as gpd

from mobility.parameters import ModeParameters
from mobility.simplified_path_graph import SimplifiedPathGraph
from mobility.contracted_path_graph import ContractedPathGraph

class PathGraph:
    
    def __init__(
        self,
        transport_zones: gpd.GeoDataFrame,
        mode_parameters: ModeParameters
    ):
        
        self.simplified = SimplifiedPathGraph(transport_zones, mode_parameters)
        self.contracted = ContractedPathGraph(self.simplified)
        
        