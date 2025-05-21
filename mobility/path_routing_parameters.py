from dataclasses import dataclass

@dataclass
class PathRoutingParameters: 
    
    # Routing parameters
    # (use these parameters to filter ODs that will be in the model, based 
    # on crowfly distance)
    filter_max_speed: float # km/h
    filter_max_time: float  # h
