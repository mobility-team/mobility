from dataclasses import dataclass
from typing import List, TypedDict

class Coordinates(TypedDict):
    lon: float
    lat: float

@dataclass
class IntermodalTransfer():
    """
    Dataclass for intermodal transfer parameters.
    
    Args:
        max_travel_time: maximum travel time to consider when estima
    """
    
    # Max travel time from or to the connection nodes, estimated from the crow
    # fly distance around connection nodes and an average speed
    max_travel_time: float = 20.0 / 60.0 # hours
    average_speed: float = 4.0 # km/h
    
    # Average transfer time between the two connected modes
    transfer_time: float = 1.0 # minutes
    
    # Optional shortcuts to make some connections faster than average
    shortcuts_transfer_time: float = None # minutes
    shortcuts_locations: List[Coordinates] = None
    
    
    