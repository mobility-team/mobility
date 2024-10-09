from dataclasses import dataclass
from typing import List, TypedDict

class Coordinates(TypedDict):
    lon: float
    lat: float

@dataclass
class ModalShift():
    
    # Max travel time from or to the connection nodes, estimated from the crow
    # fly distance around connection nodes and an average speed
    max_travel_time: float # hours
    average_speed: float # km/h
    
    # Average shift time between the two connected modes
    shift_time: float # minutes
    
    # Optional shortcuts to make some connections faster than average
    shortcuts_shift_time: float = None # minutes
    shortcuts_locations: List[Coordinates] = None
    
    
    