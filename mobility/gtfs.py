import os
import pathlib

from mobility.asset import Asset
from mobility.transport_zones import TransportZones

class GTFS():
    
    def __init__(self, files):
        self.files = files
        