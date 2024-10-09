from dataclasses import dataclass

@dataclass
class PublicTransportRoutingParameters():
    
    """     
        Args:
            public_transport_start_time_min : float containing the start hour to consider for public transport cost determination
            start_time_max : float containing the end hour to consider for public transport cost determination, should be superior to start_time_min
            public_transport_start_time_max : float with the maximum travel time to consider for public transport, in hours
            public_transport_max_traveltime : list of additional GTFS files to include in the calculations

        """
    
    start_time_min: float = 6.5
    start_time_max: float = 7.5
    max_traveltime: float = 1.0
    additional_gtfs_files: list = None
    