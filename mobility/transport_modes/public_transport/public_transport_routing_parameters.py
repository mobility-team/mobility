from dataclasses import dataclass

@dataclass
class PublicTransportRoutingParameters():
    
    """
    Dataclass for public transport routing parameters. Should be given as argument for the routing_parameters of PublicTransportMode
    By default, the period between 6:30 am and 7:30 am will be considered, with a maximum public transport traveltime of 1 hour.
    
    Args:
        start_time_min : float containing the start hour to consider for public transport cost determination
        start_time_max : float containing the end hour to consider for public transport cost determination, should be superior to start_time_min
        max_traveltime : float with the maximum travel time to consider for public transport, in hours
        public_transport_max_traveltime : list of additional GTFS files to include in the calculations
        expected_agencies : list with the names of agencies that should appear of the GTFS of the territory.
            For instance, "SNCF" should be expected in any French territory and "SBB" in any Swiss one.
            It is not needed to exactly match the full name in the GTFS agency.txt file, but the name shoudl at least appear in agency.txt.
            It is then better to use acronyms.
            The parameter is optional but helps to check if any important GTFS would have disappeared without throwing an error.

        """
    
    start_time_min: float = 6.5
    start_time_max: float = 7.5
    max_traveltime: float = 1.0
    additional_gtfs_files: list = None
    expected_agencies: list = None