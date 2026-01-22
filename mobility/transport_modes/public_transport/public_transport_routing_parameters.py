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
        wait_time_coeff (float): the ratio between the time perceived by a user and the actual time when waiting.
        transfer_time_coeff (float):
            The ratio between the time perceived by a user and the actual time 
            when in transfer between stops. 
        no_show_perceived_prob (float): 
            The perceived probability(p)  that the vehicle will not show up at the 
            first stop of the journey, and the user has to wait for the next one.
            The perceived travel time is increased by p x time to next departure.
        target_time (float):²
            The time in hours at which the user would like to arrive at destination.
        max_wait_time_at_destination (float):
            The maximum time in hours that a user is willing to wait once at destination.
            For example a user may want to arrive at 8:00 but the latest arrival 
            might 7:45, so the user has to wait at least 15 min.
        max_perceived_time (float):
            The maximum time in perceived hours that a user is willing to take
            to get to her destination.
        additional_gtfs_files : list of additional GTFS files to include in the calculations
        gtfs_edits: list of metadata to edit a given gtfs
        expected_agencies : list with the names of agencies that should appear of the GTFS of the territory.
            For instance, "SNCF" should be expected in any French territory and "SBB" in any Swiss one.
            It is not needed to exactly match the full name in the GTFS agency.txt file, but the name shoudl at least appear in agency.txt.
            It is then better to use acronyms.
            The parameter is optional but helps to check if any important GTFS would have disappeared without throwing an error.

        """
    
    start_time_min: float = 6.5
    start_time_max: float = 8.0
    max_traveltime: float = 2.0
    wait_time_coeff: float = 2.0
    transfer_time_coeff: float = 2.0
    no_show_perceived_prob: float = 0.2
    target_time: float = 8.0
    max_wait_time_at_destination: float = 0.25
    max_perceived_time: float = 2.0
    additional_gtfs_files: list = None
    gtfs_edits: list = None
    expected_agencies: list = None