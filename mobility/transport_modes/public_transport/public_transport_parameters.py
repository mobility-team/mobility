from dataclasses import dataclass
from mobility.parameters import ModeParameters

@dataclass
class PublicTransportParameters(ModeParameters):
    
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
    
    # Generalized cost parameters
    # Cost of time parameters
    cost_of_time_c0_short: float = 0.0
    cost_of_time_c0: float = 0.0
    cost_of_time_c1: float = 0.0
    cost_of_time_country_coeff_fr: float = 1.0
    cost_of_time_country_coeff_ch: float = 1.0
    
    # Cost of distance parameters
    cost_of_distance = float = 0.1
    
    # Constant
    cost_constant = float = 10.0
    
    def __post_init__(self):
        self.name = "public_transport"
