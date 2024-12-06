
class TransportMode:
    
    def __init__(
        self,
        name: str,
        travel_costs,
        generalized_cost,
        congestion: bool = False
    ):
        
        self.name = name
        self.travel_costs = travel_costs
        self.generalized_cost = generalized_cost
        self.congestion = congestion
    
        