from mobility.parameters import Parameters

class TransportMode:
    def __init__(
        self,
        name: str,
        travel_costs,
        parameters: Parameters = None
    ):
        
        self.name = name
        self.travel_costs = travel_costs
        self.parameters = parameters
    


        
    
        