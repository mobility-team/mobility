from mobility.parameters import ModeParameters

class TransportMode:
    
    def __init__(
        self,
        travel_costs,
        parameters: ModeParameters = None
    ):
        
        self.name = parameters.name
        self.travel_costs = travel_costs
        self.parameters = parameters
        


        
    
        