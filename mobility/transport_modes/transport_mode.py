from typing import List

from mobility.in_memory_asset import InMemoryAsset

class TransportMode(InMemoryAsset):
    """
    A base class for all transport modes (car, bicycle, walk...).
    
    Attributes:
        name (str): the name of the mode.
        travel_costs (PathTravelCosts | PublicTransportTravelCosts | DetailedCarpoolTravelCosts): 
            a travel costs object instance that computes and stores travel costs
            (time, distance) between transport zones.
        generalized_cost :
        congestion (bool):
            boolean flag to enable congestion, if the travel costs class can 
            handle it (only PathTravelCosts).
    """
    
    def __init__(
        self,
        name: str,
        travel_costs,
        generalized_cost,
        ghg_intensity: float,
        congestion: bool = False,
        vehicle: str = None,
        multimodal: bool = False,
        return_mode: str = None,
        survey_ids: List[str] = None
    ):
        
        if ghg_intensity is None:
            raise ValueError("Please provide a value for the ghg_intensity argument (GHG intensity in kgCO2e/pass.km).")
        
        self.name = name
        self.travel_costs = travel_costs
        self.generalized_cost = generalized_cost
        self.ghg_intensity = ghg_intensity
        self.congestion = congestion
        self.vehicle = vehicle
        self.multimodal = multimodal
        self.return_mode = return_mode
        self.survey_ids = survey_ids
        
        inputs = {}

        super().__init__(inputs)
        
        
    def clone(self):
        
        return TransportMode(
            self.name,
            self.travel_costs.clone(),
            self.generalized_cost,
            self.congestion,
            self.vehicle,
            self.multimodal
        )
        
    
        