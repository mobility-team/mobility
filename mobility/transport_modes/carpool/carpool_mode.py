from mobility.transport_modes.transport_mode import TransportMode
from mobility.transport_modes.car import CarMode
from mobility.transport_modes.carpool.detailed import DetailedCarpoolRoutingParameters, DetailedCarpoolGeneralizedCostParameters, DetailedCarpoolTravelCosts, DetailedCarpoolGeneralizedCost

from mobility.transport_modes.modal_shift import ModalShift

class CarpoolMode(TransportMode):
    
    def __init__(
        self,
        car_mode: CarMode,
        routing_parameters: DetailedCarpoolRoutingParameters = None,
        generalized_cost_parameters: DetailedCarpoolGeneralizedCostParameters = None,
        modal_shift: ModalShift = None
    ):
            
        routing_parameters = routing_parameters or DetailedCarpoolRoutingParameters()
        travel_costs = DetailedCarpoolTravelCosts("carpool", car_mode.travel_costs, routing_parameters, modal_shift)
        
        generalized_cost_parameters = generalized_cost_parameters or DetailedCarpoolGeneralizedCostParameters()
        generalized_cost = DetailedCarpoolGeneralizedCost(travel_costs, generalized_cost_parameters)
        
        congestion = car_mode.congestion
            
        super().__init__("carpool", travel_costs, generalized_cost, congestion)
        
        
