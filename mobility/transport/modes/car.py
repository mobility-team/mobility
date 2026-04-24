from typing import List, Literal

from mobility.spatial.transport_zones import TransportZones
from mobility.transport.costs.path.path_travel_costs import PathTravelCosts
from mobility.transport.modes.core.transport_mode import TransportMode, TransportModeParameters
from mobility.transport.modes.core.defaults import (
    DEFAULT_LONG_RANGE_MOTORIZED_MAX_BEELINE_DISTANCE_KM,
)
from mobility.transport.costs.parameters.path_routing_parameters import PathRoutingParameters
from mobility.transport.costs.parameters.generalized_cost_parameters import GeneralizedCostParameters
from mobility.transport.costs.parameters.cost_of_time_parameters import CostOfTimeParameters
from mobility.transport.costs.path.path_generalized_cost import PathGeneralizedCost
from mobility.transport.modes.core.osm_capacity_parameters import OSMCapacityParameters
from mobility.transport.graphs.modified.modifiers.speed_modifier import SpeedModifier
import polars as pl
from pydantic import Field

class CarMode(TransportMode):
    """
    A class for car transportation. Creates travel costs for the mode, using the provided parameters or default ones.
    
    Attributes:
        transport_zones (TransportZones): transport zones to consider
        parameters(CarParameters): composants of cost. Default: standard parameters.
    """
    
    def __init__(
        self,
        transport_zones: TransportZones,
        routing_parameters: PathRoutingParameters = None,
        osm_capacity_parameters: OSMCapacityParameters = None,
        generalized_cost_parameters: GeneralizedCostParameters = None,
        congestion: bool | None = None,
        congestion_flows_scaling_factor: float = 0.1,
        target_max_vehicles_per_od_endpoint: float | None = None,
        speed_modifiers: List[SpeedModifier] = [],
        survey_ids: List[str] | None = None,
        ghg_intensity: float | None = None,
        parameters: "CarParameters | None" = None,
    ):
        
        mode_name = "car"
        mode_parameters = TransportMode.prepare_parameters(
            parameters=parameters,
            parameters_cls=CarParameters,
            explicit_args={
                "name": mode_name,
                "ghg_intensity": ghg_intensity,
                "congestion": congestion,
                "vehicle": "car",
                "multimodal": False,
                "return_mode": None,
                "survey_ids": survey_ids,
                "target_max_vehicles_per_od_endpoint": target_max_vehicles_per_od_endpoint,
            },
            owner_name="CarMode",
        )
        mode_congestion = mode_parameters.congestion
        mode_target_max_vehicles_per_od_endpoint = mode_parameters.target_max_vehicles_per_od_endpoint

        if routing_parameters is None:
            routing_parameters = PathRoutingParameters(
                max_beeline_distance=DEFAULT_LONG_RANGE_MOTORIZED_MAX_BEELINE_DISTANCE_KM
            )
            
        if osm_capacity_parameters is None:
            osm_capacity_parameters = OSMCapacityParameters(mode_name)
        
        if generalized_cost_parameters is None:
            generalized_cost_parameters = GeneralizedCostParameters(
                cost_constant=0.0,
                cost_of_distance=0.1,
                cost_of_time=CostOfTimeParameters()
            )
            
        
        travel_costs = PathTravelCosts(
            mode_name=mode_name,
            transport_zones=transport_zones,
            routing_parameters=routing_parameters,
            osm_capacity_parameters=osm_capacity_parameters,
            congestion=mode_congestion,
            congestion_flows_scaling_factor=congestion_flows_scaling_factor,
            target_max_vehicles_per_od_endpoint=mode_target_max_vehicles_per_od_endpoint,
            speed_modifiers=speed_modifiers,
        )
        
        generalized_cost = PathGeneralizedCost(
            travel_costs,
            generalized_cost_parameters,
            mode_name=mode_name
        )
        
        super().__init__(
            mode_name,
            travel_costs,
            generalized_cost,
            congestion=mode_congestion,
            ghg_intensity=ghg_intensity,
            vehicle="car",
            survey_ids=survey_ids,
            parameters=mode_parameters,
            parameters_cls=CarParameters,
        )

    def build_congestion_flows(self, od_flows_by_mode):
        """Build road vehicle flows from car person flows."""
        return (
            od_flows_by_mode
            .filter(pl.col("mode") == "car")
            .with_columns(
                pl.col("flow_volume").alias("vehicle_volume")
            )
            .group_by(["from", "to"])
            .agg(pl.col("vehicle_volume").sum())
            .select(["from", "to", "vehicle_volume"])
        )


class CarParameters(TransportModeParameters):
    """Parameters for car mode."""

    name: Literal["car"] = "car"
    ghg_intensity: float = 0.218
    congestion: bool = False
    target_max_vehicles_per_od_endpoint: float = Field(
        default=1000.0,
        gt=0.0,
        title="Target max vehicles per OD endpoint",
        description=(
            "Target upper bound used when splitting one OD pair across "
            "representative origin/destination points before congestion assignment."
        ),
        json_schema_extra={"unit": "veh"},
    )
    vehicle: Literal["car"] = "car"
    multimodal: bool = False
    return_mode: None = None
    survey_ids: list[str] = Field(
        default_factory=lambda: ["3.30", "3.31", "3.32", "3.33", "3.39"]
    )
