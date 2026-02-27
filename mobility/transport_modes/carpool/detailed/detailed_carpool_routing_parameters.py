from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field


class DetailedCarpoolRoutingParameters(BaseModel):
    """
    Attributes:
        number_persons (int): number of persons in the vehicule.
        absolute_delay_per_passenger (int): absolute delay per supplementary passenger, in minutes. Default: 5
        relative_delay_per_passenger (float) : relative delay per supplementary passenger in proportion of the total travel time. Default: 0.05
        absolute_extra_distance_per_passenger (float): absolute extra distance per supplementary passenger, in km. Default: 1
        relative_extra_distance_per_passenger (flaot) : relative extra distance per supplementary passenger in proportion of the total distance. Default: 0.05
    """

    model_config = ConfigDict(extra="forbid")
    
    # Ridesharing parking locations (CRS : WGS 84 - 4326)
    parking_locations: Annotated[list[tuple[float, float]], Field(default_factory=list)]
    
    # Delays compared to the single occupant car mode
    absolute_delay_per_passenger: Annotated[int, Field(default=5, ge=0)]
    relative_delay_per_passenger: Annotated[float, Field(default=0.05, ge=0.0)]
    absolute_extra_distance_per_passenger: Annotated[float, Field(default=1.0, ge=0.0)]
    relative_extra_distance_per_passenger: Annotated[float, Field(default=0.05, ge=0.0)]
    
    
    
    
    
