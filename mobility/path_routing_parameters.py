from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field


class PathRoutingParameters(BaseModel):
    """Routing filter parameters for path-based travel costs."""

    model_config = ConfigDict(extra="forbid")

    filter_max_speed: Annotated[float, Field(gt=0.0)]  # km/h
    filter_max_time: Annotated[float, Field(gt=0.0)]  # h
