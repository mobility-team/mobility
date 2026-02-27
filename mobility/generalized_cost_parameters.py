from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field

from mobility.cost_of_time_parameters import CostOfTimeParameters


class GeneralizedCostParameters(BaseModel):
    """Parameters used to compute generalized travel cost."""

    model_config = ConfigDict(extra="forbid")

    cost_constant: Annotated[float, Field(default=0.0)]
    cost_of_time: Annotated[CostOfTimeParameters, Field(default_factory=CostOfTimeParameters)]
    cost_of_distance: Annotated[float, Field(default=0.0)]
