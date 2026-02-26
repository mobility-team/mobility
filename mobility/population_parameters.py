from pydantic import BaseModel, ConfigDict, Field
from typing import Annotated


class PopulationParameters(BaseModel):
    """Parameters controlling population sampling."""

    model_config = ConfigDict(extra="forbid")

    sample_size: Annotated[
        int,
        Field(
            ge=1,
            title="Population sample size",
            description=(
                "Number of inhabitants to sample within the selected transport zones."
            ),
        ),
    ]

