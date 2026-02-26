"""Pydantic parameter model for mobility survey assets."""

from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field


class MobilitySurveyParameters(BaseModel):
    """Parameters used to configure a mobility survey asset."""

    model_config = ConfigDict(extra="forbid")

    survey_name: Annotated[
        str,
        Field(
            title="Survey name",
            description="Identifier of the survey dataset folder to load.",
        ),
    ]

    country: Annotated[
        str,
        Field(
            title="Country code",
            description="ISO-like country code used to map surveys to population inputs.",
        ),
    ]

    seq_prob_cutoff: Annotated[
        float,
        Field(
            default=0.5,
            gt=0.0,
            le=1.0,
            title="Sequence probability cutoff",
            description=(
                "Cumulative contribution cutoff used to keep the most relevant "
                "motive/mode sequences per population group."
            ),
        ),
    ]
