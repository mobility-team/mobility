from pydantic import BaseModel, Field, ConfigDict
from typing import Annotated, Union

class StudyAreaParameters(BaseModel):

    model_config = ConfigDict(extra="forbid")

    local_admin_unit_id: Annotated[
        Union[str, list[str]],
        Field(
            title="Study area local admin unit ID(s)",
            description=(
                "Center local admin unit ID, or a list of local admin unit IDs "
                "to define the study area."
            ),
        ),
    ]

    radius: Annotated[
        float,
        Field(
            default=40.0,
            ge=5.0,
            le=100.0,
            title="Study area radius",
            description="Radius in km around the selected local admin unit.",
            json_schema_extra={
                "unit": "km"
            },
        ),
    ]
