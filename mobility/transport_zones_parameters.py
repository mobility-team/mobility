from pydantic import BaseModel, Field, ConfigDict, model_validator
from typing import Annotated, Literal, Union

class TransportZonesParameters(BaseModel):

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

    level_of_detail: Annotated[
        Literal[0, 1],
        Field(
            default=0,
            title="Transport zones level of detail",
            description=(
                "Whether local admin units will be split into subzones " 
                "(level of detail = 1), according to their building footprint density."
            ),
        ),
    ]

    inner_radius: Annotated[
        float | None,
        Field(
            default=None,
            title="Study area inner radius",
            description=( 
                "Radius in km around the selected local admin unit,used to flag " 
                "as local is_inner_zone. This can be used to filter out results " 
                "from the border of the simulated study area, where the simulation "
                "will be less reliable."
            ),
            json_schema_extra={
                "unit": "km"
            },
        ),
    ]

    inner_local_admin_unit_id: Annotated[
        list[str] | None,
        Field(
            default=None,
            title="Inner local admin unit IDs",
            description=( 
                "List of local admin unit IDs marked as inner zones. This can be " 
                "used to filter out results from the border of the simulated " 
                "study area, where the simulation will be less reliable."
            ),
        ),
    ]

    @model_validator(mode="after")
    def set_derived_defaults(self) -> "TransportZonesParameters":
        """Apply derived defaults that depend on other parameter values.

        Returns:
            The normalized parameters instance.
        """
        if self.inner_radius is None:
            self.inner_radius = self.radius

        if isinstance(self.local_admin_unit_id, list) and self.inner_local_admin_unit_id is None:
            self.inner_local_admin_unit_id = self.local_admin_unit_id

        return self
