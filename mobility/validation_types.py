from typing import Annotated

from pydantic import Field

NonNegativeFloat = Annotated[float, Field(ge=0.0)]
UnitIntervalFloat = Annotated[float, Field(ge=0.0, le=1.0)]
