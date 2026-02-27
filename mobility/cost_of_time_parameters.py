from typing import Annotated

import numpy as np
from numpy.typing import NDArray
from pydantic import BaseModel, ConfigDict, Field, model_validator


class CostOfTimeParameters(BaseModel):
    """Parameters used to compute time valuation as a function of distance."""

    model_config = ConfigDict(extra="forbid")

    intercept: Annotated[float, Field(default=20.0)]
    breaks: Annotated[list[float], Field(default_factory=lambda: [0.0, 10000000.0])]
    slopes: Annotated[list[float], Field(default_factory=lambda: [0.0])]
    max_value: Annotated[float, Field(default=20.0)]

    country_coeff_fr: Annotated[float, Field(default=1.0)]
    country_coeff_ch: Annotated[float, Field(default=1.0)]

    @model_validator(mode="after")
    def validate_breaks_and_slopes(self) -> "CostOfTimeParameters":
        """Validate piecewise-linear shape consistency.

        Returns:
            Current validated instance.

        Raises:
            ValueError: If slope count does not match break count minus one.
        """
        if len(self.slopes) != len(self.breaks) - 1:
            raise ValueError(
                "The number of breaks and slopes do not match, there should be N-1 slopes for N breaks."
            )
        return self

    def compute(self, distance: NDArray[np.float64], country) -> NDArray[np.float64]:
        """Compute value of time for each OD pair.

        Args:
            distance: Distance array.
            country: Country code array aligned with distance.

        Returns:
            Array of cost-of-time values.
        """
        cost = self.intercept

        if len(self.slopes) > 0:
            base = self.intercept

            for i, slope in enumerate(self.slopes):
                left_break = self.breaks[i]
                right_break = self.breaks[i + 1]

                cost = np.where(
                    distance > left_break,
                    base + slope * (distance - left_break),
                    cost,
                )

                base = base + slope * (right_break - left_break)

        cost = np.where(cost > self.max_value, self.max_value, cost)

        cost = np.where(country == "fr", cost * self.country_coeff_fr, cost)
        cost = np.where(country == "ch", cost * self.country_coeff_ch, cost)

        return cost
                   
