from __future__ import annotations

from typing import Any, Literal, TypeVar

import numpy as np
from pydantic import BaseModel, ConfigDict, Field, model_validator

T = TypeVar("T", bound=BaseModel)


class SimulationStep(BaseModel):
    """Identifies one simulation step.

    Attributes:
        iteration: One-based simulation iteration index.
    """

    model_config = ConfigDict(extra="forbid")

    iteration: int = Field(ge=1)


class ParameterProfile(BaseModel):
    """Defines a parameter value profile over simulation iterations.

    The profile is specified by control points keyed by iteration index.
    Values can be evaluated with step-wise or linear interpolation.

    Attributes:
        mode: Evaluation mode used between control points.
        points: Mapping from iteration index to parameter value.
    """

    model_config = ConfigDict(extra="forbid")

    mode: Literal["step", "linear"] = "step"
    points: dict[int, float]

    @model_validator(mode="after")
    def validate_points(self) -> "ParameterProfile":
        """Validates control points after model initialization.

        Returns:
            ParameterProfile: The validated profile instance.

        Raises:
            ValueError: If no control point is provided or if an iteration
                index is lower than 1.
        """
        if not self.points:
            raise ValueError("ParameterProfile.points must not be empty.")

        invalid_steps = [step for step in self.points if step < 1]
        if invalid_steps:
            raise ValueError("ParameterProfile.points keys must be >= 1.")

        return self

    def at(self, step: SimulationStep) -> float:
        """Evaluates the profile at a simulation step.

        Args:
            step: Simulation step at which to evaluate the profile.

        Returns:
            float: Parameter value at the requested step.
        """
        sorted_points = sorted(self.points.items())
        iterations = np.array([iteration for iteration, _ in sorted_points], dtype=float)
        values = np.array([value for _, value in sorted_points], dtype=float)

        if self.mode == "step":
            idx = np.searchsorted(iterations, step.iteration, side="right") - 1
            idx = max(idx, 0)
            return float(values[idx])

        return float(np.interp(step.iteration, iterations, values))

def resolve_value_for_step(value: Any, step: SimulationStep) -> Any:
    """Resolves one value for a simulation step.

    Supported inputs are:
    - ``ParameterProfile`` instances, which are evaluated at ``step``
    - pydantic models, which are resolved field by field
    - ``dict``/``list``/``tuple`` containers, which are resolved recursively
    - plain leaf values, which are returned unchanged

    Args:
        value: Value to resolve.
        step: Simulation step used for evaluation.

    Returns:
        Any: Resolved value for the requested step.
    """

    if isinstance(value, ParameterProfile):
        return value.at(step)

    if isinstance(value, BaseModel):
        return resolve_model_for_step(value, step)

    if isinstance(value, dict):
        return {key: resolve_value_for_step(item, step) for key, item in value.items()}

    if isinstance(value, list):
        return [resolve_value_for_step(item, step) for item in value]

    if isinstance(value, tuple):
        return tuple(resolve_value_for_step(item, step) for item in value)

    return value


def resolve_model_for_step(model: T, step: SimulationStep) -> T:
    """Resolves step-varying fields of a pydantic model.

    Args:
        model: Pydantic model whose fields may contain step-varying values.
        step: Simulation step used for evaluation.

    Returns:
        T: New validated model instance with step-varying fields replaced by
        their scalar values at ``step``.
    """

    resolved_data = {
        field_name: resolve_value_for_step(getattr(model, field_name), step)
        for field_name in model.__class__.model_fields
    }
    return model.__class__.model_validate(resolved_data)
