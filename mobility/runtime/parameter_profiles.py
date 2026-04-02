from __future__ import annotations

from typing import Any, Literal, TypeVar

import numpy as np
from pydantic import BaseModel, ConfigDict, model_validator

T = TypeVar("T", bound=BaseModel)


class ParameterProfile(BaseModel):
    """Base class for parameter profiles evaluated over simulation iterations."""

    model_config = ConfigDict(extra="forbid")

    @model_validator(mode="after")
    def validate_points(self) -> "ParameterProfile":
        points = getattr(self, "points", None)
        if not points:
            raise ValueError(f"{self.__class__.__name__}.points must not be empty.")

        invalid_steps = [step for step in points if step < 1]
        if invalid_steps:
            raise ValueError(f"{self.__class__.__name__}.points keys must be >= 1.")

        return self

    def at(self, iteration: int) -> Any:
        raise NotImplementedError


class ScalarParameterProfile(ParameterProfile):
    """Numeric parameter profile supporting step-wise and linear interpolation."""

    mode: Literal["step", "linear"] = "step"
    points: dict[int, float]

    def at(self, iteration: int) -> float:
        sorted_points = sorted(self.points.items())
        iterations = np.array([iteration for iteration, _ in sorted_points], dtype=float)
        values = np.array([value for _, value in sorted_points], dtype=float)

        if self.mode == "step":
            idx = np.searchsorted(iterations, iteration, side="right") - 1
            idx = max(idx, 0)
            return float(values[idx])

        return float(np.interp(iteration, iterations, values))


class ListParameterProfile(ParameterProfile):
    """List-valued parameter profile supporting step-wise changes only."""

    points: dict[int, list[str]]

    def at(self, iteration: int) -> list[str]:
        sorted_points = sorted(self.points.items())
        iterations = [iteration for iteration, _ in sorted_points]
        idx = np.searchsorted(iterations, iteration, side="right") - 1
        idx = max(idx, 0)
        return list(sorted_points[idx][1])


def resolve_value_for_iteration(value: Any, iteration: int) -> Any:
    """Resolve one value for a simulation iteration."""

    if isinstance(value, ParameterProfile):
        return value.at(iteration)

    if isinstance(value, BaseModel):
        return resolve_model_for_iteration(value, iteration)

    if isinstance(value, dict):
        return {key: resolve_value_for_iteration(item, iteration) for key, item in value.items()}

    if isinstance(value, list):
        return [resolve_value_for_iteration(item, iteration) for item in value]

    if isinstance(value, tuple):
        return tuple(resolve_value_for_iteration(item, iteration) for item in value)

    return value


def resolve_model_for_iteration(model: T, iteration: int) -> T:
    """Resolve iteration-varying fields of a pydantic model."""

    resolved_data = {
        field_name: resolve_value_for_iteration(getattr(model, field_name), iteration)
        for field_name in model.__class__.model_fields
    }
    return model.__class__.model_validate(resolved_data)
