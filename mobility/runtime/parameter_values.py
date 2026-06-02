from __future__ import annotations

from copy import deepcopy
from typing import Any, Literal

import numpy as np
from pydantic import BaseModel, ConfigDict, model_validator

from mobility.runtime.assets.asset import Asset

DEFAULT_SCENARIO = "default"


class ParameterValue(BaseModel):
    """Parameter value that can vary by scenario and by iteration."""

    model_config = ConfigDict(arbitrary_types_allowed=True, frozen=True)

    values_by_scenario: dict[str | None, dict[int, Any]]
    mode: Literal["step", "linear"] = "step"

    @classmethod
    def constant(cls, value: Any) -> "ParameterValue":
        """Return a parameter value that stays constant."""
        return cls(values_by_scenario={None: {1: value}})

    @classmethod
    def by_iteration(
        cls,
        points: dict[int, Any],
        *,
        mode: str = "step",
    ) -> "ParameterValue":
        """Return a parameter value that changes by simulation iteration."""
        return cls(values_by_scenario={None: dict(points)}, mode=mode)

    @classmethod
    def by_scenario(
        cls,
        values: dict[str, Any] | None = None,
        **named_values: Any,
    ) -> "ParameterValue":
        """Return a parameter value that changes by scenario."""
        scenario_values = cls._scenario_values(
            values,
            named_values,
            method_name="ParameterValue.by_scenario",
        )
        return cls(
            values_by_scenario={
                scenario: cls._points_from_value(value)
                for scenario, value in scenario_values.items()
            }
        )

    @classmethod
    def by_scenario_and_iteration(
        cls,
        values: dict[str, dict[int, Any] | Any] | None = None,
        **named_values: dict[int, Any] | Any,
    ) -> "ParameterValue":
        """Return a parameter value that changes by scenario and iteration."""
        scenario_values = cls._scenario_values(
            values,
            named_values,
            method_name="ParameterValue.by_scenario_and_iteration",
        )
        return cls(
            values_by_scenario={
                scenario: cls._points_from_value(value)
                for scenario, value in scenario_values.items()
            }
        )

    @model_validator(mode="after")
    def validate_values(self) -> "ParameterValue":
        """Validate scenario names and iteration points."""
        if not self.values_by_scenario:
            raise ValueError("ParameterValue needs at least one value.")

        for scenario, points in self.values_by_scenario.items():
            if scenario == "":
                raise ValueError("ParameterValue scenario names should not be empty.")
            if not points:
                raise ValueError("ParameterValue iteration points should not be empty.")
            invalid_iterations = [iteration for iteration in points if iteration < 1]
            if invalid_iterations:
                raise ValueError("ParameterValue iteration points should be >= 1.")

        if self.mode not in {"step", "linear"}:
            raise ValueError("ParameterValue mode should be 'step' or 'linear'.")

        return self

    def at(self, *, scenario: str | None = None, iteration: int = 1) -> Any:
        """Return the plain value for one scenario and one iteration."""
        scenario_name = DEFAULT_SCENARIO if scenario is None else scenario
        points = self._points_for_scenario(scenario_name)
        if self.mode == "linear":
            return self._linear_value(points, iteration)
        return self._copy_plain_value(self._step_value(points, iteration))

    def has_scenarios(self) -> bool:
        """Return whether this value defines named scenarios."""
        return any(scenario is not None for scenario in self.values_by_scenario)

    def scenario_names(self) -> set[str]:
        """Return the scenario names defined by this value."""
        return {
            scenario
            for scenario in self.values_by_scenario
            if scenario is not None
        }

    @classmethod
    def _points_from_value(cls, value: Any) -> dict[int, Any]:
        if isinstance(value, ParameterValue):
            if value.has_scenarios():
                raise ValueError(
                    "Nested scenario ParameterValue objects are not supported. "
                    "Use ParameterValue.by_scenario_and_iteration instead."
                )
            return dict(value.values_by_scenario[None])
        if cls._is_iteration_points(value):
            return dict(value)
        return {1: value}

    @staticmethod
    def _scenario_values(
        values: dict[str, Any] | None,
        named_values: dict[str, Any],
        *,
        method_name: str,
    ) -> dict[str, Any]:
        """Return scenario values from a mapping and keyword values."""
        scenario_values = {}
        if values is not None:
            scenario_values.update(values)
        scenario_values.update(named_values)
        if not scenario_values:
            raise ValueError(f"{method_name} needs at least one scenario.")
        return scenario_values

    @staticmethod
    def _is_iteration_points(value: Any) -> bool:
        return isinstance(value, dict) and bool(value) and all(
            isinstance(key, int) for key in value
        )

    def _points_for_scenario(self, scenario: str) -> dict[int, Any]:
        if None in self.values_by_scenario:
            return self.values_by_scenario[None]

        if scenario not in self.values_by_scenario:
            scenarios = ", ".join(sorted(self.scenario_names()))
            raise ValueError(
                f"Scenario '{scenario}' is not defined for this ParameterValue. "
                f"Available scenarios: {scenarios}."
            )

        return self.values_by_scenario[scenario]

    @staticmethod
    def _step_value(points: dict[int, Any], iteration: int) -> Any:
        sorted_points = sorted(points.items())
        iterations = [point_iteration for point_iteration, _ in sorted_points]
        idx = np.searchsorted(iterations, iteration, side="right") - 1
        idx = max(idx, 0)
        return sorted_points[idx][1]

    @staticmethod
    def _linear_value(points: dict[int, Any], iteration: int) -> float:
        sorted_points = sorted(points.items())
        iterations = np.array(
            [point_iteration for point_iteration, _ in sorted_points],
            dtype=float,
        )
        values = np.array([value for _, value in sorted_points], dtype=float)
        return float(np.interp(iteration, iterations, values))

    @staticmethod
    def _copy_plain_value(value: Any) -> Any:
        if isinstance(value, (list, dict, set)):
            return deepcopy(value)
        return value


def resolve_parameter_values(
    value: Any,
    *,
    scenario: str | None = None,
    iteration: int = 1,
) -> Any:
    """Return a copy of ``value`` where ParameterValue objects are plain values."""
    if isinstance(value, ParameterValue):
        return value.at(scenario=scenario, iteration=iteration)

    if isinstance(value, BaseModel):
        resolved_data = {
            field_name: resolve_parameter_values(
                getattr(value, field_name),
                scenario=scenario,
                iteration=iteration,
            )
            for field_name in value.__class__.model_fields
        }
        return value.__class__.model_validate(resolved_data)

    if isinstance(value, dict):
        return {
            resolve_parameter_values(key, scenario=scenario, iteration=iteration): (
                resolve_parameter_values(item, scenario=scenario, iteration=iteration)
            )
            for key, item in value.items()
        }

    if isinstance(value, list):
        return [
            resolve_parameter_values(item, scenario=scenario, iteration=iteration)
            for item in value
        ]

    if isinstance(value, tuple):
        return tuple(
            resolve_parameter_values(item, scenario=scenario, iteration=iteration)
            for item in value
        )

    if isinstance(value, set):
        return {
            resolve_parameter_values(item, scenario=scenario, iteration=iteration)
            for item in value
        }

    return value


def collect_parameter_value_scenarios(value: Any) -> set[str]:
    """Return scenario names used by ParameterValue objects inside a value."""
    seen = set()

    def collect(item: Any) -> set[str]:
        if isinstance(item, ParameterValue):
            return item.scenario_names()

        if isinstance(item, (Asset, BaseModel, dict, list, tuple, set)):
            item_id = id(item)
            if item_id in seen:
                return set()
            seen.add(item_id)

        if isinstance(item, Asset):
            return collect(item.inputs)

        if isinstance(item, BaseModel):
            scenarios = set()
            for field_name in item.__class__.model_fields:
                scenarios.update(collect(getattr(item, field_name)))
            return scenarios

        if isinstance(item, dict):
            scenarios = set()
            for key, dict_value in item.items():
                scenarios.update(collect(key))
                scenarios.update(collect(dict_value))
            return scenarios

        if isinstance(item, (list, tuple, set)):
            scenarios = set()
            for list_value in item:
                scenarios.update(collect(list_value))
            return scenarios

        return set()

    return collect(value)
