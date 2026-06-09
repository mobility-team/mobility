from __future__ import annotations

from copy import deepcopy
from typing import Any, Literal

import numpy as np
from pydantic import BaseModel, ConfigDict, model_validator

from mobility.runtime.assets.asset import Asset

DEFAULT_SCENARIO = "default"
DEFAULT_SENSITIVITY_CASE = "base"


class SensitivityCase(BaseModel):
    """One parameter variation used during a sensitivity run."""

    model_config = ConfigDict(arbitrary_types_allowed=True, frozen=True)

    case_id: str
    parameter_name: str | None = None
    parameter_label: str | None = None
    variation_type: Literal["values", "relative", "absolute"] | None = None
    variation_value: Any = None
    variation_label: str | None = None

    @property
    def is_base(self) -> bool:
        """Return whether this case leaves all sensitivity values unchanged."""
        return self.parameter_name is None

    def matches(self, parameter_name: str) -> bool:
        """Return whether this case applies to one sensitivity parameter."""
        return self.parameter_name == parameter_name


class SensitivityValue(BaseModel):
    """Value that can be varied during a sensitivity analysis.

    A sensitivity value behaves like its base value in normal runs. During a
    sensitivity run, it applies the active sensitivity case when the case targets
    this value's ``name``.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True, frozen=True)

    value: Any
    name: str
    label: str | None = None
    variation_type: Literal["values", "relative", "absolute"]
    variation_values: tuple[Any, ...]
    variation_labels: tuple[str, ...] | None = None

    @classmethod
    def values(
        cls,
        value: Any,
        values: list[Any] | tuple[Any, ...],
        *,
        name: str,
        label: str | None = None,
        labels: list[str] | tuple[str, ...] | None = None,
    ) -> "SensitivityValue":
        """Return a value with explicit candidate values for sensitivity runs."""
        return cls._build(
            value,
            variation_type="values",
            variation_values=tuple(values),
            name=name,
            label=label,
            labels=labels,
        )

    @classmethod
    def relative(
        cls,
        value: Any,
        changes: list[float] | tuple[float, ...],
        *,
        name: str,
        label: str | None = None,
        labels: list[str] | tuple[str, ...] | None = None,
    ) -> "SensitivityValue":
        """Return a value varied by relative changes around its base value."""
        return cls._build(
            value,
            variation_type="relative",
            variation_values=tuple(changes),
            name=name,
            label=label,
            labels=labels,
        )

    @classmethod
    def absolute(
        cls,
        value: Any,
        changes: list[float] | tuple[float, ...],
        *,
        name: str,
        label: str | None = None,
        labels: list[str] | tuple[str, ...] | None = None,
    ) -> "SensitivityValue":
        """Return a value varied by absolute changes around its base value."""
        return cls._build(
            value,
            variation_type="absolute",
            variation_values=tuple(changes),
            name=name,
            label=label,
            labels=labels,
        )

    @classmethod
    def _build(
        cls,
        value: Any,
        *,
        variation_type: Literal["values", "relative", "absolute"],
        variation_values: tuple[Any, ...],
        name: str,
        label: str | None,
        labels: list[str] | tuple[str, ...] | None,
    ) -> "SensitivityValue":
        if not name:
            raise ValueError("SensitivityValue.name should not be empty.")
        if not variation_values:
            raise ValueError("SensitivityValue needs at least one variation value.")
        if labels is not None and len(labels) != len(variation_values):
            raise ValueError("SensitivityValue labels should match the number of variation values.")
        if _contains_parameter_value(value) or _contains_parameter_value(variation_values):
            raise ValueError(
                "SensitivityValue should wrap final values, not ParameterValue. "
                "Put SensitivityValue inside the relevant ParameterValue scenario or iteration point."
            )
        return cls(
            value=value,
            name=name,
            label=label,
            variation_type=variation_type,
            variation_values=variation_values,
            variation_labels=tuple(labels) if labels is not None else None,
        )

    @model_validator(mode="after")
    def validate_final_values(self) -> "SensitivityValue":
        """Check sensitivity values wrap final values, not scenario rules."""
        if _contains_parameter_value(self.value) or _contains_parameter_value(self.variation_values):
            raise ValueError(
                "SensitivityValue should wrap final values, not ParameterValue. "
                "Put SensitivityValue inside the relevant ParameterValue scenario or iteration point."
            )
        return self

    def cases(self) -> list[SensitivityCase]:
        """Return sensitivity cases generated from this value."""
        cases = []
        for index, variation_value in enumerate(self.variation_values):
            if self.variation_labels is not None:
                variation_label = self.variation_labels[index]
            else:
                variation_label = self._default_variation_label(variation_value)
            cases.append(
                SensitivityCase(
                    case_id=f"{self.name}_{index + 1}",
                    parameter_name=self.name,
                    parameter_label=self.label or self.name,
                    variation_type=self.variation_type,
                    variation_value=variation_value,
                    variation_label=variation_label,
                )
            )
        return cases

    def at(
        self,
        *,
        sensitivity_case: SensitivityCase | None = None,
    ) -> Any:
        """Return the base value or the value for the active sensitivity case."""
        base_value = self._copy_plain_value(self.value)
        if sensitivity_case is None or not sensitivity_case.matches(self.name):
            return self._copy_plain_value(base_value)
        if sensitivity_case.variation_type == "values":
            return self._copy_plain_value(sensitivity_case.variation_value)
        if sensitivity_case.variation_type == "relative":
            return base_value * (1.0 + sensitivity_case.variation_value)
        if sensitivity_case.variation_type == "absolute":
            return base_value + sensitivity_case.variation_value
        return self._copy_plain_value(base_value)

    @staticmethod
    def _copy_plain_value(value: Any) -> Any:
        if isinstance(value, (list, dict, set, tuple)):
            return deepcopy(value)
        return value

    @staticmethod
    def _default_variation_label(value: Any) -> str:
        if isinstance(value, (int, float)):
            if value > 0:
                return f"+{value:g}"
            return f"{value:g}"
        return str(value)


class ParameterValue(BaseModel):
    """Parameter value that can vary by scenario and by iteration.

    Plain values apply to every scenario. When scenario-specific values include
    an explicit ``default`` scenario, that value is used as the fallback for
    scenarios that do not define their own value.
    """

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

    def at(
        self,
        *,
        scenario: str | None = None,
        iteration: int = 1,
    ) -> Any:
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
        if isinstance(value, SensitivityValue):
            return {1: value}
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

        if scenario in self.values_by_scenario:
            return self.values_by_scenario[scenario]

        if DEFAULT_SCENARIO in self.values_by_scenario:
            return self.values_by_scenario[DEFAULT_SCENARIO]

        scenarios = ", ".join(sorted(self.scenario_names()))
        raise ValueError(
            f"Scenario '{scenario}' is not defined for this ParameterValue. "
            f"Available scenarios: {scenarios}."
        )

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


def _contains_parameter_value(value: Any) -> bool:
    """Return whether a value contains a ParameterValue object."""
    seen = set()

    def contains(item: Any) -> bool:
        if isinstance(item, ParameterValue):
            return True

        if isinstance(item, (Asset, BaseModel, dict, list, tuple, set)):
            item_id = id(item)
            if item_id in seen:
                return False
            seen.add(item_id)

        if isinstance(item, Asset):
            return contains(item.inputs)

        if isinstance(item, BaseModel):
            return any(
                contains(getattr(item, field_name))
                for field_name in item.__class__.model_fields
            )

        if isinstance(item, dict):
            return any(
                contains(key) or contains(dict_value)
                for key, dict_value in item.items()
            )

        if isinstance(item, (list, tuple, set)):
            return any(contains(list_value) for list_value in item)

        return False

    return contains(value)


def resolve_parameter_values(
    value: Any,
    *,
    scenario: str | None = None,
    iteration: int = 1,
    sensitivity_case: SensitivityCase | None = None,
) -> Any:
    """Return a copy of ``value`` where ParameterValue objects are plain values."""
    if isinstance(value, SensitivityValue):
        resolved_value = value.at(
            sensitivity_case=sensitivity_case,
        )
        return resolve_parameter_values(
            resolved_value,
            scenario=scenario,
            iteration=iteration,
            sensitivity_case=sensitivity_case,
        )

    if isinstance(value, ParameterValue):
        resolved_value = value.at(
            scenario=scenario,
            iteration=iteration,
        )
        return resolve_parameter_values(
            resolved_value,
            scenario=scenario,
            iteration=iteration,
            sensitivity_case=sensitivity_case,
        )

    if isinstance(value, BaseModel):
        resolved_data = {
            field_name: resolve_parameter_values(
                getattr(value, field_name),
                scenario=scenario,
                iteration=iteration,
                sensitivity_case=sensitivity_case,
            )
            for field_name in value.__class__.model_fields
        }
        return value.__class__.model_validate(resolved_data)

    if isinstance(value, dict):
        return {
            resolve_parameter_values(
                key,
                scenario=scenario,
                iteration=iteration,
                sensitivity_case=sensitivity_case,
            ): (
                resolve_parameter_values(
                    item,
                    scenario=scenario,
                    iteration=iteration,
                    sensitivity_case=sensitivity_case,
                )
            )
            for key, item in value.items()
        }

    if isinstance(value, list):
        return [
            resolve_parameter_values(
                item,
                scenario=scenario,
                iteration=iteration,
                sensitivity_case=sensitivity_case,
            )
            for item in value
        ]

    if isinstance(value, tuple):
        return tuple(
            resolve_parameter_values(
                item,
                scenario=scenario,
                iteration=iteration,
                sensitivity_case=sensitivity_case,
            )
            for item in value
        )

    if isinstance(value, set):
        return {
            resolve_parameter_values(
                item,
                scenario=scenario,
                iteration=iteration,
                sensitivity_case=sensitivity_case,
            )
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


def collect_sensitivity_values(value: Any) -> list[SensitivityValue]:
    """Return sensitivity values found inside a setup object."""
    seen = set()
    sensitivity_values: list[SensitivityValue] = []

    def collect(item: Any) -> None:
        if isinstance(item, SensitivityValue):
            sensitivity_values.append(item)
            collect(item.value)
            return

        if isinstance(item, ParameterValue):
            item_id = id(item)
            if item_id in seen:
                return
            seen.add(item_id)
            for points in item.values_by_scenario.values():
                for point_value in points.values():
                    collect(point_value)
            return

        if isinstance(item, (Asset, BaseModel, dict, list, tuple, set)):
            item_id = id(item)
            if item_id in seen:
                return
            seen.add(item_id)

        if isinstance(item, Asset):
            collect(item.inputs)
            return

        if isinstance(item, BaseModel):
            for field_name in item.__class__.model_fields:
                collect(getattr(item, field_name))
            return

        if isinstance(item, dict):
            for key, dict_value in item.items():
                collect(key)
                collect(dict_value)
            return

        if isinstance(item, (list, tuple, set)):
            for list_value in item:
                collect(list_value)

    collect(value)
    return sensitivity_values
