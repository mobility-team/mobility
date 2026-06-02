from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable
import warnings

from pydantic import BaseModel, ConfigDict, model_validator

from mobility.runtime.assets.asset import Asset
from mobility.runtime.parameter_values import DEFAULT_SCENARIO, ParameterValue


class Scenario(BaseModel):
    """Named scenario shown in reports and checked against parameter values."""

    model_config = ConfigDict(frozen=True)

    name: str
    title: str | None = None
    description: str | None = None
    reference: str | None = None

    @model_validator(mode="after")
    def validate_name(self) -> "Scenario":
        """Check that the scenario can be used as a stable identifier."""
        if not self.name:
            raise ValueError("Scenario.name should not be empty.")
        return self

    @property
    def display_title(self) -> str:
        """Return the title to show when no explicit title was provided."""
        return self.title or self.name


class Scenarios:
    """Scenario manifest used to document and validate scenario names."""

    def __init__(
        self,
        scenarios: list[Scenario] | None = None,
        modes: Any = None,
        activities: Any = None,
        parameters: Any = None,
        changes: Iterable[ScenarioParameterChange] | None = None,
        n_iterations: int | None = None,
    ) -> None:
        """Create a scenario manifest.

        The constructor accepts a list of ``Scenario`` objects. The default
        scenario is added when it was not declared explicitly. Scenario-varying
        values outside the default scenario should be declared explicitly.
        """
        has_setup_context = (
            changes is not None
            or modes is not None
            or activities is not None
            or parameters is not None
        )
        setup_changes = self._collect_setup_changes(
            modes=modes,
            activities=activities,
            parameters=parameters,
            changes=changes,
        )
        if n_iterations is None:
            n_iterations = self._infer_n_iterations(parameters)

        if scenarios is None:
            declared_scenarios = []
        elif isinstance(scenarios, list):
            declared_scenarios = list(scenarios)
        else:
            raise TypeError("Scenarios.scenarios should be a list of Scenario objects or None.")

        invalid_scenarios = [
            scenario
            for scenario in declared_scenarios
            if not isinstance(scenario, Scenario)
        ]
        if invalid_scenarios:
            raise TypeError(
                "Scenarios should contain Scenario objects. "
                f"Received invalid values: {invalid_scenarios}."
            )

        if not declared_scenarios:
            used_scenarios = self._scenario_names_from_changes(setup_changes)
            explicit_scenarios = sorted(
                scenario
                for scenario in used_scenarios
                if scenario != DEFAULT_SCENARIO
            )
            if explicit_scenarios:
                raise ValueError(
                    "Scenario-varying ParameterValue objects use scenarios that "
                    "should be declared with mobility.Scenarios. "
                    f"Missing declarations: {explicit_scenarios}. "
                    "Create a Scenarios object and pass it as `scenarios=`."
                )
            declared_scenarios = [
                Scenario(name=DEFAULT_SCENARIO, title="Reference")
            ]

        if DEFAULT_SCENARIO not in {scenario.name for scenario in declared_scenarios}:
            declared_scenarios = [
                Scenario(name=DEFAULT_SCENARIO, title="Reference")
            ] + declared_scenarios

        self._scenarios = tuple(declared_scenarios)
        self._changes = setup_changes
        self._n_iterations = n_iterations
        self._validate_unique_names()
        self._validate_references()
        if has_setup_context:
            self.check_setup()

    @classmethod
    def for_setup(
        cls,
        scenarios: "Scenarios | None",
        *,
        modes: Any = None,
        activities: Any = None,
        parameters: Any = None,
    ) -> "Scenarios":
        """Return a scenario manifest attached to one model setup."""
        if scenarios is None:
            scenario_list = None
        elif isinstance(scenarios, Scenarios):
            scenario_list = list(scenarios)
        else:
            raise TypeError("scenarios should be a Scenarios object or None.")

        return cls(
            scenario_list,
            modes=modes,
            activities=activities,
            parameters=parameters,
        )

    @staticmethod
    def selected_names(
        scenarios: str | list[str] | tuple[str, ...] | None,
    ) -> list[str]:
        """Return selected scenario names with the default filled in."""
        if scenarios is None:
            return [DEFAULT_SCENARIO]
        if isinstance(scenarios, str):
            return [scenarios]
        if isinstance(scenarios, (list, tuple)):
            return list(scenarios)
        raise TypeError(
            "scenarios should be None, one scenario name, or a list of scenario names."
        )

    @property
    def names(self) -> list[str]:
        """Return scenario names in manifest order."""
        return [scenario.name for scenario in self._scenarios]

    @property
    def changes(self) -> list[ScenarioParameterChange]:
        """Return scenario-varying parameter values found in the setup."""
        return list(self._changes)

    def get(self, name: str) -> Scenario:
        """Return one scenario by name."""
        for scenario in self._scenarios:
            if scenario.name == name:
                return scenario
        raise KeyError(f"Scenario '{name}' is not declared.")

    def as_dicts(self) -> list[dict[str, str | None]]:
        """Return plain metadata dictionaries for reports or notebooks."""
        return [
            {
                "name": scenario.name,
                "title": scenario.title,
                "description": scenario.description,
                "reference": scenario.reference,
            }
            for scenario in self._scenarios
        ]

    def check_setup(self) -> None:
        """Validate and warn about scenario setup issues."""
        self._validate_change_scenarios_are_declared()
        self._warn_about_unused_scenarios()
        self._warn_about_unreachable_iterations()

    def validate_requested(self, scenarios: Iterable[str]) -> None:
        """Check that selected scenario names are declared."""
        selected_scenarios = list(scenarios)
        missing_scenarios = [
            scenario
            for scenario in selected_scenarios
            if scenario not in self
        ]
        if missing_scenarios:
            raise ValueError(
                "Selected scenarios should be declared in Scenarios. "
                f"Missing scenarios: {missing_scenarios}. "
                f"Available scenarios: {self.names}."
            )

    def describe(self) -> str:
        """Return a plain-language summary of scenarios and changed parameters."""
        lines = []
        for scenario in self._scenarios:
            lines.append(scenario.name)
            if scenario.title:
                lines.append(f"  Title: {scenario.title}")
            if scenario.description:
                lines.append(f"  Description: {scenario.description}")
            if scenario.reference:
                lines.append(f"  Reference: {scenario.reference}")

            scenario_changes = [
                change
                for change in self._changes
                if scenario.name in change.scenario_names
            ]
            if not scenario_changes:
                lines.append("  Changes: none declared with ParameterValue.")
                continue

            lines.append("  Changes:")
            for change in scenario_changes:
                points = change.iteration_points_by_scenario[scenario.name]
                point_text = ", ".join(str(point) for point in points)
                lines.append(f"  - {change.path}: iterations {point_text}")
        return "\n".join(lines)

    def __contains__(self, name: object) -> bool:
        """Return whether the manifest contains a scenario name."""
        return isinstance(name, str) and name in self.names

    def __iter__(self):
        """Iterate over declared scenarios."""
        return iter(self._scenarios)

    def __len__(self) -> int:
        """Return the number of declared scenarios."""
        return len(self._scenarios)

    def _validate_unique_names(self) -> None:
        duplicate_names = sorted(
            name
            for name in set(self.names)
            if self.names.count(name) > 1
        )
        if duplicate_names:
            raise ValueError(
                "Scenarios should not contain duplicate names. "
                f"Received duplicates: {duplicate_names}."
            )

    def _validate_references(self) -> None:
        names = set(self.names)
        missing_references = sorted(
            {
                scenario.reference
                for scenario in self._scenarios
                if scenario.reference is not None and scenario.reference not in names
            }
        )
        if missing_references:
            raise ValueError(
                "Scenario.reference should point to a declared scenario. "
                f"Missing references: {missing_references}."
            )

    @staticmethod
    def _collect_setup_changes(
        *,
        modes: Any,
        activities: Any,
        parameters: Any,
        changes: Iterable[ScenarioParameterChange] | None,
    ) -> tuple[ScenarioParameterChange, ...]:
        """Return explicit changes or collect them from setup objects."""
        if changes is not None:
            return tuple(changes)

        return tuple(
            collect_parameter_value_changes(
                {
                    "modes": modes,
                    "activities": activities,
                    "parameters": parameters,
                }
            )
        )

    @staticmethod
    def _infer_n_iterations(parameters: Any) -> int | None:
        """Return run length from grouped day-trip parameters when available."""
        if parameters is None:
            return None
        run_parameters = getattr(parameters, "run", None)
        if run_parameters is not None:
            return getattr(run_parameters, "n_iterations", None)
        return getattr(parameters, "n_iterations", None)

    @staticmethod
    def _scenario_names_from_changes(
        changes: Iterable[ScenarioParameterChange],
    ) -> set[str]:
        """Return scenario names used by setup changes."""
        names = {DEFAULT_SCENARIO}
        for change in changes:
            names.update(change.scenario_names)
        return names

    def _validate_change_scenarios_are_declared(self) -> None:
        """Check that parameter values only use declared scenarios."""
        used_scenarios = {
            scenario
            for change in self._changes
            for scenario in change.scenario_names
        }
        undeclared_scenarios = sorted(
            scenario
            for scenario in used_scenarios
            if scenario not in self
        )
        if undeclared_scenarios:
            raise ValueError(
                "Scenario-varying ParameterValue objects use scenarios that are "
                "not declared in Scenarios. "
                f"Missing declarations: {undeclared_scenarios}."
            )

    def _warn_about_unused_scenarios(self) -> None:
        """Warn when a manifest scenario does not change any parameter."""
        used_scenarios = {
            scenario
            for change in self._changes
            for scenario in change.scenario_names
        }
        unused_scenarios = [
            scenario
            for scenario in self.names
            if scenario != DEFAULT_SCENARIO and scenario not in used_scenarios
        ]
        if unused_scenarios:
            warnings.warn(
                "Some declared scenarios do not change any ParameterValue in this "
                f"setup: {unused_scenarios}.",
                stacklevel=3,
            )

    def _warn_about_unreachable_iterations(self) -> None:
        """Warn when scenario changes start after the configured last iteration."""
        if self._n_iterations is None:
            return

        for change in self._changes:
            for scenario, points in change.iteration_points_by_scenario.items():
                unreachable_points = [
                    point
                    for point in points
                    if point > self._n_iterations
                ]
                if unreachable_points:
                    warnings.warn(
                        f"Scenario '{scenario}' changes {change.path} at iterations "
                        f"{unreachable_points}, but the run has n_iterations="
                        f"{self._n_iterations}. These values will never be used.",
                        stacklevel=3,
                    )


@dataclass(frozen=True)
class ScenarioParameterChange:
    """One parameter value that changes by scenario or iteration."""

    path: str
    scenario_names: tuple[str, ...]
    iteration_points_by_scenario: dict[str, tuple[int, ...]]


def collect_parameter_value_changes(value: Any) -> list[ScenarioParameterChange]:
    """Return parameter values that define scenario-specific values."""
    seen = set()
    changes: list[ScenarioParameterChange] = []

    def collect(item: Any, path: str) -> None:
        if isinstance(item, ParameterValue):
            if item.has_scenarios():
                changes.append(_change_from_parameter_value(item, path))
            return

        if isinstance(item, (Asset, BaseModel, dict, list, tuple, set)):
            item_id = id(item)
            if item_id in seen:
                return
            seen.add(item_id)

        if isinstance(item, Asset):
            collect(item.inputs, f"{path}.inputs")
            return

        if isinstance(item, BaseModel):
            for field_name in item.__class__.model_fields:
                collect(getattr(item, field_name), f"{path}.{field_name}")
            return

        if isinstance(item, dict):
            for key, dict_value in item.items():
                key_path = _format_path_part(key)
                collect(dict_value, f"{path}[{key_path}]")
            return

        if isinstance(item, (list, tuple)):
            for index, list_value in enumerate(item):
                collect(list_value, f"{path}[{index}]")
            return

        if isinstance(item, set):
            for index, set_value in enumerate(sorted(item, key=repr)):
                collect(set_value, f"{path}[{index}]")

    collect(value, "setup")
    return changes


def _change_from_parameter_value(
    value: ParameterValue,
    path: str,
) -> ScenarioParameterChange:
    scenario_names = tuple(sorted(value.scenario_names()))
    iteration_points_by_scenario = {
        scenario: tuple(sorted(points))
        for scenario, points in value.values_by_scenario.items()
        if scenario is not None
    }
    return ScenarioParameterChange(
        path=path,
        scenario_names=scenario_names,
        iteration_points_by_scenario=iteration_points_by_scenario,
    )


def _format_path_part(value: Any) -> str:
    if isinstance(value, str):
        return repr(value)
    return str(value)
