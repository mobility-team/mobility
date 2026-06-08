import pytest

from mobility.runtime.parameter_values import ParameterValue, resolve_parameter_values


def test_parameter_value_accepts_scenario_mapping_with_non_identifier_names():
    value = ParameterValue.by_scenario(
        {
            "default": 1.0,
            "saleve-jura": 2.0,
        }
    )

    assert resolve_parameter_values(value) == 1.0
    assert resolve_parameter_values(value, scenario="saleve-jura") == 2.0


def test_parameter_value_accepts_scenario_iteration_mapping():
    value = ParameterValue.by_scenario_and_iteration(
        {
            "default": {1: ["base.zip"]},
            "saleve-jura": {
                1: ["base.zip"],
                3: ["base.zip", "project.zip"],
            },
        }
    )

    assert resolve_parameter_values(value, scenario="saleve-jura", iteration=1) == [
        "base.zip"
    ]
    assert resolve_parameter_values(value, scenario="saleve-jura", iteration=3) == [
        "base.zip",
        "project.zip",
    ]


def test_parameter_value_uses_explicit_default_for_other_scenarios():
    value = ParameterValue.by_scenario(default=0.0, carbon_tax=0.12)

    assert resolve_parameter_values(value, scenario="ljls") == 0.0
    assert resolve_parameter_values(value, scenario="carbon_tax") == 0.12


def test_parameter_value_uses_explicit_default_iterations_for_other_scenarios():
    value = ParameterValue.by_scenario_and_iteration(
        default=0.0,
        carbon_tax={1: 0.0, 5: 0.12},
    )

    assert resolve_parameter_values(value, scenario="ljls", iteration=1) == 0.0
    assert resolve_parameter_values(value, scenario="ljls", iteration=5) == 0.0
    assert resolve_parameter_values(value, scenario="carbon_tax", iteration=5) == 0.12


def test_parameter_value_returns_deep_copied_mutable_values():
    value = ParameterValue.constant({"files": ["base.zip"]})

    first = resolve_parameter_values(value)
    first["files"].append("project.zip")

    second = resolve_parameter_values(value)

    assert second == {"files": ["base.zip"]}


def test_parameter_value_requires_at_least_one_scenario():
    with pytest.raises(ValueError, match="needs at least one scenario"):
        ParameterValue.by_scenario()
