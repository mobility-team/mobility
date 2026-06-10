import pytest

from mobility.runtime.parameter_values import (
    ParameterValue,
    SensitivityCase,
    SensitivityValue,
    resolve_parameter_values,
)


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


def test_sensitivity_value_resolves_to_base_value_without_case():
    value = SensitivityValue.relative(
        3.0,
        changes=[-0.2, 0.2],
        name="cost_constant",
    )

    assert resolve_parameter_values(value) == 3.0


def test_sensitivity_value_applies_matching_relative_case():
    value = SensitivityValue.relative(
        3.0,
        changes=[-0.2, 0.2],
        name="cost_constant",
    )
    case = value.cases()[1]

    assert resolve_parameter_values(value, sensitivity_case=case) == pytest.approx(3.6)


def test_sensitivity_value_ignores_non_matching_case():
    value = SensitivityValue.absolute(
        3.0,
        changes=[-1.0, 1.0],
        name="cost_constant",
    )
    other_case = SensitivityCase(
        case_id="other_1",
        parameter_name="other",
        variation_type="absolute",
        variation_value=10.0,
    )

    assert resolve_parameter_values(value, sensitivity_case=other_case) == 3.0


def test_sensitivity_value_rejects_wrapping_parameter_value():
    with pytest.raises(ValueError, match="inside the relevant ParameterValue"):
        SensitivityValue.relative(
            ParameterValue.by_scenario(default=1.0, carbon_tax=2.0),
            changes=[-0.2, 0.2],
            name="carbon_tax",
        )


def test_sensitivity_value_inside_scenario_iteration_value():
    value = ParameterValue.by_scenario_and_iteration(
        default=0.0,
        carbon_tax={
            1: 0.0,
            5: SensitivityValue.relative(
                0.12,
                changes=[-0.2, 0.2],
                name="pt_carbon_tax",
                label="PT carbon tax",
            ),
        },
    )
    case = value.values_by_scenario["carbon_tax"][5].cases()[1]

    assert resolve_parameter_values(value, scenario="carbon_tax", iteration=1) == 0.0
    assert resolve_parameter_values(value, scenario="carbon_tax", iteration=5) == 0.12
    assert resolve_parameter_values(
        value,
        scenario="carbon_tax",
        iteration=5,
        sensitivity_case=case,
    ) == pytest.approx(0.144)
