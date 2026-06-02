import pytest
from pydantic import BaseModel

from mobility.runtime.parameter_values import ParameterValue
from mobility.runtime.scenarios import (
    DEFAULT_SCENARIO,
    Scenario,
    ScenarioParameterChange,
    Scenarios,
    collect_parameter_value_changes,
)


class ScenarioSetupParameters(BaseModel):
    speed_limit: object
    nested_values: object


def test_scenarios_add_default_scenario_when_no_scenario_changes_are_used():
    scenarios = Scenarios()

    assert scenarios.names == [DEFAULT_SCENARIO]
    assert len(scenarios) == 1
    assert DEFAULT_SCENARIO in scenarios
    assert scenarios.get(DEFAULT_SCENARIO).display_title == "Reference"
    assert scenarios.as_dicts() == [
        {
            "name": DEFAULT_SCENARIO,
            "title": "Reference",
            "description": None,
            "reference": None,
        }
    ]
    assert "Changes: none declared with ParameterValue." in scenarios.describe()


def test_scenarios_validate_declared_setup_changes_and_describe_them():
    scenario = Scenario(
        name="saleve-jura",
        title="Saleve and Jura",
        description="Add new public transport files.",
        reference=DEFAULT_SCENARIO,
    )
    parameters = {
        "gtfs_files": ParameterValue.by_scenario_and_iteration(
            {
                DEFAULT_SCENARIO: {1: ["base.zip"]},
                "saleve-jura": {1: ["base.zip"], 3: ["base.zip", "project.zip"]},
            }
        )
    }

    scenarios = Scenarios([scenario], parameters=parameters, n_iterations=4)

    assert scenarios.names == [DEFAULT_SCENARIO, "saleve-jura"]
    assert scenarios.selected_names(None) == [DEFAULT_SCENARIO]
    assert scenarios.selected_names("saleve-jura") == ["saleve-jura"]
    assert scenarios.selected_names(["saleve-jura", DEFAULT_SCENARIO]) == [
        "saleve-jura",
        DEFAULT_SCENARIO,
    ]
    assert scenarios.changes == [
        ScenarioParameterChange(
            path="setup['parameters']['gtfs_files']",
            scenario_names=("default", "saleve-jura"),
            iteration_points_by_scenario={
                "default": (1,),
                "saleve-jura": (1, 3),
            },
        )
    ]

    description = scenarios.describe()

    assert "saleve-jura" in description
    assert "Title: Saleve and Jura" in description
    assert "Reference: default" in description
    assert "setup['parameters']['gtfs_files']: iterations 1, 3" in description


def test_scenarios_for_setup_reuses_manifest_and_validates_selected_names():
    base_scenarios = Scenarios(
        [Scenario(name="saleve-jura")],
        changes=[
            ScenarioParameterChange(
                path="setup.parameters.speed_limit",
                scenario_names=("saleve-jura",),
                iteration_points_by_scenario={"saleve-jura": (1,)},
            )
        ],
    )

    scenarios = Scenarios.for_setup(base_scenarios)

    assert scenarios.names == [DEFAULT_SCENARIO, "saleve-jura"]
    scenarios.validate_requested(["saleve-jura"])
    with pytest.raises(ValueError, match="Missing scenarios"):
        scenarios.validate_requested(["missing"])


def test_scenarios_require_declared_scenarios_for_varying_parameter_values():
    with pytest.raises(ValueError, match="Missing declarations"):
        Scenarios(
            parameters={
                "speed_limit": ParameterValue.by_scenario(
                    {DEFAULT_SCENARIO: 50, "saleve-jura": 40}
                )
            }
        )


def test_scenarios_reject_invalid_manifest_values():
    with pytest.raises(ValueError, match="duplicate names"):
        Scenarios([Scenario(name="saleve-jura"), Scenario(name="saleve-jura")])

    with pytest.raises(ValueError, match="Missing references"):
        Scenarios([Scenario(name="saleve-jura", reference="missing")])

    with pytest.raises(TypeError, match="Scenario objects"):
        Scenarios(["saleve-jura"])

    with pytest.raises(TypeError, match="Scenarios object"):
        Scenarios.for_setup("saleve-jura")

    with pytest.raises(TypeError, match="one scenario name"):
        Scenarios.selected_names({"saleve-jura"})


def test_scenarios_warn_about_unused_and_unreachable_changes():
    changes = [
        ScenarioParameterChange(
            path="setup.parameters.gtfs_files",
            scenario_names=("saleve-jura",),
            iteration_points_by_scenario={"saleve-jura": (5,)},
        )
    ]

    with pytest.warns(UserWarning) as warnings:
        Scenarios(
            [
                Scenario(name="saleve-jura"),
                Scenario(name="unused-scenario"),
            ],
            changes=changes,
            n_iterations=3,
        )

    messages = [str(warning.message) for warning in warnings]
    assert any("do not change any ParameterValue" in message for message in messages)
    assert any("will never be used" in message for message in messages)


def test_collect_parameter_value_changes_finds_values_inside_nested_objects():
    setup = ScenarioSetupParameters(
        speed_limit=ParameterValue.by_scenario(
            {DEFAULT_SCENARIO: 50, "saleve-jura": 40}
        ),
        nested_values=[
            {
                "files": ParameterValue.by_scenario_and_iteration(
                    {"saleve-jura": {1: ["base.zip"], 2: ["project.zip"]}}
                )
            }
        ],
    )

    changes = collect_parameter_value_changes(setup)

    assert changes == [
        ScenarioParameterChange(
            path="setup.speed_limit",
            scenario_names=("default", "saleve-jura"),
            iteration_points_by_scenario={
                "default": (1,),
                "saleve-jura": (1,),
            },
        ),
        ScenarioParameterChange(
            path="setup.nested_values[0]['files']",
            scenario_names=("saleve-jura",),
            iteration_points_by_scenario={"saleve-jura": (1, 2)},
        ),
    ]
